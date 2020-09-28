#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <pthread.h>
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "binlog_func.h"
#include "binlog_read_thread.h"

static void *binlog_read_thread_func(void *arg);

int binlog_read_thread_init(BinlogReadThreadContext *ctx,
        const char *subdir_name, struct sf_binlog_writer_info *writer,
        const SFBinlogFilePosition *position, const int buffer_size)
{
    int result;
    int i;

    if ((result=binlog_reader_init(&ctx->reader, subdir_name,
                    writer, position)) != 0)
    {
        return result;
    }

    ctx->running = false;
    ctx->continue_flag = true;
    if ((result=common_blocked_queue_init_ex(&ctx->queues.waiting,
                    BINLOG_READ_THREAD_BUFFER_COUNT)) != 0)
    {
        return result;
    }
    if ((result=common_blocked_queue_init_ex(&ctx->queues.done,
                    BINLOG_READ_THREAD_BUFFER_COUNT)) != 0)
    {
        return result;
    }

    for (i=0; i<BINLOG_READ_THREAD_BUFFER_COUNT; i++) {
        if ((result=fc_init_buffer(&ctx->results[i].buffer,
                        buffer_size)) != 0)
        {
            return result;
        }

        binlog_read_thread_return_result_buffer(ctx, ctx->results + i);
    }

    return fc_create_thread(&ctx->tid, binlog_read_thread_func,
        ctx, SF_G_THREAD_STACK_SIZE);
}

void binlog_read_thread_terminate(BinlogReadThreadContext *ctx)
{
    int count;
    int i;

    ctx->continue_flag = false;
    common_blocked_queue_terminate(&ctx->queues.waiting);
    common_blocked_queue_terminate(&ctx->queues.done);

    count = 0;
    while (ctx->running && count++ < 300) {
        fc_sleep_ms(10);
    }

    if (ctx->running) {
        logWarning("file: "__FILE__", line: %d, "
                "wait thread exit timeout", __LINE__);
    }
    for (i=0; i<BINLOG_READ_THREAD_BUFFER_COUNT; i++) {
        free(ctx->results[i].buffer.buff);
        ctx->results[i].buffer.buff = NULL;
    }

    common_blocked_queue_destroy(&ctx->queues.waiting);
    common_blocked_queue_destroy(&ctx->queues.done);
    binlog_reader_destroy(&ctx->reader);
}

static void *binlog_read_thread_func(void *arg)
{
    BinlogReadThreadContext *ctx;
    BinlogReadThreadResult *r;

    ctx = (BinlogReadThreadContext *)arg;
    ctx->running = true;
    while (ctx->continue_flag) {
        r = (BinlogReadThreadResult *)common_blocked_queue_pop(
                &ctx->queues.waiting);
        if (r == NULL) {
            continue;
        }

        r->binlog_position = ctx->reader.position;
        r->err_no = binlog_reader_integral_read(&ctx->reader,
                r->buffer.buff, r->buffer.alloc_size,
                &r->buffer.length);
        common_blocked_queue_push(&ctx->queues.done, r);
    }

    ctx->running = false;
    return NULL;
}
