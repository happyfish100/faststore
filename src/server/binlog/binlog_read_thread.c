/*
 * Copyright (c) 2020 YuQing <384681@qq.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

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

    ctx->running = 0;
    ctx->continue_flag = 1;
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

    FC_ATOMIC_SET(ctx->continue_flag, 0);
    common_blocked_queue_terminate(&ctx->queues.waiting);
    common_blocked_queue_terminate(&ctx->queues.done);

    count = 0;
    while (FC_ATOMIC_GET(ctx->running) && count++ < 300) {
        if (count % 10 == 0) {
            common_blocked_queue_terminate(&ctx->queues.waiting);
            common_blocked_queue_terminate(&ctx->queues.done);
        }
        fc_sleep_ms(10);
    }

    if (FC_ATOMIC_GET(ctx->running)) {
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
    FC_ATOMIC_SET(ctx->running, 1);
    while (FC_ATOMIC_GET(ctx->continue_flag)) {
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

    FC_ATOMIC_SET(ctx->running, 0);
    return NULL;
}
