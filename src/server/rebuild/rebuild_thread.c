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

#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "sf/sf_func.h"
#include "../../common/fs_func.h"
#include "../storage/slice_op.h"
#include "../server_global.h"
#include "rebuild_binlog.h"
#include "binlog_reader.h"
#include "rebuild_thread.h"

typedef struct data_rebuild_thread_info {
    int thread_index;
    int buffer_size;
    BufferInfo buffer;
    ServerBinlogReader reader;
    FSSliceOpContext op_ctx;
    struct {
        bool finished;
        pthread_lock_cond_pair_t lcp; //for notify
    } notify;

    struct data_rebuild_context *ctx;
} DataRebuildThreadInfo;

typedef struct data_rebuild_thread_array {
    DataRebuildThreadInfo *threads;
    int count;
} DataRebuildThreadArray;

typedef struct data_rebuild_context {
    volatile int running_threads;
    DataRebuildThreadArray thread_array;
} DataRebuildContext;

static int deal_line(DataRebuildThreadInfo *thread, const string_t *line)
{
    int result;
    char op_type;
    int64_t sn;
    FSBlockSliceKeyInfo bs_key;

    if ((result=rebuild_binlog_parse_line(&thread->reader, &thread->buffer,
                    line, &sn, &op_type, &bs_key)) != 0)
    {
        return result;
    }

    //TODO
    return 0;
}

static int parse_buffer(DataRebuildThreadInfo *thread)
{
    int result;
    string_t line;
    char *line_start;
    char *buff_end;
    char *line_end;

    line_start = thread->buffer.buff;
    buff_end = thread->buffer.buff + thread->buffer.length;
    while (line_start < buff_end) {
        line_end = (char *)memchr(line_start, '\n', buff_end - line_start);
        if (line_end == NULL) {
            break;
        }

        line.str = line_start;
        line.len = line_end - line_start;
        if ((result=deal_line(thread, &line)) != 0) {
            return result;
        }

        line_start = line_end + 1;
    }

    return 0;
}

static inline int do_rebuild(DataRebuildThreadInfo *thread)
{
    int result;

    while ((result=binlog_reader_integral_read(&thread->reader,
                    thread->buffer.buff, thread->buffer.alloc_size,
                    &thread->buffer.length)) == 0)
    {
        if ((result=parse_buffer(thread)) != 0) {
            return result;
        }
    }

    return (result == ENOENT ? 0 : result);
}

static void *data_rebuild_thread_run(DataRebuildThreadInfo *thread)
{
    if (do_rebuild(thread) != 0) {
        sf_terminate_myself();
    }
    __sync_sub_and_fetch(&thread->ctx->running_threads, 1);
    return NULL;
}

static void rebuild_write_done_callback(FSSliceOpContext *op_ctx,
        DataRebuildThreadInfo *thread)
{
    PTHREAD_MUTEX_LOCK(&thread->notify.lcp.lock);
    thread->notify.finished = true;
    pthread_cond_signal(&thread->notify.lcp.cond);
    PTHREAD_MUTEX_UNLOCK(&thread->notify.lcp.lock);
}

static int init_thread(DataRebuildThreadInfo *thread)
{
    int result;

    if ((result=rebuild_binlog_reader_init(&thread->reader,
                    FS_REBUILD_BINLOG_SUBDIR_NAME,
                    thread->thread_index)) != 0)
    {
        return result;
    }

    if ((result=fc_init_buffer(&thread->buffer, 1024 * 1024)) != 0) {
        return result;
    }

    thread->op_ctx.info.source = BINLOG_SOURCE_REBUILD;
    thread->op_ctx.info.write_binlog.log_replica = false;
    thread->op_ctx.info.data_version = 0;
    thread->op_ctx.info.myself = NULL;

    thread->buffer_size = FS_FILE_BLOCK_SIZE;
    thread->op_ctx.info.buff = (char *)fc_malloc(thread->buffer_size);
    if (thread->op_ctx.info.buff == NULL) {
        return ENOMEM;
    }

    if ((result=init_pthread_lock_cond_pair(&thread->notify.lcp)) != 0) {
        return result;
    }

    thread->notify.finished = false;
    thread->op_ctx.rw_done_callback = (fs_rw_done_callback_func)
        rebuild_write_done_callback;
    thread->op_ctx.arg = thread;
    return fs_init_slice_op_ctx(&thread->op_ctx.update.sarray);
}

static void destroy_thread(DataRebuildThreadInfo *thread)
{
    binlog_reader_destroy(&thread->reader);
    fc_free_buffer(&thread->buffer);
    free(thread->op_ctx.info.buff);
    destroy_pthread_lock_cond_pair(&thread->notify.lcp);
    fs_free_slice_op_ctx(&thread->op_ctx.update.sarray);
}

int rebuild_thread_do(const int thread_count)
{
    int result;
    int bytes;
    pthread_t tid;
    DataRebuildThreadInfo *thread;
    DataRebuildThreadInfo *end;
    DataRebuildContext ctx;

    bytes = sizeof(DataRebuildThreadInfo) * thread_count;
    ctx.thread_array.threads = fc_malloc(bytes);
    if (ctx.thread_array.threads == NULL) {
        return ENOMEM;
    }

    ctx.running_threads = thread_count;
    end = ctx.thread_array.threads + thread_count;
    for (thread=ctx.thread_array.threads; thread<end; thread++) {
        thread->ctx = &ctx;
        thread->thread_index = thread - ctx.thread_array.threads;
        if ((result=init_thread(thread)) != 0) {
            return result;
        }
        if ((result=fc_create_thread(&tid, (void *(*)(void *))
                        data_rebuild_thread_run, thread,
                        SF_G_THREAD_STACK_SIZE)) != 0)
        {
            return result;
        }
    }

    do {
        fc_sleep_ms(10);
        if (__sync_add_and_fetch(&ctx.running_threads, 0) == 0) {
            break;
        }
    } while (SF_G_CONTINUE_FLAG);

    for (thread=ctx.thread_array.threads; thread<end; thread++) {
        destroy_thread(thread);
    }

    free(ctx.thread_array.threads);
    return (SF_G_CONTINUE_FLAG ? 0 : EINTR);
}
