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
#include "../server_global.h"
#include "../shared_thread_pool.h"
#include "binlog_spliter.h"

typedef struct data_read_thread_info {
    ServerBinlogReaderArray rda;
    struct binlog_spliter_context *ctx;
} DataReadThreadInfo;

typedef struct data_read_thread_array {
    DataReadThreadInfo *threads;
    int count;
} DataReadThreadArray;

typedef struct data_read_context {
    volatile int running_threads;
    DataReadThreadArray thread_array;
} DataReadContext;

typedef struct binlog_writer_ctx_array {
    SFBinlogWriterContext *contexts;
    int count;
} BinlogWriterCtxArray;

typedef struct binlog_spliter_context {
    BinlogWriterCtxArray wctx_array;
    DataReadContext read_ctx;
} BinlogSpliterContext;

static inline int read_and_dispatch(ServerBinlogReader *reader)
{
    /*
    SFBinlogWriterBuffer *sf_binlog_writer_alloc_buffer(
        SFBinlogWriterThread *thread);
        */

    return 0;
}

static void *data_read_thread_run(DataReadThreadInfo *thread)
{
    ServerBinlogReader *reader;
    ServerBinlogReader *end;

    end = thread->rda.readers + thread->rda.count;
    for (reader=thread->rda.readers; reader<end; reader++) {
        if (read_and_dispatch(reader) != 0) {
            sf_terminate_myself();
        }
    }
    __sync_sub_and_fetch(&thread->ctx->read_ctx.running_threads, 1);
    return NULL;
}

static int init_binlog_writers(BinlogSpliterContext *ctx,
        const int split_count)
{
    int result;
    int thread_index;
    char subdir_name[64];
    SFBinlogWriterContext *wctx;
    SFBinlogWriterContext *wend;

    ctx->wctx_array.contexts = fc_malloc(split_count *
            sizeof(SFBinlogWriterContext));
    if (ctx->wctx_array.contexts == NULL) {
        return ENOMEM;
    }

    wend = ctx->wctx_array.contexts + split_count;
    for (wctx=ctx->wctx_array.contexts; wctx<wend; wctx++) {
        thread_index = wctx - ctx->wctx_array.contexts;
        binlog_spliter_get_subdir_name(thread_index,
                subdir_name, sizeof(subdir_name));
        if ((result=sf_binlog_writer_init(wctx, DATA_PATH_STR,
                        subdir_name, BINLOG_BUFFER_SIZE,
                        FS_SLICE_BINLOG_MAX_RECORD_SIZE)) != 0)
        {
            return result;
        }
    }

    ctx->wctx_array.count = split_count;
    return 0;
}

static void destroy_binlog_writers(BinlogSpliterContext *ctx)
{
    SFBinlogWriterContext *wctx;
    SFBinlogWriterContext *wend;

    wend = ctx->wctx_array.contexts + ctx->wctx_array.count;
    for (wctx=ctx->wctx_array.contexts; wctx<wend; wctx++) {
        sf_binlog_writer_destroy(wctx);
    }

    free(ctx->wctx_array.contexts);
}

static int do_split(BinlogSpliterContext *ctx,
        ServerBinlogReaderArray *rda,
        const int read_threads)
{
    int result;
    int thread_count;
    int bytes;
    int avg_count;
    int remain_count;
    pthread_t tid;
    DataReadThreadInfo *thread;
    DataReadThreadInfo *end;
    ServerBinlogReader *reader;

    if (rda->count == 1) {
        return read_and_dispatch(rda->readers);
    }

    if (rda->count < read_threads) {
        thread_count = rda->count;
    } else {
        thread_count = read_threads;
    }
    avg_count = rda->count / thread_count;
    remain_count = rda->count - (avg_count * thread_count);

    bytes = sizeof(DataReadThreadInfo) * thread_count;
    ctx->read_ctx.thread_array.threads = fc_malloc(bytes);
    if (ctx->read_ctx.thread_array.threads == NULL) {
        return ENOMEM;
    }

    reader = rda->readers;
    ctx->read_ctx.running_threads = thread_count;
    end = ctx->read_ctx.thread_array.threads + thread_count;
    for (thread=ctx->read_ctx.thread_array.threads; thread<end; thread++) {
        thread->rda.readers = reader;
        if (remain_count > 0) {
            thread->rda.count = avg_count + 1;
            --remain_count;
        } else {
            thread->rda.count = avg_count;
        }
        reader += thread->rda.count;
        thread->ctx = ctx;
        if ((result=fc_create_thread(&tid, (void *(*)(void *))
                        data_read_thread_run, thread,
                        SF_G_THREAD_STACK_SIZE)) != 0)
        {
            return result;
        }
    }

    do {
        fc_sleep_ms(10);
        if (__sync_add_and_fetch(&ctx->read_ctx.running_threads, 0) == 0) {
            break;
        }
    } while (SF_G_CONTINUE_FLAG);

    free(ctx->read_ctx.thread_array.threads);
    return (SF_G_CONTINUE_FLAG ? 0 : EINTR);
}

int binlog_spliter_do(ServerBinlogReaderArray *rda,
        const int read_threads, const int split_count)
{
    int result;
    BinlogSpliterContext ctx;

    if ((result=init_binlog_writers(&ctx, split_count)) != 0) {
        return result;
    }

    result = do_split(&ctx, rda, read_threads);
    destroy_binlog_writers(&ctx);
    return result;
}
