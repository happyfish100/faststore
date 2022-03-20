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
#include "rebuild_binlog.h"
#include "binlog_spliter.h"

typedef struct data_read_thread_info {
    ServerBinlogReaderArray rda;
    BufferInfo buffer;
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

typedef struct rebuild_binlog_writer_context {
    SFBinlogWriterContext wctx;
    volatile int64_t sn;
} RebuildBinlogWriterContext;

typedef struct binlog_writer_ctx_array {
    RebuildBinlogWriterContext *contexts;
    int count;
} BinlogWriterCtxArray;

typedef struct binlog_spliter_context {
    BinlogWriterCtxArray wctx_array;
    DataReadContext read_ctx;
} BinlogSpliterContext;

static int deal_line(DataReadThreadInfo *thread,
        ServerBinlogReader *reader, const string_t *line)
{
    int result;
    char op_type;
    int64_t sn;
    FSBlockSliceKeyInfo bs_key;
    RebuildBinlogWriterContext *ctx;
    SFBinlogWriterBuffer *wbuffer;

    if ((result=rebuild_binlog_parse_line(reader, &thread->buffer,
                    line, &sn, &op_type, &bs_key)) != 0)
    {
        return result;
    }

    ctx = thread->ctx->wctx_array.contexts +
        (bs_key.block.hash_code %
         thread->ctx->wctx_array.count);

    sn = __sync_add_and_fetch(&ctx->sn, 1);
    if ((wbuffer=sf_binlog_writer_alloc_one_version_buffer(
                    &ctx->wctx.writer, sn)) == NULL)
    {
        return ENOMEM;
    }

    wbuffer->bf.length = rebuild_binlog_log_to_buff(sn, op_type,
            &bs_key.block, &bs_key.slice, wbuffer->bf.buff);
    sf_push_to_binlog_write_queue(&ctx->wctx.writer, wbuffer);
    return 0;
}

static int parse_buffer(DataReadThreadInfo *thread, ServerBinlogReader *reader)
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
        if ((result=deal_line(thread, reader, &line)) != 0) {
            return result;
        }

        line_start = line_end + 1;
    }

    return 0;
}

static inline int read_and_dispatch(DataReadThreadInfo *thread,
        ServerBinlogReader *reader)
{
    int result;

    while ((result=binlog_reader_integral_read(reader, thread->buffer.buff,
                    thread->buffer.alloc_size, &thread->buffer.length)) == 0)
    {
        if ((result=parse_buffer(thread, reader)) != 0) {
            return result;
        }
    }

    return (result == ENOENT ? 0 : result);
}

static void *data_read_thread_run(DataReadThreadInfo *thread)
{
    ServerBinlogReader *reader;
    ServerBinlogReader *end;

    end = thread->rda.readers + thread->rda.count;
    for (reader=thread->rda.readers; reader<end; reader++) {
        if (read_and_dispatch(thread, reader) != 0) {
            sf_terminate_myself();
        }
    }
    __sync_sub_and_fetch(&thread->ctx->read_ctx.running_threads, 1);
    return NULL;
}

static int init_binlog_writer(SFBinlogWriterInfo *writer,
        const char *subdir_name)
{
    const uint64_t next_version = 1;
    const int ring_size = 4096;
    int result;
    int binlog_index;
    char filename[PATH_MAX];
    SFBinlogFilePosition position;

    if ((result=sf_binlog_writer_init_by_version(writer,
                    DATA_PATH_STR, subdir_name, next_version,
                    BINLOG_BUFFER_SIZE, ring_size)) != 0)
    {
        return result;
    }

    sf_binlog_get_current_write_position(writer, &position);
    if (position.index == 0 && position.offset == 0) {
        return 0;
    }

    sf_binlog_writer_destroy_writer(writer);
    for (binlog_index=0; binlog_index<=position.index; binlog_index++) {
        sf_binlog_writer_get_filename(DATA_PATH_STR, subdir_name,
                binlog_index, filename, sizeof(filename));
        if ((result=fc_delete_file(filename)) != 0) {
            return result;
        }
    }

    sf_binlog_writer_get_index_filename(DATA_PATH_STR,
            subdir_name, filename, sizeof(filename));
    if ((result=fc_delete_file(filename)) != 0) {
        return result;
    }

    return sf_binlog_writer_init_by_version(writer, DATA_PATH_STR,
            subdir_name, next_version, BINLOG_BUFFER_SIZE, ring_size);
}

static int init_binlog_writers(BinlogSpliterContext *ctx,
        const int split_count)
{
    int result;
    int thread_index;
    char subdir_name[64];
    RebuildBinlogWriterContext *rctx;
    RebuildBinlogWriterContext *wend;

    ctx->wctx_array.contexts = fc_malloc(split_count *
            sizeof(RebuildBinlogWriterContext));
    if (ctx->wctx_array.contexts == NULL) {
        return ENOMEM;
    }

    wend = ctx->wctx_array.contexts + split_count;
    for (rctx=ctx->wctx_array.contexts; rctx<wend; rctx++) {
        rctx->sn = 0;
        thread_index = rctx - ctx->wctx_array.contexts;
        rebuild_binlog_get_repaly_subdir_name(
                REBUILD_BINLOG_SUBDIR_NAME_REPLAY_INPUT,
                thread_index, subdir_name, sizeof(subdir_name));

        if ((result=init_binlog_writer(&rctx->wctx.writer,
                        subdir_name)) != 0)
        {
            return result;
        }

        if ((result=sf_binlog_writer_init_thread(&rctx->wctx.thread,
                        subdir_name, &rctx->wctx.writer,
                        SF_BINLOG_THREAD_TYPE_ORDER_BY_VERSION,
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
    RebuildBinlogWriterContext *rctx;
    RebuildBinlogWriterContext *wend;

    wend = ctx->wctx_array.contexts + ctx->wctx_array.count;
    for (rctx=ctx->wctx_array.contexts; rctx<wend; rctx++) {
        sf_binlog_writer_destroy(&rctx->wctx);
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
        if ((result=fc_init_buffer(&thread->buffer, 1024 * 1024)) != 0) {
            return result;
        }
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

    for (thread=ctx->read_ctx.thread_array.threads; thread<end; thread++) {
        fc_free_buffer(&thread->buffer);
    }
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
