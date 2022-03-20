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
#include "../../client/fs_client.h"
#include "../binlog/slice_binlog.h"
#include "../storage/slice_op.h"
#include "../server_global.h"
#include "rebuild_binlog.h"
#include "binlog_reader.h"
#include "rebuild_thread.h"

typedef struct data_rebuild_thread_info {
    int thread_index;
    BufferInfo rbuffer;
    BufferInfo wbuffer;
    ServerBinlogReader reader;
    SFFileWriterInfo writer;
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

static inline int fetch_slice_data(DataRebuildThreadInfo *thread)
{
    int read_bytes;

    thread->op_ctx.info.data_group_id = FS_DATA_GROUP_ID(
            thread->op_ctx.info.bs_key.block);
    if ((thread->op_ctx.result=fs_client_slice_read_by_slave(
                    &g_fs_client_vars.client_ctx, 0,
                    &thread->op_ctx.info.bs_key,
                    thread->op_ctx.info.buff, &read_bytes)) == 0)
    {
        if (read_bytes != thread->op_ctx.info.bs_key.slice.length) {
            logWarning("file: "__FILE__", line: %d, "
                    "block {oid: %"PRId64", offset: %"PRId64"}, "
                    "slice {offset: %d, length: %d}, "
                    "read bytes: %d != slice length, "
                    "maybe delete later?", __LINE__,
                    thread->op_ctx.info.bs_key.block.oid,
                    thread->op_ctx.info.bs_key.block.offset,
                    thread->op_ctx.info.bs_key.slice.offset,
                    thread->op_ctx.info.bs_key.slice.length,
                    read_bytes);
            thread->op_ctx.info.bs_key.slice.length = read_bytes;
        }
    } else if (thread->op_ctx.result == ENODATA) {
        logWarning("file: "__FILE__", line: %d, "
                "block {oid: %"PRId64", offset: %"PRId64"}, "
                "slice {offset: %d, length: %d}, slice not exist, "
                "maybe delete later?", __LINE__,
                thread->op_ctx.info.bs_key.block.oid,
                thread->op_ctx.info.bs_key.block.offset,
                thread->op_ctx.info.bs_key.slice.offset,
                thread->op_ctx.info.bs_key.slice.length);
    } else {
        logError("file: "__FILE__", line: %d, "
                "block {oid: %"PRId64", offset: %"PRId64"}, "
                "slice {offset: %d, length: %d}, "
                "fetch data fail, errno: %d, error info: %s", __LINE__,
                thread->op_ctx.info.bs_key.block.oid,
                thread->op_ctx.info.bs_key.block.offset,
                thread->op_ctx.info.bs_key.slice.offset,
                thread->op_ctx.info.bs_key.slice.length,
                thread->op_ctx.result, STRERROR(thread->op_ctx.result));
    }

    return thread->op_ctx.result;
}

static int write_to_slice_binlog(DataRebuildThreadInfo *thread)
{
    FSSliceSNPair *slice_sn_pair;
    FSSliceSNPair *slice_sn_end;
    time_t current_time;
    int result;

    result = 0;
    current_time = g_current_time;
    slice_sn_end = thread->op_ctx.update.sarray.slice_sn_pairs +
        thread->op_ctx.update.sarray.count;
    for (slice_sn_pair=thread->op_ctx.update.sarray.slice_sn_pairs;
            slice_sn_pair<slice_sn_end; slice_sn_pair++)
    {
        thread->wbuffer.length = slice_binlog_log_to_buff(slice_sn_pair->
                slice, current_time, thread->op_ctx.info.data_version,
                thread->op_ctx.info.source, thread->wbuffer.buff);
        if ((result=sf_file_writer_deal_buffer(&thread->writer,
                        &thread->wbuffer)) != 0)
        {
            break;
        }
    }

    fs_slice_array_release(&thread->op_ctx.update.sarray);
    return result;
}

static int deal_line(DataRebuildThreadInfo *thread, const string_t *line)
{
    int result;
    char op_type;
    int64_t sn;

    if ((result=rebuild_binlog_parse_line(&thread->reader, &thread->rbuffer,
                    line, &sn, &op_type, &thread->op_ctx.info.bs_key)) != 0)
    {
        return result;
    }

    thread->op_ctx.info.data_version = sn;
    if (op_type == SLICE_BINLOG_OP_TYPE_ALLOC_SLICE) {
       thread->op_ctx.result = fs_slice_allocate(&thread->op_ctx);
    } else {
        if ((result=fetch_slice_data(thread)) != 0) {
            if (result == ENODATA) {
                return 0;
            }

            return result;
        }

        if ((result=fs_slice_write(&thread->op_ctx)) == 0) {
            PTHREAD_MUTEX_LOCK(&thread->notify.lcp.lock);
            while (!thread->notify.finished && SF_G_CONTINUE_FLAG) {
                pthread_cond_wait(&thread->notify.lcp.cond,
                        &thread->notify.lcp.lock);
            }
            if (thread->notify.finished) {
                thread->notify.finished = false;  /* reset for next call */
            } else {
                thread->op_ctx.result = EINTR;
            }
            PTHREAD_MUTEX_UNLOCK(&thread->notify.lcp.lock);
        } else {
            thread->op_ctx.result = result;
        }

        if (result == 0) {
            fs_write_finish(&thread->op_ctx);  //for add slice index and cleanup
        }
    }

    if (thread->op_ctx.result != 0) {
        fs_log_rw_error(&thread->op_ctx, thread->op_ctx.result,
                0, (op_type == SLICE_BINLOG_OP_TYPE_ALLOC_SLICE) ?
                "allocate" : "write");
        return thread->op_ctx.result;
    }

    return write_to_slice_binlog(thread);
}

static int parse_buffer(DataRebuildThreadInfo *thread)
{
    int result;
    string_t line;
    char *line_start;
    char *buff_end;
    char *line_end;

    line_start = thread->rbuffer.buff;
    buff_end = thread->rbuffer.buff + thread->rbuffer.length;
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
                    thread->rbuffer.buff, thread->rbuffer.alloc_size,
                    &thread->rbuffer.length)) == 0)
    {
        if ((result=parse_buffer(thread)) != 0) {
            return result;
        }
    }

    if (result == ENOENT || result == 0) {
        return sf_file_writer_flush(&thread->writer);
    } else {
        return result;
    }
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
    char name[64];
    char subdir_name[64];

    sprintf(name, "%s/%s", REBUILD_BINLOG_SUBDIR_NAME_REPLAY,
            REBUILD_BINLOG_SUBDIR_NAME_REPLAY_INPUT);
    if ((result=rebuild_binlog_reader_init(&thread->reader,
                    name, thread->thread_index)) != 0)
    {
        return result;
    }

    rebuild_binlog_get_repaly_subdir_name(
            REBUILD_BINLOG_SUBDIR_NAME_REPLAY_OUTPUT,
            thread->thread_index, subdir_name, sizeof(subdir_name));
    if ((result=sf_file_writer_init(&thread->writer, DATA_PATH_STR,
                    subdir_name, BINLOG_BUFFER_SIZE)) != 0)
    {
        return result;
    }

    if ((result=fc_init_buffer(&thread->rbuffer, 1024 * 1024)) != 0) {
        return result;
    }

    if ((result=fc_init_buffer(&thread->wbuffer,
                    FS_SLICE_BINLOG_MAX_RECORD_SIZE)) != 0)
    {
        return result;
    }

    thread->op_ctx.info.source = BINLOG_SOURCE_REBUILD;
    thread->op_ctx.info.write_binlog.log_replica = false;
    thread->op_ctx.info.data_version = 0;
    thread->op_ctx.info.myself = NULL;
    thread->op_ctx.info.buff = (char *)fc_malloc(FS_FILE_BLOCK_SIZE);
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
    return fs_slice_array_init(&thread->op_ctx.update.sarray);
}

static void destroy_thread(DataRebuildThreadInfo *thread)
{
    binlog_reader_destroy(&thread->reader);
    sf_file_writer_destroy(&thread->writer);
    fc_free_buffer(&thread->rbuffer);
    fc_free_buffer(&thread->wbuffer);
    free(thread->op_ctx.info.buff);
    destroy_pthread_lock_cond_pair(&thread->notify.lcp);
    fs_slice_array_destroy(&thread->op_ctx.update.sarray);
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
