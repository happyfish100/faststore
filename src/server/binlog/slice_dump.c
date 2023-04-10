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

#include <stdarg.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "sf/sf_func.h"
#include "diskallocator/binlog/trunk/trunk_binlog.h"
#include "diskallocator/trunk/trunk_hashtable.h"
#include "../../common/fs_func.h"
#include "../server_global.h"
#include "../server_group_info.h"
#include "../shared_thread_pool.h"
#include "../binlog/slice_binlog.h"
#include "../storage/object_block_index.h"
#include "slice_dump.h"

typedef int (*dump_slices_to_file_callback)(slice_dump_get_filename_func
        get_filename_func, const int source, const int binlog_index,
        const int64_t start_index, const int64_t end_index,
        int64_t *slice_count);

typedef struct slice_dump_thread_context {
    int thread_index;
    int64_t start_index;
    int64_t end_index;
    int64_t slice_count;
    struct slice_dump_context *dump_ctx;
} DataDumpThreadContext;

typedef struct slice_dump_thread_ctx_array {
    DataDumpThreadContext *contexts;
    int count;
} DataDumpThreadCtxArray;

typedef struct slice_dump_context {
    slice_dump_get_filename_func get_filename_func;
    int source;
    volatile int running_threads;
    DataDumpThreadCtxArray thread_array;
} DataDumpContext;

static inline int dump_slices_to_file(slice_dump_get_filename_func
        get_filename_func, const int source, const int binlog_index,
        const int64_t start_index, const int64_t end_index,
        int64_t *slice_count)
{
    const bool need_padding = false;
    const bool need_lock = false;
    char filename[PATH_MAX];

    if (get_filename_func(binlog_index, filename, sizeof(filename)) == NULL) {
        return ENAMETOOLONG;
    }
    return ob_index_dump_slices_to_file_ex(&g_ob_hashtable,
            start_index, end_index, filename, slice_count,
            source, need_padding, need_lock);
}

static void slice_dump_thread_run(DataDumpThreadContext *thread,
        void *thread_data)
{
    if (dump_slices_to_file(thread->dump_ctx->get_filename_func,
                thread->dump_ctx->source, thread->thread_index,
                thread->start_index, thread->end_index,
                &thread->slice_count) != 0)
    {
        sf_terminate_myself();
    }
    __sync_sub_and_fetch(&thread->dump_ctx->running_threads, 1);
}

static inline int remove_slices_to_file(slice_dump_get_filename_func
        get_filename_func, const int source, const int binlog_index,
        const int64_t start_index, const int64_t end_index,
        int64_t *slice_count)
{
    char filename[PATH_MAX];

    if (get_filename_func(binlog_index, filename, sizeof(filename)) == NULL) {
        return ENAMETOOLONG;
    }
    return ob_index_remove_slices_to_file(start_index,
            end_index, filename, slice_count, source);
}

static void slice_remove_thread_run(DataDumpThreadContext *thread,
        void *thread_data)
{
    if (remove_slices_to_file(thread->dump_ctx->get_filename_func,
                thread->dump_ctx->source, thread->thread_index,
                thread->start_index, thread->end_index,
                &thread->slice_count) != 0)
    {
        sf_terminate_myself();
    }
    __sync_sub_and_fetch(&thread->dump_ctx->running_threads, 1);
}

static int dump_slices(const int thread_count,
        dump_slices_to_file_callback dump_callback,
        slice_dump_get_filename_func get_filename_func,
        const int source, fc_thread_pool_callback thread_run,
        int64_t *total_slice_count)
{
    int result;
    int bytes;
    int64_t buckets_per_thread;
    int64_t start_index;
    DataDumpContext dump_ctx;
    DataDumpThreadContext *ctx;
    DataDumpThreadContext *end;

    if (thread_count == 1) {
        return dump_callback(get_filename_func, source, 0, 0,
                g_ob_hashtable.capacity, total_slice_count);
    }

    bytes = sizeof(DataDumpThreadContext) * thread_count;
    dump_ctx.thread_array.contexts = fc_malloc(bytes);
    if (dump_ctx.thread_array.contexts == NULL) {
        return ENOMEM;
    }

    dump_ctx.get_filename_func = get_filename_func;
    dump_ctx.source = source;
    dump_ctx.running_threads = thread_count;
    buckets_per_thread = (g_ob_hashtable.capacity +
            thread_count - 1) / thread_count;
    end = dump_ctx.thread_array.contexts + thread_count;
    for (ctx=dump_ctx.thread_array.contexts,
            start_index=0; ctx<end; ctx++)
    {
        ctx->thread_index = ctx - dump_ctx.thread_array.contexts;
        ctx->start_index = start_index;
        ctx->end_index = start_index + buckets_per_thread;
        if (ctx->end_index > g_ob_hashtable.capacity) {
            ctx->end_index = g_ob_hashtable.capacity;
        }
        ctx->dump_ctx = &dump_ctx;
        if ((result=shared_thread_pool_run(thread_run, ctx)) != 0) {
            return result;
        }

        start_index += buckets_per_thread;
    }

    do {
        fc_sleep_ms(10);
        if (__sync_add_and_fetch(&dump_ctx.running_threads, 0) == 0) {
            break;
        }
    } while (SF_G_CONTINUE_FLAG);

    *total_slice_count = 0;
    for (ctx=dump_ctx.thread_array.contexts; ctx<end; ctx++) {
        *total_slice_count += ctx->slice_count;
    }

    free(dump_ctx.thread_array.contexts);
    return (SF_G_CONTINUE_FLAG ? 0 : EINTR);
}

int slice_dump_to_files(slice_dump_get_filename_func get_remove_filename_func,
        slice_dump_get_filename_func get_keep_filename_func, const int source,
        const int64_t total_slice_count, int *binlog_file_count)
{
#define MIN_SLICES_PER_THREAD  2000000LL
    int thread_count;
    int64_t start_time;
    int64_t remove_slice_count;
    int64_t keep_slice_count;
    char time_used[32];
    int result;

    thread_count = (total_slice_count + MIN_SLICES_PER_THREAD - 1) /
        MIN_SLICES_PER_THREAD;
    if (thread_count == 0) {
        thread_count = 1;
    } else if (thread_count > SYSTEM_CPU_COUNT) {
        thread_count = SYSTEM_CPU_COUNT;
    }
    *binlog_file_count = thread_count;

    logInfo("file: "__FILE__", line: %d, "
            "begin remove slice binlog ...", __LINE__);
    start_time = get_current_time_ms();

    result = dump_slices(thread_count, remove_slices_to_file,
            get_remove_filename_func, source, (fc_thread_pool_callback)
            slice_remove_thread_run, &remove_slice_count);
    if (result != 0) {
        return result;
    }

    logInfo("file: "__FILE__", line: %d, "
            "remove slice binlog, slice count: %"PRId64", "
            "time used: %s ms", __LINE__, remove_slice_count,
            long_to_comma_str(get_current_time_ms() -
                start_time, time_used));

    logInfo("file: "__FILE__", line: %d, "
            "begin dump slice binlog ...", __LINE__);
    start_time = get_current_time_ms();

    result = dump_slices(thread_count, dump_slices_to_file,
            get_keep_filename_func, source, (fc_thread_pool_callback)
            slice_dump_thread_run, &keep_slice_count);
    if (result == 0) {
        FC_ATOMIC_SET(SLICE_BINLOG_COUNT, keep_slice_count);
        logInfo("file: "__FILE__", line: %d, "
                "dump slice binlog, slice count: %"PRId64", "
                "binlog file count: %d, time used: %s ms", __LINE__,
                keep_slice_count, *binlog_file_count, long_to_comma_str(
                    get_current_time_ms() - start_time, time_used));
    }

    return result;
}
