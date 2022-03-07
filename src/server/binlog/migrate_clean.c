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
#include "../storage/object_block_index.h"
#include "slice_binlog.h"
#include "migrate_clean.h"

typedef struct slice_clean_thread_context {
    int thread_index;
    int64_t start_index;
    int64_t end_index;
    struct slice_clean_context *clean_ctx;
} SliceCleanThreadContext;

typedef struct slice_clean_thread_ctx_array {
    SliceCleanThreadContext *contexts;
    int count;
} SliceCleanThreadCtxArray;

typedef struct slice_clean_context {
    volatile int running_threads;
    SliceCleanThreadCtxArray clean_thread_array;
} SliceCleanContext;

static inline const char *get_slice_tmp_filename(const
        int binlog_index, char *filename, const int size)
{
#define TMP_FILENAME_AFFIX_STR  ".tmp"
#define TMP_FILENAME_AFFIX_LEN  (sizeof(TMP_FILENAME_AFFIX_STR) - 1)
    int len;

    slice_binlog_get_filename(binlog_index, filename, size);
    len = strlen(filename);
    if (len + TMP_FILENAME_AFFIX_LEN >= size) {
        return NULL;
    }

    memcpy(filename + len, TMP_FILENAME_AFFIX_STR, TMP_FILENAME_AFFIX_LEN);
    *(filename + len + TMP_FILENAME_AFFIX_LEN) = '\0';
    return filename;
}

static inline int dump_slices_to_file(const int binlog_index,
        const int64_t start_index, const int64_t end_index)
{
    char filename[PATH_MAX];

    if (get_slice_tmp_filename(binlog_index,
                filename, sizeof(filename)) == NULL)
    {
        return ENAMETOOLONG;
    }
    return ob_index_dump_slices_to_file(start_index, end_index, filename);
}

static void slice_clean_thread_run(SliceCleanThreadContext *thread_ctx,
        void *thread_data)
{
    if (dump_slices_to_file(thread_ctx->thread_index, thread_ctx->
                start_index, thread_ctx->end_index) != 0)
    {
        sf_terminate_myself();
    }
    __sync_sub_and_fetch(&thread_ctx->clean_ctx->running_threads, 1);
}

static int clean_slice_binlog(const int64_t total_slice_count)
{
#define MIN_SLICES_PER_THREAD  2000000
    int result;
    int bytes;
    int thread_count;
    int64_t buckets_per_thread;
    int64_t start_index;
    SliceCleanContext clean_ctx;
    SliceCleanThreadContext *ctx;
    SliceCleanThreadContext *end;

    thread_count = (total_slice_count + MIN_SLICES_PER_THREAD - 1) /
        MIN_SLICES_PER_THREAD;
    if (thread_count > SYSTEM_CPU_COUNT) {
        thread_count = SYSTEM_CPU_COUNT;
    }

    if (thread_count == 1) {
        return dump_slices_to_file(0, 0, g_ob_hashtable.capacity);
    }

    bytes = sizeof(SliceCleanThreadContext) * thread_count;
    clean_ctx.clean_thread_array.contexts = fc_malloc(bytes);
    if (clean_ctx.clean_thread_array.contexts == NULL) {
        return ENOMEM;
    }

    clean_ctx.running_threads = thread_count;
    buckets_per_thread = (g_ob_hashtable.capacity +
            thread_count - 1) / thread_count;
    end = clean_ctx.clean_thread_array.contexts + thread_count;
    for (ctx=clean_ctx.clean_thread_array.contexts,
            start_index=0; ctx<end; ctx++)
    {
        ctx->thread_index = ctx - clean_ctx.clean_thread_array.contexts;
        ctx->start_index = start_index;
        ctx->end_index = start_index + buckets_per_thread;
        if (ctx->end_index > g_ob_hashtable.capacity) {
            ctx->end_index = g_ob_hashtable.capacity;
        }
        ctx->clean_ctx = &clean_ctx;
        if ((result=shared_thread_pool_run((fc_thread_pool_callback)
                        slice_clean_thread_run, ctx)) != 0)
        {
            return result;
        }

        start_index += buckets_per_thread;
    }

    fc_sleep_ms(100);
    while (SF_G_CONTINUE_FLAG) {
        if (__sync_add_and_fetch(&clean_ctx.running_threads, 0) == 0) {
            break;
        }

        fc_sleep_ms(10);
    }

    return (SF_G_CONTINUE_FLAG ? 0 : EINTR);
}

int migrate_clean_binlog(const int64_t total_slice_count,
        const bool dump_slice_index)
{
    int result;

    if (dump_slice_index) {
        if ((result=clean_slice_binlog(total_slice_count)) != 0) {
            return result;
        }
    }

    return 0;
}
