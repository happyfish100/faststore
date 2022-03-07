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

static int dump_slice_binlog(const int64_t total_slice_count,
        int *binlog_file_count)
{
#define MIN_SLICES_PER_THREAD  2000000LL
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

    *binlog_file_count = thread_count;
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

static inline const char *get_slice_mark_filename(
        char *filename, const int size)
{
    snprintf(filename, size, "%s/%s/.slice_dump.flag",
            DATA_PATH_STR, FS_SLICE_BINLOG_SUBDIR_NAME);
    return filename;
}

#define BINLOG_REDO_STAGE_REMOVE_SLICE    1
#define BINLOG_REDO_STAGE_RENAME_SLICE    2
#define BINLOG_REDO_STAGE_REMOVE_REPLICA  3

#define BINLOG_REDO_ITEM_BINLOG_COUNT  "binlog_file_count"
#define BINLOG_REDO_ITEM_CURRENT_STAGE "current_stage"

static int write_to_redo_file(const char *redo_filename,
        const int binlog_file_count, const int stage)
{
    char buff[256];
    int result;
    int len;

    len = sprintf(buff, "%s=%d\n"
            "%s=%d\n",
            BINLOG_REDO_ITEM_BINLOG_COUNT,
            binlog_file_count,
            BINLOG_REDO_ITEM_CURRENT_STAGE, stage);
    if ((result=safeWriteToFile(redo_filename, buff, len)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "write to file \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, redo_filename,
                result, STRERROR(result));
    }

    return result;
}

static int load_from_redo_file(const char *redo_filename,
        int *binlog_file_count, int *stage)
{
    IniContext ini_context;
    int result;

    if ((result=iniLoadFromFile(redo_filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load from file \"%s\" fail, error code: %d",
                __LINE__, redo_filename, result);
        return result;
    }

    *binlog_file_count = iniGetIntValue(NULL,
            BINLOG_REDO_ITEM_BINLOG_COUNT,
            &ini_context, 0);
    *stage = iniGetIntValue(NULL,
            BINLOG_REDO_ITEM_CURRENT_STAGE,
            &ini_context, 0);

    iniFreeContext(&ini_context);
    return 0;
}

static int delete_all_slice_binlogs()
{
    //TODO
    return 0;
}

static int rename_slice_binlogs(const int binlog_file_count)
{
    //TODO
    return 0;
}

static int remove_replica_binlogs()
{
    //TODO
    /*
       replica_binlog_get_filepath(
       const int data_group_id, char *filename, const int size)
     */
    return 0;
}

static int redo(const char *redo_filename, const int binlog_file_count,
        const int current_stage)
{
    int result;

    switch (current_stage) {
        case BINLOG_REDO_STAGE_REMOVE_SLICE:
            if ((result=delete_all_slice_binlogs()) != 0) {
                return result;
            }
            if ((result=write_to_redo_file(redo_filename, binlog_file_count,
                            BINLOG_REDO_STAGE_RENAME_SLICE)) != 0)
            {
                return result;
            }
        case BINLOG_REDO_STAGE_RENAME_SLICE:
            if ((result=rename_slice_binlogs(binlog_file_count)) != 0) {
                return result;
            }
            if ((result=write_to_redo_file(redo_filename, binlog_file_count,
                            BINLOG_REDO_STAGE_REMOVE_REPLICA)) != 0)
            {
                return result;
            }
        case BINLOG_REDO_STAGE_REMOVE_REPLICA:
            if ((result=remove_replica_binlogs()) != 0) {
                return result;
            }
            break;
        default:
            logError("file: "__FILE__", line: %d, "
                    "unkown stage: %d", __LINE__, current_stage);
            return EINVAL;
    }

    if ((result=slice_binlog_set_binlog_index(binlog_file_count - 1)) != 0) {
        return result;
    }

    return fc_delete_file_ex(redo_filename, "migrate clean mark");
}

int migrate_clean_redo()
{
    int result;
    int binlog_file_count;
    int current_stage;
    char redo_filename[PATH_MAX];

    get_slice_mark_filename(redo_filename, sizeof(redo_filename));
    if (access(redo_filename, F_OK) != 0) {
        if (errno == ENOENT) {
            return 0;
        }

        result = (errno != 0 ? errno : EPERM);
        logError("file: "__FILE__", line: %d, "
                "access slice mark file: %s fail, "
                "errno: %d, error info: %s", __LINE__,
                redo_filename, result, STRERROR(result));
        return result;
    }

    if ((result=load_from_redo_file(redo_filename, &binlog_file_count,
                    &current_stage)) != 0)
    {
        return result;
    }

    return redo(redo_filename, binlog_file_count, current_stage);
}

int migrate_clean_binlog(const int64_t total_slice_count,
        const bool dump_slice_index)
{
    int result;
    int binlog_file_count;
    int current_stage;
    char redo_filename[PATH_MAX];

    if (dump_slice_index) {
        if ((result=dump_slice_binlog(total_slice_count,
                        &binlog_file_count)) != 0)
        {
            return result;
        }

        current_stage = BINLOG_REDO_STAGE_REMOVE_SLICE;
    } else {
        binlog_file_count = slice_binlog_get_current_write_index() + 1;
        current_stage = BINLOG_REDO_STAGE_REMOVE_REPLICA;
    }

    get_slice_mark_filename(redo_filename, sizeof(redo_filename));
    if ((result=write_to_redo_file(redo_filename, binlog_file_count,
                    current_stage)) != 0)
    {
        return result;
    }

    return redo(redo_filename, binlog_file_count, current_stage);
}
