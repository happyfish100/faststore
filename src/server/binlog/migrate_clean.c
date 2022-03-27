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
#include "../server_group_info.h"
#include "../shared_thread_pool.h"
#include "../storage/object_block_index.h"
#include "slice_binlog.h"
#include "replica_binlog.h"
#include "migrate_clean.h"

typedef struct data_dump_thread_context {
    int thread_index;
    int64_t start_index;
    int64_t end_index;
    int64_t slice_count;
    struct data_dump_context *dump_ctx;
} DataDumpThreadContext;

typedef struct data_dump_thread_ctx_array {
    DataDumpThreadContext *contexts;
    int count;
} DataDumpThreadCtxArray;

typedef struct data_dump_context {
    volatile int running_threads;
    DataDumpThreadCtxArray thread_array;
} DataDumpContext;

typedef struct binlog_clean_redo_context {
    char redo_filename[PATH_MAX];
    char backup_subdir[NAME_MAX];
    int binlog_file_count;
    int current_stage;
} BinlogCleanRedoContext;

#define MIGRATE_SUBDIR_NAME     "migrate"
#define BACKUP_SUBDIR_NAME_STR  "bak"
#define BACKUP_SUBDIR_NAME_LEN  (sizeof(BACKUP_SUBDIR_NAME_STR) - 1)

#define BINLOG_REDO_STAGE_REMOVE_SLICE    1
#define BINLOG_REDO_STAGE_RENAME_SLICE    2
#define BINLOG_REDO_STAGE_REMOVE_REPLICA  3

#define BINLOG_REDO_ITEM_BINLOG_COUNT   "binlog_file_count"
#define BINLOG_REDO_ITEM_CURRENT_STAGE  "current_stage"
#define BINLOG_REDO_ITEM_BACKUP_SUBDIR  "backup_subdir"

static inline const char *get_slice_dump_filename(const
        int binlog_index, char *filename, const int size)
{
    snprintf(filename, size, "%s/%s/slice-%03d.dmp", DATA_PATH_STR,
            MIGRATE_SUBDIR_NAME, binlog_index);
    return filename;
}

static inline int check_make_subdir()
{
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s",
            DATA_PATH_STR, MIGRATE_SUBDIR_NAME);
    return fc_check_mkdir(path, 0755);
}

static inline int dump_slices_to_file(const int binlog_index,
        const int64_t start_index, const int64_t end_index,
        int64_t *slice_count)
{
    char filename[PATH_MAX];

    if (get_slice_dump_filename(binlog_index,
                filename, sizeof(filename)) == NULL)
    {
        return ENAMETOOLONG;
    }
    return ob_index_dump_slices_to_file(start_index,
            end_index, filename, slice_count);
}

static void data_dump_thread_run(DataDumpThreadContext *thread,
        void *thread_data)
{
    if (dump_slices_to_file(thread->thread_index, thread->start_index,
                thread->end_index, &thread->slice_count) != 0)
    {
        sf_terminate_myself();
    }
    __sync_sub_and_fetch(&thread->dump_ctx->running_threads, 1);
}

static int dump_slice_binlog(const int64_t total_slice_count,
        int64_t *dump_slice_count, int *binlog_file_count)
{
#define MIN_SLICES_PER_THREAD  2000000LL
    int result;
    int bytes;
    int thread_count;
    int64_t buckets_per_thread;
    int64_t start_index;
    DataDumpContext dump_ctx;
    DataDumpThreadContext *ctx;
    DataDumpThreadContext *end;

    thread_count = (total_slice_count + MIN_SLICES_PER_THREAD - 1) /
        MIN_SLICES_PER_THREAD;
    if (thread_count == 0) {
        thread_count = 1;
    } else if (thread_count > SYSTEM_CPU_COUNT) {
        thread_count = SYSTEM_CPU_COUNT;
    }

    *binlog_file_count = thread_count;
    if (thread_count == 1) {
        return dump_slices_to_file(0, 0, g_ob_hashtable.
                capacity, dump_slice_count);
    }

    bytes = sizeof(DataDumpThreadContext) * thread_count;
    dump_ctx.thread_array.contexts = fc_malloc(bytes);
    if (dump_ctx.thread_array.contexts == NULL) {
        return ENOMEM;
    }

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
        if ((result=shared_thread_pool_run((fc_thread_pool_callback)
                        data_dump_thread_run, ctx)) != 0)
        {
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

    *dump_slice_count = 0;
    for (ctx=dump_ctx.thread_array.contexts; ctx<end; ctx++) {
        *dump_slice_count += ctx->slice_count;
    }

    free(dump_ctx.thread_array.contexts);
    return (SF_G_CONTINUE_FLAG ? 0 : EINTR);
}

static inline const char *get_slice_mark_filename(
        char *filename, const int size)
{
    snprintf(filename, size, "%s/%s/.migrate_clean.flag",
            DATA_PATH_STR, MIGRATE_SUBDIR_NAME);
    return filename;
}

static int write_to_redo_file(BinlogCleanRedoContext *redo_ctx)
{
    char buff[256];
    int result;
    int len;

    len = sprintf(buff, "%s=%d\n"
            "%s=%d\n"
            "%s=%s\n",
            BINLOG_REDO_ITEM_BINLOG_COUNT, redo_ctx->binlog_file_count,
            BINLOG_REDO_ITEM_CURRENT_STAGE, redo_ctx->current_stage,
            BINLOG_REDO_ITEM_BACKUP_SUBDIR, redo_ctx->backup_subdir);
    if ((result=safeWriteToFile(redo_ctx->redo_filename, buff, len)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "write to file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, redo_ctx->redo_filename, result, STRERROR(result));
    }

    return result;
}

static int load_from_redo_file(BinlogCleanRedoContext *redo_ctx)
{
    IniContext ini_context;
    char *backup_subdir;
    int result;

    if ((result=iniLoadFromFile(redo_ctx->redo_filename,
                    &ini_context)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "load from file \"%s\" fail, error code: %d",
                __LINE__, redo_ctx->redo_filename, result);
        return result;
    }

    redo_ctx->binlog_file_count = iniGetIntValue(NULL,
            BINLOG_REDO_ITEM_BINLOG_COUNT, &ini_context, 0);
    redo_ctx->current_stage = iniGetIntValue(NULL,
            BINLOG_REDO_ITEM_CURRENT_STAGE, &ini_context, 0);
    backup_subdir = iniGetStrValue(NULL,
            BINLOG_REDO_ITEM_BACKUP_SUBDIR,
            &ini_context);
    if (backup_subdir == NULL || *backup_subdir == '\0') {
        logError("file: "__FILE__", line: %d, "
                "redo file: %s, item: %s not exist",
                __LINE__, redo_ctx->redo_filename,
                BINLOG_REDO_ITEM_BACKUP_SUBDIR);
        return ENOENT;
    }
    snprintf(redo_ctx->backup_subdir,
            sizeof(redo_ctx->backup_subdir),
            "%s", backup_subdir);

    iniFreeContext(&ini_context);
    return 0;
}

static inline int backup_to_path(const char *src_filename,
        const char *dest_filepath)
{
    const bool overwritten = false;
    char dest_filename[PATH_MAX];

    snprintf(dest_filename, sizeof(dest_filename), "%s%s",
            dest_filepath, strrchr(src_filename, '/'));
    return fc_check_rename_ex(src_filename,
            dest_filename, overwritten);
}

static int backup_slice_binlogs(BinlogCleanRedoContext *redo_ctx)
{
    int result;
    int last_index;
    int binlog_index;
    int len;
    char binlog_filepath[PATH_MAX];
    char binlog_filename[PATH_MAX];
    char index_filename[PATH_MAX];
    char backup_filepath[PATH_MAX];

    slice_binlog_get_filepath(binlog_filepath, sizeof(binlog_filepath));
    len = strlen(binlog_filepath);
    if (len + 2 + BACKUP_SUBDIR_NAME_LEN + strlen(redo_ctx->
                backup_subdir) >= sizeof(binlog_filepath))
    {
        logError("file: "__FILE__", line: %d, "
                "slice backup path is too long", __LINE__);
        return ENAMETOOLONG;
    }

    len = sprintf(backup_filepath, "%s/%s", binlog_filepath,
            BACKUP_SUBDIR_NAME_STR);
    if ((result=fc_check_mkdir(backup_filepath, 0775)) != 0) {
        return result;
    }

    sprintf(backup_filepath + len, "/%s", redo_ctx->backup_subdir);
    if ((result=fc_check_mkdir(backup_filepath, 0775)) != 0) {
        return result;
    }

    last_index = slice_binlog_get_current_write_index();
    for (binlog_index=0; binlog_index<=last_index; binlog_index++) {
        slice_binlog_get_filename(binlog_index, binlog_filename,
                sizeof(binlog_filename));
        if ((result=backup_to_path(binlog_filename, backup_filepath)) != 0) {
            return result;
        }
    }

    slice_binlog_get_index_filename(index_filename,
            sizeof(index_filename));
    if ((result=backup_to_path(index_filename, backup_filepath)) != 0) {
        return result;
    }

    logInfo("file: "__FILE__", line: %d, "
            "backup %d slice binlog files to %s/",
            __LINE__, last_index + 1, backup_filepath);
    return 0;
}

static int rename_slice_binlogs(BinlogCleanRedoContext *redo_ctx)
{
    const bool overwritten = true;
    int result;
    int last_index;
    int binlog_index;
    char dump_filename[PATH_MAX];
    char binlog_filename[PATH_MAX];

    last_index = redo_ctx->binlog_file_count - 1;
    for (binlog_index=0; binlog_index<=last_index; binlog_index++) {
        get_slice_dump_filename(binlog_index, dump_filename,
                sizeof(dump_filename));
        slice_binlog_get_filename(binlog_index, binlog_filename,
                sizeof(binlog_filename));
        if ((result=fc_check_rename_ex(dump_filename,
                        binlog_filename, overwritten)) != 0)
        {
            return result;
        }
    }

    return slice_binlog_set_binlog_index(last_index);
}

static int backup_replica_binlogs(BinlogCleanRedoContext *redo_ctx)
{
    int result;
    int data_group_count;
    int data_group_id;
    int len;
    int backup_count;
    char binlog_basepath[PATH_MAX];
    char binlog_filepath[PATH_MAX];
    char backup_filepath[PATH_MAX];

    replica_binlog_get_base_path(binlog_basepath, sizeof(binlog_basepath));
    len = strlen(binlog_basepath);
    if (len + 2 + BACKUP_SUBDIR_NAME_LEN + strlen(redo_ctx->
                backup_subdir) >= sizeof(binlog_basepath))
    {
        logError("file: "__FILE__", line: %d, "
                "slice backup path is too long", __LINE__);
        return ENAMETOOLONG;
    }

    len = sprintf(backup_filepath, "%s/%s", binlog_basepath,
            BACKUP_SUBDIR_NAME_STR);
    if ((result=fc_check_mkdir(backup_filepath, 0775)) != 0) {
        return result;
    }
    sprintf(backup_filepath + len, "/%s", redo_ctx->backup_subdir);

    backup_count = 0;
    data_group_count = FS_DATA_GROUP_COUNT(CLUSTER_CONFIG_CTX);
    for (data_group_id=1; data_group_id<=data_group_count; data_group_id++) {
        if (fs_is_my_data_group(data_group_id)) {
            continue;
        }

        replica_binlog_get_filepath(data_group_id,
                binlog_filepath, sizeof(binlog_filepath));
        if (access(binlog_filepath, F_OK) != 0) {
            continue;
        }

        if ((backup_count == 0) && (result=fc_check_mkdir(
                        backup_filepath, 0775)) != 0)
        {
            return result;
        }

        if ((result=backup_to_path(binlog_filepath, backup_filepath)) != 0) {
            return result;
        }
        ++backup_count;
    }

    if (backup_count > 0) {
        logInfo("file: "__FILE__", line: %d, "
                "backup %d replica data groups to %s/",
                __LINE__, backup_count, backup_filepath);
    }
    return 0;
}

static int redo(BinlogCleanRedoContext *redo_ctx)
{
    int result;

    switch (redo_ctx->current_stage) {
        case BINLOG_REDO_STAGE_REMOVE_SLICE:
            if ((result=backup_slice_binlogs(redo_ctx)) != 0) {
                return result;
            }

            redo_ctx->current_stage = BINLOG_REDO_STAGE_RENAME_SLICE;
            if ((result=write_to_redo_file(redo_ctx)) != 0) {
                return result;
            }
            //continue next stage
        case BINLOG_REDO_STAGE_RENAME_SLICE:
            if ((result=rename_slice_binlogs(redo_ctx)) != 0) {
                return result;
            }
            redo_ctx->current_stage = BINLOG_REDO_STAGE_REMOVE_REPLICA;
            if ((result=write_to_redo_file(redo_ctx)) != 0) {
                return result;
            }
            //continue next stage
        case BINLOG_REDO_STAGE_REMOVE_REPLICA:
            if ((result=backup_replica_binlogs(redo_ctx)) != 0) {
                return result;
            }
            break;
        default:
            logError("file: "__FILE__", line: %d, "
                    "unkown stage: %d", __LINE__,
                    redo_ctx->current_stage);
            return EINVAL;
    }

    return fc_delete_file_ex(redo_ctx->redo_filename, "redo mark");
}

int migrate_clean_redo()
{
    int result;
    BinlogCleanRedoContext redo_ctx;

    if ((result=check_make_subdir()) != 0) {
        return result;
    }

    get_slice_mark_filename(redo_ctx.redo_filename,
            sizeof(redo_ctx.redo_filename));
    if (access(redo_ctx.redo_filename, F_OK) != 0) {
        if (errno == ENOENT) {
            return 0;
        }

        result = (errno != 0 ? errno : EPERM);
        logError("file: "__FILE__", line: %d, "
                "access slice mark file: %s fail, "
                "errno: %d, error info: %s", __LINE__,
                redo_ctx.redo_filename, result, STRERROR(result));
        return result;
    }

    if ((result=load_from_redo_file(&redo_ctx)) != 0) {
        return result;
    }

    return redo(&redo_ctx);
}

int migrate_clean_binlog(const int64_t total_slice_count,
        const bool dump_slice_index)
{
    int result;
    BinlogCleanRedoContext redo_ctx;
    int64_t dump_slice_count;
    int64_t start_time;
    time_t current_time;
    struct tm tm_current;
    char time_used[32];

    if ((result=check_make_subdir()) != 0) {
        return result;
    }

    if (dump_slice_index) {
        logInfo("file: "__FILE__", line: %d, "
                "begin dump slice binlog ...", __LINE__);

        start_time = get_current_time_ms();
        if ((result=dump_slice_binlog(total_slice_count, &dump_slice_count,
                        &redo_ctx.binlog_file_count)) != 0)
        {
            return result;
        }

        logInfo("file: "__FILE__", line: %d, "
                "dump slice binlog, slice count: %"PRId64", "
                "binlog file count: %d, time used: %s ms", __LINE__,
                dump_slice_count, redo_ctx.binlog_file_count,
                long_to_comma_str(get_current_time_ms() -
                    start_time, time_used));

        redo_ctx.current_stage = BINLOG_REDO_STAGE_REMOVE_SLICE;
    } else {
        redo_ctx.binlog_file_count =
            slice_binlog_get_current_write_index() + 1;
        redo_ctx.current_stage = BINLOG_REDO_STAGE_REMOVE_REPLICA;
    }

    current_time = g_current_time;
    localtime_r(&current_time, &tm_current);
    strftime(redo_ctx.backup_subdir, sizeof(redo_ctx.backup_subdir),
            "%Y%m%d%H%M%S", &tm_current);

    get_slice_mark_filename(redo_ctx.redo_filename,
            sizeof(redo_ctx.redo_filename));
    if ((result=write_to_redo_file(&redo_ctx)) != 0) {
        return result;
    }

    return redo(&redo_ctx);
}
