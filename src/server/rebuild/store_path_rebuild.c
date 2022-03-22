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
#include "../../common/fs_func.h"
#include "../server_global.h"
#include "../server_group_info.h"
#include "../shared_thread_pool.h"
#include "../binlog/slice_binlog.h"
#include "../storage/object_block_index.h"
#include "binlog_spliter.h"
#include "rebuild_thread.h"
#include "store_path_rebuild.h"

#define DATA_REBUILD_REDO_STAGE_DUMP_SLICE      1
#define DATA_REBUILD_REDO_STAGE_RENAME_SLICE    2
#define DATA_REBUILD_REDO_STAGE_SPLIT_BINLOG    3
#define DATA_REBUILD_REDO_STAGE_RESPLIT_BINLOG  4 //for rebuild_threads change
#define DATA_REBUILD_REDO_STAGE_REBUILDING      5
#define DATA_REBUILD_REDO_STAGE_CLEANUP         6

#define DATA_REBUILD_REDO_ITEM_CURRENT_STAGE  "current_stage"
#define DATA_REBUILD_REDO_ITEM_BINLOG_COUNT   "binlog_file_count"
#define DATA_REBUILD_REDO_ITEM_BACKUP_SUBDIR  "backup_subdir"


typedef int (*dump_slices_to_file_callback)(const int binlog_index,
        const int64_t start_index, const int64_t end_index);

typedef struct data_dump_thread_context {
    int thread_index;
    int64_t start_index;
    int64_t end_index;
    struct data_rebuild_context *dump_ctx;
} DataDumpThreadContext;

typedef struct data_dump_thread_ctx_array {
    DataDumpThreadContext *contexts;
    int count;
} DataDumpThreadCtxArray;

typedef struct data_rebuild_context {
    volatile int running_threads;
    DataDumpThreadCtxArray thread_array;
} DataDumpContext;

typedef struct data_rebuild_redo_context {
    char redo_filename[PATH_MAX];
    char backup_subdir[NAME_MAX];
    int binlog_file_count;
    int current_stage;
} DataRebuildRedoContext;

static inline const char *get_slice_dump_filename(
        const int binlog_index, char *filename, const int size)
{
    snprintf(filename, size, "%s/%s/%s/slice-%03d.dmp",
            DATA_PATH_STR, FS_REBUILD_BINLOG_SUBDIR_NAME,
            REBUILD_BINLOG_SUBDIR_NAME_DUMP, binlog_index);
    return filename;
}

static inline const char *get_slice_mark_filename(
        char *filename, const int size)
{
    snprintf(filename, size, "%s/%s/.data_rebuild.flag",
            DATA_PATH_STR, FS_REBUILD_BINLOG_SUBDIR_NAME);
    return filename;
}

static int check_make_subdir_ex(const int count, ...)
{
    int result;
    char path[PATH_MAX];
    const char *subname1;
    const char *subname2;
    va_list ap;

    va_start(ap, count);
    switch (count) {
        case 1:
            subname1 = va_arg(ap, const char *);
            snprintf(path, sizeof(path), "%s/%s/%s", DATA_PATH_STR,
                    FS_REBUILD_BINLOG_SUBDIR_NAME, subname1);
            break;
        case 2:
            subname1 = va_arg(ap, const char *);
            subname2 = va_arg(ap, const char *);
            snprintf(path, sizeof(path), "%s/%s/%s/%s", DATA_PATH_STR,
                    FS_REBUILD_BINLOG_SUBDIR_NAME, subname1, subname2);
            break;
        default:
            snprintf(path, sizeof(path), "%s/%s",
                    DATA_PATH_STR, FS_REBUILD_BINLOG_SUBDIR_NAME);
            break;
    }
    va_end(ap);

    result = fc_check_mkdir(path, 0755);
    return result;
}

#define check_make_subdir() check_make_subdir_ex(0)
#define check_make_subdir1(...) check_make_subdir_ex(1, __VA_ARGS__)
#define check_make_subdir2(...) check_make_subdir_ex(2, __VA_ARGS__)

static inline int check_make_subdirs()
{
    int result;

    if ((result=check_make_subdir()) != 0) {
        return result;
    }

    if ((result=check_make_subdir1(REBUILD_BINLOG_SUBDIR_NAME_DUMP)) != 0) {
        return result;
    }

    if ((result=check_make_subdir1(REBUILD_BINLOG_SUBDIR_NAME_REPLAY)) != 0) {
        return result;
    }

    if ((result=check_make_subdir2(REBUILD_BINLOG_SUBDIR_NAME_REPLAY,
                    REBUILD_BINLOG_SUBDIR_NAME_REPLAY_INPUT)) != 0)
    {
        return result;
    }

    if ((result=check_make_subdir2(REBUILD_BINLOG_SUBDIR_NAME_REPLAY,
                    REBUILD_BINLOG_SUBDIR_NAME_REPLAY_OUTPUT)) != 0)
    {
        return result;
    }

    return 0;
}

static inline int dump_slices_to_file(const int binlog_index,
        const int64_t start_index, const int64_t end_index)
{
    char filename[PATH_MAX];

    if (get_slice_dump_filename(binlog_index, filename,
                sizeof(filename)) == NULL)
    {
        return ENAMETOOLONG;
    }
    return ob_index_dump_slices_to_file_ex(&g_ob_hashtable,
            start_index, end_index, filename, false);
}

static void data_dump_thread_run(DataDumpThreadContext *thread_ctx,
        void *thread_data)
{
    if (dump_slices_to_file(thread_ctx->thread_index, thread_ctx->
                start_index, thread_ctx->end_index) != 0)
    {
        sf_terminate_myself();
    }
    __sync_sub_and_fetch(&thread_ctx->dump_ctx->running_threads, 1);
}

static inline int remove_slices_to_file(const int binlog_index,
        const int64_t start_index, const int64_t end_index)
{
    char subdir_name[64];
    char filename[PATH_MAX];

    snprintf(subdir_name, sizeof(subdir_name), "%s/%s",
            FS_REBUILD_BINLOG_SUBDIR_NAME,
            REBUILD_BINLOG_SUBDIR_NAME_DUMP);
    if (sf_binlog_writer_get_filename(DATA_PATH_STR, subdir_name,
                binlog_index, filename, sizeof(filename)) == NULL)
    {
        return ENAMETOOLONG;
    }
    return ob_index_remove_slices_to_file(start_index, end_index,
            DATA_REBUILD_PATH_INDEX, filename);
}

static void rebuild_dump_thread_run(DataDumpThreadContext *thread_ctx,
        void *thread_data)
{
    if (remove_slices_to_file(thread_ctx->thread_index, thread_ctx->
                start_index, thread_ctx->end_index) != 0)
    {
        sf_terminate_myself();
    }
    __sync_sub_and_fetch(&thread_ctx->dump_ctx->running_threads, 1);
}

static int dump_slices(const int thread_count,
        dump_slices_to_file_callback dump_callback,
        fc_thread_pool_callback thread_run)
{
    int result;
    int bytes;
    int64_t buckets_per_thread;
    int64_t start_index;
    DataDumpContext dump_ctx;
    DataDumpThreadContext *ctx;
    DataDumpThreadContext *end;

    if (thread_count == 1) {
        return dump_callback(0, 0, g_ob_hashtable.capacity);
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

    free(dump_ctx.thread_array.contexts);
    return (SF_G_CONTINUE_FLAG ? 0 : EINTR);
}

static int dump_slice_binlog(const int64_t total_slice_count,
        int *binlog_file_count)
{
#define MIN_SLICES_PER_THREAD  2000000LL
    int thread_count;
    int result;

    thread_count = (total_slice_count + MIN_SLICES_PER_THREAD - 1) /
        MIN_SLICES_PER_THREAD;
    if (thread_count == 0) {
        thread_count = 1;
    } else if (thread_count > SYSTEM_CPU_COUNT) {
        thread_count = SYSTEM_CPU_COUNT;
    }
    *binlog_file_count = thread_count;

    result = dump_slices(thread_count, remove_slices_to_file,
            (fc_thread_pool_callback)rebuild_dump_thread_run);
    if (result != 0) {
        return result;
    }

    return dump_slices(thread_count, dump_slices_to_file,
            (fc_thread_pool_callback)data_dump_thread_run);
}

static int write_to_redo_file(DataRebuildRedoContext *redo_ctx)
{
    char buff[256];
    int result;
    int len;

    len = sprintf(buff, "%s=%d\n"
            "%s=%d\n"
            "%s=%s\n",
            DATA_REBUILD_REDO_ITEM_BINLOG_COUNT, redo_ctx->binlog_file_count,
            DATA_REBUILD_REDO_ITEM_CURRENT_STAGE, redo_ctx->current_stage,
            DATA_REBUILD_REDO_ITEM_BACKUP_SUBDIR, redo_ctx->backup_subdir);
    if ((result=safeWriteToFile(redo_ctx->redo_filename, buff, len)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "write to file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, redo_ctx->redo_filename, result, STRERROR(result));
    }

    return result;
}

static int load_from_redo_file(DataRebuildRedoContext *redo_ctx)
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
            DATA_REBUILD_REDO_ITEM_BINLOG_COUNT, &ini_context, 0);
    redo_ctx->current_stage = iniGetIntValue(NULL,
            DATA_REBUILD_REDO_ITEM_CURRENT_STAGE, &ini_context, 0);
    backup_subdir = iniGetStrValue(NULL,
            DATA_REBUILD_REDO_ITEM_BACKUP_SUBDIR,
            &ini_context);
    if (backup_subdir == NULL || *backup_subdir == '\0') {
        logError("file: "__FILE__", line: %d, "
                "redo file: %s, item: %s not exist",
                __LINE__, redo_ctx->redo_filename,
                DATA_REBUILD_REDO_ITEM_BACKUP_SUBDIR);
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

static int backup_slice_binlogs(DataRebuildRedoContext *redo_ctx)
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
    if (len + 1 + strlen(redo_ctx->backup_subdir) >=
            sizeof(binlog_filepath))
    {
        logError("file: "__FILE__", line: %d, "
                "slice backup path is too long", __LINE__);
        return ENAMETOOLONG;
    }

    sprintf(backup_filepath, "%s/%s", binlog_filepath,
            redo_ctx->backup_subdir);
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

static int rename_slice_binlogs(DataRebuildRedoContext *redo_ctx)
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

static int split_binlog(DataRebuildRedoContext *redo_ctx)
{
    ServerBinlogReaderArray rda;
    char subdir_name[64];
    ServerBinlogReader *reader;
    ServerBinlogReader *end;
    SFBinlogFilePosition position;
    int read_threads;
    int split_count;
    int result;

    if ((rda.readers=fc_malloc(sizeof(ServerBinlogReader) *
                    redo_ctx->binlog_file_count)) == NULL)
    {
        return ENOMEM;
    }

    snprintf(subdir_name, sizeof(subdir_name), "%s/%s",
            FS_REBUILD_BINLOG_SUBDIR_NAME,
            REBUILD_BINLOG_SUBDIR_NAME_DUMP);
    position.offset = 0;
    end = rda.readers + redo_ctx->binlog_file_count;
    for (reader=rda.readers; reader<end; reader++) {
        position.index = reader - rda.readers;
        if ((result=binlog_reader_init(reader, subdir_name,
                        NULL, &position)) != 0)
        {
            return result;
        }
    }

    rda.count = redo_ctx->binlog_file_count;
    split_count = DATA_REBUILD_THREADS;
    read_threads = FC_MIN(SYSTEM_CPU_COUNT, split_count);
    result = binlog_spliter_do(&rda, read_threads, split_count);

    for (reader=rda.readers; reader<end; reader++) {
        binlog_reader_destroy(reader);
    }
    free(rda.readers);

    return result;
}

static int redo(DataRebuildRedoContext *redo_ctx)
{
    int result;

    switch (redo_ctx->current_stage) {
        case DATA_REBUILD_REDO_STAGE_DUMP_SLICE:
            if ((result=backup_slice_binlogs(redo_ctx)) != 0) {
                return result;
            }

            redo_ctx->current_stage = DATA_REBUILD_REDO_STAGE_RENAME_SLICE;
            if ((result=write_to_redo_file(redo_ctx)) != 0) {
                return result;
            }
            //continue next stage
        case DATA_REBUILD_REDO_STAGE_RENAME_SLICE:
            if ((result=rename_slice_binlogs(redo_ctx)) != 0) {
                logError("file: "__FILE__", line: %d, "
                        "rename slice binlogs fail, "
                        "errno: %d, error info: %s",
                        __LINE__, result, STRERROR(result));
                return result;
            }
            redo_ctx->current_stage = DATA_REBUILD_REDO_STAGE_SPLIT_BINLOG;
            if ((result=write_to_redo_file(redo_ctx)) != 0) {
                return result;
            }
            //continue next stage
        case DATA_REBUILD_REDO_STAGE_SPLIT_BINLOG:
            if ((result=split_binlog(redo_ctx)) != 0) {
                return result;
            }
            redo_ctx->current_stage = DATA_REBUILD_REDO_STAGE_REBUILDING;
            if ((result=write_to_redo_file(redo_ctx)) != 0) {
                return result;
            }
        case DATA_REBUILD_REDO_STAGE_RESPLIT_BINLOG:
        case DATA_REBUILD_REDO_STAGE_REBUILDING:
            if ((result=rebuild_thread_do(DATA_REBUILD_THREADS)) != 0) {
                return result;
            }

            redo_ctx->current_stage = DATA_REBUILD_REDO_STAGE_CLEANUP;
            if ((result=write_to_redo_file(redo_ctx)) != 0) {
                return result;
            }
        case DATA_REBUILD_REDO_STAGE_CLEANUP:
            //TODO
            break;
        default:
            logError("file: "__FILE__", line: %d, "
                    "unkown stage: %d", __LINE__,
                    redo_ctx->current_stage);
            return EINVAL;
    }

    return fc_delete_file_ex(redo_ctx->redo_filename, "redo mark");
}

int store_path_rebuild_redo()
{
    int result;
    DataRebuildRedoContext redo_ctx;

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

int store_path_rebuild_dump_data(const int64_t total_slice_count)
{
    int result;
    DataRebuildRedoContext redo_ctx;
    int64_t start_time;
    time_t current_time;
    struct tm tm_current;
    char time_used[32];

    get_slice_mark_filename(redo_ctx.redo_filename,
            sizeof(redo_ctx.redo_filename));
    if (access(redo_ctx.redo_filename, F_OK) == 0) {
        logError("file: "__FILE__", line: %d, "
                "rebuild mark file: %s already exist, you must start "
                "fs_serverd without option: --%s!", __LINE__,
                redo_ctx.redo_filename, FS_DATA_REBUILD_LONG_OPTION_STR);
        return EEXIST;
    } else {
        result = errno != 0 ? errno : EPERM;
        if (result != ENOENT) {
            logError("file: "__FILE__", line: %d, "
                    "access rebuild mark file: %s fail, "
                    "errno: %d, error info: %s", __LINE__,
                    redo_ctx.redo_filename, result, STRERROR(result));
            return result;
        }
    }

    if ((result=check_make_subdirs()) != 0) {
        return result;
    }

    logInfo("file: "__FILE__", line: %d, "
            "begin dump slice binlog ...", __LINE__);

    start_time = get_current_time_ms();
    if ((result=dump_slice_binlog(total_slice_count,
                    &redo_ctx.binlog_file_count)) != 0)
    {
        return result;
    }

    logInfo("file: "__FILE__", line: %d, "
            "dump slice binlog, total slice count: %"PRId64", "
            "binlog file count: %d, time used: %s ms", __LINE__,
            total_slice_count + LOCAL_BINLOG_CHECK_LAST_SECONDS,
            redo_ctx.binlog_file_count, long_to_comma_str(
                get_current_time_ms() - start_time, time_used));

    redo_ctx.current_stage = DATA_REBUILD_REDO_STAGE_DUMP_SLICE;

    current_time = g_current_time;
    localtime_r(&current_time, &tm_current);
    strftime(redo_ctx.backup_subdir, sizeof(redo_ctx.backup_subdir),
            "%Y%m%d%H%M%S", &tm_current);

    if ((result=write_to_redo_file(&redo_ctx)) != 0) {
        return result;
    }

    return 0;
}
