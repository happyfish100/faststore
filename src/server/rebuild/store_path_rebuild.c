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
#include "../binlog/slice_dump.h"
#include "../binlog/db_remove.h"
#include "../storage/object_block_index.h"
#include "rebuild_binlog.h"
#include "binlog_spliter.h"
#include "binlog_reader.h"
#include "rebuild_thread.h"
#include "store_path_rebuild.h"

#define DATA_REBUILD_REDO_STAGE_BACKUP_TRUNK    1
#define DATA_REBUILD_REDO_STAGE_RENAME_TRUNK    2
#define DATA_REBUILD_REDO_STAGE_BACKUP_SLICE    3
#define DATA_REBUILD_REDO_STAGE_RENAME_SLICE    4
#define DATA_REBUILD_REDO_STAGE_PADDING_SLICE   5
#define DATA_REBUILD_REDO_STAGE_REMOVE_DB       6  //for storage engine only
#define DATA_REBUILD_REDO_STAGE_SPLIT_BINLOG    7
#define DATA_REBUILD_REDO_STAGE_REBUILDING      8
#define DATA_REBUILD_REDO_STAGE_CLEANUP         9

#define DATA_REBUILD_REDO_ITEM_CURRENT_STAGE   "current_stage"
#define DATA_REBUILD_REDO_ITEM_BINLOG_COUNT    "binlog_file_count" //dump output
#define DATA_REBUILD_REDO_ITEM_REBUILD_THREADS "rebuild_threads"
#define DATA_REBUILD_REDO_ITEM_LAST_SN         "last_sn"
#define DATA_REBUILD_REDO_ITEM_BACKUP_SUBDIR   "backup_subdir"

typedef struct data_rebuild_redo_context {
    char redo_filename[PATH_MAX];
    char backup_subdir[NAME_MAX];
    int current_stage;
    int binlog_file_count;  //slice binlog file count
    int rebuild_threads;
    int64_t last_sn;
} DataRebuildRedoContext;

static inline const char *get_trunk_dump_filename(
        char *filename, const int size)
{
    snprintf(filename, size, "%s/%s/%s/trunk.dmp",
            DATA_PATH_STR, FS_REBUILD_BINLOG_SUBDIR_NAME,
            REBUILD_BINLOG_SUBDIR_NAME_DUMP);
    return filename;
}

static const char *get_slice_remove_filename(const int binlog_index,
        char *filename, const int size)
{
    char subdir_name[64];

    snprintf(subdir_name, sizeof(subdir_name), "%s/%s",
            FS_REBUILD_BINLOG_SUBDIR_NAME,
            REBUILD_BINLOG_SUBDIR_NAME_DUMP);
    return sf_binlog_writer_get_filename(DATA_PATH_STR,
            subdir_name, binlog_index, filename, size);
}

static const char *get_slice_dump_filename(const int binlog_index,
        char *filename, const int size)
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

static inline int check_make_subdir(const char *subname)
{
    char path[PATH_MAX];

    snprintf(path, sizeof(path), "%s/%s/%s", DATA_PATH_STR,
            FS_REBUILD_BINLOG_SUBDIR_NAME, subname);
    return fc_check_mkdir(path, 0755);
}

static inline int check_make_subdirs()
{
    int result;
    char path[PATH_MAX];

    snprintf(path, sizeof(path), "%s/%s", DATA_PATH_STR,
            FS_REBUILD_BINLOG_SUBDIR_NAME);
    if ((result=fc_check_mkdir(path, 0755)) != 0) {
        return result;
    }

    if ((result=check_make_subdir(REBUILD_BINLOG_SUBDIR_NAME_DUMP)) != 0) {
        return result;
    }

    if ((result=check_make_subdir(REBUILD_BINLOG_SUBDIR_NAME_REPLAY)) != 0) {
        return result;
    }

    return 0;
}

static int write_to_redo_file(DataRebuildRedoContext *redo_ctx)
{
    char buff[256];
    int result;
    int len;

    len = sprintf(buff, "%s=%d\n"
            "%s=%d\n"
            "%s=%d\n"
            "%s=%"PRId64"\n"
            "%s=%s\n",
            DATA_REBUILD_REDO_ITEM_CURRENT_STAGE, redo_ctx->current_stage,
            DATA_REBUILD_REDO_ITEM_BINLOG_COUNT, redo_ctx->binlog_file_count,
            DATA_REBUILD_REDO_ITEM_REBUILD_THREADS, redo_ctx->rebuild_threads,
            DATA_REBUILD_REDO_ITEM_LAST_SN, redo_ctx->last_sn,
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

    redo_ctx->current_stage = iniGetIntValue(NULL,
            DATA_REBUILD_REDO_ITEM_CURRENT_STAGE, &ini_context, 0);
    redo_ctx->binlog_file_count = iniGetIntValue(NULL,
            DATA_REBUILD_REDO_ITEM_BINLOG_COUNT, &ini_context, 0);
    redo_ctx->rebuild_threads = iniGetIntValue(NULL,
            DATA_REBUILD_REDO_ITEM_REBUILD_THREADS, &ini_context, 0);
    redo_ctx->last_sn = iniGetInt64Value(NULL,
            DATA_REBUILD_REDO_ITEM_LAST_SN, &ini_context, 0);
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

static int backup_binlogs(DataRebuildRedoContext *redo_ctx,
        const char *subdir_name, const int last_index)
{
    int result;
    int binlog_index;
    int len;
    char binlog_filepath[PATH_MAX];
    char binlog_filename[PATH_MAX];
    char index_filename[PATH_MAX];
    char backup_filepath[PATH_MAX];

    sf_binlog_writer_get_filepath(DATA_PATH_STR, subdir_name,
            binlog_filepath, sizeof(binlog_filepath));
    len = strlen(binlog_filepath);
    if (len + 2 + + REBUILD_BACKUP_SUBDIR_NAME_LEN + strlen(redo_ctx->
                backup_subdir) >= sizeof(binlog_filepath))
    {
        logError("file: "__FILE__", line: %d, "
                "%s backup path is too long",
                __LINE__, subdir_name);
        return ENAMETOOLONG;
    }

    len = sprintf(backup_filepath, "%s/%s", binlog_filepath,
            REBUILD_BACKUP_SUBDIR_NAME_STR);
    if ((result=fc_check_mkdir(backup_filepath, 0775)) != 0) {
        return result;
    }

    sprintf(backup_filepath + len, "/%s", redo_ctx->backup_subdir);
    if ((result=fc_check_mkdir(backup_filepath, 0775)) != 0) {
        return result;
    }

    for (binlog_index=0; binlog_index<=last_index; binlog_index++) {
        sf_binlog_writer_get_filename(DATA_PATH_STR, subdir_name,
                binlog_index, binlog_filename, sizeof(binlog_filename));
        if ((result=backup_to_path(binlog_filename, backup_filepath)) != 0) {
            return result;
        }
    }

    sf_binlog_writer_get_index_filename(DATA_PATH_STR, subdir_name,
            index_filename, sizeof(index_filename));
    if ((result=backup_to_path(index_filename, backup_filepath)) != 0) {
        return result;
    }

    logInfo("file: "__FILE__", line: %d, "
            "backup %d %s binlog files to %s/", __LINE__,
            last_index + 1, subdir_name, backup_filepath);
    return 0;
}

static inline int backup_trunk_binlogs(DataRebuildRedoContext *redo_ctx)
{
    int last_index;
    last_index = da_trunk_binlog_get_current_write_index(&DA_CTX);
    return backup_binlogs(redo_ctx, FS_TRUNK_BINLOG_SUBDIR_NAME, last_index);
}

static inline int backup_slice_binlogs(DataRebuildRedoContext *redo_ctx)
{
    int last_index;
    last_index = slice_binlog_get_current_write_index();
    return backup_binlogs(redo_ctx, FS_SLICE_BINLOG_SUBDIR_NAME, last_index);
}

static int rename_trunk_binlogs(DataRebuildRedoContext *redo_ctx)
{
    const bool overwritten = true;
    const int binlog_index = 0;
    const int last_index = 0;
    int result;
    char dump_filename[PATH_MAX];
    char binlog_filename[PATH_MAX];

    get_trunk_dump_filename(dump_filename, sizeof(dump_filename));
    da_trunk_binlog_get_filename(&DA_CTX, binlog_index,
            binlog_filename, sizeof(binlog_filename));
    if ((result=fc_check_rename_ex(dump_filename,
                    binlog_filename, overwritten)) != 0)
    {
        return result;
    }

    return da_trunk_binlog_set_binlog_write_index(&DA_CTX, last_index);
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

    if ((result=slice_binlog_set_binlog_indexes(0, last_index)) != 0) {
        return result;
    }
    return slice_binlog_set_binlog_write_index(last_index); //for reopen binlog
}

static int padding_slice_binlog(DataRebuildRedoContext *redo_ctx)
{
    int result;

    if (redo_ctx->last_sn > FC_ATOMIC_GET(SLICE_BINLOG_SN)) {
        if ((result=slice_binlog_set_sn(redo_ctx->last_sn - 1)) != 0) {
            return result;
        }
    }

    return slice_binlog_padding_one(BINLOG_SOURCE_REBUILD);
}

static int split_binlog(DataRebuildRedoContext *redo_ctx)
{
    ServerBinlogReaderArray rda;
    int64_t slice_count;
    int64_t start_time;
    char time_used[32];
    char subdir_name[64];
    ServerBinlogReader *reader;
    ServerBinlogReader *end;
    SFBinlogFilePosition position;
    int read_threads;
    int split_count;
    int result;

    logInfo("file: "__FILE__", line: %d, "
            "begin split slice binlog ...", __LINE__);
    start_time = get_current_time_ms();

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
    if ((result=binlog_spliter_do(&rda, read_threads,
                    split_count, &slice_count)) == 0)
    {
        logInfo("file: "__FILE__", line: %d, "
                "split slice binlog done, slice count: %"PRId64", "
                "time used: %s ms", __LINE__, slice_count,
                long_to_comma_str(get_current_time_ms() -
                    start_time, time_used));
    }

    for (reader=rda.readers; reader<end; reader++) {
        binlog_reader_destroy(reader);
    }
    free(rda.readers);

    return result;
}

static int resplit_binlog(DataRebuildRedoContext *redo_ctx)
{
    ServerBinlogReaderArray rda;
    int64_t slice_count;
    int64_t start_time;
    char time_used[32];
    char subdir_name[64];
    char input_path[PATH_MAX];
    char replay_path[PATH_MAX];
    ServerBinlogReader *reader;
    ServerBinlogReader *end;
    int old_rebuild_threads;
    int new_rebuild_threads;
    int thread_index;
    int read_threads;
    int result;

    start_time = get_current_time_ms();
    snprintf(input_path, sizeof(input_path), "%s/%s/%s", DATA_PATH_STR,
            FS_REBUILD_BINLOG_SUBDIR_NAME, redo_ctx->backup_subdir);
    if (access(input_path, F_OK) != 0) {
        snprintf(replay_path, sizeof(replay_path), "%s/%s/%s",
                DATA_PATH_STR, FS_REBUILD_BINLOG_SUBDIR_NAME,
                REBUILD_BINLOG_SUBDIR_NAME_REPLAY);
        if (rename(replay_path, input_path) < 0) {
            result = (errno != 0 ? errno : EPERM);
            logError("file: "__FILE__", line: %d, "
                    "rename %s to %s fail, error info: %s", __LINE__,
                    replay_path, input_path, STRERROR(result));
            return result;
        }

        result = check_make_subdir(REBUILD_BINLOG_SUBDIR_NAME_REPLAY);
        if (result != 0) {
            return result;
        }
    }

    old_rebuild_threads = redo_ctx->rebuild_threads;
    new_rebuild_threads = DATA_REBUILD_THREADS;
    if ((rda.readers=fc_malloc(sizeof(ServerBinlogReader) *
                    old_rebuild_threads)) == NULL)
    {
        return ENOMEM;
    }

    end = rda.readers + old_rebuild_threads;
    for (reader=rda.readers; reader<end; reader++) {
        thread_index = reader - rda.readers;
        rebuild_binlog_get_subdir_name(redo_ctx->backup_subdir,
                thread_index, subdir_name, sizeof(subdir_name));
        if ((result=rebuild_binlog_reader_init(reader,
                        subdir_name)) != 0)
        {
            return result;
        }
    }

    rda.count = old_rebuild_threads;
    read_threads = FC_MIN(SYSTEM_CPU_COUNT, new_rebuild_threads);
    if ((result=binlog_spliter_do(&rda, read_threads,
                    new_rebuild_threads, &slice_count)) == 0)
    {
        redo_ctx->rebuild_threads = new_rebuild_threads;
        if ((result=write_to_redo_file(redo_ctx)) != 0) {
            return result;
        }

        for (thread_index=0; thread_index<old_rebuild_threads; thread_index++) {
            rebuild_binlog_get_subdir_name(redo_ctx->backup_subdir,
                    thread_index, subdir_name, sizeof(subdir_name));
            rebuild_binlog_reader_unlink_subdir(subdir_name);
        }
        fs_rmdir(input_path);

        logInfo("file: "__FILE__", line: %d, "
                "re-split slice binlog done, slice count: %"PRId64", "
                "time used: %s ms", __LINE__, slice_count,
                long_to_comma_str(get_current_time_ms() -
                    start_time, time_used));
    }

    for (reader=rda.readers; reader<end; reader++) {
        binlog_reader_destroy(reader);
    }
    free(rda.readers);

    return result;
}

static int unlink_dump_subdir(DataRebuildRedoContext *redo_ctx)
{
    int result;
    int binlog_index;
    char subdir_name[64];
    char filepath[PATH_MAX];
    char filename[PATH_MAX];

    snprintf(subdir_name, sizeof(subdir_name), "%s/%s",
            FS_REBUILD_BINLOG_SUBDIR_NAME,
            REBUILD_BINLOG_SUBDIR_NAME_DUMP);
    snprintf(filepath, sizeof(filepath), "%s/%s",
            DATA_PATH_STR, subdir_name);
    if (access(filepath, F_OK) != 0) {
        result = errno != 0 ? errno : EPERM;
        if (result == ENOENT) {
            return 0;
        }
        logError("file: "__FILE__", line: %d, access path %s fail, "
                "errno: %d, error info: %s", __LINE__, filepath,
                result, STRERROR(result));
        return result;
    }

    for (binlog_index = 0; binlog_index < redo_ctx->
            binlog_file_count; binlog_index++)
    {
        sf_binlog_writer_get_filename(DATA_PATH_STR, subdir_name,
                binlog_index, filename, sizeof(filename));
        if ((result=fc_delete_file_ex(filename, "binlog")) != 0) {
            return result;
        }
    }

    return fs_rmdir(filepath);
}


static int redo_prepare(DataRebuildRedoContext *redo_ctx)
{
    int result;

    get_slice_mark_filename(redo_ctx->redo_filename,
            sizeof(redo_ctx->redo_filename));
    if (access(redo_ctx->redo_filename, F_OK) != 0) {
        if (errno == ENOENT) {
            return ENOENT;
        }

        result = (errno != 0 ? errno : EPERM);
        logError("file: "__FILE__", line: %d, "
                "access slice mark file: %s fail, "
                "errno: %d, error info: %s", __LINE__,
                redo_ctx->redo_filename, result, STRERROR(result));
        return result;
    }

    return load_from_redo_file(redo_ctx);
}

int store_path_rebuild_redo_step1()
{
    DataRebuildRedoContext redo_ctx;
    int result;

    if ((result=redo_prepare(&redo_ctx)) != 0) {
        return (result == ENOENT ? 0 : result);
    }

    switch (redo_ctx.current_stage) {
        case DATA_REBUILD_REDO_STAGE_BACKUP_TRUNK:
            if ((result=backup_trunk_binlogs(&redo_ctx)) != 0) {
                return result;
            }

            redo_ctx.current_stage = DATA_REBUILD_REDO_STAGE_RENAME_TRUNK;
            if ((result=write_to_redo_file(&redo_ctx)) != 0) {
                return result;
            }
            //continue next stage
        case DATA_REBUILD_REDO_STAGE_RENAME_TRUNK:
            if ((result=rename_trunk_binlogs(&redo_ctx)) != 0) {
                return result;
            }

            redo_ctx.current_stage = (redo_ctx.binlog_file_count > 0 ?
                    DATA_REBUILD_REDO_STAGE_BACKUP_SLICE :
                    DATA_REBUILD_REDO_STAGE_CLEANUP);
            if ((result=write_to_redo_file(&redo_ctx)) != 0) {
                return result;
            }

            if (redo_ctx.binlog_file_count == 0) {
                break;
            }
            //continue next stage
        case DATA_REBUILD_REDO_STAGE_BACKUP_SLICE:
            if ((result=backup_slice_binlogs(&redo_ctx)) != 0) {
                return result;
            }

            redo_ctx.current_stage = DATA_REBUILD_REDO_STAGE_RENAME_SLICE;
            if ((result=write_to_redo_file(&redo_ctx)) != 0) {
                return result;
            }
            //continue next stage
        case DATA_REBUILD_REDO_STAGE_RENAME_SLICE:
            if ((result=rename_slice_binlogs(&redo_ctx)) != 0) {
                logError("file: "__FILE__", line: %d, rename slice binlogs "
                        "fail, errno: %d, error info: %s", __LINE__, result,
                        STRERROR(result));
                return result;
            }
            redo_ctx.current_stage = DATA_REBUILD_REDO_STAGE_PADDING_SLICE;
            if ((result=write_to_redo_file(&redo_ctx)) != 0) {
                return result;
            }
            //continue next stage
        case DATA_REBUILD_REDO_STAGE_PADDING_SLICE:
            if ((result=padding_slice_binlog(&redo_ctx)) != 0) {
                logError("file: "__FILE__", line: %d, padding slice binlog "
                        "fail, errno: %d, error info: %s", __LINE__, result,
                        STRERROR(result));
                return result;
            }

            if (STORAGE_ENABLED) {
                redo_ctx.current_stage = DATA_REBUILD_REDO_STAGE_REMOVE_DB;
            } else {
                redo_ctx.current_stage = DATA_REBUILD_REDO_STAGE_SPLIT_BINLOG;
            }
            result = write_to_redo_file(&redo_ctx);
            break;
        default:
            return 0;
    }

    return result;
}

int store_path_rebuild_redo_step2()
{
    DataRebuildRedoContext redo_ctx;
    char subdir_name[64];
    char filepath[PATH_MAX];
    int result;

    if ((result=redo_prepare(&redo_ctx)) != 0) {
        return (result == ENOENT ? 0 : result);
    }

    if (redo_ctx.current_stage == DATA_REBUILD_REDO_STAGE_REBUILDING &&
            DATA_REBUILD_THREADS != redo_ctx.rebuild_threads)
    {
        logInfo("file: "__FILE__", line: %d, "
                "rebuild_threads changed from %d to %d, "
                "re-split binlog", __LINE__, redo_ctx.
                rebuild_threads, DATA_REBUILD_THREADS);
        if ((result=resplit_binlog(&redo_ctx)) != 0) {
            return result;
        }
    }

    switch (redo_ctx.current_stage) {
        case DATA_REBUILD_REDO_STAGE_REMOVE_DB:
            if (!STORAGE_ENABLED) {
                logError("file: "__FILE__", line: %d, "
                        "can't change storage engine enabled to false "
                        "during data path rebuild!", __LINE__);
                return EINVAL;
            }
            snprintf(subdir_name, sizeof(subdir_name), "%s/%s",
                    FS_REBUILD_BINLOG_SUBDIR_NAME,
                    REBUILD_BINLOG_SUBDIR_NAME_DUMP);
            if ((result=db_remove_slices(subdir_name, redo_ctx.
                            binlog_file_count)) != 0)
            {
                return result;
            }

            redo_ctx.current_stage = DATA_REBUILD_REDO_STAGE_SPLIT_BINLOG;
            if ((result=write_to_redo_file(&redo_ctx)) != 0) {
                return result;
            }
            //continue next stage
        case DATA_REBUILD_REDO_STAGE_SPLIT_BINLOG:
            if ((result=split_binlog(&redo_ctx)) != 0) {
                return result;
            }
            redo_ctx.current_stage = DATA_REBUILD_REDO_STAGE_REBUILDING;
            if ((result=write_to_redo_file(&redo_ctx)) != 0) {
                return result;
            }
            //continue next stage
        case DATA_REBUILD_REDO_STAGE_REBUILDING:
            if ((result=unlink_dump_subdir(&redo_ctx)) != 0) {
                return result;
            }
            if ((result=rebuild_thread_do(DATA_REBUILD_THREADS)) != 0) {
                return result;
            }

            redo_ctx.current_stage = DATA_REBUILD_REDO_STAGE_CLEANUP;
            if ((result=write_to_redo_file(&redo_ctx)) != 0) {
                return result;
            }
            //continue next stage
        case DATA_REBUILD_REDO_STAGE_CLEANUP:
            if ((result=rebuild_cleanup(REBUILD_BINLOG_SUBDIR_NAME_REPLAY,
                            DATA_REBUILD_THREADS)) != 0)
            {
                return result;
            }
            break;
        default:
            logError("file: "__FILE__", line: %d, "
                    "unkown stage: %d", __LINE__,
                    redo_ctx.current_stage);
            return EINVAL;
    }

    if (redo_ctx.binlog_file_count > 0) {
        if ((result=slice_binlog_padding_for_check(
                        BINLOG_SOURCE_REBUILD)) != 0)
        {
            return result;
        }
    }

    if ((result=fc_delete_file_ex(redo_ctx.redo_filename,
                    "redo mark")) != 0)
    {
        return result;
    }

    snprintf(filepath, sizeof(filepath),
            "%s/%s/%s", DATA_PATH_STR,
            FS_REBUILD_BINLOG_SUBDIR_NAME,
            REBUILD_BINLOG_SUBDIR_NAME_REPLAY);
    return fs_rmdir(filepath);
}

static inline int dump_trunk_binlog()
{
    char filename[PATH_MAX];
    int64_t start_time;
    int64_t total_trunk_count;
    char time_used[32];
    int result;

    logInfo("file: "__FILE__", line: %d, "
            "begin dump trunk binlog ...", __LINE__);

    start_time = get_current_time_ms();
    get_trunk_dump_filename(filename, sizeof(filename));
    if ((result=da_trunk_hashtable_dump_to_file(&DA_CTX.trunk_htable_ctx,
                    filename, &total_trunk_count)) == 0)
    {
        logInfo("file: "__FILE__", line: %d, "
                "dump trunk binlog done, total trunk count: %"PRId64", "
                "time used: %s ms", __LINE__, total_trunk_count,
                long_to_comma_str(get_current_time_ms() -
                    start_time, time_used));
    }

    return result;
}

int store_path_rebuild_dump_data(const int64_t total_slice_count)
{
    int result;
    DataRebuildRedoContext redo_ctx;
    time_t current_time;
    struct tm tm_current;

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

    current_time = g_current_time;
    localtime_r(&current_time, &tm_current);
    strftime(redo_ctx.backup_subdir, sizeof(redo_ctx.backup_subdir),
            "%Y%m%d%H%M%S", &tm_current);

    if ((result=dump_trunk_binlog()) != 0) {
        return result;
    }

    if (total_slice_count > 0) {
        if ((result=slice_dump_to_files(get_slice_remove_filename,
                get_slice_dump_filename, BINLOG_SOURCE_REBUILD,
                total_slice_count, &redo_ctx.binlog_file_count)) != 0)
        {
            return result;
        }
    } else {
        redo_ctx.binlog_file_count = 0;
    }

    redo_ctx.last_sn = FC_ATOMIC_GET(SLICE_BINLOG_SN);
    redo_ctx.rebuild_threads = DATA_REBUILD_THREADS;
    redo_ctx.current_stage = DATA_REBUILD_REDO_STAGE_BACKUP_TRUNK;
    if ((result=write_to_redo_file(&redo_ctx)) != 0) {
        return result;
    }

    return store_path_rebuild_redo_step1();
}
