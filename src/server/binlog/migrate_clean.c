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
#include "../server_func.h"
#include "../server_group_info.h"
#include "../shared_thread_pool.h"
#include "../storage/object_block_index.h"
#include "slice_binlog.h"
#include "replica_binlog.h"
#include "slice_dump.h"
#include "db_remove.h"
#include "slice_space_migrate.h"
#include "migrate_clean.h"

typedef struct binlog_clean_redo_context {
    char redo_filename[PATH_MAX];
    char backup_subdir[NAME_MAX];
    int binlog_file_count;
    int current_stage;
    int64_t last_sn;
} BinlogCleanRedoContext;

#define MIGRATE_SUBDIR_NAME              "migrate"
#define MIGRATE_BINLOG_SUBDIR_NAME_DUMP  "dbremove"
#define MIGRATE_BINLOG_SUBDIR_NAME_SPACE "space"
#define MIGRATE_DUMP_SUBDIR_FULLNAME     MIGRATE_SUBDIR_NAME"/" \
    MIGRATE_BINLOG_SUBDIR_NAME_DUMP
#define MIGRATE_SPACE_SUBDIR_FULLNAME    MIGRATE_SUBDIR_NAME"/" \
    MIGRATE_BINLOG_SUBDIR_NAME_SPACE
#define BACKUP_SUBDIR_NAME_STR          "bak"
#define BACKUP_SUBDIR_NAME_LEN  (sizeof(BACKUP_SUBDIR_NAME_STR) - 1)

#define MIGRATE_REDO_STAGE_BACKUP_SLICE            1
#define MIGRATE_REDO_STAGE_RENAME_SLICE            2
#define MIGRATE_REDO_STAGE_PADDING_SLICE           3
#define MIGRATE_REDO_STAGE_RECLAIM_SPACE_PREPARE   4  //reclaim slice spaces
#define MIGRATE_REDO_STAGE_RECLAIM_SPACE_REDO      5  //reclaim slice spaces
#define MIGRATE_REDO_STAGE_REMOVE_DB               6  //for storage engine only
#define MIGRATE_REDO_STAGE_REMOVE_REPLICA          7
#define MIGRATE_REDO_STAGE_CLEANUP                 8

#define MIGRATE_REDO_ITEM_BINLOG_COUNT   "binlog_file_count"
#define MIGRATE_REDO_ITEM_CURRENT_STAGE  "current_stage"
#define MIGRATE_REDO_ITEM_BACKUP_SUBDIR  "backup_subdir"
#define MIGRATE_REDO_ITEM_LAST_SN        "last_sn"

static const char *get_slice_remove_filename(const int binlog_index,
        char *filename, const int size)
{
    return sf_binlog_writer_get_filename(DATA_PATH_STR,
            MIGRATE_DUMP_SUBDIR_FULLNAME,
            binlog_index, filename, size);
}

static const char *get_slice_dump_filename(const
        int binlog_index, char *filename, const int size)
{
    snprintf(filename, size, "%s/%s/slice-%03d.dmp", DATA_PATH_STR,
            MIGRATE_SUBDIR_NAME, binlog_index);
    return filename;
}

static inline int check_make_subdirs()
{
    int result;
    char migrage_path[PATH_MAX];
    char subdir_path[PATH_MAX];

    snprintf(migrage_path, sizeof(migrage_path), "%s/%s",
            DATA_PATH_STR, MIGRATE_SUBDIR_NAME);
    if ((result=fc_check_mkdir(migrage_path, 0755)) != 0) {
        return result;
    }

    if (STORAGE_ENABLED) {
        snprintf(subdir_path, sizeof(subdir_path), "%s/%s", migrage_path,
                MIGRATE_BINLOG_SUBDIR_NAME_DUMP);
        if ((result=fc_check_mkdir(subdir_path, 0755)) != 0) {
            return result;
        }
    }

    snprintf(subdir_path, sizeof(subdir_path), "%s/%s", migrage_path,
            MIGRATE_BINLOG_SUBDIR_NAME_SPACE);
    return fc_check_mkdir(subdir_path, 0755);
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
            "%s=%"PRId64"\n"
            "%s=%s\n",
            MIGRATE_REDO_ITEM_BINLOG_COUNT, redo_ctx->binlog_file_count,
            MIGRATE_REDO_ITEM_CURRENT_STAGE, redo_ctx->current_stage,
            MIGRATE_REDO_ITEM_LAST_SN, redo_ctx->last_sn,
            MIGRATE_REDO_ITEM_BACKUP_SUBDIR, redo_ctx->backup_subdir);
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
            MIGRATE_REDO_ITEM_BINLOG_COUNT, &ini_context, 0);
    redo_ctx->current_stage = iniGetIntValue(NULL,
            MIGRATE_REDO_ITEM_CURRENT_STAGE, &ini_context, 0);
    redo_ctx->last_sn = iniGetInt64Value(NULL,
            MIGRATE_REDO_ITEM_LAST_SN, &ini_context, 0);
    backup_subdir = iniGetStrValue(NULL,
            MIGRATE_REDO_ITEM_BACKUP_SUBDIR,
            &ini_context);
    if (backup_subdir == NULL || *backup_subdir == '\0') {
        logError("file: "__FILE__", line: %d, "
                "redo file: %s, item: %s not exist",
                __LINE__, redo_ctx->redo_filename,
                MIGRATE_REDO_ITEM_BACKUP_SUBDIR);
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

    if ((result=slice_binlog_set_binlog_start_index(0)) != 0) {
        return result;
    }
    return slice_binlog_set_binlog_write_index(last_index);
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
                "replica backup path is too long", __LINE__);
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

static int padding_slice_binlog(BinlogCleanRedoContext *redo_ctx)
{
    int result;

    if (redo_ctx->last_sn > FC_ATOMIC_GET(SLICE_BINLOG_SN)) {
        if ((result=slice_binlog_set_sn(redo_ctx->last_sn - 1)) != 0) {
            return result;
        }
    }

    return slice_binlog_padding_one(BINLOG_SOURCE_MIGRATE_CLEAN);
}

static int unlink_migrate_subdir(BinlogCleanRedoContext *redo_ctx,
        const char *subdir_name, const int binlog_file_count)
{
    int result;
    int binlog_index;
    char filepath[PATH_MAX];
    char filename[PATH_MAX];

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

    for (binlog_index=0; binlog_index<binlog_file_count; binlog_index++) {
        sf_binlog_writer_get_filename(DATA_PATH_STR, subdir_name,
                binlog_index, filename, sizeof(filename));
        if ((result=fc_delete_file_ex(filename, "binlog")) != 0) {
            return result;
        }
    }

    return fs_rmdir(filepath);
}

static int cleanup(BinlogCleanRedoContext *redo_ctx)
{
    int result;
    char migrate_path[PATH_MAX];

    if (STORAGE_ENABLED) {
        if ((result=unlink_migrate_subdir(redo_ctx,
                        MIGRATE_DUMP_SUBDIR_FULLNAME,
                        redo_ctx->binlog_file_count)) != 0)
        {
            return result;
        }
    }

    if ((result=unlink_migrate_subdir(redo_ctx,
                    MIGRATE_SPACE_SUBDIR_FULLNAME, 1)) != 0)
    {
        return result;
    }

    if ((result=fc_delete_file_ex(redo_ctx->redo_filename,
                    "redo mark")) != 0)
    {
        return result;
    }

    snprintf(migrate_path, sizeof(migrate_path), "%s/%s",
            DATA_PATH_STR, MIGRATE_SUBDIR_NAME);
    return fs_rmdir(migrate_path);
}

static int reclaim_space_prepare(BinlogCleanRedoContext *redo_ctx)
{
    const int binlog_index = 0;
    const bool dump_slice = false;
    int result;
    char filename[PATH_MAX];
    int64_t slice_count;
    int64_t start_time_ms;
    char time_used[32];

    logInfo("file: "__FILE__", line: %d, "
            "dump migrated slices to file ...", __LINE__);
    start_time_ms = get_current_time_ms();
    sf_binlog_writer_get_filename(DATA_PATH_STR,
            MIGRATE_SPACE_SUBDIR_FULLNAME, binlog_index,
            filename, sizeof(filename));
    if ((result=ob_index_remove_slices_to_file_for_reclaim(
                    filename, &slice_count)) != 0)
    {
        return result;
    }

    long_to_comma_str(get_current_time_ms() - start_time_ms, time_used);
    logInfo("file: "__FILE__", line: %d, "
            "dump migrated slices to file done, slice count: %"PRId64", "
            "time_used: %s ms", __LINE__, slice_count, time_used);

    return slice_space_migrate_create(MIGRATE_SPACE_SUBDIR_FULLNAME,
            binlog_index, dump_slice, da_binlog_op_type_reclaim_space,
            FS_SLICE_BINLOG_IN_CURRENT_SUBDIR);
}

static int redo(BinlogCleanRedoContext *redo_ctx)
{
    int result;
    bool need_restart;
    FSStorageSNType old_sn_type;
    int write_index;

    switch (redo_ctx->current_stage) {
        case MIGRATE_REDO_STAGE_BACKUP_SLICE:
            if ((result=backup_slice_binlogs(redo_ctx)) != 0) {
                return result;
            }

            redo_ctx->current_stage = MIGRATE_REDO_STAGE_RENAME_SLICE;
            if ((result=write_to_redo_file(redo_ctx)) != 0) {
                return result;
            }
            //continue next stage
        case MIGRATE_REDO_STAGE_RENAME_SLICE:
            if ((result=rename_slice_binlogs(redo_ctx)) != 0) {
                return result;
            }
            redo_ctx->current_stage = MIGRATE_REDO_STAGE_PADDING_SLICE;
            if ((result=write_to_redo_file(redo_ctx)) != 0) {
                return result;
            }
            //continue next stage
        case MIGRATE_REDO_STAGE_PADDING_SLICE:
            if ((result=padding_slice_binlog(redo_ctx)) != 0) {
                logError("file: "__FILE__", line: %d, padding slice binlog "
                        "fail, errno: %d, error info: %s", __LINE__, result,
                        STRERROR(result));
                return result;
            }
            redo_ctx->current_stage = MIGRATE_REDO_STAGE_RECLAIM_SPACE_PREPARE;
            if ((result=write_to_redo_file(redo_ctx)) != 0) {
                return result;
            }
            //continue next stage
        case MIGRATE_REDO_STAGE_RECLAIM_SPACE_PREPARE:
            if ((result=reclaim_space_prepare(redo_ctx)) != 0) {
                return result;
            }
            redo_ctx->current_stage = MIGRATE_REDO_STAGE_RECLAIM_SPACE_REDO;
            if ((result=write_to_redo_file(redo_ctx)) != 0) {
                return result;
            }
            //continue next stage
        case MIGRATE_REDO_STAGE_RECLAIM_SPACE_REDO:
            result = slice_space_migrate_redo(MIGRATE_SPACE_SUBDIR_FULLNAME,
                    &need_restart);
            if (result != 0) {
                return result;
            }
            if (STORAGE_ENABLED) {
                redo_ctx->current_stage = MIGRATE_REDO_STAGE_REMOVE_DB;
            } else {
                redo_ctx->current_stage = MIGRATE_REDO_STAGE_REMOVE_REPLICA;
            }
            if ((result=write_to_redo_file(redo_ctx)) != 0) {
                return result;
            }

            if (need_restart) {
                fs_server_restart("slice space migrate done for migrate clean");
                return EINTR;
            }
        case MIGRATE_REDO_STAGE_REMOVE_DB:
        case MIGRATE_REDO_STAGE_REMOVE_REPLICA:
        case MIGRATE_REDO_STAGE_CLEANUP:
            break;
        default:
            logError("file: "__FILE__", line: %d, "
                    "unkown stage: %d", __LINE__,
                    redo_ctx->current_stage);
            return EINVAL;
    }

    switch (redo_ctx->current_stage) {
        case MIGRATE_REDO_STAGE_REMOVE_DB:
            if (!STORAGE_ENABLED) {
                logError("file: "__FILE__", line: %d, "
                        "can't change storage engine enabled to false "
                        "during migrate clean!", __LINE__);
                return EINVAL;
            }

            old_sn_type = STORAGE_SN_TYPE;
            STORAGE_SN_TYPE = fs_sn_type_block_removing;
            write_index = redo_ctx->binlog_file_count - 1;
            if ((result=db_remove_slices(MIGRATE_DUMP_SUBDIR_FULLNAME,
                            write_index)) != 0)
            {
                return result;
            }
            STORAGE_SN_TYPE = old_sn_type;

            redo_ctx->current_stage = MIGRATE_REDO_STAGE_REMOVE_REPLICA;
            if ((result=write_to_redo_file(redo_ctx)) != 0) {
                return result;
            }
            //continue next stage
        case MIGRATE_REDO_STAGE_REMOVE_REPLICA:
            if ((result=backup_replica_binlogs(redo_ctx)) != 0) {
                return result;
            }
            redo_ctx->current_stage = MIGRATE_REDO_STAGE_CLEANUP;
            if ((result=write_to_redo_file(redo_ctx)) != 0) {
                return result;
            }
            //continue next stage
        case MIGRATE_REDO_STAGE_CLEANUP:
            if ((result=cleanup(redo_ctx)) != 0) {
                return result;
            }
            break;
    }

    return 0;
}

int migrate_clean_redo()
{
    int result;
    BinlogCleanRedoContext redo_ctx;

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
    time_t current_time;
    struct tm tm_current;

    if ((result=check_make_subdirs()) != 0) {
        return result;
    }

    if (dump_slice_index) {
        if ((result=slice_dump_to_files((STORAGE_ENABLED ?
                            get_slice_remove_filename : NULL),
                        get_slice_dump_filename, BINLOG_SOURCE_MIGRATE_CLEAN,
                        total_slice_count, &redo_ctx.binlog_file_count)) != 0)
        {
            return result;
        }
        redo_ctx.current_stage = MIGRATE_REDO_STAGE_BACKUP_SLICE;
    } else {
        redo_ctx.binlog_file_count = 0;
        redo_ctx.current_stage = MIGRATE_REDO_STAGE_REMOVE_REPLICA;
    }

    current_time = g_current_time;
    localtime_r(&current_time, &tm_current);
    strftime(redo_ctx.backup_subdir, sizeof(redo_ctx.backup_subdir),
            "%Y%m%d%H%M%S", &tm_current);

    redo_ctx.last_sn = FC_ATOMIC_GET(SLICE_BINLOG_SN);
    get_slice_mark_filename(redo_ctx.redo_filename,
            sizeof(redo_ctx.redo_filename));
    if ((result=write_to_redo_file(&redo_ctx)) != 0) {
        return result;
    }

    return redo(&redo_ctx);
}
