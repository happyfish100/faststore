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
#include "trunk_migrate.h"

typedef struct trunk_migrate_redo_context {
    char mark_filename[PATH_MAX];
    int64_t last_sn;
    int current_stage;
} TrunkMigrateRedoContext;

#define MIGRATE_SUBDIR_NAME   FS_TRUNK_BINLOG_SUBDIR_NAME"/migrate"

#define BINLOG_REDO_STAGE_DUMP_SLICE     1
#define BINLOG_REDO_STAGE_SPACE_LOG      2
#define BINLOG_REDO_STAGE_CLEANUP        3

#define BINLOG_REDO_ITEM_LAST_SN        "last_sn"
#define BINLOG_REDO_ITEM_CURRENT_STAGE  "current_stage"

static inline int check_make_subdir()
{
    char migrate_path[PATH_MAX];
    snprintf(migrate_path, sizeof(migrate_path), "%s/%s",
            DATA_PATH_STR, MIGRATE_SUBDIR_NAME);
    return fc_check_mkdir(migrate_path, 0755);
}

static inline const char *get_trunk_migrate_mark_filename(
        char *filename, const int size)
{
    snprintf(filename, size, "%s/%s/.migrate.flag",
            DATA_PATH_STR, MIGRATE_SUBDIR_NAME);
    return filename;
}

static int write_to_redo_file(TrunkMigrateRedoContext *redo_ctx)
{
    char buff[256];
    int result;
    int len;

    len = sprintf(buff, "%s=%"PRId64"\n"
            "%s=%d\n",
            BINLOG_REDO_ITEM_LAST_SN, redo_ctx->last_sn,
            BINLOG_REDO_ITEM_CURRENT_STAGE, redo_ctx->current_stage);
    if ((result=safeWriteToFile(redo_ctx->mark_filename, buff, len)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "write to file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, redo_ctx->mark_filename, result, STRERROR(result));
    }

    return result;
}

static int load_from_redo_file(TrunkMigrateRedoContext *redo_ctx)
{
    IniContext ini_context;
    int result;

    if ((result=iniLoadFromFile(redo_ctx->mark_filename,
                    &ini_context)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "load from file \"%s\" fail, error code: %d",
                __LINE__, redo_ctx->mark_filename, result);
        return result;
    }

    redo_ctx->last_sn = iniGetInt64Value(NULL,
            BINLOG_REDO_ITEM_LAST_SN, &ini_context, 0);
    redo_ctx->current_stage = iniGetIntValue(NULL,
            BINLOG_REDO_ITEM_CURRENT_STAGE, &ini_context, 0);
    iniFreeContext(&ini_context);
    return 0;
}

static int redo(TrunkMigrateRedoContext *redo_ctx)
{
    int result;

    switch (redo_ctx->current_stage) {
        case BINLOG_REDO_STAGE_DUMP_SLICE:

            redo_ctx->current_stage = BINLOG_REDO_STAGE_SPACE_LOG;
            if ((result=write_to_redo_file(redo_ctx)) != 0) {
                return result;
            }
            //continue next stage
        case BINLOG_REDO_STAGE_SPACE_LOG:
            redo_ctx->current_stage = BINLOG_REDO_STAGE_CLEANUP;
            if ((result=write_to_redo_file(redo_ctx)) != 0) {
                return result;
            }
            //continue next stage
        case BINLOG_REDO_STAGE_CLEANUP:
            break;
        default:
            logError("file: "__FILE__", line: %d, "
                    "unkown stage: %d", __LINE__,
                    redo_ctx->current_stage);
            return EINVAL;
    }

    return fc_delete_file_ex(redo_ctx->mark_filename, "redo mark");
}

int trunk_migrate_redo()
{
    int result;
    TrunkMigrateRedoContext redo_ctx;

    get_trunk_migrate_mark_filename(redo_ctx.mark_filename,
            sizeof(redo_ctx.mark_filename));
    if (access(redo_ctx.mark_filename, F_OK) != 0) {
        if (errno == ENOENT) {
            return 0;
        }

        result = (errno != 0 ? errno : EPERM);
        logError("file: "__FILE__", line: %d, "
                "access slice mark file: %s fail, "
                "errno: %d, error info: %s", __LINE__,
                redo_ctx.mark_filename, result, STRERROR(result));
        return result;
    }

    if ((result=load_from_redo_file(&redo_ctx)) != 0) {
        return result;
    }

    return redo(&redo_ctx);
}

int trunk_migrate_create()
{
    int result;
    TrunkMigrateRedoContext redo_ctx;

    if ((result=check_make_subdir()) != 0) {
        return result;
    }

    redo_ctx.last_sn = 0;
    redo_ctx.current_stage = BINLOG_REDO_STAGE_DUMP_SLICE;
    get_trunk_migrate_mark_filename(redo_ctx.mark_filename,
            sizeof(redo_ctx.mark_filename));
    if ((result=write_to_redo_file(&redo_ctx)) != 0) {
        return result;
    }

    return 0;
}
