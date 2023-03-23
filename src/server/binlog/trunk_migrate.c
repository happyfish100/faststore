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
#include "diskallocator/binlog/trunk/trunk_space_log.h"
#include "../../common/fs_func.h"
#include "../server_global.h"
#include "../server_group_info.h"
#include "../shared_thread_pool.h"
#include "../storage/object_block_index.h"
#include "slice_binlog.h"
#include "slice_dedup.h"
#include "trunk_migrate.h"

typedef struct trunk_migrate_context {
    char mark_filename[PATH_MAX];
    int64_t last_sn;
    int current_stage;
    int record_count;
    BufferInfo buffer;
    FilenameFDPair fpair;
    struct fc_queue_info space_chain;
} TrunkMigrateContext;

#define MIGRATE_SUBDIR_NAME   FS_TRUNK_BINLOG_SUBDIR_NAME"/migrate"
#define MIGRATE_BUFFER_SIZE   (256 * 1024)

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

static int write_to_mark_file(TrunkMigrateContext *ctx)
{
    char buff[256];
    int result;
    int len;

    len = sprintf(buff, "%s=%"PRId64"\n%s=%d\n",
            BINLOG_REDO_ITEM_LAST_SN, ctx->last_sn,
            BINLOG_REDO_ITEM_CURRENT_STAGE, ctx->current_stage);
    if ((result=safeWriteToFile(ctx->mark_filename, buff, len)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "write to file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, ctx->mark_filename, result, STRERROR(result));
    }

    return result;
}

static int load_from_redo_file(TrunkMigrateContext *ctx)
{
    IniContext ini_context;
    int result;

    if ((result=iniLoadFromFile(ctx->mark_filename,
                    &ini_context)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "load from file \"%s\" fail, error code: %d",
                __LINE__, ctx->mark_filename, result);
        return result;
    }

    ctx->last_sn = iniGetInt64Value(NULL,
            BINLOG_REDO_ITEM_LAST_SN, &ini_context, 0);
    ctx->current_stage = iniGetIntValue(NULL,
            BINLOG_REDO_ITEM_CURRENT_STAGE, &ini_context, 0);
    iniFreeContext(&ini_context);
    return 0;
}

static int parse_to_chain(TrunkMigrateContext *ctx, char *error_info)
{
    int result;
    string_t line;
    char *line_start;
    char *buff_end;
    char *line_end;
    SliceBinlogRecord r;
    DATrunkSpaceLogRecord *record;

    result = 0;
    line_start = ctx->buffer.buff;
    buff_end = ctx->buffer.buff + ctx->buffer.length;
    while (line_start < buff_end) {
        line_end = (char *)memchr(line_start, '\n', buff_end - line_start);
        if (line_end == NULL) {
            break;
        }

        ++line_end;
        line.str = line_start;
        line.len = line_end - line_start;
        if ((result=slice_binlog_record_unpack(&line, &r, error_info)) != 0) {
            return result;
        }

        if (r.slice_type == DA_SLICE_TYPE_FILE) {
            if ((record=da_trunk_space_log_alloc_record(&DA_CTX)) == NULL) {
                sprintf(error_info, "alloc record object fail "
                        "because out of memory");
                return ENOMEM;
            }

            record->oid = r.bs_key.block.oid;
            record->fid = r.bs_key.block.offset;
            record->extra = r.bs_key.slice.offset;
            record->op_type = r.slice_type;
            record->storage.version = r.data_version;
            record->storage.trunk_id = r.space.id_info.id;
            record->storage.length = r.bs_key.slice.length;  //data length
            record->storage.offset = r.space.offset;
            record->storage.size = r.space.size;
            DA_SPACE_LOG_ADD_TO_CHAIN(&ctx->space_chain, record);
            ctx->record_count++;
            ctx->last_sn = r.sn;
        }

        line_start = line_end;
    }

    return 0;
}

static int load_slice_to_space_chain(TrunkMigrateContext *ctx)
{
    int result;
    char error_info[256];

    result = 0;
    *error_info = '\0';
    if ((ctx->buffer.length=fc_read_lines(ctx->fpair.fd, ctx->
                    buffer.buff, ctx->buffer.alloc_size)) > 0)
    {
        if ((result=parse_to_chain(ctx, error_info)) != 0) {
            logError("file: "__FILE__", line: %d, "
                    "parse file: %s fail, errno: %d, error info: %s",
                    __LINE__, ctx->fpair.filename, result, error_info);
            return result;
        }
    }

    if (ctx->buffer.length < 0) {
        result = errno != 0 ? errno : EACCES;
        logError("file: "__FILE__", line: %d, "
                "read from file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, ctx->fpair.filename, result, STRERROR(result));
        return result;
    }

    return 0;
}

static int find_last_sn(TrunkMigrateContext *ctx, char *error_info)
{
    int result;
    int remain;
    string_t line;
    char *line_start;
    char *buff_end;
    char *line_end;
    SliceBinlogRecord r;

    result = 0;
    line_start = ctx->buffer.buff;
    buff_end = ctx->buffer.buff + ctx->buffer.length;
    while (line_start < buff_end) {
        line_end = (char *)memchr(line_start, '\n', buff_end - line_start);
        if (line_end == NULL) {
            break;
        }

        ++line_end;
        line.str = line_start;
        line.len = line_end - line_start;
        if ((result=slice_binlog_record_unpack(&line, &r, error_info)) != 0) {
            return result;
        }

        if (r.sn >= ctx->last_sn) {
            if (r.sn == ctx->last_sn) {
                remain = buff_end - line_end;
            } else {
                remain = buff_end - line_start;
            }
            if (remain > 0) {
                if (lseek(ctx->fpair.fd, -1 * remain, SEEK_CUR) < 0) {
                    result = errno != 0 ? errno : EIO;
                    sprintf(error_info, "lseek fail, %s", STRERROR(result));
                    return result;
                }
            }
            return 0;
        }

        line_start = line_end;
    }

    return EAGAIN;
}

static int skip_to_last_sn(TrunkMigrateContext *ctx)
{
    int result;
    char error_info[256];

    result = 0;
    *error_info = '\0';
    while ((ctx->buffer.length=fc_read_lines(ctx->fpair.fd, ctx->
                    buffer.buff, ctx->buffer.alloc_size)) > 0)
    {
        result = find_last_sn(ctx, error_info);
        if (result == 0) {
            return 0;
        } else if (result != EAGAIN) {
            logError("file: "__FILE__", line: %d, "
                    "parse file: %s fail, errno: %d, error info: %s",
                    __LINE__, ctx->fpair.filename, result, error_info);
            return result;
        }
    }

    if (ctx->buffer.length < 0) {
        result = errno != 0 ? errno : EACCES;
        logError("file: "__FILE__", line: %d, "
                "read from file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, ctx->fpair.filename, result, STRERROR(result));
        return result;
    }

    return ENOENT;
}

static int open_slice_binlog(TrunkMigrateContext *ctx)
{
    int result;
    int binlog_index;

    binlog_index = slice_binlog_get_binlog_start_index();
    slice_binlog_get_filename(binlog_index, ctx->fpair.
            filename, sizeof(ctx->fpair.filename));
    if ((ctx->fpair.fd=open(ctx->fpair.filename,
                    O_RDONLY | O_CLOEXEC)) < 0)
    {
        result = errno != 0 ? errno : EACCES;
        logError("file: "__FILE__", line: %d, "
                "open file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, ctx->fpair.filename,
                result, STRERROR(result));
        return result;
    }

    if (ctx->last_sn == 0) {
        return 0;
    } else {
        return skip_to_last_sn(ctx);
    }
}

static int do_migrate(TrunkMigrateContext *ctx)
{
    int result;

    while (SF_G_CONTINUE_FLAG) {
        if ((result=write_to_mark_file(ctx)) != 0) {
            return result;
        }

        ctx->record_count = 0;
        ctx->space_chain.head = ctx->space_chain.tail = NULL;
        if ((result=load_slice_to_space_chain(ctx)) != 0) {
            return result;
        }

        if (ctx->space_chain.head == NULL) {
            return 0;
        }

        da_trunk_space_log_inc_waiting_count(&DA_CTX, ctx->record_count);
        da_trunk_space_log_push_chain(&DA_CTX, &ctx->space_chain);
        da_trunk_space_log_wait(&DA_CTX);
    }

    return EINTR;
}

static int migrate_space_log(TrunkMigrateContext *ctx)
{
    int result;

    if ((result=fc_init_buffer(&ctx->buffer, MIGRATE_BUFFER_SIZE)) != 0) {
        return result;
    }

    if ((result=open_slice_binlog(ctx)) != 0) {
        return result;
    }

    if (ctx->last_sn > 0) {
        ctx->record_count = 0;
        ctx->space_chain.head = ctx->space_chain.tail = NULL;
        if ((result=load_slice_to_space_chain(ctx)) != 0) {
            return result;
        }
        if (ctx->space_chain.head != NULL) {
            if ((result=da_trunk_space_log_redo_by_chain(&DA_CTX,
                            &ctx->space_chain)) != 0)
            {
                return result;
            }
        }
    }

    if ((result=do_migrate(ctx)) != 0) {
        return result;
    }
    close(ctx->fpair.fd);
    fc_free_buffer(&ctx->buffer);

    ctx->current_stage = BINLOG_REDO_STAGE_CLEANUP;
    if ((result=write_to_mark_file(ctx)) != 0) {
        return result;
    }

    return 0;
}

static int redo(TrunkMigrateContext *ctx)
{
    int result;

    switch (ctx->current_stage) {
        case BINLOG_REDO_STAGE_DUMP_SLICE:
            if ((result=slice_dedup_binlog()) != 0) {
                break;
            }
            //continue next stage
        case BINLOG_REDO_STAGE_SPACE_LOG:
            if ((result=migrate_space_log(ctx)) != 0) {
                break;
            }
            //continue next stage
        case BINLOG_REDO_STAGE_CLEANUP:
            break;
        default:
            logError("file: "__FILE__", line: %d, "
                    "unkown stage: %d", __LINE__,
                    ctx->current_stage);
            return EINVAL;
    }

    return fc_delete_file_ex(ctx->mark_filename, "redo mark");
}

int trunk_migrate_redo()
{
    int result;
    TrunkMigrateContext ctx;

    get_trunk_migrate_mark_filename(ctx.mark_filename,
            sizeof(ctx.mark_filename));
    if (access(ctx.mark_filename, F_OK) != 0) {
        if (errno == ENOENT) {
            return 0;
        }

        result = (errno != 0 ? errno : EPERM);
        logError("file: "__FILE__", line: %d, "
                "access slice mark file: %s fail, "
                "errno: %d, error info: %s", __LINE__,
                ctx.mark_filename, result, STRERROR(result));
        return result;
    }

    if ((result=load_from_redo_file(&ctx)) != 0) {
        return result;
    }

    return redo(&ctx);
}

int trunk_migrate_create()
{
    int result;
    TrunkMigrateContext ctx;

    if ((result=check_make_subdir()) != 0) {
        return result;
    }

    ctx.last_sn = 0;
    ctx.current_stage = BINLOG_REDO_STAGE_DUMP_SLICE;
    get_trunk_migrate_mark_filename(ctx.mark_filename,
            sizeof(ctx.mark_filename));
    if ((result=write_to_mark_file(&ctx)) != 0) {
        return result;
    }

    return 0;
}

int trunk_migrate_slice_dedup_done_callback()
{
    TrunkMigrateContext ctx;

    ctx.last_sn = 0;
    ctx.current_stage = BINLOG_REDO_STAGE_SPACE_LOG;
    get_trunk_migrate_mark_filename(ctx.mark_filename,
            sizeof(ctx.mark_filename));
    return write_to_mark_file(&ctx);
}
