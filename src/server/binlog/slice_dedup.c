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
#include "fastcommon/fc_atomic.h"
#include "../../common/fs_func.h"
#include "slice_binlog.h"
#include "slice_dedup.h"

#define DEDUP_SUBDIR_NAME    "dedup"

#define DEDUP_REDO_ITEM_SLICE_COUNT        "slice_count"
#define DEDUP_REDO_ITEM_BINLOG_START       "binlog_start"
#define DEDUP_REDO_ITEM_BINLOG_INDEX       "binlog_index"
#define DEDUP_REDO_ITEM_CURRENT_STAGE      "current_stage"

#define DEDUP_REDO_STAGE_RENAME_BINLOG    1
#define DEDUP_REDO_STAGE_REMOVE_BINLOG    2
#define DEDUP_REDO_STAGE_FINISH           3

typedef struct slice_binlog_dedup_redo_context {
    char mark_filename[PATH_MAX];
    int binlog_start;
    int binlog_index;
    int current_stage;
    int64_t slice_count;
} SliceBinlogDedupRedoContext;

static inline const char *get_slice_dedup_filename(
        char *filename, const int size)
{
    snprintf(filename, size, "%s/%s/%s/slice.dat", DATA_PATH_STR,
            FS_SLICE_BINLOG_SUBDIR_NAME, DEDUP_SUBDIR_NAME);
    return filename;
}

static inline int check_make_subdir()
{
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/%s", DATA_PATH_STR,
            FS_SLICE_BINLOG_SUBDIR_NAME, DEDUP_SUBDIR_NAME);
    return fc_check_mkdir(path, 0755);
}

static inline const char *get_slice_mark_filename(
        char *filename, const int size)
{
    snprintf(filename, size, "%s/%s/%s/.dedup.flag", DATA_PATH_STR,
            FS_SLICE_BINLOG_SUBDIR_NAME, DEDUP_SUBDIR_NAME);
    return filename;
}

static int write_to_redo_file(SliceBinlogDedupRedoContext *redo_ctx)
{
    char buff[256];
    int result;
    int len;

    len = sprintf(buff, "%s=%d\n"
            "%s=%d\n"
            "%s=%d\n"
            "%s=%"PRId64"\n",
            DEDUP_REDO_ITEM_BINLOG_START, redo_ctx->binlog_start,
            DEDUP_REDO_ITEM_BINLOG_INDEX, redo_ctx->binlog_index,
            DEDUP_REDO_ITEM_CURRENT_STAGE, redo_ctx->current_stage,
            DEDUP_REDO_ITEM_SLICE_COUNT, redo_ctx->slice_count);
    if ((result=safeWriteToFile(redo_ctx->mark_filename, buff, len)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "write to file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, redo_ctx->mark_filename, result, STRERROR(result));
    }

    return result;
}

static int load_from_redo_file(SliceBinlogDedupRedoContext *redo_ctx)
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

    redo_ctx->binlog_start = iniGetIntValue(NULL,
            DEDUP_REDO_ITEM_BINLOG_START, &ini_context, 0);
    redo_ctx->binlog_index = iniGetIntValue(NULL,
            DEDUP_REDO_ITEM_BINLOG_INDEX, &ini_context, 0);
    redo_ctx->current_stage = iniGetIntValue(NULL,
            DEDUP_REDO_ITEM_CURRENT_STAGE, &ini_context, 0);
    redo_ctx->slice_count = iniGetInt64Value(NULL,
            DEDUP_REDO_ITEM_SLICE_COUNT, &ini_context, 0);

    iniFreeContext(&ini_context);
    return 0;
}

static int dedup_rename(SliceBinlogDedupRedoContext *redo_ctx)
{
    char dedup_filename[PATH_MAX];
    char binlog_filename[PATH_MAX];

    if (redo_ctx->slice_count == 0) {
        return 0;
    }

    get_slice_dedup_filename(dedup_filename, sizeof(dedup_filename));
    slice_binlog_get_filename(redo_ctx->binlog_index, binlog_filename,
            sizeof(binlog_filename));
    return fc_check_rename(dedup_filename, binlog_filename);
}

static int remove_binlog_files(SliceBinlogDedupRedoContext *redo_ctx)
{
    int result;
    int end_index;
    int binlog_index;
    char binlog_filename[PATH_MAX];

    if (redo_ctx->slice_count > 0) {
        end_index = redo_ctx->binlog_index;
    } else {
        end_index = redo_ctx->binlog_index + 1;
    }

    for (binlog_index=redo_ctx->binlog_start;
            binlog_index<end_index; binlog_index++)
    {
        slice_binlog_get_filename(binlog_index, binlog_filename,
                sizeof(binlog_filename));
        if ((result=fc_delete_file_ex(binlog_filename,
                        "slice binlog")) != 0)
        {
            return result;
        }
    }

    if ((result=slice_binlog_set_binlog_start_index(end_index)) != 0) {
        return result;
    }

    return 0;
}

static int dedup_finish(SliceBinlogDedupRedoContext *redo_ctx)
{
    const int source = BINLOG_SOURCE_DUMP;
    int result;

    if ((result=slice_binlog_padding_for_check(source)) != 0) {
        return result;
    }

    return fc_delete_file_ex(redo_ctx->mark_filename, "slice dedup mark");
}

static int redo(SliceBinlogDedupRedoContext *redo_ctx)
{
    int result;

    switch (redo_ctx->current_stage) {
        case DEDUP_REDO_STAGE_RENAME_BINLOG:
            if ((result=dedup_rename(redo_ctx)) != 0) {
                break;
            }

            redo_ctx->current_stage = DEDUP_REDO_STAGE_REMOVE_BINLOG;
            if ((result=write_to_redo_file(redo_ctx)) != 0) {
                break;
            }
            //continue
        case DEDUP_REDO_STAGE_REMOVE_BINLOG:
            if ((result=remove_binlog_files(redo_ctx)) != 0) {
                break;
            }

            redo_ctx->current_stage = DEDUP_REDO_STAGE_FINISH;
            if ((result=write_to_redo_file(redo_ctx)) != 0) {
                break;
            }
            //continue
        case DEDUP_REDO_STAGE_FINISH:
            result = dedup_finish(redo_ctx);
            break;
        default:
            logError("file: "__FILE__", line: %d, "
                    "unkown current stage: %d", __LINE__,
                    redo_ctx->current_stage);
            return EINVAL;
    }

    return result;
}

static int do_dedup()
{
    const bool need_padding = false;
    const bool need_lock = true;
    int result;
    int i;
    SliceBinlogDedupRedoContext redo_ctx;
    char dedup_filename[PATH_MAX];

    if ((result=slice_binlog_get_binlog_indexes(&redo_ctx.binlog_start,
                    &redo_ctx.binlog_index)) != 0)
    {
        return result;
    }

    if ((result=slice_binlog_rotate_file()) != 0) {
        return result;
    }

    get_slice_dedup_filename(dedup_filename, sizeof(dedup_filename));
    if ((result=ob_index_dump_slices_to_file_ex(&g_ob_hashtable,
                    0, g_ob_hashtable.capacity, dedup_filename,
                    &redo_ctx.slice_count, need_padding, need_lock)) != 0)
    {
        return result;
    }

    for (i=0; i<10; i++) {
        if (slice_binlog_get_current_write_index() > redo_ctx.binlog_index) {
            break;
        }
        sleep(1);
    }

    if (slice_binlog_get_current_write_index() <= redo_ctx.binlog_index) {
        logError("file: "__FILE__", line: %d, "
                "wait slice binlog rotate done timeout", __LINE__);
        fc_delete_file_ex(dedup_filename, "binlog dedup");
        return EBUSY;
    }

    get_slice_mark_filename(redo_ctx.mark_filename,
            sizeof(redo_ctx.mark_filename));
    redo_ctx.current_stage = DEDUP_REDO_STAGE_RENAME_BINLOG;
    if ((result=write_to_redo_file(&redo_ctx)) != 0) {
        return result;
    }

    return redo(&redo_ctx);
}

int slice_dedup_redo()
{
    int result;
    SliceBinlogDedupRedoContext redo_ctx;

    if ((result=check_make_subdir()) != 0) {
        return result;
    }

    get_slice_mark_filename(redo_ctx.mark_filename,
            sizeof(redo_ctx.mark_filename));
    if (access(redo_ctx.mark_filename, F_OK) != 0) {
        result = errno != 0 ? errno : EPERM;
        if (result == ENOENT) {
            return 0;
        }

        logError("file: "__FILE__", line: %d, "
                "access file %s fail, errno: %d, error info: %s",
                __LINE__, redo_ctx.mark_filename, result, STRERROR(result));
        return result;
    }

    if ((result=load_from_redo_file(&redo_ctx)) != 0) {
        return result;
    }

    return redo(&redo_ctx);
}

static int slice_dedup_func(void *args)
{
    static volatile bool dedup_in_progress = false;
    int result;
    double dedup_ratio;
    int64_t slice_count;
    int64_t old_binlog_count;
    int64_t new_binlog_count;
    int64_t start_time_ms;
    int64_t time_used;
    char time_buff[32];

    if (!dedup_in_progress) {
        dedup_in_progress = true;

        slice_count = ob_index_get_total_slice_count();
        if (slice_count > 0) {
            dedup_ratio = (double)(SLICE_BINLOG_COUNT -
                    slice_count) / (double)slice_count;
        } else {
            dedup_ratio = 0.00;
        }

        if (dedup_ratio >= SLICE_DEDUP_RATIO) {
            start_time_ms = get_current_time_ms();
            logInfo("file: "__FILE__", line: %d, "
                    "slice count: %"PRId64", binlog_count: %"PRId64", "
                    "dedup_ratio: %.2f%%, dedup slice binlog ...", __LINE__,
                    slice_count, SLICE_BINLOG_COUNT, dedup_ratio * 100.00);

            old_binlog_count = FC_ATOMIC_GET(SLICE_BINLOG_COUNT);
            result = do_dedup();
            new_binlog_count = FC_ATOMIC_GET(SLICE_BINLOG_COUNT);
            if (result == 0) {
                int64_t inc_count;
                inc_count =  new_binlog_count - old_binlog_count;
                new_binlog_count = slice_count + inc_count;
                FC_ATOMIC_SET(SLICE_BINLOG_COUNT, new_binlog_count);
            }
            time_used = get_current_time_ms() - start_time_ms;
            long_to_comma_str(time_used, time_buff);
            logInfo("file: "__FILE__", line: %d, "
                    "slice dedup %s, new binlog count: %"PRId64", time "
                    "used: %s ms", __LINE__, (result == 0 ? "success" :
                        "fail"), new_binlog_count, time_buff);
        }

        dedup_in_progress = false;
    }

    return 0;
}

int slice_dedup_add_schedule()
{
    int result;
    ScheduleArray scheduleArray;
    ScheduleEntry scheduleEntry;

    if (!SLICE_DEDUP_ENABLED) {
        return 0;
    }

    if ((result=check_make_subdir()) != 0) {
        return result;
    }

    INIT_SCHEDULE_ENTRY_EX1(scheduleEntry, sched_generate_next_id(),
            SLICE_DEDUP_TIME, 86400, slice_dedup_func, NULL, true);
    scheduleArray.entries = &scheduleEntry;
    scheduleArray.count = 1;
    return sched_add_entries(&scheduleArray);
}
