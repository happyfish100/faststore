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

#include <limits.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "sf/sf_binlog_writer.h"
#include "../server_global.h"
#include "../dio/trunk_write_thread.h"
#include "../storage/storage_allocator.h"
#include "../storage/trunk_id_info.h"
#include "binlog_loader.h"
#include "trunk_binlog.h"

static SFBinlogWriterContext binlog_writer;

#define TRUNK_GET_FILENAME_LINE_COUNT(r, binlog_filename, \
        line_str, line_count) \
        BINLOG_GET_FILENAME_LINE_COUNT(r, FS_TRUNK_BINLOG_SUBDIR_NAME, \
        binlog_filename, line_str, line_count)

#define TRUNK_PARSE_INT_EX(var, caption, index, endchr, min_val) \
    BINLOG_PARSE_INT_EX(FS_TRUNK_BINLOG_SUBDIR_NAME, var, caption,  \
            index, endchr, min_val)

#define TRUNK_PARSE_INT(var, index, endchr, min_val)  \
    BINLOG_PARSE_INT_EX(FS_TRUNK_BINLOG_SUBDIR_NAME, var, #var, \
            index, endchr, min_val)

static int trunk_parse_line(BinlogReadThreadResult *r, string_t *line)
{
#define MAX_FIELD_COUNT     8
#define EXPECT_FIELD_COUNT  6
#define FIELD_INDEX_TIMESTAMP   0
#define FIELD_INDEX_OP_TYPE     1
#define FIELD_INDEX_PATH_INDEX  2
#define FIELD_INDEX_TRUNK_ID    3
#define FIELD_INDEX_SUBDIR      4
#define FIELD_INDEX_TRUNK_SIZE  5

    int result;
    int count;
    int64_t line_count;
    string_t cols[MAX_FIELD_COUNT];
    char binlog_filename[PATH_MAX];
    char error_info[256];
    char *endptr;
    char op_type;
    int path_index;
    FSTrunkIdInfo id_info;
    int64_t trunk_size;

    count = split_string_ex(line, ' ', cols,
            MAX_FIELD_COUNT, false);
    if (count < EXPECT_FIELD_COUNT) {
        TRUNK_GET_FILENAME_LINE_COUNT(r, binlog_filename,
                line->str, line_count);
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", "
                "field count: %d < %d", __LINE__,
                binlog_filename, line_count,
                count, EXPECT_FIELD_COUNT);
        return EINVAL;
    }

    op_type = cols[FIELD_INDEX_OP_TYPE].str[0];
    TRUNK_PARSE_INT(path_index, FIELD_INDEX_PATH_INDEX, ' ', 0);
    if (path_index > STORAGE_CFG.max_store_path_index) {
        TRUNK_GET_FILENAME_LINE_COUNT(r, binlog_filename,
                line->str, line_count);
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", "
                "invalid path_index: %d > max_store_path_index: %d",
                __LINE__, binlog_filename, line_count,
                path_index, STORAGE_CFG.max_store_path_index);
        return EINVAL;
    }

    TRUNK_PARSE_INT_EX(id_info.id, "trunk_id", FIELD_INDEX_TRUNK_ID, ' ', 1);
    TRUNK_PARSE_INT_EX(id_info.subdir, "subdir", FIELD_INDEX_SUBDIR, ' ', 1);
    TRUNK_PARSE_INT(trunk_size, FIELD_INDEX_TRUNK_SIZE,
            '\n', FS_TRUNK_FILE_MIN_SIZE);
    if (trunk_size > FS_TRUNK_FILE_MAX_SIZE) {
        TRUNK_GET_FILENAME_LINE_COUNT(r, binlog_filename,
                line->str, line_count);
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", "
                "invalid trunk size: %"PRId64, __LINE__,
                binlog_filename, line_count, trunk_size);
        return EINVAL;
    }

    if (op_type == FS_IO_TYPE_CREATE_TRUNK) {
        if (path_index == DATA_REBUILD_PATH_INDEX) {
            DATA_REBUILD_TRUNK_COUNT++;
            result = 0;
        } else if ((result=storage_allocator_add_trunk(path_index,
                        &id_info, trunk_size)) != 0)
        {
            snprintf(error_info, sizeof(error_info),
                    "add trunk fail, errno: %d, error info: %s",
                    result, STRERROR(result));
        }
    } else if (op_type == FS_IO_TYPE_DELETE_TRUNK) {
        if (path_index == DATA_REBUILD_PATH_INDEX) {
            DATA_REBUILD_TRUNK_COUNT--;
            result = 0;
        } else if ((result=storage_allocator_delete_trunk(
                        path_index, &id_info)) != 0)
        {
            snprintf(error_info, sizeof(error_info),
                    "delete trunk fail, errno: %d, error info: %s",
                    result, STRERROR(result));
        }
    } else {
        sprintf(error_info, "invalid op_type: %c (0x%02x)",
                op_type, (unsigned char)op_type);
        result = EINVAL;
    }

    if (result != 0) {
        TRUNK_GET_FILENAME_LINE_COUNT(r, binlog_filename,
                line->str, line_count);
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", %s",
                __LINE__, binlog_filename, line_count, error_info);
    }
    return result;
}

int trunk_binlog_get_current_write_index()
{
    return sf_binlog_get_current_write_index(&binlog_writer.writer);
}

int trunk_binlog_set_binlog_index(const int binlog_index)
{
    /* force write to binlog index file */
    binlog_writer.writer.fw.binlog.index = -1;
    return sf_binlog_writer_set_binlog_index(&binlog_writer.
            writer, binlog_index);
}

static int init_binlog_writer()
{
    return sf_binlog_writer_init(&binlog_writer, DATA_PATH_STR,
            FS_TRUNK_BINLOG_SUBDIR_NAME, BINLOG_BUFFER_SIZE,
            FS_TRUNK_BINLOG_MAX_RECORD_SIZE);
}

int trunk_binlog_init()
{
    int result;
    if ((result=init_binlog_writer()) != 0) {
        return result;
    }

    return binlog_loader_load(FS_TRUNK_BINLOG_SUBDIR_NAME,
            &binlog_writer.writer, trunk_parse_line);
}

void trunk_binlog_destroy()
{
    sf_binlog_writer_finish(&binlog_writer.writer);
}

int trunk_binlog_write(const char op_type, const int path_index,
        const FSTrunkIdInfo *id_info, const int64_t file_size)
{
    SFBinlogWriterBuffer *wbuffer;

    if ((wbuffer=sf_binlog_writer_alloc_buffer(&binlog_writer.thread)) == NULL) {
        return ENOMEM;
    }

    wbuffer->bf.length = trunk_binlog_log_to_buff(op_type,
            path_index, id_info, file_size, wbuffer->bf.buff);
    sf_push_to_binlog_thread_queue(&binlog_writer.thread, wbuffer);
    return 0;
}
