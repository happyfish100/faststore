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
#include "rebuild_binlog.h"

#define REBUILD_BINLOG_FIELD_INDEX_SN            0
#define REBUILD_BINLOG_FIELD_INDEX_OP_TYPE       1
#define REBUILD_BINLOG_FIELD_INDEX_BLOCK_OID     2
#define REBUILD_BINLOG_FIELD_INDEX_BLOCK_OFFSET  3
#define REBUILD_BINLOG_FIELD_INDEX_SLICE_OFFSET  4
#define REBUILD_BINLOG_FIELD_INDEX_SLICE_LENGTH  5

#define REBUILD_BINLOG_FIELD_COUNT  6

#define REBUILD_BINLOG_GET_FILENAME_LINE_COUNT(reader, buffer, line_str, line_count) \
    do { \
        fc_get_file_line_count_ex(reader->filename, reader->position.offset - \
                ((buffer)->length - (line_str - (buffer)->buff)), &line_count); \
        line_count++; \
    } while (0)

#define REBUILD_BINLOG_PARSE_INT_EX(reader, buffer, \
        var, caption, index, endchr, min_val) \
    do {   \
        var = strtol(cols[index].str, &endptr, 10);  \
        if (*endptr != endchr || var < min_val) {    \
            REBUILD_BINLOG_GET_FILENAME_LINE_COUNT(reader, \
                    buffer, line->str, line_count);  \
            logError("file: "__FILE__", line: %d, "  \
                    "binlog file %s, line no: %"PRId64", " \
                    "invalid %s: %.*s", __LINE__,          \
                    reader->filename, line_count,  \
                    caption, cols[index].len, cols[index].str); \
            return EINVAL;  \
        }  \
    } while (0)

int rebuild_binlog_parse_line(ServerBinlogReader *reader,
        BufferInfo *buffer, const string_t *line, int64_t *sn,
        char *op_type, FSBlockSliceKeyInfo *bs_key)
{
    int count;
    int64_t line_count;
    string_t cols[REBUILD_BINLOG_FIELD_COUNT];
    char *endptr;

    count = split_string_ex(line, ' ', cols,
            REBUILD_BINLOG_FIELD_COUNT, false);
    if (count != REBUILD_BINLOG_FIELD_COUNT) {
        REBUILD_BINLOG_GET_FILENAME_LINE_COUNT(reader, buffer,
                line->str, line_count);
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", "
                "field count: %d != %d", __LINE__,
                reader->filename, line_count,
                count, REBUILD_BINLOG_FIELD_COUNT);
        return EINVAL;
    }

    REBUILD_BINLOG_PARSE_INT_EX(reader, buffer, *sn, "sn",
            REBUILD_BINLOG_FIELD_INDEX_SN, ' ', 1);
    *op_type = cols[REBUILD_BINLOG_FIELD_INDEX_OP_TYPE].str[0];
    REBUILD_BINLOG_PARSE_INT_EX(reader, buffer,
            bs_key->block.oid, "object ID",
            REBUILD_BINLOG_FIELD_INDEX_BLOCK_OID, ' ', 1);
    REBUILD_BINLOG_PARSE_INT_EX(reader, buffer,
            bs_key->block.offset, "block offset",
            REBUILD_BINLOG_FIELD_INDEX_BLOCK_OFFSET, ' ', 0);
    REBUILD_BINLOG_PARSE_INT_EX(reader, buffer,
            bs_key->slice.offset, "slice offset",
            REBUILD_BINLOG_FIELD_INDEX_SLICE_OFFSET, ' ', 0);
    REBUILD_BINLOG_PARSE_INT_EX(reader, buffer,
            bs_key->slice.length, "slice length",
            REBUILD_BINLOG_FIELD_INDEX_SLICE_LENGTH, '\n', 1);

    fs_calc_block_hashcode(&bs_key->block);
    return 0;
}
