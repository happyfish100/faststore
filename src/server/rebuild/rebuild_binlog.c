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
#include "../binlog/binlog_loader.h"
#include "../server_global.h"
#include "rebuild_binlog.h"

#define REBUILD_BINLOG_FIELD_INDEX_OP_TYPE       0
#define REBUILD_BINLOG_FIELD_INDEX_BLOCK_OID     1
#define REBUILD_BINLOG_FIELD_INDEX_BLOCK_OFFSET  2
#define REBUILD_BINLOG_FIELD_INDEX_SLICE_OFFSET  3
#define REBUILD_BINLOG_FIELD_INDEX_SLICE_LENGTH  4

#define REBUILD_BINLOG_FIELD_COUNT  5

int rebuild_binlog_record_unpack(const string_t *line,
        RebuildBinlogRecord *record, char *error_info)
{
    int count;
    string_t cols[REBUILD_BINLOG_FIELD_COUNT];
    char *endptr;

    count = split_string_ex(line, ' ', cols,
            REBUILD_BINLOG_FIELD_COUNT, false);
    if (count != REBUILD_BINLOG_FIELD_COUNT) {
        sprintf(error_info, "field count: %d != %d",
                count, REBUILD_BINLOG_FIELD_COUNT);
        return EINVAL;
    }

    record->op_type = cols[REBUILD_BINLOG_FIELD_INDEX_OP_TYPE].str[0];
    BINLOG_PARSE_INT_SILENCE(record->bs_key.block.oid, "object ID",
            REBUILD_BINLOG_FIELD_INDEX_BLOCK_OID, ' ', 1);
    BINLOG_PARSE_INT_SILENCE(record->bs_key.block.offset, "block offset",
            REBUILD_BINLOG_FIELD_INDEX_BLOCK_OFFSET, ' ', 0);
    BINLOG_PARSE_INT_SILENCE(record->bs_key.slice.offset, "slice offset",
            REBUILD_BINLOG_FIELD_INDEX_SLICE_OFFSET, ' ', 0);
    BINLOG_PARSE_INT_SILENCE(record->bs_key.slice.length, "slice length",
            REBUILD_BINLOG_FIELD_INDEX_SLICE_LENGTH, '\n', 1);

    fs_calc_block_hashcode(&record->bs_key.block);
    return 0;
}

int rebuild_binlog_parse_line(ServerBinlogReader *reader,
        BufferInfo *buffer, const string_t *line,
        RebuildBinlogRecord *record)
{
    int result;
    int64_t line_count;
    char error_info[256];

    if ((result=rebuild_binlog_record_unpack(line,
                    record, error_info)) != 0)
    {
        REBUILD_BINLOG_GET_FILENAME_LINE_COUNT(reader, buffer,
                line->str, line_count);
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", %s", __LINE__,
                reader->filename, line_count, error_info);
    }

    return result;
}
