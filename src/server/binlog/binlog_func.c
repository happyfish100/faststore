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

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <pthread.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "binlog_loader.h"
#include "binlog_func.h"

int binlog_unpack_common_fields(const string_t *line,
        BinlogCommonFields *fields, char *error_info)
{
    int count;
    char *endptr;
    string_t cols[BINLOG_MAX_FIELD_COUNT];

    count = split_string_ex(line, ' ', cols,
            BINLOG_MAX_FIELD_COUNT, false);
    if (count < BINLOG_MIN_FIELD_COUNT) {
        sprintf(error_info, "field count: %d < %d",
                count, BINLOG_MIN_FIELD_COUNT);
        return EINVAL;
    }

    BINLOG_PARSE_INT_SILENCE(fields->timestamp, "timestamp",
            BINLOG_COMMON_FIELD_INDEX_TIMESTAMP, ' ', 0);
    BINLOG_PARSE_INT_SILENCE(fields->data_version, "data version",
            BINLOG_COMMON_FIELD_INDEX_DATA_VERSION, ' ', 0);
    fields->source = cols[BINLOG_COMMON_FIELD_INDEX_SOURCE].str[0];
    fields->op_type = cols[BINLOG_COMMON_FIELD_INDEX_OP_TYPE].str[0];
    BINLOG_PARSE_INT_SILENCE(fields->bkey.oid, "object ID",
            BINLOG_COMMON_FIELD_INDEX_BLOCK_OID, ' ', 1);
    BINLOG_PARSE_INT_SILENCE2(fields->bkey.offset, "block offset",
            BINLOG_COMMON_FIELD_INDEX_BLOCK_OFFSET, ' ', '\n', 0);
    return 0;
}

static int binlog_unpack_timestamp(const string_t *line,
        time_t *timestamp, char *error_info)
{
    int count;
    char *endptr;
    string_t cols[BINLOG_MAX_FIELD_COUNT];

    count = split_string_ex(line, ' ', cols,
            BINLOG_MAX_FIELD_COUNT, false);
    if (count < BINLOG_MIN_FIELD_COUNT) {
        sprintf(error_info, "field count: %d < %d",
                count, BINLOG_MIN_FIELD_COUNT);
        return EINVAL;
    }

    BINLOG_PARSE_INT_SILENCE(*timestamp, "timestamp",
            BINLOG_COMMON_FIELD_INDEX_TIMESTAMP, ' ', 0);
    return 0;
}

int binlog_get_first_timestamp(const char *filename, time_t *timestamp)
{
    char buff[FS_BINLOG_MAX_RECORD_SIZE];
    char error_info[256];
    string_t line;
    int result;

    if ((result=fc_get_first_line(filename, buff,
                    sizeof(buff), &line)) != 0)
    {
        return result;
    }

    if ((result=binlog_unpack_timestamp(&line, timestamp, error_info)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "binlog file: %s, unpack last line fail, %s",
                __LINE__, filename, error_info);
    }

    return result;
}

int binlog_get_last_timestamp(const char *filename, time_t *timestamp)
{
    char buff[FS_BINLOG_MAX_RECORD_SIZE];
    char error_info[256];
    string_t line;
    int64_t file_size;
    int result;

    if ((result=fc_get_last_line(filename, buff,
                    sizeof(buff), &file_size, &line)) != 0)
    {
        return result;
    }

    if ((result=binlog_unpack_timestamp(&line, timestamp, error_info)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "binlog file: %s, unpack last line fail, %s",
                __LINE__, filename, error_info);
    }

    return result;
}

static int get_start_binlog_index_by_timestamp(const char *subdir_name,
        const time_t from_timestamp, const int start_index, int *binlog_index)
{
    char filename[PATH_MAX];
    int result;
    time_t timestamp;

    while (*binlog_index >= start_index) {
        binlog_reader_get_filename(subdir_name, *binlog_index,
                filename, sizeof(filename));
        result = binlog_get_first_timestamp(filename, &timestamp);
        if (result == 0) {
            if (timestamp < from_timestamp) {
                break;
            }
        } else if (result != ENOENT) {  //ENOENT for empty file
            return result;
        }

        if (*binlog_index == start_index) {
            break;
        }
        (*binlog_index)--;
    }

    return 0;
}

static int find_timestamp(ServerBinlogReader *reader, const int length,
        const time_t from_timestamp, int *offset)
{
    int result;
    string_t line;
    char *buff;
    char *line_start;
    char *buff_end;
    char *line_end;
    time_t timestamp;
    char error_info[256];

    result = 0;
    buff = reader->binlog_buffer.buff;
    line_start = buff;
    buff_end = buff + length;
    while (line_start < buff_end) {
        line_end = (char *)memchr(line_start, '\n', buff_end - line_start);
        if (line_end == NULL) {
            result = EINVAL;
            sprintf(error_info, "expect line end char (\\n)");
            break;
        }

        line.str = line_start;
        line.len = line_end - line_start;
        if ((result=binlog_unpack_timestamp(&line,
                        &timestamp, error_info)) != 0)
        {
            break;
        }

        if (timestamp >= from_timestamp) {
            *offset = line_start - buff;
            break;
        }

        line_start = line_end + 1;
    }

    if (result != 0) {
        int64_t file_offset;
        int64_t line_count;
        int remain_bytes;

        remain_bytes = length - (line_start - buff);
        file_offset = reader->position.offset - remain_bytes;
        fc_get_file_line_count_ex(reader->filename,
                file_offset, &line_count);
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", %s",
                __LINE__, reader->filename, line_count, error_info);
    }
    return result;
}

int binlog_get_position_by_timestamp(const char *subdir_name,
        struct sf_binlog_writer_info *writer, const time_t from_timestamp,
        SFBinlogFilePosition *pos)
{
    int result;
    int offset;
    int start_index;
    int last_index;
    int binlog_index;
    int read_bytes;
    int remain_bytes;
    ServerBinlogReader reader;

    if ((result=sf_binlog_get_indexes(writer, &start_index,
                    &last_index)) != 0)
    {
        return result;
    }
    binlog_index = last_index;
    if ((result=get_start_binlog_index_by_timestamp(subdir_name,
                    from_timestamp, start_index, &binlog_index)) != 0)
    {
        return result;
    }

    pos->index = binlog_index;
    pos->offset = 0;
    if ((result=binlog_reader_init(&reader, subdir_name, writer, pos)) != 0) {
        return result;
    }

    remain_bytes = 0;
    offset = -1;
    while ((result=binlog_reader_integral_read(&reader,
                    reader.binlog_buffer.buff,
                    reader.binlog_buffer.size,
                    &read_bytes)) == 0)
    {
        if ((result=find_timestamp(&reader, read_bytes,
                        from_timestamp, &offset)) != 0)
        {
            break;
        }

        if (offset >= 0) {  //found
            remain_bytes = read_bytes - offset;
            break;
        }
    }

    if (result == ENOENT) {
        result = 0;
    }
    if (result == 0) {
        pos->index = reader.position.index;
        pos->offset = reader.position.offset - remain_bytes;
    }

    binlog_reader_destroy(&reader);
    return result;
}
