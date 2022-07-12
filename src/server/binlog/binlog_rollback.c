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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include "fastcommon/logger.h"
#include "fastcommon/shared_func.h"
#include "sf/sf_global.h"
#include "../../common/fs_func.h"
#include "../server_global.h"
#include "binlog_func.h"
#include "binlog_reader.h"
#include "slice_binlog.h"
#include "replica_binlog.h"
#include "binlog_rollback.h"

static int check_alloc_record_array(BinlogBinlogCommonFieldsArray *array)
{
    BinlogCommonFields *records;
    int64_t new_alloc;
    int64_t bytes;

    if (array->alloc > array->count) {
        return 0;
    }

    new_alloc = (array->alloc > 0) ? 2 * array->alloc : 256;
    bytes = sizeof(BinlogCommonFields) * new_alloc;
    records = (BinlogCommonFields *)fc_malloc(bytes);
    if (records == NULL) {
        return ENOMEM;
    }

    if (array->records != NULL) {
        if (array->count > 0) {
            memcpy(records, array->records, array->count *
                    sizeof(BinlogCommonFields));
        }
        free(array->records);
    }

    array->alloc = new_alloc;
    array->records = records;
    return 0;
}

static int binlog_parse_buffer(ServerBinlogReader *reader,
        const int length, const time_t from_timestamp,
        BinlogBinlogCommonFieldsArray *array)
{
    int result;
    string_t line;
    char *buff;
    char *line_start;
    char *buff_end;
    char *line_end;
    BinlogCommonFields *record;
    BinlogCommonFields fields;
    char error_info[256];

    *error_info = '\0';
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
        line.len = ++line_end - line_start;
        if ((result=binlog_unpack_common_fields(&line,
                        &fields, error_info)) != 0)
        {
            break;
        }

        if ((fields.timestamp >= from_timestamp) &&
                FS_IS_BINLOG_SOURCE_RPC(fields.source))
        {
            fs_calc_block_hashcode(&fields.bkey);
            if ((result=check_alloc_record_array(array)) != 0) {
                sprintf(error_info, "out of memory");
                break;
            }

            record = array->records + array->count++;
            record->data_version = fields.data_version;
            //record->data_group_id = FS_DATA_GROUP_ID(fields.bkey);
        }

        line_start = line_end;
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

static int load_replica_binlogs(const int data_group_id,
        SFBinlogWriterInfo *writer, const SFBinlogFilePosition *pos,
        BinlogBinlogCommonFieldsArray *array)
{
    int result;
    int read_bytes;
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];
    ServerBinlogReader reader;

    sprintf(subdir_name, "%s/%d", FS_REPLICA_BINLOG_SUBDIR_NAME,
            data_group_id);
    if ((result=binlog_reader_init(&reader, subdir_name, writer, pos)) != 0) {
        return result;
    }

    while ((result=binlog_reader_integral_read(&reader,
                    reader.binlog_buffer.buff,
                    reader.binlog_buffer.size,
                    &read_bytes)) == 0)
    {
        if ((result=binlog_parse_buffer(&reader, read_bytes,
                        data_group_id, array)) != 0)
        {
            break;
        }
    }

    if (result == ENOENT) {
        result = 0;
    }

    binlog_reader_destroy(&reader);
    return result;
}

static int compare_version_and_source(const BinlogCommonFields *p1,
        const BinlogCommonFields *p2)
{
    return fc_compare_int64(p1->data_version, p2->data_version);
}

static int load_slice_binlogs(const int data_group_id,
        const int64_t my_confirmed_version, SFBinlogWriterInfo *writer,
        BinlogBinlogCommonFieldsArray *array)
{
    int result;
    int read_bytes;
    ServerBinlogReader reader;
    SFBinlogFilePosition pos;

    //TODO: find the position
    if ((result=binlog_reader_init(&reader, FS_SLICE_BINLOG_SUBDIR_NAME,
                    writer, &pos)) != 0)
    {
        return result;
    }

    while ((result=binlog_reader_integral_read(&reader,
                    reader.binlog_buffer.buff,
                    reader.binlog_buffer.size,
                    &read_bytes)) == 0)
    {
        if ((result=binlog_parse_buffer(&reader, read_bytes,
                        data_group_id, array)) != 0)
        {
            break;
        }
    }

    if (result == ENOENT) {
        result = 0;
    }

    if (result == 0 && array->count > 1) {
        qsort(array->records, array->count,
                sizeof(BinlogCommonFields),
                (int (*)(const void *, const void *))
                compare_version_and_source);
    }

    binlog_reader_destroy(&reader);
    return result;
}

static int binlog_load_data_records(const int data_group_id,
        const uint64_t my_confirmed_version,
        const SFBinlogFilePosition *position)
{
    int result;
    BinlogBinlogCommonFieldsArray slice_record_array;

    if ((result=load_replica_binlogs(data_group_id,
                    replica_binlog_get_writer(data_group_id),
                    position, &slice_record_array)) != 0)
    {
        return result;
    }

    if ((result=load_slice_binlogs(data_group_id, my_confirmed_version,
                    slice_binlog_get_writer(), &slice_record_array)) != 0)
    {
        return result;
    }

    return 0;
}

static int rollback_slice_binlogs(FSClusterDataServerInfo *myself,
        const uint64_t my_confirmed_version, const SFBinlogFilePosition
        *position, const bool detect_slice_binlog)
{
    return 0;
}

int binlog_rollback(FSClusterDataServerInfo *myself, const uint64_t
        my_confirmed_version, const bool detect_slice_binlog)
{
    const bool ignore_dv_overflow = false;
    int result;
    int start_index;
    int last_index;
    int binlog_index;
    uint64_t last_data_version;
    SFBinlogFilePosition position;
    char filename[PATH_MAX];

    if ((result=replica_binlog_get_last_dv(myself->dg->id,
                    &last_data_version)) != 0)
    {
        return result;
    }

    if (my_confirmed_version >= last_data_version) {
        return 0;
    }

    if ((result=replica_binlog_get_binlog_indexes(myself->dg->id,
                    &start_index, &last_index)) != 0)
    {
        return result;
    }

    if ((result=replica_binlog_get_position_by_dv(myself->dg->id,
                    my_confirmed_version, &position,
                    ignore_dv_overflow)) != 0)
    {
        return result;
    }

    if ((result=rollback_slice_binlogs(myself, my_confirmed_version,
                    &position, detect_slice_binlog)) != 0)
    {
        return result;
    }

    if (position.index < last_index) {
        if ((result=replica_binlog_set_binlog_indexes(myself->dg->id,
                        start_index, position.index)) != 0)
        {
            return result;
        }

        for (binlog_index=position.index+1;
                binlog_index<last_index;
                binlog_index++)
        {
            replica_binlog_get_filename(myself->dg->id, binlog_index,
                    filename, sizeof(filename));
            if ((result=fc_delete_file_ex(filename, "binlog")) != 0) {
                return result;
            }
        }
    }

    replica_binlog_get_filename(myself->dg->id, position.index,
            filename, sizeof(filename));
    if (truncate(filename, position.offset) != 0) {
        result = (errno != 0 ? errno : EPERM);
        logError("file: "__FILE__", line: %d, "
                "truncate file %s to length: %"PRId64" fail, "
                "errno: %d, error info: %s", __LINE__, filename,
                position.offset, result, STRERROR(result));
        return result;
    }

    if ((result=replica_binlog_get_last_dv(myself->dg->id,
                    &last_data_version)) != 0)
    {
        return result;
    }
    if (last_data_version != my_confirmed_version) {
        logError("file: "__FILE__", line: %d, "
                "binlog last_data_version: %"PRId64" != "
                "confirmed data version: %"PRId64", program exit!",
                __LINE__, last_data_version, my_confirmed_version);
        return EBUSY;
    }

    if ((result=replica_binlog_writer_change_write_index(
                    myself->dg->id, position.index)) != 0)
    {
        return result;
    }

    if ((result=replica_binlog_set_data_version(myself,
                    my_confirmed_version)) != 0)
    {
        return result;
    }

    return 0;
}
