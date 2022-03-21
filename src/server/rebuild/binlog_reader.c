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
#include "../binlog/slice_binlog.h"
#include "../storage/slice_op.h"
#include "../server_global.h"
#include "rebuild_binlog.h"
#include "binlog_reader.h"

#define BINLOG_POSITION_FILENAME  "position.dat"

#define BINLOG_INDEX_ITEM_NAME_INDEX   "index"
#define BINLOG_INDEX_ITEM_NAME_OFFSET  "offset"

static const char *get_position_filename(const char *subdir_name,
        char *filename, const int size)
{
    snprintf(filename, size, "%s/%s/%s", DATA_PATH_STR,
            subdir_name, BINLOG_POSITION_FILENAME);
    return filename;
}

static int write_to_position_file(const char *subdir_name,
        const SFBinlogFilePosition *position)
{
    char filename[PATH_MAX];
    char buff[256];
    int result;
    int len;

    get_position_filename(subdir_name, filename, sizeof(filename));
    len = sprintf(buff, "%s=%d\n"
            "%s=%"PRId64"\n",
            BINLOG_INDEX_ITEM_NAME_INDEX, position->index,
            BINLOG_INDEX_ITEM_NAME_OFFSET, position->offset);
    if ((result=safeWriteToFile(filename, buff, len)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "write to file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, filename, result, STRERROR(result));
    }

    return result;
}

static int get_position_from_file(const char *subdir_name,
        SFBinlogFilePosition *position)
{
    char filename[PATH_MAX];
    IniContext ini_context;
    int result;

    get_position_filename(subdir_name, filename, sizeof(filename));
    if (access(filename, F_OK) != 0) {
        result =  errno != 0 ? errno : EPERM;
        if (result == ENOENT) {
            position->index = 0;
            position->offset = 0;
            return 0;
        } else {
            logError("file: "__FILE__", line: %d, "
                    "access file %s fail, errno: %d, error info: %s",
                    __LINE__, filename, result, STRERROR(result));
            return result;
        }
    }

    if ((result=iniLoadFromFile(filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load from file \"%s\" fail, error code: %d",
                __LINE__, filename, result);
        return result;
    }

    position->index = iniGetIntValue(NULL,
            BINLOG_INDEX_ITEM_NAME_INDEX,
            &ini_context, 0);
    position->offset = iniGetInt64Value(NULL,
            BINLOG_INDEX_ITEM_NAME_OFFSET,
            &ini_context, 0);

    iniFreeContext(&ini_context);
    return 0;
}

int rebuild_binlog_reader_init(ServerBinlogReader *reader,
        const char *subdir_name)
{
    int result;
    int write_index;
    SFBinlogFilePosition position;

    if ((result=sf_binlog_writer_get_binlog_index(DATA_PATH_STR,
                    subdir_name, &write_index)) != 0)
    {
        return result;
    }

    if ((result=get_position_from_file(subdir_name, &position)) != 0) {
        return result;
    }

    return binlog_reader_init1(reader, subdir_name,
            write_index, &position);
}

static int get_output_last_version(const char *subdir_name,
        int64_t *data_version)
{
    int result;
    int write_index;
    int count;
    string_t line;
    char buff[FS_SLICE_BINLOG_MAX_RECORD_SIZE];
    char error_info[256];
    SliceBinlogRecord record;

    if ((result=sf_binlog_writer_get_binlog_index(DATA_PATH_STR,
                    subdir_name, &write_index)) != 0)
    {
        if (result == ENOENT) {
            *data_version = 0;
            return 0;
        } else {
            return result;
        }
    }

    count = 1;
    line.str = buff;
    if ((result=sf_binlog_writer_get_last_lines(DATA_PATH_STR,
                    subdir_name, write_index, buff, sizeof(buff),
                    &count, &line.len)) != 0)
    {
        return result;
    }

    if (count == 0) {
        *data_version = 0;
        return 0;
    }

    if ((result=slice_binlog_record_unpack(&line,
                    &record, error_info)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "parse slice binlog last line fail, %s",
                __LINE__, error_info);
        return result;
    }

    *data_version = record.data_version;
    return 0;
}

static int find_position(const char *subdir_name,
        const int64_t target_data_version,
        SFBinlogFilePosition *position)
{
    int result;
    ServerBinlogReader reader;
    BufferInfo buffer;
    RebuildBinlogRecord record;
    string_t line;
    int64_t line_count;
    char error_info[256];
    char *line_start;
    char *buff_end;
    char *line_end;

    if ((result=fc_init_buffer(&buffer, 1024 * 1024)) != 0) {
        return result;
    }

    if ((result=binlog_reader_init(&reader, subdir_name,
                    NULL, position)) != 0)
    {
        return result;
    }

    while ((result=binlog_reader_integral_read(&reader, buffer.buff,
                    buffer.alloc_size, &buffer.length)) == 0)
    {
        line_start = buffer.buff;
        buff_end = buffer.buff + buffer.length;
        while (line_start < buff_end) {
            line_end = (char *)memchr(line_start,
                    '\n', buff_end - line_start);
            if (line_end == NULL) {
                result = ENOENT;
                break;
            }

            ++line_end;
            line.str = line_start;
            line.len = line_end - line_start;
            if ((result=rebuild_binlog_record_unpack(&line,
                            &record, error_info)) != 0)
            {
                REBUILD_BINLOG_GET_FILENAME_LINE_COUNT(&reader,
                        &buffer, line.str, line_count);
                logError("file: "__FILE__", line: %d, "
                        "binlog file %s, line no: %"PRId64", %s", __LINE__,
                        reader.filename, line_count, error_info);
                break;
            }

            if (record.data_version == target_data_version) {
                position->offset = reader.position.offset -
                    (buffer.length - (line_end - buffer.buff));
                break;
            } else if (record.data_version > target_data_version) {
                result = ENOENT;
                break;
            }

            line_start = line_end;
        }
    }

    binlog_reader_destroy(&reader);
    fc_free_buffer(&buffer);
    return result;
}

static int find_input_position(const char *subdir_name,
        const int64_t target_data_version,
        SFBinlogFilePosition *position)
{
    int result;
    int last_index;
    char filename[PATH_MAX];
    int64_t last_data_version;

    if ((result=sf_binlog_writer_get_binlog_index(DATA_PATH_STR,
                    subdir_name, &last_index)) != 0)
    {
        return result;
    }

    for (position->index=0; position->index<=last_index; position->index++) {
        if ((result=rebuild_binlog_get_last_data_version(subdir_name,
                        position->index, &last_data_version)) != 0)
        {
            return result;
        }

        if (last_data_version == target_data_version) {
            if (position->index < last_index) {
                position->index++;
                position->offset = 0;
            } else {
                binlog_reader_get_filename(subdir_name, position->index,
                        filename, sizeof(filename));
                if ((result=getFileSize(filename, &position->offset)) != 0) {
                    return result;
                }
            }
            return 0;
        } else if (last_data_version > target_data_version) {
            break;
        }
    }

    if (position->index > last_index) {
        return EOVERFLOW;
    }

    position->offset = 0;
    return find_position(subdir_name,
            target_data_version, position);
}

int rebuild_binlog_reader_save_position(
        const char *in_subdir_name,
        const char *out_subdir_name)
{
    int result;
    int64_t last_data_version;
    SFBinlogFilePosition position;

    if ((result=get_output_last_version(out_subdir_name,
                    &last_data_version)) != 0)
    {
        return result;
    }

    if (last_data_version > 0) {
        if ((result=find_input_position(in_subdir_name,
                        last_data_version, &position)) != 0)
        {
            logError("file: "__FILE__", line: %d, "
                    "subdir_name: %s, can't found data version: %"PRId64,
                    __LINE__, in_subdir_name, last_data_version);
            return result;
        }
    } else {
        position.index = 0;
        position.offset = 0;
    }

    return write_to_position_file(in_subdir_name, &position);
}
