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
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "sf/sf_global.h"
#include "sf/sf_binlog_writer.h"
#include "../server_global.h"
#include "binlog_func.h"
#include "binlog_reader.h"

static inline int get_current_write_index(ServerBinlogInfo *binlog_info)
{
    if (binlog_info->type == binlog_index_type_writer_ptr) {
        return sf_binlog_get_current_write_index(binlog_info->writer);
    } else {
        return binlog_info->write_index;
    }
}

static int open_readable_binlog(ServerBinlogReader *reader)
{
    int result;

    if (reader->fd >= 0) {
        close(reader->fd);
    }

    binlog_reader_get_filename_ex(reader->subdir_name,
            reader->fname_suffix, reader->position.index,
            reader->filename, sizeof(reader->filename));
    reader->fd = open(reader->filename, O_RDONLY);
    if (reader->fd < 0) {
        result = errno != 0 ? errno : EACCES;
        logError("file: "__FILE__", line: %d, "
                "open file \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, reader->filename,
                result, STRERROR(result));
        return result;
    }

    if (reader->position.offset > 0) {
        int64_t file_size;
        if ((file_size=lseek(reader->fd, 0L, SEEK_END)) < 0) {
            result = errno != 0 ? errno : EACCES;
            logError("file: "__FILE__", line: %d, "
                    "lseek file \"%s\" fail, "
                    "errno: %d, error info: %s", __LINE__,
                    reader->filename, result, STRERROR(result));
            return result;
        }

        if (reader->position.offset > file_size) {
            logWarning("file: "__FILE__", line: %d, "
                    "offset %"PRId64" > file size: %"PRId64,
                    __LINE__, reader->position.offset, file_size);
            reader->position.offset = file_size;
        }

        if (lseek(reader->fd, reader->position.offset, SEEK_SET) < 0) {
            result = errno != 0 ? errno : EACCES;
            logError("file: "__FILE__", line: %d, "
                    "lseek file \"%s\" fail,  offset: %"PRId64", "
                    "errno: %d, error info: %s", __LINE__,
                    reader->filename, reader->position.offset,
                    result, STRERROR(result));
            return result;
        }
    }

    reader->binlog_buffer.current = reader->binlog_buffer.data_end =
        reader->binlog_buffer.buff;
    return 0;
}

static int do_read_to_buffer(ServerBinlogReader *reader,
        char *buff, const int size, int *read_bytes)
{
    int result;

    *read_bytes = read(reader->fd, buff, size);
    if (*read_bytes == 0) {
        return ENOENT;
    }
    if (*read_bytes < 0) {
        *read_bytes = 0;
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "read binlog file: %s, errno: %d, error info: %s",
                __LINE__, reader->filename, result, STRERROR(result));
        return result;
    }

    reader->position.offset += *read_bytes;
    return 0;
}

static int do_binlog_read(ServerBinlogReader *reader)
{
    int remain;
    int result;
    int read_bytes;

    if (reader->binlog_buffer.current != reader->binlog_buffer.buff) {
        remain = SF_BINLOG_BUFFER_CONSUMER_DATA_REMAIN(reader->binlog_buffer);
        if (remain > 0) {
            memmove(reader->binlog_buffer.buff, reader->
                    binlog_buffer.current, remain);
        }

        reader->binlog_buffer.current = reader->binlog_buffer.buff;
        reader->binlog_buffer.data_end = reader->binlog_buffer.buff + remain;
    }

    read_bytes = reader->binlog_buffer.size -
        SF_BINLOG_BUFFER_PRODUCER_DATA_LENGTH(
                reader->binlog_buffer);
    if (read_bytes == 0) {
        return ENOSPC;
    }
    if ((result=do_read_to_buffer(reader, reader->binlog_buffer.
                    data_end, read_bytes, &read_bytes)) != 0)
    {
        return result;
    }

    reader->binlog_buffer.data_end += read_bytes;
    return 0;
}

int binlog_reader_read(ServerBinlogReader *reader)
{
    int result;

    result = do_binlog_read(reader);
    if (result == 0 || result != ENOENT) {
        return result;
    }

    if (reader->position.index < get_current_write_index(
            &reader->binlog_info))
    {
        reader->position.offset = 0;
        reader->position.index++;
        if ((result=open_readable_binlog(reader)) != 0) {
            return result;
        }
        result = do_binlog_read(reader);
    }

    return result;
}

int binlog_reader_read_to_buffer(ServerBinlogReader *reader,
        char *buff, const int size, int *read_bytes)
{
    int result;

    result = do_read_to_buffer(reader, buff, size, read_bytes);
    if (result == 0 || result != ENOENT) {
        return result;
    }

    if (reader->position.index < get_current_write_index(
            &reader->binlog_info))
    {
        reader->position.offset = 0;
        reader->position.index++;
        if ((result=open_readable_binlog(reader)) != 0) {
            return result;
        }

        result = do_read_to_buffer(reader, buff, size, read_bytes);
    }

    return result;
}

int binlog_reader_integral_read(ServerBinlogReader *reader,
        char *buff, const int size, int *read_bytes)
{
    int result;
    int remain_len;
    char *line_end;

    if ((result=binlog_reader_read_to_buffer(reader,
                    buff, size, read_bytes)) != 0)
    {
        return result;
    }

    line_end = (char *)fc_memrchr(buff, '\n', *read_bytes);
    if (line_end == NULL) {
        int64_t line_count;

        fc_get_file_line_count_ex(reader->filename, reader->position.
                offset + *read_bytes, &line_count);
        logError("file: "__FILE__", line: %d, "
                "expect new line (\\n), "
                "binlog file: %s, line no: %"PRId64,
                __LINE__, reader->filename, line_count);
        return EAGAIN;
    }

    remain_len = (buff + *read_bytes) - (line_end + 1);
    if (remain_len > 0) {
        *read_bytes -= remain_len;
        reader->position.offset -= remain_len;
        if (lseek(reader->fd, reader->position.offset, SEEK_SET) < 0) {
            result = errno != 0 ? errno : EACCES;
            logError("file: "__FILE__", line: %d, "
                    "lseek file \"%s\" fail,  offset: %"PRId64", "
                    "errno: %d, error info: %s", __LINE__,
                    reader->filename, reader->position.offset,
                    result, STRERROR(result));
            return result;
        }
    }

    return 0;
}

int binlog_reader_integral_full_read(ServerBinlogReader *reader,
        char *buff, const int size, int *read_bytes)
{
    int result;
    int remain_bytes;
    int bytes;

    if ((result=binlog_reader_integral_read(reader, buff,
                    size, read_bytes)) != 0)
    {
        return result;
    }

    remain_bytes = size - (*read_bytes);
    if (remain_bytes >= FS_REPLICA_BINLOG_MAX_RECORD_SIZE && reader->
            position.index < get_current_write_index(&reader->binlog_info))
    {
        result = binlog_reader_integral_read(reader, buff +
                (*read_bytes), remain_bytes, &bytes);
        if (result == 0) {
            *read_bytes += bytes;
        } else if (result == ENOENT) {
            result = 0;
        }
    }

    return result;
}

static int do_reader_init(ServerBinlogReader *reader,
        const char *subdir_name, const char *fname_suffix,
        ServerBinlogInfo *binlog_info, const SFBinlogFilePosition *pos,
        const int buffer_size)
{
    int result;

    if ((result=sf_binlog_buffer_init(&reader->binlog_buffer,
                    buffer_size)) != 0)
    {
        return result;
    }

    reader->fd = -1;
    fc_safe_strcpy(reader->subdir_name, subdir_name);
    if (fname_suffix == NULL) {
        *reader->fname_suffix = '\0';
    } else {
        fc_safe_strcpy(reader->fname_suffix, fname_suffix);
    }
    reader->binlog_info = *binlog_info;
    if (pos == NULL) {
        reader->position.index = 0;
        reader->position.offset = 0;
    } else {
        reader->position = *pos;
    }
    return open_readable_binlog(reader);
}

int binlog_reader_init_ex(ServerBinlogReader *reader,
        const char *subdir_name, const char *fname_suffix,
        SFBinlogWriterInfo *writer, const SFBinlogFilePosition *pos)
{
    ServerBinlogInfo binlog_info;

    binlog_info.type = binlog_index_type_writer_ptr;
    binlog_info.writer = writer;
    return do_reader_init(reader, subdir_name, fname_suffix,
            &binlog_info, pos, BINLOG_BUFFER_SIZE);
}

int binlog_reader_init1_ex(ServerBinlogReader *reader,
        const char *subdir_name, const char *fname_suffix,
        const int write_index, const SFBinlogFilePosition *pos,
        const int buffer_size)
{
    ServerBinlogInfo binlog_info;
    
    binlog_info.type = binlog_index_type_index_val;
    binlog_info.write_index = write_index;
    return do_reader_init(reader, subdir_name, fname_suffix,
            &binlog_info, pos, buffer_size);
}

void binlog_reader_destroy(ServerBinlogReader *reader)
{
    if (reader->fd >= 0) {
        close(reader->fd);
        reader->fd = -1;
    }

    sf_binlog_buffer_destroy(&reader->binlog_buffer);
}

bool binlog_reader_is_last_file(ServerBinlogReader *reader)
{
    return reader->position.index == get_current_write_index(
            &reader->binlog_info);
}
