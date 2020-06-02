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
#include "../server_global.h"
#include "binlog_func.h"
#include "binlog_writer.h"
#include "binlog_reader.h"

static int open_readable_binlog(ServerBinlogReader *reader)
{
    int result;

    if (reader->fd >= 0) {
        close(reader->fd);
    }

    binlog_reader_get_filename(reader->subdir_name,
            reader->position.index, reader->filename,
            sizeof(reader->filename));
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

    reader->binlog_buffer.current = reader->binlog_buffer.end =
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
        remain = BINLOG_BUFFER_REMAIN(reader->binlog_buffer);
        if (remain > 0) {
            memmove(reader->binlog_buffer.buff, reader->binlog_buffer.current,
                    remain);
        }

        reader->binlog_buffer.current = reader->binlog_buffer.buff;
        reader->binlog_buffer.end = reader->binlog_buffer.buff + remain;
    }

    read_bytes = reader->binlog_buffer.size -
        BINLOG_BUFFER_LENGTH(reader->binlog_buffer);
    if (read_bytes == 0) {
        return ENOSPC;
    }
    if ((result=do_read_to_buffer(reader, reader->binlog_buffer.end,
                    read_bytes, &read_bytes)) != 0)
    {
        return result;
    }

    reader->binlog_buffer.end += read_bytes;
    return 0;
}

int binlog_reader_read(ServerBinlogReader *reader)
{
    int result;

    result = do_binlog_read(reader);
    if (result == 0 || result != ENOENT) {
        return result;
    }

    if (reader->position.index < reader->get_current_write_index()) {
        reader->position.offset = 0;
        reader->position.index++;
        if ((result=open_readable_binlog(reader)) != 0) {
            return result;
        }
        result = do_binlog_read(reader);
    }

    return result;
}

int binlog_read_to_buffer(ServerBinlogReader *reader,
        char *buff, const int size, int *read_bytes)
{
    int result;

    result = do_read_to_buffer(reader, buff, size, read_bytes);
    if (result == 0 || result != ENOENT) {
        return result;
    }

    if (reader->position.index < reader->get_current_write_index()) {
        reader->position.offset = 0;
        reader->position.index++;
        if ((result=open_readable_binlog(reader)) != 0) {
            return result;
        }

        result = do_read_to_buffer(reader, buff, size, read_bytes);
    }

    return result;
}

int binlog_reader_integral_read(ServerBinlogReader *reader, char *buff,
        const int size, int *read_bytes)
{
    int result;
    int remain_len;
    char *line_end;

    if ((result=binlog_read_to_buffer(reader, buff, size,
                    read_bytes)) != 0)
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
        return EINVAL;
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

int binlog_reader_init(ServerBinlogReader *reader, const char *subdir_name,
        get_current_write_index_func get_current_write_index,
        const FSBinlogFilePosition *position)
{
    int result;

    if ((result=binlog_buffer_init(&reader->binlog_buffer)) != 0) {
        return result;
    }

    reader->fd = -1;
    reader->subdir_name = subdir_name;
    reader->get_current_write_index = get_current_write_index;
    if (position == NULL) {
        reader->position.index = 0;
        reader->position.offset = 0;
    } else {
        reader->position = *position;
    }
    return open_readable_binlog(reader);
}

void binlog_reader_destroy(ServerBinlogReader *reader)
{
    if (reader->fd >= 0) {
        close(reader->fd);
        reader->fd = -1;
    }

    binlog_buffer_destroy(&reader->binlog_buffer);
}
