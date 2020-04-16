#include <limits.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fast_mblock.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "trunk_binlog.h"

typedef struct {
    int fd;
    int64_t line_count;
    BufferInfo buffer;
    char *current;
    const char *binlog_filename;
} TrunkBinlogReader;

#define TRUNK_BINLOG_FILENAME        ".trunk_binlog.dat"

static char *get_trunk_binlog_filename(char *full_filename, const int size)
{
    snprintf(full_filename, size, "%s/%s",
            DATA_PATH_STR, TRUNK_BINLOG_FILENAME);
    return full_filename;
}

static int read_data(TrunkBinlogReader *reader)
{
    int result;
    int bytes;
    int remain;

    remain = (reader->buffer.buff + reader->buffer.length) - reader->current;
    if (remain > 0) {
        memmove(reader->buffer.buff, reader->current, remain);
    }
    reader->buffer.length = remain;

    bytes = read(reader->fd, reader->buffer.buff + reader->buffer.length,
            reader->buffer.alloc_size - reader->buffer.length);
    if (bytes < 0) {
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "read from file %s fail, errno: %d, error info: %s",
                __LINE__, reader->binlog_filename, result, STRERROR(result));
        return result;
    } else if (bytes == 0) {
        return ENOENT;
    }

    reader->buffer.length += bytes;
    reader->current = reader->buffer.buff;
    return 0;
}

static int parse_line(TrunkBinlogReader *reader, char *line_end)
{
#define MAX_FIELD_COUNT     16
#define EXPECT_FIELD_COUNT  6
    int count;
    string_t line;
    string_t cols[MAX_FIELD_COUNT];

    line.str = reader->current;
    line.len = line_end - reader->current;
    count = split_string_ex(&line, ' ', cols,
            MAX_FIELD_COUNT, false);
    if (count < EXPECT_FIELD_COUNT) {
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", "
                "field count: %d < %d", __LINE__,
                reader->binlog_filename, reader->line_count,
                count, EXPECT_FIELD_COUNT);
        return EINVAL;
    }

    return 0;
}

static int parse_binlog(TrunkBinlogReader *reader)
{
    int result;
    char *buff_end;
    char *line_end;

    buff_end = reader->buffer.buff + reader->buffer.length;
    while (reader->current < buff_end) {
        line_end = (char *)memchr(reader->current, '\n',
                buff_end - reader->current);
        if (line_end == NULL) {
            break;
        }

        reader->line_count++;
        if ((result=parse_line(reader, line_end)) != 0) {
            break;
        }

        reader->current = line_end + 1;
    }

    return 0;
}

static int load_data(TrunkBinlogReader *reader)
{
    int result;

    while ((result=read_data(reader)) == 0) {
        if ((result=parse_binlog(reader)) != 0) {
            break;
        }
    }

    return result == ENOENT ? 0 : result;
}

static int trunk_binlog_load(const char *binlog_filename)
{
    int result;
    TrunkBinlogReader reader;

    if ((result=fc_init_buffer(&reader.buffer, 256 * 1024)) != 0) {
        return result;
    }

    reader.line_count = 0;
    reader.current = reader.buffer.buff;
    reader.binlog_filename = binlog_filename;
    reader.fd = open(binlog_filename, O_RDONLY);
    if (reader.fd < 0) {
        result = errno != 0 ? errno : EACCES;
        logError("file: "__FILE__", line: %d, "
                "open file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, binlog_filename, result, STRERROR(result));
        return result;
    }

    result = load_data(&reader);
    fc_free_buffer(&reader.buffer);
    close(reader.fd);
    return result;
}

int trunk_binlog_init()
{
    int result;
    char full_filename[PATH_MAX];

    get_trunk_binlog_filename(full_filename, sizeof(full_filename));
    if (access(full_filename, F_OK) != 0) {
        if (errno == ENOENT) {
            return 0;
        }

        result = errno != 0 ? errno : EPERM;
        logError("file: "__FILE__", line: %d, "
                "access file %s fail, errno: %d, error info: %s",
                __LINE__, full_filename, result, STRERROR(result));
        return result;
    }

    return trunk_binlog_load(full_filename);
}
