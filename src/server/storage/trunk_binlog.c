#include <limits.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "../dio/trunk_io_thread.h"
#include "storage_allocator.h"
#include "trunk_id_info.h"
#include "trunk_binlog.h"

typedef struct {
    int fd;
    int64_t line_count;
    BufferInfo buffer;
    char *current;
    const char *filename;
} TrunkBinlogReader;

typedef struct {
    int fd;
    char filename[PATH_MAX];
    pthread_mutex_t lock;
} TrunkBinlogWriter;

#define TRUNK_BINLOG_FILENAME        "trunk_binlog.dat"

static TrunkBinlogWriter binlog_writer = {-1, {0}};

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
                __LINE__, reader->filename, result, STRERROR(result));
        return result;
    } else if (bytes == 0) {
        return ENOENT;
    }

    reader->buffer.length += bytes;
    reader->current = reader->buffer.buff;
    return 0;
}

#define PARSE_INTEGER_EX(var, caption, index, endchr, min_val) \
    do {   \
        var = strtol(cols[index].str, &endptr, 10);  \
        if (*endptr != endchr || var < min_val) {    \
            logError("file: "__FILE__", line: %d, "  \
                    "binlog file %s, line no: %"PRId64", " \
                    "invalid %s: %.*s", __LINE__,          \
                    reader->filename, reader->line_count,  \
                    caption, cols[index].len, cols[index].str); \
            return EINVAL;  \
        }  \
    } while (0)

#define PARSE_INTEGER(var, index, endchr, min_val)  \
    PARSE_INTEGER_EX(var, #var, index, endchr, min_val)

static int parse_line(TrunkBinlogReader *reader, char *line_end)
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
    string_t line;
    string_t cols[MAX_FIELD_COUNT];
    char *endptr;
    char op_type;
    int path_index;
    FSTrunkIdInfo id_info;
    int64_t trunk_size;

    line.str = reader->current;
    line.len = line_end - reader->current;
    count = split_string_ex(&line, ' ', cols,
            MAX_FIELD_COUNT, false);
    if (count < EXPECT_FIELD_COUNT) {
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", "
                "field count: %d < %d", __LINE__,
                reader->filename, reader->line_count,
                count, EXPECT_FIELD_COUNT);
        return EINVAL;
    }

    op_type = cols[FIELD_INDEX_OP_TYPE].str[0];
    PARSE_INTEGER(path_index, FIELD_INDEX_PATH_INDEX, ' ', 0);
    if (path_index > STORAGE_CFG.max_store_path_index) {
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", "
                "invalid path_index: %d > max_store_path_index: %d",
                __LINE__, reader->filename, reader->line_count,
                path_index, STORAGE_CFG.max_store_path_index);
        return EINVAL;
    }

    PARSE_INTEGER_EX(id_info.id, "trunk_id", FIELD_INDEX_TRUNK_ID, ' ', 1);
    PARSE_INTEGER_EX(id_info.subdir, "subdir", FIELD_INDEX_SUBDIR, ' ', 1);
    PARSE_INTEGER(trunk_size, FIELD_INDEX_TRUNK_SIZE,
            '\n', FS_TRUNK_FILE_MIN_SIZE);
    if (trunk_size > FS_TRUNK_FILE_MAX_SIZE) {
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", "
                "invalid trunk size: %"PRId64, __LINE__,
                reader->filename, reader->line_count, trunk_size);
        return EINVAL;
    }

    if (op_type == FS_IO_TYPE_CREATE_TRUNK) {
       if ((result=storage_allocator_add_trunk(path_index,
                       &id_info, trunk_size)) != 0)
       {
           return result;
       }
    } else if (op_type == FS_IO_TYPE_DELETE_TRUNK) {
        if ((result=storage_allocator_delete_trunk(path_index,
                        &id_info)) != 0)
        {
            return result;
        }
    } else {
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", "
                "invalid op_type: %c (0x%02x)", __LINE__,
                reader->filename, reader->line_count,
                op_type, (unsigned char)op_type);
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
    reader.filename = binlog_filename;
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

static int init_binlog_writer()
{
    int result;

    if ((result=init_pthread_lock(&binlog_writer.lock)) != 0) {
        logError("file: "__FILE__", line: %d, "
            "init_pthread_lock fail, errno: %d, error info: %s",
            __LINE__, result, STRERROR(result));
        return result;
    }

    binlog_writer.fd = open(binlog_writer.filename,
            O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (binlog_writer.fd < 0) {
        logError("file: "__FILE__", line: %d, "
                "open file \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, binlog_writer.filename,
                errno, STRERROR(errno));
        return errno != 0 ? errno : EACCES;
    }

    return 0;
}

int trunk_binlog_init()
{
    int result;

    get_trunk_binlog_filename(binlog_writer.filename,
            sizeof(binlog_writer.filename));
    if (access(binlog_writer.filename, F_OK) != 0) {
        if (errno != ENOENT) {
            result = errno != 0 ? errno : EPERM;
            logError("file: "__FILE__", line: %d, "
                    "access file %s fail, errno: %d, error info: %s",
                    __LINE__, binlog_writer.filename, result, STRERROR(result));
            return result;
        }
    } else if ((result=trunk_binlog_load(binlog_writer.filename)) != 0) {
        return result;
    }

    return init_binlog_writer();
}

void trunk_binlog_destroy()
{
    if (binlog_writer.fd >= 0) {
        close(binlog_writer.fd);
        binlog_writer.fd = -1;
    }
}

int trunk_binlog_write(const char op_type, const int path_index,
        const FSTrunkIdInfo *id_info, const int64_t file_size)
{
    int result;
    int len;
    char buff[1024];

    len = sprintf(buff, "%d %c %d %"PRId64" %"PRId64" %"PRId64"\n",
            (int)g_current_time, op_type, path_index, id_info->id,
            id_info->subdir, file_size);

    pthread_mutex_lock(&binlog_writer.lock);
    do {
        if (fc_safe_write(binlog_writer.fd, buff, len) != len) {
            result = errno != 0 ? errno : EIO;
            logError("file: "__FILE__", line: %d, "
                    "write to binlog file \"%s\" fail, fd: %d, "
                    "errno: %d, error info: %s",
                    __LINE__, binlog_writer.filename,
                    binlog_writer.fd, result, STRERROR(result));
            break;
        } else if (fsync(binlog_writer.fd) != 0) {
            result = errno != 0 ? errno : EIO;
            logError("file: "__FILE__", line: %d, "
                    "fsync to binlog file \"%s\" fail, "
                    "errno: %d, error info: %s",
                    __LINE__, binlog_writer.filename,
                    result, STRERROR(result));
            break;
        }
        result = 0;
    } while (0);
    pthread_mutex_unlock(&binlog_writer.lock);

    return result;
}
