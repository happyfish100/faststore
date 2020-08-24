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
#include "binlog_writer.h"
#include "binlog_func.h"

#define MAX_BINLOG_FIELD_COUNT  24
#define MIN_EXPECT_FIELD_COUNT   4

int binlog_buffer_init_ex(ServerBinlogBuffer *buffer, const int size)
{
    buffer->buff = (char *)fc_malloc(size);
    if (buffer->buff == NULL) {
        return ENOMEM;
    }

    buffer->current = buffer->end = buffer->buff;
    buffer->size = size;
    return 0;
}

int binlog_unpack_ts_and_dv(const string_t *line, time_t *timestamp,
        uint64_t *data_version, char *error_info)
{
    int count;
    char *endptr;
    string_t cols[MAX_BINLOG_FIELD_COUNT];

    count = split_string_ex(line, ' ', cols,
            MAX_BINLOG_FIELD_COUNT, false);
    if (count < MIN_EXPECT_FIELD_COUNT) {
        sprintf(error_info, "field count: %d < %d",
                count, MIN_EXPECT_FIELD_COUNT);
        return EINVAL;
    }

    BINLOG_PARSE_INT_SILENCE(*timestamp, "timestamp",
            BINLOG_COMMON_FIELD_INDEX_TIMESTAMP, ' ', 0);
    BINLOG_PARSE_INT_SILENCE(*data_version, "data version",
            BINLOG_COMMON_FIELD_INDEX_DATA_VERSION, ' ', 1);
    return 0;
}

int binlog_get_first_timestamp(const char *filename, time_t *timestamp)
{
    char buff[FS_BINLOG_MAX_RECORD_SIZE];
    char error_info[256];
    string_t line;
    uint64_t data_version;
    int result;

    if ((result=fc_get_first_line(filename, buff,
                    sizeof(buff), &line)) != 0)
    {
        return result;
    }

    if ((result=binlog_unpack_ts_and_dv(&line, timestamp,
                    &data_version, error_info)) != 0)
    {
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
    uint64_t data_version;
    int result;

    if ((result=fc_get_last_line(filename, buff,
                    sizeof(buff), &file_size, &line)) != 0)
    {
        return result;
    }

    if ((result=binlog_unpack_ts_and_dv(&line, timestamp,
                    &data_version, error_info)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "binlog file: %s, unpack last line fail, %s",
                __LINE__, filename, error_info);
    }

    return result;
}

static int get_start_binlog_index_by_timestamp(const char *subdir_name,
        const time_t from_timestamp, int *binlog_index)
{
    char filename[PATH_MAX];
    int result;
    time_t timestamp;

    while (*binlog_index >= 0) {
        binlog_reader_get_filename(subdir_name, *binlog_index,
                filename, sizeof(filename));

        if ((result=binlog_get_first_timestamp(filename, &timestamp)) != 0) {
            return result;
        }

        if (timestamp < from_timestamp) {
            break;
        }

        if (*binlog_index == 0) {
            break;
        }
        (*binlog_index)--;
    }

    return 0;
}

int binlog_get_position_by_timestamp(const char *subdir_name,
        struct binlog_writer_info *writer, const time_t from_timestamp,
        FSBinlogFilePosition *pos)
{
    int result;
    int binlog_index;
    ServerBinlogReader reader;

    binlog_index = binlog_get_current_write_index(writer);
    if ((result=get_start_binlog_index_by_timestamp(subdir_name,
                    from_timestamp, &binlog_index)) != 0)
    {
        return result;
    }

    pos->index = binlog_index;
    pos->offset = 0;
    if ((result=binlog_reader_init(&reader, subdir_name, writer, pos)) != 0) {
        return result;
    }

    //TODO

    binlog_reader_destroy(&reader);
    return result;
}
