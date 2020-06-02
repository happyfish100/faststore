//binlog_reader.h

#ifndef _BINLOG_READER_H_
#define _BINLOG_READER_H_

#include "binlog_types.h"

typedef int (*get_current_write_index_func)();

typedef struct {
    const char *subdir_name;
    get_current_write_index_func get_current_write_index;
    char filename[PATH_MAX];
    int fd;
    FSBinlogFilePosition position;
    ServerBinlogBuffer binlog_buffer;
} ServerBinlogReader;

#ifdef __cplusplus
extern "C" {
#endif

int binlog_reader_init(ServerBinlogReader *reader, const char *subdir_name,
        get_current_write_index_func get_current_write_index,
        const FSBinlogFilePosition *position);

void binlog_reader_destroy(ServerBinlogReader *reader);

int binlog_reader_read(ServerBinlogReader *reader);

static inline void binlog_reader_get_filename(const char *subdir_name,
        const int binlog_index, char *full_filename, const int size)
{
    snprintf(full_filename, size, "%s/%s/%s"BINLOG_FILE_EXT_FMT,
            DATA_PATH_STR, subdir_name, BINLOG_FILE_PREFIX, binlog_index);
}

int binlog_reader_integral_read(ServerBinlogReader *reader, char *buff,
        const int size, int *read_bytes);

#ifdef __cplusplus
}
#endif

#endif
