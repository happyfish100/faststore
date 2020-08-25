//binlog_reader.h

#ifndef _BINLOG_READER_H_
#define _BINLOG_READER_H_

#include "binlog_types.h"

struct binlog_writer_info;

typedef struct server_binlog_reader {
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];
    char fname_suffix[FS_BINLOG_FILENAME_SUFFIX_SIZE];
    struct binlog_writer_info *writer;  //for get current write index
    char filename[PATH_MAX];
    int fd;
    FSBinlogFilePosition position;
    ServerBinlogBuffer binlog_buffer;
} ServerBinlogReader;

#ifdef __cplusplus
extern "C" {
#endif

#define binlog_reader_init(reader, subdir_name, writer, pos) \
    binlog_reader_init_ex(reader, subdir_name, "", writer, pos)

int binlog_reader_init_ex(ServerBinlogReader *reader, const char *subdir_name,
        const char *fname_suffix, struct binlog_writer_info *writer,
        const FSBinlogFilePosition *pos);

void binlog_reader_destroy(ServerBinlogReader *reader);

int binlog_reader_read(ServerBinlogReader *reader);

#define binlog_reader_get_filename(subdir_name, binlog_index, \
        full_filename, size) \
    binlog_reader_get_filename_ex(subdir_name, "", binlog_index, \
        full_filename, size)

static inline void binlog_reader_get_filename_ex(const char *subdir_name,
        const char *fname_suffix, const int binlog_index,
        char *full_filename, const int size)
{
    snprintf(full_filename, size, "%s/%s/%s"BINLOG_FILE_EXT_FMT"%s",
            DATA_PATH_STR, subdir_name, BINLOG_FILE_PREFIX,
            binlog_index, fname_suffix);
}

int binlog_reader_integral_read(ServerBinlogReader *reader, char *buff,
        const int size, int *read_bytes);

bool binlog_reader_is_last_file(ServerBinlogReader *reader);

#ifdef __cplusplus
}
#endif

#endif
