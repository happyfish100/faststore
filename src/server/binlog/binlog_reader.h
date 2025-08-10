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

//binlog_reader.h

#ifndef _BINLOG_READER_H_
#define _BINLOG_READER_H_

#include "sf/sf_binlog_writer.h"
#include "../server_global.h"
#include "binlog_types.h"

typedef enum {
    binlog_index_type_writer_ptr,
    binlog_index_type_index_val
} BinlogIndexType;

typedef struct server_binlog_info {
    BinlogIndexType type;
    union {
        SFBinlogWriterInfo *writer;  //for get current write index
        int write_index;
    };
} ServerBinlogInfo; 

typedef struct server_binlog_reader {
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];
    char fname_suffix[FS_BINLOG_FILENAME_SUFFIX_SIZE];
    ServerBinlogInfo binlog_info;
    int fd;
    char filename[PATH_MAX];
    SFBinlogFilePosition position;
    SFBinlogBuffer binlog_buffer;
} ServerBinlogReader;

typedef struct server_binlog_reader_array {
    ServerBinlogReader *readers;
    int count;
} ServerBinlogReaderArray;

#ifdef __cplusplus
extern "C" {
#endif

#define binlog_reader_init(reader, subdir_name, writer, pos) \
    binlog_reader_init_ex(reader, subdir_name, "", writer, pos)

#define binlog_reader_init1(reader, subdir_name, write_index, pos) \
    binlog_reader_init1_ex(reader, subdir_name, "", write_index,   \
            pos, BINLOG_BUFFER_SIZE)

int binlog_reader_init_ex(ServerBinlogReader *reader,
        const char *subdir_name, const char *fname_suffix,
        SFBinlogWriterInfo *writer, const SFBinlogFilePosition *pos);

int binlog_reader_init1_ex(ServerBinlogReader *reader,
        const char *subdir_name, const char *fname_suffix,
        const int write_index, const SFBinlogFilePosition *pos,
        const int buffer_size);

void binlog_reader_destroy(ServerBinlogReader *reader);

int binlog_reader_read(ServerBinlogReader *reader);

static inline SFBinlogWriterInfo *binlog_reader_get_writer(
        ServerBinlogReader *reader)
{
    if (reader->binlog_info.type == binlog_index_type_writer_ptr) {
        return reader->binlog_info.writer;
    } else {
        return NULL;
    }
}

#define binlog_reader_get_filename(subdir_name, binlog_index, \
        full_filename, size) \
    binlog_reader_get_filename_ex(subdir_name, "", binlog_index, \
        full_filename, size)

static inline void binlog_reader_get_filename_ex(const char *subdir_name,
        const char *fname_suffix, const int binlog_index,
        char *full_filename, const int size)
{
    int file_len;
    int suffix_len;

    sf_file_writer_get_filename(DATA_PATH_STR, subdir_name,
            binlog_index, full_filename, size);
    if (*fname_suffix != '\0') {
        file_len = strlen(full_filename);
        suffix_len = strlen(fname_suffix);
        if (file_len + suffix_len >= size) {
            suffix_len = size - file_len - 1;
        }
        if (suffix_len > 0) {
            memcpy(full_filename + file_len, fname_suffix, suffix_len);
            *(full_filename + (file_len + suffix_len)) = '\0';
        }
    }
}

int binlog_reader_read_to_buffer(ServerBinlogReader *reader,
        char *buff, const int size, int *read_bytes);

int binlog_reader_integral_read(ServerBinlogReader *reader,
        char *buff, const int size, int *read_bytes);

int binlog_reader_integral_full_read(ServerBinlogReader *reader,
        char *buff, const int size, int *read_bytes);

bool binlog_reader_is_last_file(ServerBinlogReader *reader);

#ifdef __cplusplus
}
#endif

#endif
