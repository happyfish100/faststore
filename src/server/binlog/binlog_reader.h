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
    binlog_reader_init1_ex(reader, subdir_name, "", write_index, pos)

int binlog_reader_init_ex(ServerBinlogReader *reader,
        const char *subdir_name, const char *fname_suffix,
        SFBinlogWriterInfo *writer, const SFBinlogFilePosition *pos);

int binlog_reader_init1_ex(ServerBinlogReader *reader,
        const char *subdir_name, const char *fname_suffix,
        const int write_index, const SFBinlogFilePosition *pos);

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
    snprintf(full_filename, size, "%s/%s/%s"SF_BINLOG_FILE_EXT_FMT"%s",
            DATA_PATH_STR, subdir_name, SF_BINLOG_FILE_PREFIX,
            binlog_index, fname_suffix);
}

int binlog_reader_integral_read(ServerBinlogReader *reader, char *buff,
        const int size, int *read_bytes);

bool binlog_reader_is_last_file(ServerBinlogReader *reader);

#ifdef __cplusplus
}
#endif

#endif
