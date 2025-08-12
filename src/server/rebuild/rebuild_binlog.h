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


#ifndef _FS_REBUILD_BINLOG_H
#define _FS_REBUILD_BINLOG_H

#include "../server_types.h"
#include "../binlog/binlog_reader.h"
#include "rebuild_types.h"

#define FS_REBUILD_BINLOG_MAX_RECORD_SIZE  128

typedef struct rebuild_binlog_record {
    char op_type;
    FSBlockSliceKeyInfo bs_key;
} RebuildBinlogRecord;

#define REBUILD_BINLOG_GET_FILENAME_LINE_COUNT( \
        reader, buffer, line_str, line_count) \
    do { \
        fc_get_file_line_count_ex((reader)->filename, \
                (reader)->position.offset - ((buffer)->length - \
                    (line_str - (buffer)->buff)), &line_count); \
        line_count++; \
    } while (0)

#ifdef __cplusplus
extern "C" {
#endif

    static inline char *rebuild_binlog_get_subdir_name(const char *name,
            const int tindex, char *subdir_name, const int size)
    {
        char *p;
        int name_len;

        name_len = strlen(name);
        if (FS_REBUILD_BINLOG_SUBDIR_NAME_LEN + name_len + 12 > size) {
            snprintf(subdir_name, size, "%s/%s/%d",
                    FS_REBUILD_BINLOG_SUBDIR_NAME_STR,
                    name, tindex + 1);
        } else {
            p = subdir_name;
            memcpy(p, FS_REBUILD_BINLOG_SUBDIR_NAME_STR,
                    FS_REBUILD_BINLOG_SUBDIR_NAME_LEN);
            p += FS_REBUILD_BINLOG_SUBDIR_NAME_LEN;
            *p++ = '/';
            memcpy(p, name, name_len);
            p += name_len;
            *p++ = '/';
            p += fc_itoa(tindex + 1, p);
            *p = '\0';
        }
        return subdir_name;
    }

    static inline int rebuild_binlog_log_to_buff(
            const char op_type, const FSBlockKey *bkey,
            const FSSliceSize *ssize, char *buff)
    {
        char *p;

        p = buff;
        *p++ = op_type;
        *p++ = ' ';
        p += fc_itoa(bkey->oid, p);
        *p++ = ' ';
        p += fc_itoa(bkey->offset, p);
        *p++ = ' ';
        p += fc_itoa(ssize->offset, p);
        *p++ = ' ';
        p += fc_itoa(ssize->length, p);
        *p++ = '\n';
        *p = '\0';
        return p - buff;
    }

    int rebuild_binlog_record_unpack(const string_t *line,
            RebuildBinlogRecord *record, char *error_info);

    int rebuild_binlog_parse_line(ServerBinlogReader *reader,
            BufferInfo *buffer, const string_t *line,
            RebuildBinlogRecord *record);

#ifdef __cplusplus
}
#endif

#endif
