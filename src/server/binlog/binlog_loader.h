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


#ifndef _BINLOG_LOADER_H
#define _BINLOG_LOADER_H

#include "../../common/fs_types.h"
#include "binlog_read_thread.h"

#define BINLOG_GET_FILENAME_LINE_COUNT(r, subdir_name, \
        binlog_filename, line_str, line_count) \
    do { \
        binlog_reader_get_filename(subdir_name, \
                r->binlog_position.index, binlog_filename,   \
                sizeof(binlog_filename));  \
        fc_get_file_line_count_ex(binlog_filename, r->binlog_position.offset + \
                (line_str - r->buffer.buff), &line_count); \
        line_count++;   \
    } while (0)


#define BINLOG_PARSE_INT_EX(subdir_name, var, caption, index, endchr, min_val) \
    do {   \
        var = strtol(cols[index].str, &endptr, 10);  \
        if (*endptr != endchr || var < min_val) {    \
            BINLOG_GET_FILENAME_LINE_COUNT(r, subdir_name, binlog_filename, \
                    line->str, line_count);  \
            logError("file: "__FILE__", line: %d, "  \
                    "binlog file %s, line no: %"PRId64", " \
                    "invalid %s: %.*s", __LINE__,          \
                    binlog_filename, line_count,  \
                    caption, cols[index].len, cols[index].str); \
            return EINVAL;  \
        }  \
    } while (0)


#define BINLOG_PARSE_INT(subdir_name, var, index, endchr, min_val)  \
    BINLOG_PARSE_INT_EX(subdir_name, var, #var, index, endchr, min_val)


#define BINLOG_PARSE_INT_SILENCE(var, caption, index, endchr, min_val) \
    do {   \
        var = strtol(cols[index].str, &endptr, 10);  \
        if (*endptr != endchr || var < min_val) {    \
            sprintf(error_info, "invalid %s: %.*s",  \
                    caption, cols[index].len, cols[index].str); \
            return EINVAL;  \
        }  \
    } while (0)

#define BINLOG_PARSE_INT_SILENCE2(var, caption, index, echr1, echr2, min_val) \
    do {   \
        var = strtol(cols[index].str, &endptr, 10);  \
        if (!(*endptr == echr1 || *endptr == echr2) || (var < min_val)) { \
            sprintf(error_info, "invalid %s: %.*s",  \
                    caption, cols[index].len, cols[index].str); \
            return EINVAL;  \
        }  \
    } while (0)


typedef int (*binlog_parse_line_func)(BinlogReadThreadResult *r, \
        string_t *line);

#ifdef __cplusplus
extern "C" {
#endif

    int binlog_loader_load(const char *subdir_name,
            struct sf_binlog_writer_info *writer,
            binlog_parse_line_func parse_line);


#ifdef __cplusplus
}
#endif

#endif
