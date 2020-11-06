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

//binlog_func.h

#ifndef _BINLOG_FUNC_H_
#define _BINLOG_FUNC_H_

#include "sf/sf_func.h"
#include "sf/sf_binlog_writer.h"
#include "binlog_types.h"
#include "../server_global.h"

#ifdef __cplusplus
extern "C" {
#endif

int binlog_get_first_timestamp(const char *filename, time_t *timestamp);

int binlog_get_last_timestamp(const char *filename, time_t *timestamp);

int binlog_unpack_common_fields(const string_t *line,
        BinlogCommonFields *fields, char *error_info);

int binlog_get_position_by_timestamp(const char *subdir_name,
        struct sf_binlog_writer_info *writer, const time_t from_timestamp,
        SFBinlogFilePosition *pos);

static inline int binlog_buffer_init(SFBinlogBuffer *buffer)
{
    const int size = BINLOG_BUFFER_SIZE;
    return sf_binlog_buffer_init(buffer, size);
}

#ifdef __cplusplus
}
#endif

#endif
