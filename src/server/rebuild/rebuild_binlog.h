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

#ifdef __cplusplus
extern "C" {
#endif

    static inline char *rebuild_binlog_get_subdir_name(const char *name,
            const int tindex, char *subdir_name, const int size)
    {
        snprintf(subdir_name, size, "%s/%s/%d",
                FS_REBUILD_BINLOG_SUBDIR_NAME,
                name, tindex + 1);
        return subdir_name;
    }

    static inline char *rebuild_binlog_get_repaly_subdir_name(
            const char *name, const int tindex,
            char *subdir_name, const int size)
    {
        snprintf(subdir_name, size, "%s/%s/%s/%d",
                FS_REBUILD_BINLOG_SUBDIR_NAME,
                REBUILD_BINLOG_SUBDIR_NAME_REPLAY,
                name, tindex + 1);
        return subdir_name;
    }

    static inline int rebuild_binlog_log_to_buff(const uint64_t sn,
            const char op_type, const FSBlockKey *bkey,
            const FSSliceSize *ssize, char *buff)
    {
        return sprintf(buff, "%"PRId64" %c %"PRId64" %"PRId64" %d %d\n",
                sn, op_type, bkey->oid, bkey->offset,
                ssize->offset, ssize->length);
    }

    int rebuild_binlog_parse_line(ServerBinlogReader *reader,
            BufferInfo *buffer, const string_t *line, int64_t *sn,
            char *op_type, FSBlockSliceKeyInfo *bs_key);

#ifdef __cplusplus
}
#endif

#endif
