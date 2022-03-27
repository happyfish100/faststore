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


#ifndef _FS_REBUILD_BINLOG_SPLITER_H
#define _FS_REBUILD_BINLOG_SPLITER_H

#include "../server_types.h"
#include "../binlog/binlog_reader.h"

#ifdef __cplusplus
extern "C" {
#endif

    int binlog_spliter_do(ServerBinlogReaderArray *rda,
            const int read_threads, const int split_count,
            int64_t *slice_count);

#ifdef __cplusplus
}
#endif

#endif
