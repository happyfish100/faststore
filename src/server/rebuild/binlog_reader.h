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


#ifndef _FS_REBUILD_BINLOG_READER_H
#define _FS_REBUILD_BINLOG_READER_H

#include "../server_types.h"

#ifdef __cplusplus
extern "C" {
#endif

    int rebuild_binlog_reader_init(ServerBinlogReader *reader,
            const char *name, const int binlog_index);

#ifdef __cplusplus
}
#endif

#endif
