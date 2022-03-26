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


#ifndef _FS_STORE_PATH_REBUILD_H
#define _FS_STORE_PATH_REBUILD_H

#include "../server_types.h"
#include "rebuild_types.h"

#ifdef __cplusplus
extern "C" {
#endif

    int store_path_rebuild_dump_data(const int64_t total_trunk_count,
            const int64_t total_slice_count);

    // finish binlog dump
    int store_path_rebuild_redo_step1();

    //split binlog and replay
    int store_path_rebuild_redo_step2();

#ifdef __cplusplus
}
#endif

#endif
