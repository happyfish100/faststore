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


#ifndef _STORE_PATH_INDEX_H
#define _STORE_PATH_INDEX_H

#include <limits.h>
#include "../../common/fs_types.h"
#include "storage_config.h"

typedef struct {
    int index;
    char path[PATH_MAX];
    char mark[64];
} StorePathEntry;

#ifdef __cplusplus
extern "C" {
#endif

    int store_path_index_init();

    void store_path_index_destroy();

    int store_path_index_count();

    int store_path_index_max();

    int store_path_check_mark(StorePathEntry *pentry, bool *regenerated);

    StorePathEntry *store_path_index_get(const char *path);

    int store_path_index_add(const char *path, int *index);

    int store_path_index_save();

#ifdef __cplusplus
}
#endif

#endif
