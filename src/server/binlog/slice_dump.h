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


#ifndef _FS_STORE_SLICE_DUMP_H
#define _FS_STORE_SLICE_DUMP_H

#include "../server_types.h"

typedef const char *(*slice_dump_get_filename_func)(
        const int binlog_index, char *filename, const int size);

#ifdef __cplusplus
extern "C" {
#endif

int slice_dump_to_files(slice_dump_get_filename_func get_remove_filename_func,
        slice_dump_get_filename_func get_keep_filename_func, const int source,
        const int64_t total_slice_count, int *binlog_file_count);

#ifdef __cplusplus
}
#endif

#endif
