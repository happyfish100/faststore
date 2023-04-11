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


#ifndef _FS_SLICE_SPACE_MIGRATE_H
#define _FS_SLICE_SPACE_MIGRATE_H

#include "binlog_types.h"

#ifdef __cplusplus
extern "C" {
#endif

    int slice_space_migrate_create(const char *subdir_name,
            const int binlog_index, const bool dump_slice,
            const DABinlogOpType op_type);

    int slice_space_migrate_redo(const char *subdir_name);

#ifdef __cplusplus
}
#endif

#endif
