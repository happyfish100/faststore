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

#include "../server_global.h"
#include "../server_func.h"
#include "slice_binlog.h"
#include "slice_space_migrate.h"
#include "trunk_migrate.h"

int trunk_migrate_redo()
{
    int result;
    bool need_restart;

    if ((result=slice_space_migrate_redo(FS_TRUNK_BINLOG_SUBDIR_NAME_STR,
                    &need_restart)) != 0)
    {
        return result;
    }

    if (need_restart) {
        fs_server_restart("slice space migrate done for trunk migrate");
        return EINTR;
    } else {
        return 0;
    }
}

static inline int migrate_create(const bool dump_slice)
{
    int binlog_index;

    binlog_index = slice_binlog_get_binlog_start_index();
    return slice_space_migrate_create(FS_TRUNK_BINLOG_SUBDIR_NAME_STR,
            binlog_index, dump_slice, da_binlog_op_type_consume_space,
            FS_SLICE_BINLOG_IN_SYSTEM_SUBDIR);
}

int trunk_migrate_create()
{
    const bool dump_slice = true;
    return migrate_create(dump_slice);
}

int trunk_migrate_slice_dedup_done_callback()
{
    const bool dump_slice = false;
    return migrate_create(dump_slice);
}
