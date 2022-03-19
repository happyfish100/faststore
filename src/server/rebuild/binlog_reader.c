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

#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "sf/sf_func.h"
#include "../../common/fs_func.h"
#include "../storage/slice_op.h"
#include "../server_global.h"
#include "rebuild_binlog.h"
#include "binlog_reader.h"

int rebuild_binlog_reader_init(ServerBinlogReader *reader,
        const char *name, const int binlog_index)
{
    int result;
    int write_index;
    char subdir_name[64];
    SFBinlogFilePosition position;

    rebuild_binlog_get_subdir_name(name, binlog_index,
            subdir_name, sizeof(subdir_name));
    if ((result=sf_binlog_writer_get_binlog_index(DATA_PATH_STR,
                    subdir_name, &write_index)) != 0)
    {
        return result;
    }

    //TODO set position
    return binlog_reader_init1(reader, subdir_name,
            write_index, &position);
}
