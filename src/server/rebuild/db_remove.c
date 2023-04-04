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
#include "../binlog/binlog_reader.h"
#include "db_remove.h"

int db_remove_slices(const char *subdir_name, const int write_index)
{
    int result;
    ServerBinlogReader reader;
    SFBinlogFilePosition pos;

    //TODO find position
    if ((result=binlog_reader_init1(&reader, subdir_name,
                    write_index, &pos)) != 0)
    {
        return result;
    }

    /*
    while ((result=binlog_reader_integral_read(&reader,
                    thread->rbuffer.buff, thread->rbuffer.alloc_size,
                    &thread->rbuffer.length)) == 0 && SF_G_CONTINUE_FLAG)
    {
        if ((result=parse_buffer(thread)) != 0) {
            return result;
        }
    }
    */

    /*
       OBEntry *ob_index_get_ob_entry_ex(&g_ob_hashtable,
        const FSBlockKey *bkey, const bool create_flag);

       int change_notify_push_del_slice(const int64_t sn,
       OBEntry *ob, const FSSliceSize *ssize);
     */

    binlog_reader_destroy(&reader);
    return 0;
}
