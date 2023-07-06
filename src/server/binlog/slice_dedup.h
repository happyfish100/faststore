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


#ifndef _SLICE_DEDUP_H
#define _SLICE_DEDUP_H

#include "fastcommon/sched_thread.h"
#include "binlog_types.h"
#include "../storage/object_block_index.h"
#include "../server_global.h"

#define FS_SLICE_DEDUP_CALL_BY_CRONTAB  'C'
#define FS_SLICE_DEDUP_CALL_BY_MIGRATE  'M'

#ifdef __cplusplus
extern "C" {
#endif

    int slice_dedup_redo(const char caller);

    int slice_dedup_add_schedule();

    int slice_dedup_binlog_ex(const char caller, const int64_t slice_count);

    static inline int slice_dedup_binlog()
    {
        int64_t slice_count;
        slice_count = ob_index_get_total_slice_count();
        return slice_dedup_binlog_ex(FS_SLICE_DEDUP_CALL_BY_MIGRATE,
                slice_count);
    }

#ifdef __cplusplus
}
#endif

#endif
