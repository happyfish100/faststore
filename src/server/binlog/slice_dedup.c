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
#include "fastcommon/fc_atomic.h"
#include "../../common/fs_func.h"
#include "slice_dedup.h"

static int slice_dedup_func(void *args)
{
    static volatile bool dedup_in_progress = false;

    if (!dedup_in_progress) {
        dedup_in_progress = true;

        //TODO

        dedup_in_progress = false;
    }

    return 0;
}

int slice_dedup_redo()
{
    return 0;
}

int slice_dedup_add_schedule()
{
    ScheduleArray scheduleArray;
    ScheduleEntry scheduleEntry;

    if (!SLICE_DEDUP_ENABLED) {
        return 0;
    }

    INIT_SCHEDULE_ENTRY_EX1(scheduleEntry, sched_generate_next_id(),
            SLICE_DEDUP_TIME, 86400, slice_dedup_func, NULL, true);
    scheduleArray.entries = &scheduleEntry;
    scheduleArray.count = 1;
    return sched_add_entries(&scheduleArray);
}
