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

#ifndef _TIME_HANDLER_H
#define _TIME_HANDLER_H

#include "fastcommon/fast_timer.h"
#include "fs_api_types.h"

#ifdef __cplusplus
extern "C" {
#endif

    extern volatile int64_t g_current_time_ms;

    int time_handler_init(const int precision_ms, const int slot_count);
    int time_handler_add(FastTimerEntry *entry);
    int time_handler_remove(FastTimerEntry *entry);
    int time_handler_modify(FastTimerEntry *entry, const int64_t new_expires);
    int time_handler_timeouts_get(FastTimerEntry *head);

#ifdef __cplusplus
}
#endif

#endif
