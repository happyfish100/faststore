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

#ifndef _TIMEOUT_HANDLER_H
#define _TIMEOUT_HANDLER_H

#include "fs_api_types.h"

typedef struct {
    int precision_ms;
    volatile int64_t current_time_ms;
    volatile int64_t current_time_ticks;  //unit: precision_ms
    FastTimer timer;
} TimeHandlerContext;

#ifdef __cplusplus
extern "C" {
#endif

    extern TimeHandlerContext g_timer_ms_ctx;

    int timeout_handler_init(const int precision_ms, const int max_timeout_ms);

    void timeout_handler_terminate();

#define TIMEOUT_TO_EXPIRES(timeout)  \
    (g_timer_ms_ctx.current_time_ticks + timeout / g_timer_ms_ctx.precision_ms)

#define timeout_handler_add(entry, timeout) \
    fast_timer_add_ex(&g_timer_ms_ctx.timer, entry, \
            TIMEOUT_TO_EXPIRES(timeout), true)

#define timeout_handler_remove(entry) \
    fast_timer_remove(&g_timer_ms_ctx.timer, entry)

#define timeout_handler_modify(entry, new_timeout) \
    fast_timer_modify(&g_timer_ms_ctx.timer, entry, \
            TIMEOUT_TO_EXPIRES(new_timeout))

#define timeout_handler_timeouts_get(head) \
    fast_timer_timeouts_get(&g_timer_ms_ctx.timer, \
            g_timer_ms_ctx.current_time_ticks, head)

#ifdef __cplusplus
}
#endif

#endif
