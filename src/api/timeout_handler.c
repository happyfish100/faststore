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

#include <stdlib.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "sf/sf_global.h"
#include "combine_handler.h"
#include "timeout_handler.h"

TimeHandlerContext g_timer_ms_ctx;

#define SET_CURRENT_TIME_TICKS()  \
    g_timer_ms_ctx.current_time_ms = get_current_time_ms(); \
    g_timer_ms_ctx.current_time_ticks = g_timer_ms_ctx.current_time_ms / \
        g_timer_ms_ctx.precision_ms

static void deal_timeouts(FastTimerEntry *head)
{
    FastTimerEntry *entry;
    FastTimerEntry *current;

    entry = head->next;
    while (entry != NULL) {
        current = entry;
        entry = entry->next;

        current->prev = current->next = NULL;
        combine_handler_push((FSAPISliceEntry *)current);
    }
}

void timeout_handler_terminate()
{
    int i;
    int count;
    int64_t time_ticks;
    FastTimerEntry head;

    i = 0;
    while (g_fs_api_ctx.write_combine.enabled && i++ < 100) {
        fc_sleep_ms(10);
    }

    time_ticks = g_timer_ms_ctx.current_time_ticks +
        g_fs_api_ctx.write_combine.max_wait_time_ms /
        g_timer_ms_ctx.precision_ms + 1;
    count = fast_timer_timeouts_get(&g_timer_ms_ctx.timer, time_ticks, &head);
    if (count > 0) {
        logInfo("on terminate, current_time_ms: %"PRId64", timeout count: %d",
                g_timer_ms_ctx.current_time_ms, count);
        deal_timeouts(&head);
    }
}

static void *timeout_handler_thread_func(void *arg)
{
    int64_t last_time_ticks;
    FastTimerEntry head;
    int count;
    int half_precision_ms;

    last_time_ticks = 0;
    half_precision_ms = (g_timer_ms_ctx.precision_ms + 1) / 2;
    while (SF_G_CONTINUE_FLAG) {
        SET_CURRENT_TIME_TICKS();
        if (g_timer_ms_ctx.current_time_ticks != last_time_ticks) {
            last_time_ticks = g_timer_ms_ctx.current_time_ticks;
            count = timeout_handler_timeouts_get(&head);
            if (count > 0) {
                logInfo("current_time_ms: %"PRId64", timeout count: %d",
                        g_timer_ms_ctx.current_time_ms, count);
                deal_timeouts(&head);
            }
        }

        fc_sleep_ms(1);
    }

    return NULL;
}

int timeout_handler_init(const int precision_ms, const int max_timeout_ms)
{
    int result;
    int slot_count;
    pthread_t tid;

    g_timer_ms_ctx.precision_ms = precision_ms;
    SET_CURRENT_TIME_TICKS();
    slot_count = fc_ceil_prime(max_timeout_ms / precision_ms);
    if ((result=fast_timer_init_ex(&g_timer_ms_ctx.timer, slot_count,
                    g_timer_ms_ctx.current_time_ticks, true)) != 0)
    {
        return result;
    }

    return fc_create_thread(&tid, timeout_handler_thread_func, NULL,
            SF_G_THREAD_STACK_SIZE);
}
