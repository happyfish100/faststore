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
#include "time_handler.h"

typedef struct {
    int precision_ms;
    FastTimer timer;
} TimeHandlerContext;

volatile int64_t g_current_time_ms;
static TimeHandlerContext time_ctx;

static void *time_handler_thread_func(void *arg)
{
    while (SF_G_CONTINUE_FLAG) {
        g_current_time_ms = get_current_time_ms();
        fc_sleep_ms(time_ctx.precision_ms);
    }

    return NULL;
}

int time_handler_init(const int precision_ms, const int slot_count)
{
    int result;
    pthread_t tid;

    g_current_time_ms = get_current_time_ms();
    if ((result=fast_timer_init(&time_ctx.timer, slot_count,
                    g_current_time_ms)) != 0)
    {
        return result;
    }

    time_ctx.precision_ms = precision_ms;
    return fc_create_thread(&tid, time_handler_thread_func, NULL,
            SF_G_THREAD_STACK_SIZE);
}
