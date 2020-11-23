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

#ifndef _COMBINE_HANDLER_H
#define _COMBINE_HANDLER_H

#include "fastcommon/fc_queue.h"
#include "fastcommon/thread_pool.h"
#include "fs_api_types.h"

typedef struct {
    struct fc_queue queue;
    FCThreadPool thread_pool;
} CombineHandlerContext;

#ifdef __cplusplus
extern "C" {
#endif

    extern CombineHandlerContext g_combine_handler_ctx;

    int combine_handler_init(const int thread_limit,
            const int min_idle_count, const int max_idle_time);

    static inline int combine_handler_push(FSAPISliceEntry *slice)
    {
        if (__sync_bool_compare_and_swap(&slice->in_queue, 0, 1)) {
            fc_queue_push(&g_combine_handler_ctx.queue, slice);
            return 0;
        } else {
            return EINPROGRESS;
        }
    }

#ifdef __cplusplus
}
#endif

#endif
