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
#include "../fs_api_types.h"
#include "timeout_handler.h"
#include "obid_htable.h"

typedef struct {
    volatile int waiting_slice_count;
    struct fc_queue queue;
    FCThreadPool thread_pool;
    volatile bool continue_flag;
} CombineHandlerContext;

#ifdef __cplusplus
extern "C" {
#endif

    extern CombineHandlerContext g_combine_handler_ctx;

    int combine_handler_init(const int thread_limit,
            const int min_idle_count, const int max_idle_time);

    void combine_handler_terminate();

    static inline int combine_handler_push(FSAPISliceEntry *slice)
    {
        int result;

        if ((result=fs_api_swap_slice_stage(slice,
                        FS_API_COMBINED_WRITER_STAGE_MERGING,
                        FS_API_COMBINED_WRITER_STAGE_PROCESSING)) == 0)
        {
            __sync_add_and_fetch(&g_combine_handler_ctx.waiting_slice_count, 1);
            fc_queue_push(&g_combine_handler_ctx.queue, slice);
        }

        /*
        logInfo("combine_handler_push result: %d, timer status: %d, "
                "slice stage: %d", result, slice->timer.status, slice->stage);
                */
        return result;
    }

    static inline void combine_handler_push_within_lock(FSAPISliceEntry *slice)
    {
        int result;

        slice->stage = FS_API_COMBINED_WRITER_STAGE_PROCESSING;
        if ((result=timeout_handler_remove(&slice->timer)) != 0) {
            logWarning("timeout_handler_remove %p errno: %d, error info: %s, "
                    "timer status: %d, slice stage: %d", slice, result,
                    strerror(result), slice->timer.status, slice->stage);
        }
        __sync_add_and_fetch(&g_combine_handler_ctx.waiting_slice_count, 1);
        fc_queue_push(&g_combine_handler_ctx.queue, slice);
    }

#ifdef __cplusplus
}
#endif

#endif
