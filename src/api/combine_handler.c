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
#include "sf/sf_func.h"
#include "combine_handler.h"

CombineHandlerContext g_combine_handler_ctx;

static void combine_handler_run(void *arg, void *thread_data)
{
    FSAPISliceEntry *slice;
    int write_bytes;
    int inc_alloc;
    int result;

    slice = (FSAPISliceEntry *)arg;
    if ((result=fs_client_slice_write(g_fs_api_ctx.fs, &slice->bs_key,
            slice->buff, &write_bytes, &inc_alloc)) != 0)
    {
        sf_terminate_myself();
        return;
    }

    //TODO notify finish and cleanup
}

static void *combine_handler_thread_func(void *arg)
{
    FSAPISliceEntry *slice;
    FSAPISliceEntry *current;
    int result;

    while (SF_G_CONTINUE_FLAG) {
        slice = (FSAPISliceEntry *)fc_queue_pop_all(
                &g_combine_handler_ctx.queue);
        if (slice == NULL) {
            continue;
        }

        do {
            current = slice;
            slice = slice->next;

            if ((result=fc_thread_pool_run(&g_combine_handler_ctx.thread_pool,
                            combine_handler_run, current)) != 0)
            {
                break;
            }
        } while (slice != NULL);
    }

    return NULL;
}

int combine_handler_init(const int thread_limit,
        const int min_idle_count, const int max_idle_time)
{
    int result;
    pthread_t tid;

    if ((result=fc_queue_init(&g_combine_handler_ctx.queue, (long)
                    (&((FSAPISliceEntry *)NULL)->next))) != 0)
    {
        return result;
    }

    if ((result=fc_thread_pool_init(&g_combine_handler_ctx.thread_pool,
                    "merged slice dealer", thread_limit,
                    SF_G_THREAD_STACK_SIZE, max_idle_time,
                    min_idle_count, (bool *)&SF_G_CONTINUE_FLAG)) != 0)
    {
        return result;
    }

    return fc_create_thread(&tid, combine_handler_thread_func, NULL,
            SF_G_THREAD_STACK_SIZE);
}
