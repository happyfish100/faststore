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
#include "obid_htable.h"
#include "otid_htable.h"
#include "combine_handler.h"

CombineHandlerContext g_combine_handler_ctx;

static void combine_handler_run(void *arg, void *thread_data)
{
    FSAPISliceEntry *slice;
    /*
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
    */

    slice = (FSAPISliceEntry *)arg;
    /*
    logInfo("slice write block {oid: %"PRId64", offset: %"PRId64"}, "
            "slice {offset: %d, length: %d}, merged slices: %d",
            slice->bs_key.block.oid, slice->bs_key.block.offset,
            slice->bs_key.slice.offset, slice->bs_key.slice.length,
            slice->merged_slices);
            */

    //TODO notify finish and cleanup
    otid_htable_release_slice(slice);
}

static inline void deal_slices(FSAPISliceEntry *head)
{
    FSAPISliceEntry *current;
    do {
        current = head;
        head = head->next;

        fc_thread_pool_run(&g_combine_handler_ctx.thread_pool,
                combine_handler_run, current);
    } while (head != NULL);
}

void combine_handler_terminate()
{
    FSAPISliceEntry *head;
    int i;

    head = (FSAPISliceEntry *)fc_queue_try_pop_all(
            &g_combine_handler_ctx.queue);
    if (head != NULL) {
        deal_slices(head);
        for (i=0; i<=10; i++) {
            if (fc_thread_pool_dealing_count(
                        &g_combine_handler_ctx.thread_pool) > 0)
            {
                break;
            }
            fc_sleep_ms(10);
        }
    } else {
        fc_sleep_ms(30);
    }

    //waiting for thread finish
    g_combine_handler_ctx.continue_flag = false;
    while (fc_thread_pool_dealing_count(
                &g_combine_handler_ctx.thread_pool) > 0)
    {
        fc_sleep_ms(10);
    }

    fc_sleep_ms(100);
    logInfo("combine_handler_terminate, running: %d",
            fc_thread_pool_running_count(
                &g_combine_handler_ctx.thread_pool));
}

static void *combine_handler_thread_func(void *arg)
{
    FSAPISliceEntry *head;

    while (SF_G_CONTINUE_FLAG) {
        head = (FSAPISliceEntry *)fc_queue_pop_all(
                &g_combine_handler_ctx.queue);
        if (head != NULL) {
            deal_slices(head);
        }
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

    g_combine_handler_ctx.continue_flag = true;
    if ((result=fc_thread_pool_init(&g_combine_handler_ctx.
                    thread_pool, "merged slice dealer",
                    thread_limit, SF_G_THREAD_STACK_SIZE,
                    max_idle_time, min_idle_count, (bool *)
                    &g_combine_handler_ctx.continue_flag)) != 0)
    {
        return result;
    }

    return fc_create_thread(&tid, combine_handler_thread_func, NULL,
            SF_G_THREAD_STACK_SIZE);
}
