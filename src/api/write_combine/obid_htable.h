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

#ifndef _OBID_HTABLE_H
#define _OBID_HTABLE_H

#include "sf/sf_sharding_htable.h"
#include "../fs_api_types.h"

typedef struct fs_api_block_entry {
    SFShardingHashEntry hentry; //must be the first
    struct {
        struct fc_list_head head;  //element: FSAPISliceEntry
    } slices;
} FSAPIBlockEntry;

#ifdef __cplusplus
extern "C" {
#endif

    int obid_htable_init(const int sharding_count,
            const int64_t htable_capacity, const int allocator_count,
            int64_t element_limit, const int64_t min_ttl_sec,
            const int64_t max_ttl_sec);

    int obid_htable_check_conflict_and_wait(FSAPIOperationContext *op_ctx,
            int *conflict_count);

    int obid_htable_check_combine_slice(FSAPIInsertSliceContext *ictx);

    static inline int fs_api_swap_slice_stage(FSAPISliceEntry *slice,
            const int old_stage, const int new_stage)
    {
        int result;
        FSAPIBlockEntry *block;

        block = FS_API_FETCH_SLICE_BLOCK(slice);
        PTHREAD_MUTEX_LOCK(&block->hentry.sharding->lock);
        if (slice->stage == old_stage) {
            slice->stage = new_stage;
            result = 0;
        } else {
            result = EEXIST;
        }
        PTHREAD_MUTEX_UNLOCK(&block->hentry.sharding->lock);

        return result;
    }

    static inline void fs_api_notify_waiting_tasks(FSAPISliceEntry *slice)
    {
        FSAPIWaitingTaskSlicePair *ts_pair;
        FSAPIWaitingTask *task;

        while (slice->waitings.head != NULL) {
            ts_pair = slice->waitings.head;
            slice->waitings.head = slice->waitings.head->next;

            task = (FSAPIWaitingTask *)__sync_add_and_fetch(&ts_pair->task, 0);
            PTHREAD_MUTEX_LOCK(&task->lcp.lock);
            fc_list_del_init(&ts_pair->dlink);
            pthread_cond_signal(&task->lcp.cond);
            PTHREAD_MUTEX_UNLOCK(&task->lcp.lock);

            if (ts_pair != &task->waitings.fixed_pair) {
                fast_mblock_free_object(ts_pair->allocator, ts_pair);
            }
        }
    }

    static inline void fs_api_add_to_slice_waiting_list(
            FSAPIWaitingTask *task, FSAPISliceEntry *slice,
            FSAPIWaitingTaskSlicePair *ts_pair)
    {
        FSAPIWaitingTask *old_task;

        old_task = (FSAPIWaitingTask *)ts_pair->task;
        while (!__sync_bool_compare_and_swap(&ts_pair->task, old_task, task)) {
            old_task = (FSAPIWaitingTask *)__sync_add_and_fetch(
                    &ts_pair->task, 0);
        }

        PTHREAD_MUTEX_LOCK(&task->lcp.lock);
        fc_list_add_tail(&ts_pair->dlink, &task->waitings.head);
        PTHREAD_MUTEX_UNLOCK(&task->lcp.lock);

        ts_pair->next = slice->waitings.head;
        slice->waitings.head = ts_pair;
    }

    static inline void fs_api_wait_write_done_and_release(
            FSAPIWaitingTask *waiting_task)
    {
        FSAPIWaitingTaskSlicePair *ts_pair;

        PTHREAD_MUTEX_LOCK(&waiting_task->lcp.lock);
        while ((ts_pair=fc_list_first_entry(&waiting_task->waitings.head,
                        FSAPIWaitingTaskSlicePair, dlink)) != NULL)
        {
            pthread_cond_wait(&waiting_task->lcp.cond,
                    &waiting_task->lcp.lock);
        }
        PTHREAD_MUTEX_UNLOCK(&waiting_task->lcp.lock);
        fast_mblock_free_object(waiting_task->allocator, waiting_task);
    }

#ifdef __cplusplus
}
#endif

#endif
