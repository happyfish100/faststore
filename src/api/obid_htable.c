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
#include "fs_api_allocator.h"
#include "combine_handler.h"
#include "obid_htable.h"

static FSAPIHtableShardingContext obid_ctx;

typedef struct fs_api_insert_callback_arg {
    FSAPIOperationContext *op_ctx;
    FSAPISliceEntry *slice;
} FSAPIInsertCallbackArg;

typedef struct fs_api_find_callback_arg {
    FSAPIOperationContext *op_ctx;
    FSAPIWaitingTask *waiting_task;
    int *conflict_count;
} FSAPIFindCallbackArg;

static inline bool slice_is_overlap(const FSSliceSize *s1,
        const FSSliceSize *s2)
{
    if (s1->offset < s2->offset) {
        return s1->offset + s1->length > s2->offset;
    } else {
        return s2->offset + s2->length > s1->offset;
    }
}

static int obid_htable_find_position(FSAPIBlockEntry *block,
        FSAPIInsertCallbackArg *callback_arg, struct fc_list_head **previous)
{
    struct fc_list_head *current;
    FSAPISliceEntry *slice;
    int end_offset;

    if (fc_list_empty(&block->slices.head)) {
        return 0;
    }

    end_offset = callback_arg->op_ctx->bs_key.slice.offset +
        callback_arg->op_ctx->bs_key.slice.length;
    fc_list_for_each(current, &block->slices.head) {
        slice = fc_list_entry(current, FSAPISliceEntry, dlink);
        if (end_offset <= slice->bs_key.slice.offset) {
            break;
        }

        if (slice_is_overlap(&callback_arg->op_ctx->
                    bs_key.slice, &slice->bs_key.slice))
        {
            fast_mblock_free_object(callback_arg->slice->
                    allocator, callback_arg->slice);
            return EEXIST;
        }

        *previous = current;
    }

    return 0;
}

static int obid_htable_insert_callback(struct fs_api_hash_entry *he,
        void *arg, const bool new_create)
{
    FSAPIBlockEntry *block;
    struct fc_list_head *previous;
    FSAPIInsertCallbackArg *callback_arg;
    int result;

    block = (FSAPIBlockEntry *)he;
    callback_arg = (FSAPIInsertCallbackArg *)arg;
    previous = NULL;
    if (new_create) {
        FC_INIT_LIST_HEAD(&block->slices.head);
    } else {
        if ((result=obid_htable_find_position(block,
                        callback_arg, &previous)) != 0)
        {
            return result;
        }
    }

    callback_arg->slice->stage = FS_API_COMBINED_WRITER_STAGE_MERGING;
    callback_arg->slice->block = block;
    if (previous == NULL) {
        fc_list_add(&callback_arg->slice->dlink, &block->slices.head);
    } else {
        fc_list_add_internal(&callback_arg->slice->dlink,
                previous, previous->next);
    }

    return 0;
}

static int deal_confilct_slice(FSAPIFindCallbackArg *callback_arg,
        FSAPISliceEntry *slice)
{
    FSAPIWaitingTaskSlicePair *ts_pair;
    if (callback_arg->waiting_task == NULL) {
        callback_arg->waiting_task = (FSAPIWaitingTask *)
            fast_mblock_alloc_object(&callback_arg->
                    op_ctx->allocator_ctx->waiting_task);
        if (callback_arg->waiting_task == NULL) {
            return ENOMEM;
        }

        ts_pair = &callback_arg->waiting_task->waitings.fixed_pair;
    } else {
        ts_pair = (FSAPIWaitingTaskSlicePair *)
            fast_mblock_alloc_object(&callback_arg->
                    op_ctx->allocator_ctx->task_slice_pair);
        if (ts_pair == NULL) {
            return ENOMEM;
        }
    }

    PTHREAD_MUTEX_LOCK(&callback_arg->waiting_task->lcp.lock);
    ts_pair->task = callback_arg->waiting_task;
    ts_pair->slice = slice;
    ts_pair->next = slice->waitings.head;
    slice->waitings.head = ts_pair;
    fc_list_add_tail(&ts_pair->dlink, &callback_arg->
            waiting_task->waitings.head);
    PTHREAD_MUTEX_UNLOCK(&callback_arg->waiting_task->lcp.lock);

    return 0;
}

static void *obid_htable_find_callback(struct fs_api_hash_entry *he,
        void *arg)
{
    FSAPIBlockEntry *block;
    FSAPISliceEntry *slice;
    int end_offset;
    FSAPIFindCallbackArg *callback_arg;

    block = (FSAPIBlockEntry *)he;
    if (fc_list_empty(&block->slices.head)) {
        return NULL;
    }

    callback_arg = (FSAPIFindCallbackArg *)arg;
    end_offset = callback_arg->op_ctx->bs_key.slice.offset +
        callback_arg->op_ctx->bs_key.slice.length;
    fc_list_for_each_entry(slice, &block->slices.head, dlink) {
        if (end_offset <= slice->bs_key.slice.offset) {
            break;
        }

        if (slice_is_overlap(&callback_arg->op_ctx->bs_key.slice,
                    &slice->bs_key.slice) &&
                (slice->stage == FS_API_COMBINED_WRITER_STAGE_MERGING ||
                 slice->stage == FS_API_COMBINED_WRITER_STAGE_PROCESSING))
        {
            if (slice->stage == FS_API_COMBINED_WRITER_STAGE_MERGING) {
                combine_handler_push_within_lock(slice);
            }

            (*callback_arg->conflict_count)++;
            if (deal_confilct_slice(callback_arg, slice) != 0) {
                break;
            }
        }
    }

    return block;
}

static bool obid_htable_accept_reclaim_callback(struct fs_api_hash_entry *he)
{
    return fc_list_empty(&((FSAPIBlockEntry *)he)->slices.head);
}

int obid_htable_init(const int sharding_count, const int64_t htable_capacity,
        const int allocator_count, int64_t element_limit,
        const int64_t min_ttl_ms, const int64_t max_ttl_ms)
{
    return sharding_htable_init(&obid_ctx, obid_htable_insert_callback,
            obid_htable_find_callback, obid_htable_accept_reclaim_callback,
            sharding_count, htable_capacity, allocator_count,
            sizeof(FSAPIBlockEntry), element_limit, min_ttl_ms, max_ttl_ms);
}

int obid_htable_insert(FSAPIOperationContext *op_ctx, FSAPISliceEntry *slice)
{
    FSAPITwoIdsHashKey key;
    FSAPIInsertCallbackArg callback_arg;

    key.oid = op_ctx->bs_key.block.oid;
    key.bid = op_ctx->bid;
    callback_arg.op_ctx = op_ctx;
    callback_arg.slice = slice;
    return sharding_htable_insert(&obid_ctx, &key, &callback_arg);
}

void obid_htable_notify_waiting_tasks(FSAPISliceEntry *slice)
{
    FSAPIWaitingTaskSlicePair *ts_pair;

    while (slice->waitings.head != NULL) {
        ts_pair = slice->waitings.head;

        PTHREAD_MUTEX_LOCK(&ts_pair->task->lcp.lock);
        fc_list_del_init(&ts_pair->dlink);
        pthread_cond_signal(&ts_pair->task->lcp.cond);
        PTHREAD_MUTEX_UNLOCK(&ts_pair->task->lcp.lock);

        if (ts_pair != &ts_pair->task->waitings.fixed_pair) {
            fast_mblock_free_object(ts_pair->allocator, ts_pair);
        }
        slice->waitings.head = slice->waitings.head->next;
    }
}

static void wait_slice_write_done(FSAPIWaitingTask *waiting_task)
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
}

int obid_htable_check_conflict_and_wait(FSAPIOperationContext *op_ctx,
        int *conflict_count)
{
    FSAPITwoIdsHashKey key;
    FSAPIFindCallbackArg callback_arg;

    *conflict_count = 0;
    key.oid = op_ctx->bs_key.block.oid;
    key.bid = op_ctx->bid;
    callback_arg.op_ctx = op_ctx;
    callback_arg.waiting_task = NULL;
    callback_arg.conflict_count = conflict_count;
    if (sharding_htable_find(&obid_ctx, &key, &callback_arg) == NULL) {
        return 0;
    }

    if (callback_arg.waiting_task != NULL) {
        wait_slice_write_done(callback_arg.waiting_task);
        fast_mblock_free_object(callback_arg.waiting_task->
                allocator, callback_arg.waiting_task);
    }

    return 0;
}
