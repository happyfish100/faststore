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
#include "../fs_api_allocator.h"
#include "combine_handler.h"
#include "oid_htable.h"
#include "otid_htable.h"
#include "obid_htable.h"

static SFHtableShardingContext obid_ctx;

typedef struct fs_api_find_callback_arg {
    FSAPIOperationContext *op_ctx;
    FSAPIWaitingTask *waiting_task;
    int *conflict_count;
} FSAPIFindCallbackArg;

#define IF_COMBINE_BY_SLICE_SIZE(op_ctx)  \
    (op_ctx->bs_key.slice.length < op_ctx->api_ctx->write_combine. \
     skip_combine_on_slice_size)

#define IF_COMBINE_BY_SLICE_MERGED(slice, op_ctx)   \
    (slice->merged_slices > op_ctx->api_ctx-> \
     write_combine.skip_combine_on_last_merged_slices)

#define IF_COMBINE_BY_SLICE_POSITION(slice) \
    (FS_FILE_BLOCK_SIZE - (slice.offset + slice.length) >= 4096)

static inline void add_to_slice_waiting_list(FSAPISliceEntry *slice,
        FSAPIInsertSliceContext *ictx)
{
    ictx->waiting_task = (FSAPIWaitingTask *)
        fast_mblock_alloc_object(&ictx->op_ctx->allocator_ctx->
                write_combine.waiting_task);
    if (ictx->waiting_task == NULL) {
        return;
    }

    fs_api_add_to_slice_waiting_list(ictx->waiting_task, slice,
            &ictx->waiting_task->waitings.fixed_pair);
}

static void copy_to_buff(char *buff, FSAPIWriteBuffer *wbuffer,
        const int length)
{
    const struct iovec *iob;
    const struct iovec *end;
    char *current;
    int remain;
    int bytes;

    if (wbuffer->is_writev) {
        current = buff;
        remain = length;
        end = wbuffer->iov + wbuffer->iovcnt;
        for (iob=wbuffer->iov; iob<end; iob++) {
            bytes = FC_MIN(remain, iob->iov_len);
            memcpy(current, iob->iov_base, bytes);

            remain -= bytes;
            if (remain == 0) {
                break;
            }
            current += bytes;
        }
    } else {
        memcpy(buff, wbuffer->buff, length);
    }
}

static int do_combine_slice(FSAPISliceEntry *slice,
        FSAPIInsertSliceContext *ictx)
{
    int current_timeout;
    int remain_timeout;
    int timeout;

    if (!IF_COMBINE_BY_SLICE_SIZE(ictx->op_ctx)) {
        combine_handler_push_within_lock(slice);
        ictx->wbuffer->reason = FS_NOT_COMBINED_REASON_SLICE_SIZE;
        return 0;
    }

    copy_to_buff(slice->buff + slice->bs_key.slice.length,
            ictx->wbuffer, ictx->op_ctx->bs_key.slice.length);
    slice->bs_key.slice.length += ictx->op_ctx->bs_key.slice.length;
    slice->merged_slices++;
    ictx->wbuffer->combined = true;
    if (ictx->op_ctx->api_ctx->write_combine.buffer_size -
                slice->bs_key.slice.length < 4096)
    {
        combine_handler_push_within_lock(slice);
        return 0;
    }

    current_timeout = FS_API_CALC_TIMEOUT_BY_SUCCESSIVE(
            ictx->op_ctx, ictx->otid.successive_count);
    remain_timeout = (slice->start_time +
            ictx->op_ctx->api_ctx->
            write_combine.max_wait_time_ms) -
        g_timer_ms_ctx.current_time_ms;
    if (remain_timeout >= g_timer_ms_ctx.precision_ms) {
        timeout = FC_MIN(current_timeout, remain_timeout);
        timeout_handler_modify(&slice->timer, timeout);
    }

    /*
       logInfo("slice: %p === offset: %d, length: %d, timeout: %d",
       slice, slice->bs_key.slice.offset, slice->bs_key.slice.length,
       timeout);
     */
    return 0;
}

static int try_combine_slice(FSAPISliceEntry *slice,
        FSAPIInsertSliceContext *ictx, bool *is_new_slice)
{
    int slice_end;
    bool is_jump;

    if (ictx->op_ctx->bs_key.block.oid != slice->bs_key.block.oid) {
        logWarning("file: "__FILE__", line: %d, "
                "op_ctx oid: %"PRId64" != slice oid: %"PRId64, __LINE__,
                ictx->op_ctx->bs_key.block.oid, slice->bs_key.block.oid);
        *is_new_slice = false;
        ictx->wbuffer->reason = FS_NOT_COMBINED_REASON_DIFFERENT_OID;
        return 0;
    }

    slice_end = slice->bs_key.slice.offset + slice->bs_key.slice.length;
    if (ictx->op_ctx->bs_key.block.offset == slice->bs_key.block.offset &&
        ictx->op_ctx->bs_key.slice.offset == slice_end)
    {
        /* current slice is successive */
        if (slice->stage == FS_API_COMBINED_WRITER_STAGE_MERGING) {
            if (slice->bs_key.slice.length + ictx->op_ctx->bs_key.slice.length >
                    ictx->op_ctx->api_ctx->write_combine.buffer_size)
            {  //buffer full, should start new combine
                combine_handler_push_within_lock(slice);
            } else {
                *is_new_slice = false;
                return do_combine_slice(slice, ictx);
            }
        }
        is_jump = false;
    } else {
        /*
           logInfo("slice NOT successive! slice {stage: %d, oid: %"PRId64", "
           "offset: %"PRId64"}, input {oid: %"PRId64", offset: %"PRId64"}",
           slice->stage, slice->bs_key.block.oid, slice->bs_key.block.offset,
           ictx->op_ctx->bs_key.block.oid, ictx->op_ctx->bs_key.block.offset);
         */
        if (slice->stage == FS_API_COMBINED_WRITER_STAGE_MERGING) {
            combine_handler_push_within_lock(slice);
        }
        is_jump = true;
    }

    if (!IF_COMBINE_BY_SLICE_SIZE(ictx->op_ctx)) {
        *is_new_slice = false;
        ictx->wbuffer->reason = FS_NOT_COMBINED_REASON_SLICE_SIZE;
        return 0;
    }

    ictx->wbuffer->combined = is_jump || IF_COMBINE_BY_SLICE_MERGED(
            slice, ictx->op_ctx);
    if (!ictx->wbuffer->combined) {
        ictx->wbuffer->reason = FS_NOT_COMBINED_REASON_LAST_MERGED_SLICES;
    }
    if (ictx->wbuffer->combined && (slice->stage ==
                FS_API_COMBINED_WRITER_STAGE_PROCESSING) &&
            (__sync_add_and_fetch(&g_combine_handler_ctx.
                                  waiting_slice_count, 0) > 0))
    {
        add_to_slice_waiting_list(slice, ictx);  //for flow control
        *is_new_slice = false;
    } else {
        *is_new_slice = true;
    }

    return 0;
}

static inline int obid_htable_insert(FSAPIInsertSliceContext *ictx)
{
    SFTwoIdsHashKey key;

    key.oid = ictx->op_ctx->bs_key.block.oid;
    key.bid = ictx->op_ctx->bid;
    return sf_sharding_htable_insert(&obid_ctx, &key, ictx);
}

static int create_slice(FSAPIInsertSliceContext *ictx)
{
    int result;

    if (ictx->op_ctx->bs_key.slice.length + 4096 >= ictx->op_ctx->
            api_ctx->write_combine.buffer_size)
    {
        ictx->wbuffer->combined = false;
        ictx->wbuffer->reason = FS_NOT_COMBINED_REASON_REACH_BUFF_SIZE;
        return 0;
    }

    if (!IF_COMBINE_BY_SLICE_POSITION(ictx->op_ctx->bs_key.slice)) {
        /* remain buffer is too small */
        ictx->wbuffer->combined = false;
        ictx->wbuffer->reason = FS_NOT_COMBINED_REASON_SLICE_POSITION;
        return 0;
    }

    ictx->slice = (FSAPISliceEntry *)fast_mblock_alloc_object(&ictx->
            op_ctx->allocator_ctx->write_combine.slice.allocator);
    if (ictx->slice == NULL) {
        return ENOMEM;
    }

    if ((result=obid_htable_insert(ictx)) != 0) {
        fast_mblock_free_object(&ictx->slice->allocator_ctx->
                write_combine.slice.allocator, ictx->slice);
    }

    return result;
}

static int check_combine_slice(FSAPIInsertSliceContext *ictx)
{
    int result;
    bool is_new_slice;
    int64_t old_version;
    FSAPIBlockEntry *block;

    while (1) {
        ictx->otid.old_slice = (FSAPISliceEntry *)__sync_add_and_fetch(
                &ictx->otid.entry->slice, 0);
        if (ictx->otid.old_slice == NULL) {
            ictx->wbuffer->combined = IF_COMBINE_BY_SLICE_SIZE(ictx->op_ctx);
            if (!ictx->wbuffer->combined) {
                ictx->wbuffer->reason = FS_NOT_COMBINED_REASON_SLICE_SIZE;
            }
            is_new_slice = true;
            break;
        } else {
            old_version = __sync_add_and_fetch(&ictx->
                    otid.old_slice->version, 0);
            block = FS_API_FETCH_SLICE_BLOCK(ictx->otid.old_slice);
            PTHREAD_MUTEX_LOCK(&block->hentry.sharding->lock);
            if (__sync_add_and_fetch(&ictx->otid.old_slice->
                        version, 0) == old_version)
            {
                result = try_combine_slice(ictx->otid.old_slice,
                        ictx, &is_new_slice);
            } else {
                is_new_slice = false;
                result = -EAGAIN;
            }
            PTHREAD_MUTEX_UNLOCK(&block->hentry.sharding->lock);

            if (result == 0) {
                break;
            } else if (result != -EAGAIN) {
                return result;
            }
        }
    }

    if (is_new_slice && ictx->wbuffer->combined) {
        if ((result=create_slice(ictx)) != 0) {
            ictx->wbuffer->combined = false;
            ictx->wbuffer->reason = result;
        }
        return result;
    } else {
        return 0;
    }
}

static int obid_htable_find_position(FSAPIBlockEntry *block,
        FSAPIInsertSliceContext *ictx, struct fc_list_head **previous)
{
    struct fc_list_head *current;
    FSAPISliceEntry *slice;
    int end_offset;

    if (fc_list_empty(&block->slices.head)) {
        return 0;
    }

    end_offset = ictx->op_ctx->bs_key.slice.offset +
        ictx->op_ctx->bs_key.slice.length;
    fc_list_for_each(current, &block->slices.head) {
        slice = fc_list_entry(current, FSAPISliceEntry, dlink);
        if (end_offset <= slice->bs_key.slice.offset) {
            break;
        }

        if (fs_slice_is_overlap(&ictx->op_ctx->bs_key.slice,
                    &slice->bs_key.slice))
        {
            return EEXIST;
        }

        *previous = current;
    }

    return 0;
}

static int obid_htable_insert_callback(SFShardingHashEntry *he,
        void *arg, const bool new_create)
{
    FSAPIBlockEntry *block;
    FSWCombineOTIDEntry *old_otid;
    FSAPIBlockEntry *old_block;
    struct fc_list_head *previous;
    FSAPIInsertSliceContext *ictx;
    int result;
    int current_timeout;
    int timeout;

    block = (FSAPIBlockEntry *)he;
    ictx = (FSAPIInsertSliceContext *)arg;
    previous = NULL;
    if (new_create) {
        FC_INIT_LIST_HEAD(&block->slices.head);
    } else {
        if ((result=obid_htable_find_position(block, ictx, &previous)) != 0) {
            return result;
        }
    }

    ictx->slice->api_ctx = ictx->op_ctx->api_ctx;
    ictx->slice->done_callback_arg = (FSAPIWriteDoneCallbackArg *)
        fast_mblock_alloc_object(&ictx->op_ctx->allocator_ctx->
                write_combine.callback_arg);
    if (ictx->slice->done_callback_arg == NULL) {
        return ENOMEM;
    }

    if (ictx->wbuffer->extra_data != NULL && ictx->op_ctx->api_ctx->
                write_done_callback.arg_extra_size > 0)
    {
        memcpy(ictx->slice->done_callback_arg->extra_data,
                ictx->wbuffer->extra_data, ictx->op_ctx->api_ctx->
                write_done_callback.arg_extra_size);
    }
    ictx->slice->done_callback_arg->bs_key = &ictx->slice->bs_key;

    old_otid = (FSWCombineOTIDEntry *)ictx->slice->otid;
    while (!__sync_bool_compare_and_swap(&ictx->slice->otid,
            old_otid, ictx->otid.entry))
    {
        old_otid = FS_API_FETCH_SLICE_OTID(ictx->slice);
    }

    old_block = (FSAPIBlockEntry *)ictx->slice->block;
    while (!__sync_bool_compare_and_swap(&ictx->slice->block,
                old_block, block))
    {
        old_block = FS_API_FETCH_SLICE_BLOCK(ictx->slice);
    }

    copy_to_buff(ictx->slice->buff, ictx->wbuffer,
            ictx->op_ctx->bs_key.slice.length);
    ictx->slice->bs_key = ictx->op_ctx->bs_key;
    ictx->slice->merged_slices = 1;
    ictx->slice->start_time = g_timer_ms_ctx.current_time_ms;
    ictx->slice->stage = FS_API_COMBINED_WRITER_STAGE_MERGING;

    if (fc_list_empty(&block->slices.head)) {
        wcombine_oid_htable_insert(block);
    }

    if (previous == NULL) {
        fc_list_add(&ictx->slice->dlink, &block->slices.head);
    } else {
        fc_list_add_internal(&ictx->slice->dlink,
                previous, previous->next);
    }

    current_timeout = FS_API_CALC_TIMEOUT_BY_SUCCESSIVE(
            ictx->op_ctx, ictx->otid.successive_count);
    timeout = FC_MIN(current_timeout, ictx->op_ctx->
            api_ctx->write_combine.max_wait_time_ms);
    timeout_handler_add(&ictx->slice->timer, timeout);

    if (!__sync_bool_compare_and_swap(&ictx->otid.entry->slice,
                ictx->otid.old_slice, ictx->slice))
    {
        combine_handler_push_within_lock(ictx->slice);
    }
    return 0;
}

static int deal_confilct_slice(FSAPIFindCallbackArg *farg,
        FSAPISliceEntry *slice)
{
    FSAPIWaitingTaskSlicePair *ts_pair;
    if (farg->waiting_task == NULL) {
        farg->waiting_task = (FSAPIWaitingTask *)
            fast_mblock_alloc_object(&farg->op_ctx->allocator_ctx->
                    write_combine.waiting_task);
        if (farg->waiting_task == NULL) {
            return ENOMEM;
        }

        ts_pair = &farg->waiting_task->waitings.fixed_pair;
    } else {
        ts_pair = (FSAPIWaitingTaskSlicePair *)
            fast_mblock_alloc_object(&farg->op_ctx->allocator_ctx->
                    write_combine.task_slice_pair);
        if (ts_pair == NULL) {
            return ENOMEM;
        }
    }

    fs_api_add_to_slice_waiting_list(farg->waiting_task, slice, ts_pair);
    return 0;
}

static void *obid_htable_find_callback(SFShardingHashEntry *he,
        void *arg)
{
    FSAPIBlockEntry *block;
    FSAPISliceEntry *slice;
    int end_offset;
    FSAPIFindCallbackArg *farg;

    block = (FSAPIBlockEntry *)he;
    if (fc_list_empty(&block->slices.head)) {
        return NULL;
    }

    farg = (FSAPIFindCallbackArg *)arg;
    end_offset = farg->op_ctx->bs_key.slice.offset +
        farg->op_ctx->bs_key.slice.length;
    fc_list_for_each_entry(slice, &block->slices.head, dlink) {
        if (end_offset <= slice->bs_key.slice.offset) {
            break;
        }

        if (fs_slice_is_overlap(&farg->op_ctx->bs_key.slice,
                    &slice->bs_key.slice) &&
                (slice->stage == FS_API_COMBINED_WRITER_STAGE_MERGING ||
                 slice->stage == FS_API_COMBINED_WRITER_STAGE_PROCESSING))
        {
            /*
            logInfo("file: "__FILE__", line: %d, slice conflict! "
                    "tid: %"PRId64", operation: %c, slice stage: %s,"
                    " merged_slices: %d, block {oid: %"PRId64", "
                    "offset: %"PRId64"}, slice {offset: %d, length: %d}",
                    __LINE__, farg->op_ctx->tid, farg->op_ctx->op_type,
                    fs_api_get_combine_stage(slice->stage), slice->merged_slices,
                    slice->bs_key.block.oid, slice->bs_key.block.offset,
                    slice->bs_key.slice.offset, slice->bs_key.slice.length);
                    */

            if (slice->stage == FS_API_COMBINED_WRITER_STAGE_MERGING) {
                combine_handler_push_within_lock(slice);
            }

            (*farg->conflict_count)++;
            if (deal_confilct_slice(farg, slice) != 0) {
                break;
            }
        }
    }

    return block;
}

static bool obid_htable_accept_reclaim_callback(SFShardingHashEntry *he)
{
    return fc_list_empty(&((FSAPIBlockEntry *)he)->slices.head);
}

int wcombine_obid_htable_init(const int sharding_count,
        const int64_t htable_capacity, const int allocator_count,
        int64_t element_limit, const int64_t min_ttl_ms,
        const int64_t max_ttl_ms, const double low_water_mark_ratio)
{
    return sf_sharding_htable_init_ex(&obid_ctx, sf_sharding_htable_key_ids_two,
            obid_htable_insert_callback, obid_htable_find_callback, NULL,
            obid_htable_accept_reclaim_callback, sharding_count,
            htable_capacity, allocator_count, sizeof(FSAPIBlockEntry),
            element_limit, min_ttl_ms, max_ttl_ms, low_water_mark_ratio);
}

int wcombine_obid_htable_check_conflict_and_wait(FSAPIOperationContext *op_ctx)
{
    SFTwoIdsHashKey key;
    FSAPIFindCallbackArg callback_arg;
    int conflict_count;

    conflict_count = 0;
    key.oid = op_ctx->bs_key.block.oid;
    key.bid = op_ctx->bid;
    callback_arg.op_ctx = op_ctx;
    callback_arg.waiting_task = NULL;
    callback_arg.conflict_count = &conflict_count;
    if (sf_sharding_htable_find(&obid_ctx, &key, &callback_arg) == NULL) {
        return 0;
    }

    if (callback_arg.waiting_task != NULL) {
        fs_api_wait_write_done_and_release(callback_arg.waiting_task);
    }

    return conflict_count;
}

int wcombine_obid_htable_check_combine_slice(FSAPIInsertSliceContext *ictx)
{
    int result;
    int count;

    count = 0;
    do {
        ictx->waiting_task = NULL;
        result = check_combine_slice(ictx);
        if (ictx->waiting_task != NULL) {
            ictx->wbuffer->combined = false;
            ictx->wbuffer->reason = FS_NOT_COMBINED_REASON_WAITING_TIMEOUT;
            fs_api_wait_write_done_and_release(ictx->waiting_task);
        }
    } while (result == 0 && ictx->waiting_task != NULL && count++ < 3);

    return result;
}

ssize_t wcombine_obid_htable_datasync(const int64_t oid, const uint64_t tid)
{
    FSWCombineIDArray array;
    SFTwoIdsHashKey key;
    FSAPIFindCallbackArg callback_arg;
    FSAPIOperationContext op_ctx;
    int64_t *bid;
    int64_t *end;
    int total_count;
    int conflict_count;

    wcombine_id_array_init(&array);
    if (wcombine_oid_htable_find(oid, &array) != 0) {
        return 0;
    }

    op_ctx.allocator_ctx = fs_api_allocator_get(tid);
    op_ctx.bs_key.slice.offset = 0;
    op_ctx.bs_key.slice.length = FS_FILE_BLOCK_SIZE;
    callback_arg.op_ctx = &op_ctx;
    callback_arg.conflict_count = &conflict_count;
    key.oid = oid;
    total_count = 0;
    end = array.elts + array.count;
    for (bid=array.elts; bid<end; bid++) {
        key.bid = *bid;
        *(callback_arg.conflict_count) = 0;
        callback_arg.waiting_task = NULL;
        if (sf_sharding_htable_find(&obid_ctx, &key, &callback_arg) != NULL) {
            total_count += *(callback_arg.conflict_count);
            if (callback_arg.waiting_task != NULL) {
                fs_api_wait_write_done_and_release(callback_arg.waiting_task);
            }
        }
    }

    wcombine_id_array_destroy(&array);
    return total_count;
}
