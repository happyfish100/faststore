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
#include "timeout_handler.h"
#include "combine_handler.h"
#include "obid_htable.h"
#include "otid_htable.h"

typedef struct fs_api_opid_insert_callback_arg {
    FSAPIOperationContext *op_ctx;
    const char *buff;
    bool *combined;
    FSAPIWaitingTask *waiting_task;
} FSAPIOTIDInsertCallbackArg;

static FSAPIHtableShardingContext otid_ctx;

#define IF_COMBINE_BY_SLICE_SIZE(op_ctx)  \
    (op_ctx->bs_key.slice.length < op_ctx->api_ctx->write_combine. \
     skip_combine_on_slice_size)


static inline void add_to_slice_waiting_list(FSAPISliceEntry *slice,
        FSAPIOTIDInsertCallbackArg *callback_arg)
{
    callback_arg->waiting_task = (FSAPIWaitingTask *)
        fast_mblock_alloc_object(&callback_arg->
                op_ctx->allocator_ctx->waiting_task);
    if (callback_arg->waiting_task == NULL) {
        return;
    }

    fs_api_add_to_slice_waiting_list(callback_arg->waiting_task, slice,
            &callback_arg->waiting_task->waitings.fixed_pair);
}

static int combine_slice(FSAPISliceEntry *slice,
        FSAPIOTIDInsertCallbackArg *callback_arg,
        FSAPIOTIDEntry *entry, bool *new_slice)
{
    int result;
    int merged_length;

    if (slice->stage == FS_API_COMBINED_WRITER_STAGE_MERGING) {
        merged_length = slice->bs_key.slice.length + callback_arg->
            op_ctx->bs_key.slice.length;
        if (merged_length <= FS_FILE_BLOCK_SIZE) {
            if (IF_COMBINE_BY_SLICE_SIZE(callback_arg->op_ctx)) {
                memcpy(slice->buff + slice->bs_key.slice.length,
                        callback_arg->buff, callback_arg->
                        op_ctx->bs_key.slice.length);
                slice->bs_key.slice.length = merged_length;
                slice->merged_slices++;
                *callback_arg->combined = true;

                if (FS_FILE_BLOCK_SIZE - (slice->bs_key.slice.offset +
                            slice->bs_key.slice.length) >= 4096)
                {
                    int current_timeout;
                    int remain_timeout;
                    int timeout;

                    current_timeout = FS_API_CALC_TIMEOUT_BY_SUCCESSIVE(
                            callback_arg->op_ctx, entry->successive_count);
                    remain_timeout = (slice->start_time +
                            callback_arg->op_ctx->api_ctx->
                            write_combine.max_wait_time_ms) -
                            g_timer_ms_ctx.current_time_ms;
                    timeout = FC_MIN(current_timeout, remain_timeout);

                    /*
                    logInfo("slice: %p === offset: %d, length: %d, timeout: %d",
                            slice, slice->bs_key.slice.offset, slice->bs_key.slice.length,
                            timeout);
                            */
                    timeout_handler_modify(&slice->timer, timeout);
                } else {
                    combine_handler_push_within_lock(slice);
                }
            } else {
                combine_handler_push_within_lock(slice);
            }
            result = 0;
        } else {
            result = EOVERFLOW;
        }
        *new_slice = false;
    } else {
        *callback_arg->combined = slice->merged_slices >
            callback_arg->op_ctx->api_ctx->write_combine.
            skip_combine_on_last_merged_slices;
        if (*callback_arg->combined && slice->stage ==
                FS_API_COMBINED_WRITER_STAGE_PROCESSING &&
                __sync_add_and_fetch(&g_combine_handler_ctx.
                    waiting_slice_count, 0) > 0)
        {
            add_to_slice_waiting_list(slice, callback_arg);  //for flow control
            *new_slice = false;
        } else {
            *new_slice = true;
        }
        result = 0;
    }

    return result;
}

static int create_slice(FSAPIOTIDInsertCallbackArg *callback_arg,
        FSAPIOTIDEntry *entry)
{
    int result;
    FSAPIOTIDEntry *old;
    FSAPISliceEntry *slice;

    if (FS_FILE_BLOCK_SIZE - (callback_arg->op_ctx->bs_key.slice.offset +
                callback_arg->op_ctx->bs_key.slice.length) < 4096)
    {
        *callback_arg->combined = false;
        return 0;
    }

    if (callback_arg->op_ctx->bs_key.slice.length > FS_FILE_BLOCK_SIZE) {
        *callback_arg->combined = false;
        return EOVERFLOW;
    }

    slice = (FSAPISliceEntry *)fast_mblock_alloc_object(
            &callback_arg->op_ctx->allocator_ctx->slice_entry);
    if (slice == NULL) {
        *callback_arg->combined = false;
        return ENOMEM;
    }

    old = FS_API_FETCH_SLICE_OTID(slice);
    __sync_bool_compare_and_swap(&slice->otid, old, entry);
    memcpy(slice->buff, callback_arg->buff, callback_arg->
            op_ctx->bs_key.slice.length);
    if ((result=obid_htable_insert(callback_arg->op_ctx, slice,
                    entry->successive_count)) == 0)
    {
        entry->slice = slice;
    } else {
        *callback_arg->combined = false;
    }

    return result;
}

static int check_combine_slice(FSAPIOTIDInsertCallbackArg
        *callback_arg, FSAPIOTIDEntry *entry)
{
    int result;
    bool new_slice;
    FSAPISliceEntry *slice;
    FSAPIBlockEntry *block;

    slice = entry->slice;
    if (slice == NULL) {
        *callback_arg->combined = IF_COMBINE_BY_SLICE_SIZE(
                callback_arg->op_ctx);
        new_slice = true;
    } else {
        block = FS_API_FETCH_SLICE_BLOCK(slice);
        PTHREAD_MUTEX_LOCK(&block->hentry.sharding->lock);
        result = combine_slice(slice, callback_arg, entry, &new_slice);
        PTHREAD_MUTEX_UNLOCK(&block->hentry.sharding->lock);

        if (result != 0) {
            return result;
        }
    }

    if (new_slice && *callback_arg->combined) {
        return create_slice(callback_arg, entry);
    }
    return 0;
}

static int otid_htable_insert_callback(struct fs_api_hash_entry *he,
        void *arg, const bool new_create)
{
    int result;
    FSAPIOTIDEntry *entry;
    FSAPIOTIDInsertCallbackArg *callback_arg;
    int64_t offset;

    entry = (FSAPIOTIDEntry *)he;
    callback_arg = (FSAPIOTIDInsertCallbackArg *)arg;
    offset = callback_arg->op_ctx->bs_key.block.offset +
        callback_arg->op_ctx->bs_key.slice.offset;
    if (new_create) {
        entry->successive_count = 0;
        result = 0;
    } else {
        if (offset == entry->last_write_offset) {
            entry->successive_count++;
            result = check_combine_slice(callback_arg, entry);
        } else {
            entry->successive_count = 0;
            result = 0;
        }
    }
    entry->last_write_offset = offset + callback_arg->
        op_ctx->bs_key.slice.length;
    return result;
}

static bool otid_htable_accept_reclaim_callback(struct fs_api_hash_entry *he)
{
    return ((FSAPIOTIDEntry *)he)->slice == NULL;
}

int otid_htable_init(const int sharding_count,
        const int64_t htable_capacity,
        const int allocator_count, int64_t element_limit,
        const int64_t min_ttl_ms, const int64_t max_ttl_ms)
{
    return sharding_htable_init(&otid_ctx, otid_htable_insert_callback,
            NULL, otid_htable_accept_reclaim_callback, sharding_count,
            htable_capacity, allocator_count, sizeof(FSAPIOTIDEntry),
            element_limit, min_ttl_ms, max_ttl_ms);
}

int otid_htable_insert(FSAPIOperationContext *op_ctx,
        const char *buff, bool *combined)
{
    FSAPITwoIdsHashKey key;
    FSAPIOTIDInsertCallbackArg callback_arg;
    int result;

    *combined = false;
    key.oid = op_ctx->bs_key.block.oid;
    key.tid = op_ctx->tid;
    callback_arg.op_ctx = op_ctx;
    callback_arg.buff = buff;
    callback_arg.combined = combined;

    do {
        callback_arg.waiting_task = NULL;
        result = sharding_htable_insert(&otid_ctx, &key, &callback_arg);
        if (callback_arg.waiting_task != NULL) {
            fs_api_wait_write_done_and_release(callback_arg.waiting_task);
        }
    } while (0);
    //while (result == 0 && callback_arg.waiting_task != NULL);

    return result;
}
