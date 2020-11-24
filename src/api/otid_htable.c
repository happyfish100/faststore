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
    int result;
} FSAPIOTIDInsertCallbackArg;

static FSAPIHtableShardingContext otid_ctx;

#define WAIT_TIME_CACL_BY_SUCCESSIVE(entry)  \
    (entry->successive_count * g_fs_api_ctx.write_combine.min_wait_time_ms)

#define IF_COMBINE_BY_SLICE_SIZE(bs_key)  \
    (bs_key.slice.length < g_fs_api_ctx.write_combine. \
     skip_combine_on_slice_size)

static int combine_slice(FSAPIOTIDInsertCallbackArg *callback_arg,
        FSAPIOTIDEntry *entry)
{
    FSAPISliceEntry *slice;
    int result;
    bool new_slice;

    if (entry->slice == NULL) {
        *callback_arg->combined = IF_COMBINE_BY_SLICE_SIZE(
                callback_arg->op_ctx->bs_key);
        new_slice = true;
    } else {
        slice = entry->slice;
        PTHREAD_MUTEX_LOCK(&slice->block->hentry.sharding->lock);
        if (slice->stage == FS_API_COMBINED_WRITER_STAGE_MERGING) {
            int merged_length;
            merged_length = slice->bs_key.slice.length + callback_arg->
                op_ctx->bs_key.slice.length;
            if (merged_length <= FS_FILE_BLOCK_SIZE) {
                if (IF_COMBINE_BY_SLICE_SIZE(callback_arg->op_ctx->bs_key)) {
                    memcpy(slice->buff + slice->bs_key.slice.length,
                            callback_arg->buff, callback_arg->
                            op_ctx->bs_key.slice.length);
                    slice->bs_key.slice.length = merged_length;
                    slice->merged_slices++;
                    *callback_arg->combined = true;

                    if (FS_FILE_BLOCK_SIZE - (slice->bs_key.slice.offset +
                            slice->bs_key.slice.length) >= 4096)
                    {
                        timeout_handler_modify(&slice->timer, FC_MIN(
                                    WAIT_TIME_CACL_BY_SUCCESSIVE(entry),
                                    slice->start_time + g_fs_api_ctx.
                                    write_combine.max_wait_time_ms -
                                    g_timer_ms_ctx.current_time_ms));
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
            new_slice = false;
        } else {
            *callback_arg->combined = slice->merged_slices > g_fs_api_ctx.
                write_combine.skip_combine_on_last_merged_slices;
            new_slice = true;
        }
        PTHREAD_MUTEX_UNLOCK(&slice->block->hentry.sharding->lock);
    }

    if (new_slice && *callback_arg->combined) {
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
        slice->otid = entry;
        slice->stage = FS_API_COMBINED_WRITER_STAGE_MERGING;
        slice->merged_slices = 1;
        slice->start_time = g_timer_ms_ctx.current_time_ms;
        slice->bs_key = callback_arg->op_ctx->bs_key;
        memcpy(slice->buff, callback_arg->buff, callback_arg->
                op_ctx->bs_key.slice.length);
        if (obid_htable_insert(callback_arg->op_ctx,
                    slice, &result) != NULL)
        {
            entry->slice = slice;
            timeout_handler_add(&slice->timer, FC_MIN(
                        WAIT_TIME_CACL_BY_SUCCESSIVE(entry),
                        g_fs_api_ctx.write_combine.max_wait_time_ms));
        } else {
            *callback_arg->combined = false;
        }
    }

    return result;
}

static void *otid_htable_insert_callback(struct fs_api_hash_entry *he,
        void *arg, const bool new_create)
{
    FSAPIOTIDEntry *entry;
    FSAPIOTIDInsertCallbackArg *callback_arg;
    int64_t offset;

    entry = (FSAPIOTIDEntry *)he;
    callback_arg = (FSAPIOTIDInsertCallbackArg *)arg;
    offset = callback_arg->op_ctx->bs_key.block.offset +
        callback_arg->op_ctx->bs_key.slice.offset;
    if (new_create) {
        entry->successive_count = 0;
        callback_arg->result = 0;
    } else {
        if (offset == entry->last_write_offset) {
            entry->successive_count++;
            callback_arg->result = combine_slice(
                    callback_arg, entry);
        } else {
            entry->successive_count = 0;
            callback_arg->result = 0;
        }
    }
    entry->last_write_offset = offset + callback_arg->
        op_ctx->bs_key.slice.length;
    return entry;
}

int otid_htable_init(const int sharding_count,
        const int64_t htable_capacity,
        const int allocator_count, int64_t element_limit,
        const int64_t min_ttl_ms, const int64_t max_ttl_ms)
{
    return sharding_htable_init(&otid_ctx, otid_htable_insert_callback,
            NULL, sharding_count, htable_capacity, allocator_count,
            sizeof(FSAPIOTIDEntry), element_limit, min_ttl_ms, max_ttl_ms);
}

int otid_htable_insert(FSAPIOperationContext *op_ctx,
        const char *buff, bool *combined)
{
    FSAPITwoIdsHashKey key;
    FSAPIOTIDInsertCallbackArg callback_arg;
    FSAPIOTIDEntry *entry;

    *combined = false;
    key.oid = op_ctx->bs_key.block.oid;
    key.tid = op_ctx->tid;
    callback_arg.op_ctx = op_ctx;
    callback_arg.buff = buff;
    callback_arg.combined = combined;
    if ((entry=(FSAPIOTIDEntry *)sharding_htable_insert(
                    &otid_ctx, &key, &callback_arg)) != NULL)
    {
        return callback_arg.result;
    } else {
        return ENOMEM;
    }
}
