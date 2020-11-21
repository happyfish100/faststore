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
#include "obid_htable.h"

static FSAPIHtableShardingContext obid_ctx;

typedef struct fs_api_insert_callback_arg {
    int *rp;
    FSAPIOperationContext *op_ctx;
    FSAPICombinedWriter *writer;
} FSAPIInsertCallbackArg;

static inline bool slice_is_overlap(const FSSliceSize *s1,
        const FSSliceSize *s2)
{
    if (s1->offset < s2->offset) {
        return s1->offset + s1->length > s2->offset;
    } else {
        return s2->offset + s2->length > s1->offset;
    }
}

static void *obid_htable_insert_callback(struct fs_api_hash_entry *he,
        void *arg, FSAPIHtableSharding *sharding, const bool new_create)
{
    FSAPIBlockEntry *block;
    FSAPISliceEntry *slice;
    struct fc_list_head *previous;
    FSAPIInsertCallbackArg *callback_arg;

    block = (FSAPIBlockEntry *)he;
    callback_arg = (FSAPIInsertCallbackArg *)arg;
    previous = NULL;
    if (new_create) {
        block->sharding = sharding;
        FC_INIT_LIST_HEAD(&block->slices.head);
    } else {
        struct fc_list_head *current;
        int end_offset;
        if (!fc_list_empty(&block->slices.head)) {
            end_offset = callback_arg->op_ctx->bs_key.slice.offset +
                callback_arg->op_ctx->bs_key.slice.length;
            fc_list_for_each(current, &block->slices.head) {
                slice = fc_list_entry(current, FSAPISliceEntry, dlink);
                if (end_offset <= slice->ssize.offset) {
                    break;
                }

                if (slice_is_overlap(&callback_arg->op_ctx->
                            bs_key.slice, &slice->ssize))
                {
                    *callback_arg->rp = EEXIST;
                    return NULL;
                }

                previous = current;
            }
        }
    }

    slice = (FSAPISliceEntry *)fast_mblock_alloc_object(
            &callback_arg->op_ctx->allocator_ctx->slice_entry);
    if (slice == NULL) {
        *callback_arg->rp = ENOMEM;
        return NULL;
    }

    slice->block = block;
    slice->writer = callback_arg->writer;
    slice->ssize = callback_arg->op_ctx->bs_key.slice;
    if (previous == NULL) {
        fc_list_add(&slice->dlink, &block->slices.head);
    } else {
        fc_list_add_internal(&slice->dlink, previous, previous->next);
    }
    *callback_arg->rp = 0;
    return slice;
}

static void *obid_htable_find_callback(struct fs_api_hash_entry *he,
        void *arg)
{
    FSAPIBlockEntry *block;
    FSAPISliceEntry *slice;
    int end_offset;
    FSAPIInsertCallbackArg *callback_arg;

    block = (FSAPIBlockEntry *)he;
    if (fc_list_empty(&block->slices.head)) {
        return NULL;
    }

    callback_arg = (FSAPIInsertCallbackArg *)arg;
    end_offset = callback_arg->op_ctx->bs_key.slice.offset +
        callback_arg->op_ctx->bs_key.slice.length;
    fc_list_for_each_entry(slice, &block->slices.head, dlink) {
        if (end_offset <= slice->ssize.offset) {
            break;
        }
        if (slice_is_overlap(&callback_arg->op_ctx->bs_key.slice,
                    &slice->ssize))
        {
            //TODO
        }
    }

    //TODO
    return block;
}

int obid_htable_init(const int sharding_count, const int64_t htable_capacity,
        const int allocator_count, int64_t element_limit,
        const int64_t min_ttl_ms, const int64_t max_ttl_ms)
{
    return sharding_htable_init(&obid_ctx, obid_htable_insert_callback,
            obid_htable_find_callback, sharding_count, htable_capacity,
            allocator_count, sizeof(FSAPIBlockEntry), element_limit,
            min_ttl_ms, max_ttl_ms);
}

FSAPISliceEntry *obid_htable_insert(FSAPIOperationContext *op_ctx,
        FSAPICombinedWriter *writer, int *err_no)
{
    FSAPITwoIdsHashKey key;
    FSAPIInsertCallbackArg callback_arg;

    key.oid = op_ctx->bs_key.block.oid;
    key.bid = op_ctx->bid;
    callback_arg.op_ctx = op_ctx;
    callback_arg.writer = writer;
    callback_arg.rp = err_no;
    return (FSAPISliceEntry *)sharding_htable_insert(
            &obid_ctx, &key, &callback_arg);
}

int obid_htable_check_conflict_and_wait(FSAPIOperationContext *op_ctx,
        int *conflict_count)
{
    FSAPITwoIdsHashKey key;
    FSAPIInsertCallbackArg callback_arg;

    key.oid = op_ctx->bs_key.block.oid;
    key.bid = op_ctx->bid;
    callback_arg.op_ctx = op_ctx;
    /*
    return (FSAPISliceEntry *)sharding_htable_find(
            &obid_ctx, &key, &callback_arg);
            */
    return 0;
}
