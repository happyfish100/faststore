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

static FSAPIHtableShardingContext otid_ctx;

static int otid_htable_insert_callback(struct fs_api_hash_entry *he,
        void *arg, const bool new_create)
{
    FSAPIOTIDEntry *entry;
    FSAPIInsertSliceContext *ictx;
    int64_t offset;

    entry = (FSAPIOTIDEntry *)he;
    ictx = (FSAPIInsertSliceContext *)arg;
    offset = ictx->op_ctx->bs_key.block.offset +
        ictx->op_ctx->bs_key.slice.offset;
    if (new_create) {
        FSAPISliceEntry *old_slice;
        old_slice = (FSAPISliceEntry *)__sync_add_and_fetch(&entry->slice, 0);
        if (old_slice != NULL) {
            __sync_bool_compare_and_swap(&entry->slice, old_slice, NULL);
        }
        entry->successive_count = 0;
    } else {
        if (offset == entry->last_write_offset) {
            entry->successive_count++;
        } else {
            entry->successive_count = 0;
        }
    }
    entry->last_write_offset = offset + ictx->op_ctx->bs_key.slice.length;
    ictx->otid.entry = entry;
    ictx->otid.successive_count = entry->successive_count;
    return 0;
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
    FSAPIInsertSliceContext ictx;
    int result;

    *combined = false;
    key.oid = op_ctx->bs_key.block.oid;
    key.tid = op_ctx->tid;
    ictx.op_ctx = op_ctx;
    ictx.buff = buff;
    ictx.combined = combined;
    if ((result=sharding_htable_insert(&otid_ctx, &key, &ictx)) != 0) {
        return result;
    } 
    if (ictx.otid.successive_count > 0) {
        return obid_htable_check_combine_slice(&ictx);
    }

    return 0;
}
