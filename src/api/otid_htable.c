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
#include "otid_htable.h"

typedef struct fs_api_otid_entry {
    FSAPIHashEntry hentry;  //must be the first
    int successive_count;
    int64_t last_write_offset;
    FSAPICombinedWriter *writer;
} FSAPIOTIDEntry;

typedef struct fs_api_opid_insert_callback_arg {
    FSAPIOperationContext *op_ctx;
    int *successive_count;
} FSAPIOTIDInsertCallbackArg;

static FSAPIHtableShardingContext otid_ctx;

static void *otid_htable_insert_callback(struct fs_api_hash_entry *he,
        void *arg, FSAPIHtableSharding *sharding, const bool new_create)
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
    } else {
        if (offset == entry->last_write_offset) {
            entry->successive_count++;
        } else {
            entry->successive_count = 0;
        }
    }
    *callback_arg->successive_count = entry->successive_count;
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

int otid_htable_insert(FSAPIOperationContext *op_ctx, int *successive_count)
{
    FSAPITwoIdsHashKey key;
    FSAPIOTIDInsertCallbackArg callback_arg;
    FSAPIOTIDEntry *entry;

    key.oid = op_ctx->bs_key.block.oid;
    key.tid = op_ctx->tid;
    callback_arg.op_ctx = op_ctx;
    callback_arg.successive_count = successive_count;
    if ((entry=(FSAPIOTIDEntry *)sharding_htable_insert(
                    &otid_ctx, &key, &callback_arg)) != NULL)
    {
        return 0;
    } else {
        *successive_count = 0;
        return ENOMEM;
    }
}
