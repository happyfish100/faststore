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
#include "opid_htable.h"

typedef struct fs_api_opid_entry {
    FSAPIHashEntry hentry;  //must be the first
    int successive_count;
    int64_t last_write_offset;
    FSAPICombinedWriter *writer;
} FSAPIOPIDEntry;

typedef struct fs_api_set_entry_callback_arg {
    int64_t offset;
    int length;
    int *successive_count;
} FSAPIOPIDSetEntryCallbackArg;

static FSAPIHtableShardingContext opid_ctx;

static void *opid_htable_set_entry(struct fs_api_hash_entry *he,
        void *arg, const bool new_create)
{
    FSAPIOPIDEntry *entry;
    FSAPIOPIDSetEntryCallbackArg *callback_arg;

    entry = (FSAPIOPIDEntry *)he;
    callback_arg = (FSAPIOPIDSetEntryCallbackArg *)arg;
    if (new_create) {
        entry->successive_count = 0;
    } else {
        if (callback_arg->offset == entry->last_write_offset) {
            entry->successive_count++;
        } else {
            entry->successive_count = 0;
        }
    }
    *callback_arg->successive_count = entry->successive_count;
    entry->last_write_offset = callback_arg->offset + callback_arg->length;
    return entry;
}

int opid_htable_init(const int sharding_count,
        const int64_t htable_capacity,
        const int allocator_count, int64_t element_limit,
        const int64_t min_ttl_ms, const int64_t max_ttl_ms)
{
    return sharding_htable_init(&opid_ctx, opid_htable_set_entry,
            NULL, sharding_count, htable_capacity, allocator_count,
            sizeof(FSAPIOPIDEntry), element_limit, min_ttl_ms, max_ttl_ms);
}

int opid_htable_insert(const FSAPITwoIdsHashKey *key,
        const int64_t offset, const int length, int *successive_count)
{
    FSAPIOPIDSetEntryCallbackArg callback_arg;
    FSAPIOPIDEntry *entry;

    callback_arg.offset = offset;
    callback_arg.length = length;
    callback_arg.successive_count = successive_count;
    if ((entry=(FSAPIOPIDEntry *)sharding_htable_insert(
                    &opid_ctx, key, &callback_arg)) != NULL)
    {
        return 0;
    } else {
        *successive_count = 0;
        return ENOMEM;
    }
}
