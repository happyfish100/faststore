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
#include "obid_htable.h"

static FSAPIHtableShardingContext obid_ctx;

typedef struct fs_api_set_entry_callback_arg {
    FSSliceSize ssize;
    FSAPICombinedWriter *writer;
} FSAPISetEntryCallbackArg;

static void *obid_htable_insert_callback(struct fs_api_hash_entry *he,
        void *arg, FSAPIHtableSharding *sharding, const bool new_create)
{
    FSAPIBlockEntry *block;
    FSAPISliceEntry *slice;
    bool empty;
    FSAPISetEntryCallbackArg *callback_arg;

    block = (FSAPIBlockEntry *)he;
    callback_arg = (FSAPISetEntryCallbackArg *)arg;
    if (new_create) {
        block->sharding = sharding;
        FC_INIT_LIST_HEAD(&block->slices.head);
        empty = true;
    } else {
        empty = fc_list_empty(&block->slices.head);
        if (!empty) {
            fc_list_for_each_entry(slice, &block->slices.head, dlink) {
            }
        }
    }

    //TODO
    slice = NULL;
    fc_list_add(&slice->dlink, &block->slices.head);

    return slice;
}

static void *obid_htable_find_callback(struct fs_api_hash_entry *he,
        void *arg)
{
    FSAPIBlockEntry *entry;
    FSAPISetEntryCallbackArg *callback_arg;

    entry = (FSAPIBlockEntry *)he;
    callback_arg = (FSAPISetEntryCallbackArg *)arg;

    //TODO
    return entry;
}

int obid_htable_init(const int sharding_count,
        const int64_t htable_capacity,
        const int allocator_count, int64_t element_limit,
        const int64_t min_ttl_ms, const int64_t max_ttl_ms)
{
    return sharding_htable_init(&obid_ctx, obid_htable_insert_callback,
            obid_htable_find_callback, sharding_count, htable_capacity,
            allocator_count, sizeof(FSAPIBlockEntry), element_limit,
            min_ttl_ms, max_ttl_ms);
}

FSAPISliceEntry *obid_htable_insert(const FSBlockSliceKeyInfo *bs_key,
        FSAPICombinedWriter *writer)
{
    FSAPITwoIdsHashKey key;
    FSAPISetEntryCallbackArg callback_arg;
    FSAPIBlockEntry *entry;

    key.oid = bs_key->block.oid;
    key.bid = FS_FILE_BLOCK_ALIGN(bs_key->block.offset) / FS_FILE_BLOCK_SIZE;
    callback_arg.ssize = bs_key->slice;
    callback_arg.writer = writer;
    if ((entry=(FSAPIBlockEntry *)sharding_htable_insert(
                    &obid_ctx, &key, &callback_arg)) != NULL)
    {
        //TODO
        return NULL;
    } else {
        return NULL;
    }
}
