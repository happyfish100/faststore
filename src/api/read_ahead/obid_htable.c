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
#include "obid_htable.h"

typedef struct fs_preread_block_hashtable {
    FSPrereadBlockHEntry **buckets;
    int capacity;
} FSPrereadBlockHashtable;

typedef struct fs_preread_shared_lock_array {
    pthread_mutex_t *locks;
    int count;
} FSPrereadSharedLockArray;

typedef struct fs_preread_obid_context {
    FSPrereadBlockHashtable htable;
    FSPrereadSharedLockArray larray;
} FSPrereadOBIDContext;

static FSPrereadOBIDContext obid_ctx;

#define OBID_SET_HASHTABLE_LOCK(key) \
    uint64_t hash_code;    \
    int64_t bucket_index;  \
    pthread_mutex_t *lock; \
    hash_code = (key)->oid + (key)->bid; \
    bucket_index = hash_code % obid_ctx.htable.capacity; \
    lock = obid_ctx.larray.locks + bucket_index % obid_ctx.larray.count

#define OBID_SET_BUCKET_AND_LOCK(key) \
    OBEntry **bucket;   \
    OBID_SET_HASHTABLE_LOCK(bkey);  \
    bucket = obid_ctx.htable.buckets + bucket_index


int preread_obid_htable_init(const int64_t htable_capacity,
            const int shared_lock_count)
{
    int result;
    int bytes;
    pthread_mutex_t *lock;
    pthread_mutex_t *lend;

    obid_ctx.htable.capacity = fc_ceil_prime(htable_capacity);
    bytes = sizeof(FSPrereadBlockHEntry *) * obid_ctx.htable.capacity;
    obid_ctx.htable.buckets = (FSPrereadBlockHEntry **)fc_malloc(bytes);
    if (obid_ctx.htable.buckets == NULL) {
        return ENOMEM;
    }
    memset(obid_ctx.htable.buckets, 0, bytes);

    obid_ctx.larray.count = fc_ceil_prime(shared_lock_count);
    obid_ctx.larray.locks = (pthread_mutex_t *)fc_malloc(
            sizeof(pthread_mutex_t) * obid_ctx.larray.count);
    if (obid_ctx.larray.locks == NULL) {
        return ENOMEM;
    }

    lend = obid_ctx.larray.locks + obid_ctx.larray.count;
    for (lock=obid_ctx.larray.locks; lock<lend; lock++) {
        if ((result=init_pthread_lock(lock)) != 0) {
            return result;
        }
    }

    return 0;
}

static inline int compare_key(const SFTwoIdsHashKey *key1,
        const SFTwoIdsHashKey *key2)
{
    int sub;

    if ((sub=fc_compare_int64(key1->oid, key2->oid)) != 0) {
        return sub;
    }

    return fc_compare_int64(key1->bid, key2->bid);
}

int preread_obid_htable_insert(FSAPIOperationContext *op_ctx,
        const FSSliceSize *sszie, FSAPIBuffer *buffer)
{
    /*
    SFTwoIdsHashKey key;

    key.oid = op_ctx->bs_key.block.oid;
    key.bid = op_ctx->bid;
    */
    return 0;
}
