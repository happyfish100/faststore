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

typedef struct {
    FSPrereadSliceEntry *previous;
    FSPrereadSliceEntry *current;
} FSPrereadSlicePair;

typedef struct {
    FSPrereadBlockHEntry *previous;
    FSPrereadBlockHEntry *current;
} FSPrereadBlockPair;

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

#define OBID_SET_HASHTABLE_KEY(op_ctx, key) \
    SFTwoIdsHashKey key;   \
    key.oid = op_ctx->bs_key.block.oid;  \
    key.bid = op_ctx->bid;

#define OBID_SET_HASHTABLE_LOCK(key) \
    uint64_t hash_code;    \
    int64_t bucket_index;  \
    pthread_mutex_t *lock; \
    hash_code = (key).oid + (key).bid; \
    bucket_index = hash_code % obid_ctx.htable.capacity; \
    lock = obid_ctx.larray.locks + bucket_index % obid_ctx.larray.count

#define OBID_SET_BUCKET_AND_LOCK(key) \
    FSPrereadBlockHEntry **bucket;    \
    OBID_SET_HASHTABLE_LOCK(key);     \
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

static inline int obid_htable_find_block(const SFTwoIdsHashKey *key,
        FSPrereadBlockPair *block)
{
    block->previous = NULL;
    while (block->current != NULL) {
        if (compare_key(key, &block->current->key) == 0) {
            return 0;
        }

        block->previous = block->current;
        block->current = block->current->next;
    }

    return ENOENT;
}

static inline int obid_htable_find_slice(const SFTwoIdsHashKey *key,
        const uint64_t tid, FSPrereadBlockPair *block,
        FSPrereadSlicePair *slice)
{
    if (obid_htable_find_block(key, block) != 0) {
        return ENOENT;
    }

    slice->previous = NULL;
    slice->current = block->current->slices.head;
    while (slice->current != NULL) {
        if (tid == slice->current->tid) {
            return 0;
        }

        slice->previous = slice->current;
        slice->current = slice->current->next;
    }

    return ENOENT;
}

static inline void check_remove_block(FSPrereadBlockHEntry **bucket,
        FSPrereadBlockPair *block, bool *release_block)
{
    *release_block = (block->current->slices.head == NULL);
    if (*release_block) {
        if (block->previous == NULL) {
            *bucket = block->current->next;
        } else {
            block->previous->next = block->current->next;
        }
    }
}

static inline int remove_conflict_slices(
        const SFTwoIdsHashKey *key, const FSSliceSize *ssize,
        FSPrereadBlockHEntry **bucket, FSPrereadBlockPair *block,
        FSPrereadSliceEntry **slices, bool *release_block)
{
    FSPrereadSliceEntry *previous;
    FSPrereadSliceEntry *current;
    FSPrereadSliceEntry *tail;

    *slices = NULL;
    *release_block = false;
    if (obid_htable_find_block(key, block) != 0) {
        return ENOENT;
    }

    previous = tail = NULL;
    current = block->current->slices.head;
    while (current != NULL) {
        if (fs_slice_is_overlap(ssize, &current->ssize)) {
            if (previous == NULL) {
                block->current->slices.head = current->next;
            } else {
                previous->next = current->next;
            }

            if (*slices == NULL) {
                *slices = current;
            } else {
                tail->next = current;
            }
            tail = current;
        }

        previous = current;
        current = current->next;
    }

    if (*slices == NULL) {
        return ENOENT;
    }

    tail->next = NULL;
    check_remove_block(bucket, block, release_block);
    return 0;
}

static inline void obid_htable_remove(FSPrereadBlockHEntry **bucket,
        FSPrereadBlockPair *block, FSPrereadSlicePair *slice,
        bool *release_block)
{
    if (slice->previous == NULL) {
        block->current->slices.head = slice->current->next;
        check_remove_block(bucket, block, release_block);
    } else {
        slice->previous->next = slice->current->next;
        *release_block = false;
        return;
    }
}

static inline void obid_htable_free_slice(FSPrereadSliceEntry *slice,
        const bool set_conflict)
{
    if (set_conflict) {
        PTHREAD_MUTEX_LOCK(slice->buffer->lock);
        slice->buffer->conflict = true;
        PTHREAD_MUTEX_UNLOCK(slice->buffer->lock);
    }
    fs_api_buffer_release(slice->buffer);
    fast_mblock_free_object(slice->allocator, slice);
}

static inline void obid_htable_free_slice_and_block(FSPrereadBlockHEntry
        *block, FSPrereadSliceEntry *slice, const bool release_block,
        const bool set_conflict)
{
    obid_htable_free_slice(slice, set_conflict);
    if (release_block) {
        fast_mblock_free_object(block->allocator, block);
    }
}

int preread_obid_htable_insert(FSAPIOperationContext *op_ctx,
        const FSSliceSize *ssize, FSAPIBuffer *buffer)
{
    int result;
    bool found;
    bool release_block;
    FSPrereadBlockPair block;
    FSPrereadSlicePair slice;
    FSPrereadBlockHEntry *be;
    FSPrereadSliceEntry *se;

    OBID_SET_HASHTABLE_KEY(op_ctx, key);

    if ((se=(FSPrereadSliceEntry *)fast_mblock_alloc_object(&op_ctx->
                    allocator_ctx->read_ahead.slice)) == NULL)
    {
        return ENOMEM;
    }
    se->tid = op_ctx->tid;
    se->ssize = *ssize;
    se->buffer = buffer;

    do {
        OBID_SET_BUCKET_AND_LOCK(key);

        PTHREAD_MUTEX_LOCK(lock);
        if (*bucket != NULL) {
            block.current = *bucket;
            if (obid_htable_find_slice(&key, op_ctx->tid,
                        &block, &slice) == 0)
            {
                found = true;
                obid_htable_remove(bucket, &block,
                        &slice, &release_block);
                be = (release_block ? NULL : block.current);
            } else {
                be = NULL;
                found = release_block = false;
            }
        } else {
            be = NULL;
            found = release_block = false;
        }

        if (be != NULL) {
            se->next = be->slices.head;
            be->slices.head = se;
            result = 0;
        } else if ((be=(FSPrereadBlockHEntry *)fast_mblock_alloc_object(
                        &op_ctx->allocator_ctx->read_ahead.block)) != NULL)
        {
            se->next = NULL;
            be->key.oid = op_ctx->bs_key.block.oid;
            be->key.bid = op_ctx->bid;
            be->slices.head = se;

            be->next = *bucket;
            *bucket = be;
            result = 0;
        } else {
            fast_mblock_free_object(se->allocator, se);
            result = ENOMEM;
        }
        PTHREAD_MUTEX_UNLOCK(lock);
    } while (0);

    if (found) {
        obid_htable_free_slice_and_block(block.current,
                slice.current, release_block, true);
    }

    return result;
}

int preread_obid_htable_delete(const int64_t oid,
        const int64_t bid, const int64_t tid)
{
    int result;
    bool release_block;
    FSPrereadBlockPair block;
    FSPrereadSlicePair slice;
    SFTwoIdsHashKey key;

    key.oid = oid;
    key.bid = bid;
    do {
        OBID_SET_BUCKET_AND_LOCK(key);

        PTHREAD_MUTEX_LOCK(lock);
        if (*bucket != NULL) {
            block.current = *bucket;
            if ((result=obid_htable_find_slice(&key, tid,
                            &block, &slice)) == 0)
            {
                obid_htable_remove(bucket, &block,
                        &slice, &release_block);
            } else {
                release_block = false;
            }
        } else {
            release_block = false;
            result = ENOENT;
        }
        PTHREAD_MUTEX_UNLOCK(lock);
    } while (0);

    if (result == 0) {
        obid_htable_free_slice_and_block(block.current,
                slice.current, release_block, false);
    }

    return result;
}

int preread_invalidate_conflict_slices(FSAPIOperationContext *op_ctx)
{
    int result;
    int count;
    bool release_block;
    FSPrereadBlockPair block;
    FSPrereadSliceEntry *slices;
    FSPrereadSliceEntry *slice;
    FSPrereadSliceEntry *deleted;
    OBID_SET_HASHTABLE_KEY(op_ctx, key);

    do {
        OBID_SET_BUCKET_AND_LOCK(key);

        PTHREAD_MUTEX_LOCK(lock);
        if (*bucket != NULL) {
            block.current = *bucket;
            result = remove_conflict_slices(&key, &op_ctx->bs_key.slice,
                    bucket, &block, &slices, &release_block);
        } else {
            release_block = false;
            result = ENOENT;
        }
        PTHREAD_MUTEX_UNLOCK(lock);
    } while (0);

    if (result != 0) {
        return 0;
    }

    count = 0;
    slice = slices;
    while (slice != NULL) {
        deleted = slice;
        slice = slice->next;

        obid_htable_free_slice(deleted, true);
        ++count;
    }
    if (release_block) {
        fast_mblock_free_object(block.current->allocator, block.current);
    }

    return count;
}
