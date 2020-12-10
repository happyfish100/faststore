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
#include "fastcommon/shared_func.h"
#include "sharding_htable.h"

static int init_allocators(FSAPIHtableShardingContext *sharding_ctx,
        const int allocator_count, const int element_size,
        const int64_t element_limit)
{
    int result;
    int bytes;
    int alloc_elts_once;
    int64_t max_elts_per_allocator;
    struct fast_mblock_man *pa;
    struct fast_mblock_man *end;

    bytes = sizeof(struct fast_mblock_man) * allocator_count;
    sharding_ctx->allocators.elts = (struct fast_mblock_man *)fc_malloc(bytes);
    if (sharding_ctx->allocators.elts == NULL) {
        return ENOMEM;
    }

    max_elts_per_allocator = element_limit +
        (allocator_count - 1) / allocator_count;
    if (max_elts_per_allocator < 8 * 1024) {
        alloc_elts_once = max_elts_per_allocator;
    } else {
        alloc_elts_once = 8 * 1024;
    }

    end = sharding_ctx->allocators.elts + allocator_count;
    for (pa=sharding_ctx->allocators.elts; pa<end; pa++) {
        if ((result=fast_mblock_init_ex1(pa, "sharding_hkey", element_size,
                        alloc_elts_once, 0, NULL, NULL, true)) != 0)
        {
            return result;
        }
    }
    sharding_ctx->allocators.count = allocator_count;
    return 0;
}

static int init_sharding(FSAPIHtableSharding *sharding,
        const int64_t per_capacity)
{
    int result;
    int bytes;
    struct fc_list_head *ph;
    struct fc_list_head *end;

    if ((result=init_pthread_lock(&sharding->lock)) != 0) {
        return result;
    }

    bytes = sizeof(struct fc_list_head) * per_capacity;
    sharding->hashtable.buckets = (struct fc_list_head *)fc_malloc(bytes);
    if (sharding->hashtable.buckets == NULL) {
        return ENOMEM;
    }
    end = sharding->hashtable.buckets + per_capacity;
    for (ph=sharding->hashtable.buckets; ph<end; ph++) {
        FC_INIT_LIST_HEAD(ph);
    }

    sharding->hashtable.capacity = per_capacity;
    sharding->element_count = 0;
    sharding->last_reclaim_time_sec = get_current_time();
    FC_INIT_LIST_HEAD(&sharding->lru);
    return 0;
}

static int init_sharding_array(FSAPIHtableShardingContext *sharding_ctx,
        const int sharding_count, const int64_t per_elt_limit,
        const int64_t per_capacity)
{
    int result;
    int bytes;
    FSAPIHtableSharding *ps;
    FSAPIHtableSharding *end;

    bytes = sizeof(FSAPIHtableSharding) * sharding_count;
    sharding_ctx->sharding_array.entries = (FSAPIHtableSharding *)fc_malloc(bytes);
    if (sharding_ctx->sharding_array.entries == NULL) {
        return ENOMEM;
    }

    end = sharding_ctx->sharding_array.entries + sharding_count;
    for (ps=sharding_ctx->sharding_array.entries; ps<end; ps++) {
        ps->allocator = sharding_ctx->allocators.elts +
            (ps - sharding_ctx->sharding_array.entries) %
            sharding_ctx->allocators.count;
        ps->element_limit = per_elt_limit;
        ps->ctx = sharding_ctx;
        if ((result=init_sharding(ps, per_capacity)) != 0) {
            return result;
        }
    }

    sharding_ctx->sharding_array.count = sharding_count;
    return 0;
}

int sharding_htable_init(FSAPIHtableShardingContext *sharding_ctx,
        fs_api_sharding_htable_insert_callback insert_callback,
        fs_api_sharding_htable_find_callback find_callback,
        fs_api_sharding_htable_accept_reclaim_callback reclaim_callback,
        const int sharding_count, const int64_t htable_capacity,
        const int allocator_count, const int element_size,
        int64_t element_limit, const int64_t min_ttl_sec,
        const int64_t max_ttl_sec)
{
    int result;
    int64_t per_elt_limit;
    int64_t per_capacity;

    if (element_limit <= 0) {
        element_limit = 1000 * 1000;
    }
    if ((result=init_allocators(sharding_ctx, allocator_count,
                    element_size, element_limit)) != 0)
    {
        return result;
    }

    per_elt_limit = (element_limit + sharding_count - 1) / sharding_count;
    per_capacity = fc_ceil_prime(htable_capacity / sharding_count);
    if ((result=init_sharding_array(sharding_ctx, sharding_count,
                    per_elt_limit, per_capacity)) != 0)
    {
        return result;
    }

    sharding_ctx->insert_callback = insert_callback;
    sharding_ctx->find_callback = find_callback;
    sharding_ctx->accept_reclaim_callback = reclaim_callback;
    sharding_ctx->sharding_reclaim.elt_water_mark = per_elt_limit * 0.10;
    sharding_ctx->sharding_reclaim.min_ttl_sec = min_ttl_sec;
    sharding_ctx->sharding_reclaim.max_ttl_sec = max_ttl_sec;
    sharding_ctx->sharding_reclaim.elt_ttl_sec = (double)(sharding_ctx->
            sharding_reclaim.max_ttl_sec - sharding_ctx->
            sharding_reclaim.min_ttl_sec) / per_elt_limit;

    /*
    logInfo("per_elt_limit: %"PRId64", elt_water_mark: %d, "
            "elt_ttl_sec: %.2f", per_elt_limit, (int)sharding_ctx->
            sharding_reclaim.elt_water_mark, sharding_ctx->
            sharding_reclaim.elt_ttl_sec);
            */
    return 0;
}

static inline int compare_key(const FSAPITwoIdsHashKey *key1,
        const FSAPITwoIdsHashKey *key2)
{
    int sub;
    if ((sub=fc_compare_int64(key1->id1, key2->id1)) != 0) {
        return sub;
    }

    return fc_compare_int64(key1->id2, key2->id2);
}

static inline FSAPIHashEntry *htable_find(const FSAPITwoIdsHashKey *key,
        struct fc_list_head *bucket)
{
    int r;
    FSAPIHashEntry *current;

    fc_list_for_each_entry(current, bucket, dlinks.htable) {
        r = compare_key(key, &current->key);
        if (r < 0) {
            return NULL;
        } else if (r == 0) {
            return current;
        }
    }

    return NULL;
}

static inline void htable_insert(FSAPIHashEntry *entry,
        struct fc_list_head *bucket)
{
    struct fc_list_head *previous;
    struct fc_list_head *current;
    FSAPIHashEntry *pe;

    previous = bucket;
    fc_list_for_each(current, bucket) {
        pe = fc_list_entry(current, FSAPIHashEntry, dlinks.htable);
        if (compare_key(&entry->key, &pe->key) < 0) {
            break;
        }

        previous = current;
    }

    fc_list_add_internal(&entry->dlinks.htable, previous, previous->next);
}

static FSAPIHashEntry *otid_entry_reclaim(FSAPIHtableSharding *sharding)
{
    int64_t reclaim_ttl_sec;
    int64_t delta;
    int64_t reclaim_count;
    int64_t reclaim_limit;
    FSAPIHashEntry *first;
    FSAPIHashEntry *entry;
    FSAPIHashEntry *tmp;

    if (sharding->element_count <= sharding->element_limit) {
        delta = sharding->element_count;
        reclaim_limit = sharding->ctx->sharding_reclaim.elt_water_mark;
    } else {
        delta = sharding->element_limit;
        reclaim_limit = (sharding->element_count - sharding->element_limit) +
            sharding->ctx->sharding_reclaim.elt_water_mark;
    }

    first = NULL;
    reclaim_count = 0;
    reclaim_ttl_sec = (int64_t)(sharding->ctx->sharding_reclaim.max_ttl_sec -
            sharding->ctx->sharding_reclaim.elt_ttl_sec * delta);
    fc_list_for_each_entry_safe(entry, tmp, &sharding->lru, dlinks.lru) {
        if (get_current_time() - entry->last_update_time_sec <=
                reclaim_ttl_sec)
        {
            break;
        }

        if (sharding->ctx->accept_reclaim_callback != NULL &&
                !sharding->ctx->accept_reclaim_callback(entry))
        {
            continue;
        }

        fc_list_del_init(&entry->dlinks.htable);
        fc_list_del_init(&entry->dlinks.lru);
        if (first == NULL) {
            first = entry;  //keep the first
        } else {
            fast_mblock_free_object(sharding->allocator, entry);
            sharding->element_count--;
        }

        if (++reclaim_count > reclaim_limit) {
            break;
        }
    }

    if (reclaim_count > 0) {
        logInfo("sharding index: %d, element_count: %"PRId64", "
                "reclaim_ttl_sec: %"PRId64" ms, reclaim_count: %"PRId64", "
                "reclaim_limit: %"PRId64, (int)(sharding - sharding->ctx->
                    sharding_array.entries), sharding->element_count,
                reclaim_ttl_sec, reclaim_count, reclaim_limit);
    }

    return first;
}

static inline FSAPIHashEntry *htable_entry_alloc(
        FSAPIHtableSharding *sharding)
{
    FSAPIHashEntry *entry;

    if (sharding->element_count > sharding->ctx->sharding_reclaim.
            elt_water_mark && get_current_time() - sharding->
            last_reclaim_time_sec > 1000)
    {
        sharding->last_reclaim_time_sec = get_current_time();
        if ((entry=otid_entry_reclaim(sharding)) != NULL) {
            return entry;
        }
    }

    entry = (FSAPIHashEntry *)fast_mblock_alloc_object(
            sharding->allocator);
    if (entry != NULL) {
        sharding->element_count++;
        entry->sharding = sharding;
    }

    return entry;
}

#define SET_SHARDING_AND_BUCKET(sharding_ctx, key) \
    FSAPIHtableSharding *sharding; \
    struct fc_list_head *bucket;   \
    FSAPIHashEntry *entry; \
    uint64_t hash_code;    \
    \
    hash_code = key->id1 + key->id2;  \
    sharding = sharding_ctx->sharding_array.entries +   \
        hash_code % sharding_ctx->sharding_array.count; \
    bucket = sharding->hashtable.buckets +   \
        key->id1 % sharding->hashtable.capacity


void *sharding_htable_find(FSAPIHtableShardingContext
        *sharding_ctx, const FSAPITwoIdsHashKey *key, void *arg)
{
    void *data;
    SET_SHARDING_AND_BUCKET(sharding_ctx, key);

    PTHREAD_MUTEX_LOCK(&sharding->lock);
    entry = htable_find(key, bucket);
    if (entry != NULL && sharding_ctx->find_callback != NULL) {
        data = sharding_ctx->find_callback(entry, arg);
    } else {
        data = entry;
    }
    PTHREAD_MUTEX_UNLOCK(&sharding->lock);

    return data;
}

int sharding_htable_insert(FSAPIHtableShardingContext
        *sharding_ctx, const FSAPITwoIdsHashKey *key, void *arg)
{
    bool new_create;
    int result;
    SET_SHARDING_AND_BUCKET(sharding_ctx, key);

    PTHREAD_MUTEX_LOCK(&sharding->lock);
    do {
        if ((entry=htable_find(key, bucket)) == NULL) {
            if ((entry=htable_entry_alloc(sharding)) == NULL) {
                result = ENOMEM;
                break;
            }

            new_create = true;
            entry->key = *key;
            htable_insert(entry, bucket);
            fc_list_add_tail(&entry->dlinks.lru, &sharding->lru);
        } else {
            new_create = false;
            fc_list_move_tail(&entry->dlinks.lru, &sharding->lru);
        }

        entry->last_update_time_sec = get_current_time();
        result = sharding_ctx->insert_callback(
                entry, arg, new_create);
    } while (0);
    PTHREAD_MUTEX_UNLOCK(&sharding->lock);

    return result;
}
