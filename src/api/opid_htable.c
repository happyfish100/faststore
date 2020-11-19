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
#include "time_handler.h"
#include "opid_htable.h"

typedef struct fs_api_opid_entry {
    uint64_t oid;  //object id such as inode
    pid_t pid;
    int successive_count;
    struct {
        int64_t offset;
        int64_t time_ms;
    } last_write;
    struct {
        struct fc_list_head htable;  //for hashtable
        struct fc_list_head lru;     //for LRU chain
    } dlinks;
} FSAPIOPIDEntry;

typedef struct fs_api_opid_hashtable {
    struct fc_list_head *buckets;
    int64_t capacity;
} FSAPIOPIDHashtable;

typedef struct fs_api_oid_sharding {
    pthread_mutex_t lock;
    struct fast_mblock_man *allocator;
    struct fc_list_head lru;
    FSAPIOPIDHashtable hashtable;
    int64_t element_count;
    int64_t element_limit;
    int64_t last_reclaim_time_ms;
} FSAPIOPIDSharding;

typedef struct fs_api_opid_sharding_array {
    FSAPIOPIDSharding *entries;
    int count;
} FSAPIOPIDShardingArray;

typedef struct fs_api_opid_context {
    struct {
        int64_t min_ttl_ms;
        int64_t max_ttl_ms;
        double elt_ttl_ms;
        int elt_water_mark;  //trigger reclaim when elements exceeds water mark
    } sharding_reclaim;

    struct {
        int count;
        struct fast_mblock_man *els;
    } allocators;

    FSAPIOPIDShardingArray opid_shardings;
} FSAPIOPIDContext;

static FSAPIOPIDContext opid_ctx;


static int init_allocators(const int allocator_count, const int64_t element_limit)
{
    int result;
    int bytes;
    int alloc_els_once;
    int64_t max_els_per_allocator;
    struct fast_mblock_man *pa;
    struct fast_mblock_man *end;

    bytes = sizeof(struct fast_mblock_man) * allocator_count;
    opid_ctx.allocators.els = (struct fast_mblock_man *)fc_malloc(bytes);
    if (opid_ctx.allocators.els == NULL) {
        return ENOMEM;
    }

    max_els_per_allocator = element_limit +
        (allocator_count - 1) / allocator_count;
    if (max_els_per_allocator < 8 * 1024) {
        alloc_els_once = max_els_per_allocator;
    } else {
        alloc_els_once = 8 * 1024;
    }

    end = opid_ctx.allocators.els + allocator_count;
    for (pa=opid_ctx.allocators.els; pa<end; pa++) {
        if ((result=fast_mblock_init_ex1(pa, "opid_entry",
                        sizeof(FSAPIOPIDEntry), alloc_els_once,
                        0, NULL, NULL, true)) != 0)
        {
            return result;
        }
    }
    opid_ctx.allocators.count = allocator_count;
    return 0;
}

static int init_sharding(FSAPIOPIDSharding *sharding,
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
    sharding->last_reclaim_time_ms = g_current_time_ms;
    FC_INIT_LIST_HEAD(&sharding->lru);
    return 0;
}

static int init_opid_shardings(const int sharding_count,
        const int64_t per_elt_limit, const int64_t per_capacity)
{
    int result;
    int bytes;
    FSAPIOPIDSharding *ps;
    FSAPIOPIDSharding *end;

    bytes = sizeof(FSAPIOPIDSharding) * sharding_count;
    opid_ctx.opid_shardings.entries = (FSAPIOPIDSharding *)fc_malloc(bytes);
    if (opid_ctx.opid_shardings.entries == NULL) {
        return ENOMEM;
    }

    end = opid_ctx.opid_shardings.entries + sharding_count;
    for (ps=opid_ctx.opid_shardings.entries; ps<end; ps++) {
        ps->allocator = opid_ctx.allocators.els +
            (ps - opid_ctx.opid_shardings.entries) %
            opid_ctx.allocators.count;
        ps->element_limit = per_elt_limit;
        if ((result=init_sharding(ps, per_capacity)) != 0) {
            return result;
        }
    }

    opid_ctx.opid_shardings.count = sharding_count;
    return 0;
}

int opid_htable_init(const int allocator_count, int64_t element_limit,
        const int sharding_count, const int64_t htable_capacity,
        const int64_t min_ttl_ms, const int64_t max_ttl_ms)
{
    int result;
    int64_t per_elt_limit;
    int64_t per_capacity;

    if (element_limit <= 0) {
        element_limit = 1000 * 1000;
    }
    if ((result=init_allocators(allocator_count, element_limit)) != 0) {
        return result;
    }

    per_elt_limit = (element_limit + sharding_count - 1) / sharding_count;
    per_capacity = fc_ceil_prime(htable_capacity / sharding_count);
    if ((result=init_opid_shardings(sharding_count, per_elt_limit,
                    per_capacity)) != 0)
    {
        return result;
    }

    opid_ctx.sharding_reclaim.elt_water_mark = per_elt_limit * 0.10;
    opid_ctx.sharding_reclaim.min_ttl_ms = min_ttl_ms;
    opid_ctx.sharding_reclaim.max_ttl_ms = max_ttl_ms;
    opid_ctx.sharding_reclaim.elt_ttl_ms = (double)(opid_ctx.
            sharding_reclaim.max_ttl_ms - opid_ctx.
            sharding_reclaim.min_ttl_ms) / per_elt_limit;

    /*
    logInfo("per_elt_limit: %"PRId64", elt_water_mark: %d, "
            "elt_ttl_ms: %.2f", per_elt_limit, (int)opid_ctx.
            sharding_reclaim.elt_water_mark, opid_ctx.
            sharding_reclaim.elt_ttl_ms);
            */
    return 0;
}

static inline int compare_opid(const pid_t pid,
        const uint64_t oid, const FSAPIOPIDEntry *opid)
{
    int sub;
    if ((sub=fc_compare_int64(oid, opid->oid)) != 0) {
        return sub;
    }

    return (int)pid - (int)opid->pid;
}

static inline FSAPIOPIDEntry *htable_find(const pid_t pid,
        const uint64_t oid, struct fc_list_head *bucket)
{
    int r;
    FSAPIOPIDEntry *current;

    fc_list_for_each_entry(current, bucket, dlinks.htable) {
        r = compare_opid(pid, oid, current);
        if (r < 0) {
            return NULL;
        } else if (r == 0) {
            return current;
        }
    }

    return NULL;
}

static inline void htable_insert(FSAPIOPIDEntry *entry,
        struct fc_list_head *bucket)
{
    struct fc_list_head *previous;
    struct fc_list_head *current;
    FSAPIOPIDEntry *pe;

    previous = bucket;
    fc_list_for_each(current, bucket) {
        pe = fc_list_entry(current, FSAPIOPIDEntry, dlinks.htable);
        if (compare_opid(entry->pid, entry->oid, pe) < 0) {
            break;
        }

        previous = current;
    }

    fc_list_add_internal(&entry->dlinks.htable, previous, previous->next);
}

static FSAPIOPIDEntry *opid_entry_reclaim(FSAPIOPIDSharding *sharding)
{
    int64_t reclaim_ttl_ms;
    int64_t delta;
    int64_t reclaim_count;
    int64_t reclaim_limit;
    FSAPIOPIDEntry *first;
    FSAPIOPIDEntry *entry;
    FSAPIOPIDEntry *tmp;

    if (sharding->element_count <= sharding->element_limit) {
        delta = sharding->element_count;
        reclaim_limit = opid_ctx.sharding_reclaim.elt_water_mark;
    } else {
        delta = sharding->element_limit;
        reclaim_limit = (sharding->element_count - sharding->element_limit) +
            opid_ctx.sharding_reclaim.elt_water_mark;
    }

    first = NULL;
    reclaim_count = 0;
    reclaim_ttl_ms = (int64_t)(opid_ctx.sharding_reclaim.max_ttl_ms -
        opid_ctx.sharding_reclaim.elt_ttl_ms * delta);
    fc_list_for_each_entry_safe(entry, tmp, &sharding->lru, dlinks.lru) {
        if (g_current_time_ms - entry->last_write.time_ms <= reclaim_ttl_ms) {
            break;
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

    /*
    logInfo("sharding index: %d, element_count: %"PRId64", "
            "reclaim_ttl_ms: %"PRId64" ms, reclaim_count: %"PRId64", "
            "reclaim_limit: %"PRId64, (int)(sharding - opid_ctx.opid_shardings.entries),
            sharding->element_count, reclaim_ttl_ms, reclaim_count, reclaim_limit);
            */

    return first;
}

static inline FSAPIOPIDEntry *opid_entry_alloc(FSAPIOPIDSharding *sharding,
        const pid_t pid, const uint64_t oid)
{
    FSAPIOPIDEntry *entry;

    do {
        if (sharding->element_count > opid_ctx.sharding_reclaim.
                elt_water_mark && g_current_time_ms - sharding->
                last_reclaim_time_ms > 1000)
        {
            sharding->last_reclaim_time_ms = g_current_time_ms;
            if ((entry=opid_entry_reclaim(sharding)) != NULL) {
                break;
            }
        }

        entry = (FSAPIOPIDEntry *)fast_mblock_alloc_object(
                sharding->allocator);
        if (entry == NULL) {
            return NULL;
        }

        sharding->element_count++;
    } while (0);

    entry->pid = pid;
    entry->oid = oid;
    return entry;
}

int opid_htable_insert(const pid_t pid, const uint64_t oid,
        const int64_t offset, const int length, int *successive_count)
{
    FSAPIOPIDSharding *sharding;
    struct fc_list_head *bucket;
    FSAPIOPIDEntry *entry;
    int result;

    sharding = opid_ctx.opid_shardings.entries +
        oid % opid_ctx.opid_shardings.count;
    bucket = sharding->hashtable.buckets +
        oid % sharding->hashtable.capacity;

    PTHREAD_MUTEX_LOCK(&sharding->lock);
    do {
        if ((entry=htable_find(pid, oid, bucket)) == NULL) {
            if ((entry=opid_entry_alloc(sharding, pid, oid)) == NULL) {
                *successive_count = 0;
                result = ENOMEM;
                break;
            }
            entry->successive_count = 0;
            htable_insert(entry, bucket);
            fc_list_add_tail(&entry->dlinks.lru, &sharding->lru);
        } else {
            if (offset == entry->last_write.offset) {
                entry->successive_count++;
            } else {
                entry->successive_count = 0;
            }
            fc_list_move_tail(&entry->dlinks.lru, &sharding->lru);
        }

        *successive_count = entry->successive_count;
        entry->last_write.offset = offset + length;
        entry->last_write.time_ms = g_current_time_ms;
        result = 0;
    } while (0);
    PTHREAD_MUTEX_UNLOCK(&sharding->lock);

    return result;
}
