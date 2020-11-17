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

typedef struct fs_api_opid_hashtable {
    FSAPIOPIDEntry **buckets;
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
        int count;
        struct fast_mblock_man *els;
    } allocators;

    int64_t min_ttl_ms;
    int64_t max_ttl_ms;
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

    if ((result=init_pthread_lock(&sharding->lock)) != 0) {
        return result;
    }

    bytes = sizeof(FSAPIOPIDEntry *) * per_capacity;
    sharding->hashtable.buckets = (FSAPIOPIDEntry **)fc_malloc(bytes);
    if (sharding->hashtable.buckets == NULL) {
        return ENOMEM;
    }
    memset(sharding->hashtable.buckets, 0, bytes);

    sharding->hashtable.capacity = per_capacity;
    sharding->element_count = 0;
    sharding->last_reclaim_time_ms = 0;
    FC_INIT_LIST_HEAD(&sharding->lru);
    return 0;
}

static int init_opid_shardings(const int sharding_count,
        const int64_t element_limit, const int64_t htable_capacity)
{
    int result;
    int bytes;
    int64_t per_capacity;
    int64_t per_elt_limit;
    FSAPIOPIDSharding *ps;
    FSAPIOPIDSharding *end;

    bytes = sizeof(FSAPIOPIDSharding) * sharding_count;
    opid_ctx.opid_shardings.entries = (FSAPIOPIDSharding *)fc_malloc(bytes);
    if (opid_ctx.opid_shardings.entries == NULL) {
        return ENOMEM;
    }

    per_elt_limit = (element_limit + sharding_count - 1) / sharding_count;
    per_capacity = fc_ceil_prime(htable_capacity / sharding_count);
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

    if (element_limit <= 0) {
        element_limit = 1000 * 1000;
    }
    if ((result=init_allocators(allocator_count, element_limit)) != 0) {
        return result;
    }

    if ((result=init_opid_shardings(sharding_count, element_limit,
                    htable_capacity)) != 0)
    {
        return result;
    }

    opid_ctx.min_ttl_ms = min_ttl_ms;
    opid_ctx.max_ttl_ms = max_ttl_ms;
    return 0;
}

int opid_htable_insert(const pid_t pid, const uint64_t oid,
        const int64_t offset, int *successive_count)
{
    return 0;
}
