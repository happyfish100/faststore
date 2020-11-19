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

#ifndef _SHARDING_HTABLE_H
#define _SHARDING_HTABLE_H

#include "fs_api_types.h"

struct fs_api_hash_entry;
typedef void (*fs_api_sharding_htable_set_entry_callback)
    (struct fs_api_hash_entry *entry, void *arg,
     const bool new_create);

typedef struct fs_api_two_ids_hash_key {
    union {
        uint64_t id1;
        uint64_t oid;  //object id such as inode
    };

    union {
        uint64_t id2;
        pid_t pid;
        uint64_t bid;  //file block id
    };
} FSAPITwoIdsHashKey;

typedef struct fs_api_hash_entry {
    FSAPITwoIdsHashKey key;
    struct {
        struct fc_list_head htable;  //for hashtable
        struct fc_list_head lru;     //for LRU chain
    } dlinks;
    int64_t last_update_time_ms;
} FSAPIHashEntry;

typedef struct fs_api_hashtable {
    struct fc_list_head *buckets;
    int64_t capacity;
} FSAPIHashtable;

struct fs_api_htable_sharding_context;
typedef struct fs_api_htable_sharding {
    pthread_mutex_t lock;
    struct fast_mblock_man *allocator;
    struct fc_list_head lru;
    FSAPIHashtable hashtable;
    int64_t element_count;
    int64_t element_limit;
    int64_t last_reclaim_time_ms;
    struct fs_api_htable_sharding_context *ctx;
} FSAPIHtableSharding;

typedef struct fs_api_htable_sharding_array {
    FSAPIHtableSharding *entries;
    int count;
} FSAPIHtableShardingArray;

typedef struct fs_api_htable_sharding_context {
    struct {
        int64_t min_ttl_ms;
        int64_t max_ttl_ms;
        double elt_ttl_ms;
        int elt_water_mark;  //trigger reclaim when elements exceeds water mark
    } sharding_reclaim;

    struct {
        int count;
        struct fast_mblock_man *elts;
    } allocators;

    fs_api_sharding_htable_set_entry_callback set_entry_callback;
    FSAPIHtableShardingArray sharding_array;
} FSAPIHtableShardingContext;


#ifdef __cplusplus
extern "C" {
#endif

    int sharding_htable_init(FSAPIHtableShardingContext *sharding_ctx,
            fs_api_sharding_htable_set_entry_callback set_entry_callback,
            const int sharding_count, const int64_t htable_capacity,
            const int allocator_count, const int element_size,
            int64_t element_limit, const int64_t min_ttl_ms,
            const int64_t max_ttl_ms);

    FSAPIHashEntry *sharding_htable_insert(FSAPIHtableShardingContext
            *sharding_ctx, const FSAPITwoIdsHashKey *key, void *arg);

#ifdef __cplusplus
}
#endif

#endif
