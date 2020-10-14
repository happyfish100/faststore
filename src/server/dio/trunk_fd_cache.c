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

#include <limits.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fast_mblock.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "trunk_fd_cache.h"

int trunk_fd_cache_init(TrunkFDCacheContext *cache_ctx, const int capacity)
{
    int result;
    int bytes;
    int alloc_elements_once;
    unsigned int *prime_capacity;

    if ((prime_capacity=hash_get_prime_capacity(capacity)) != NULL) {
        cache_ctx->htable.size = *prime_capacity;
    } else {
        cache_ctx->htable.size = capacity;
    }

    bytes = sizeof(TrunkFDCacheEntry *) * cache_ctx->htable.size;
    cache_ctx->htable.buckets = (TrunkFDCacheEntry **)fc_malloc(bytes);
    if (cache_ctx->htable.buckets == NULL) {
        return ENOMEM;
    }
    memset(cache_ctx->htable.buckets, 0, bytes);

    if (capacity < 1024) {
        alloc_elements_once = 512;
    } else if (capacity < 2 * 1024) {
        alloc_elements_once = 1 * 1024;
    } else if (capacity < 4 * 1024) {
        alloc_elements_once = 2 * 1024;
    } else if (capacity < 8 * 1024) {
        alloc_elements_once = 4 * 1024;
    } else {
        alloc_elements_once = 8 * 1024;
    }
    if ((result=fast_mblock_init_ex1(&cache_ctx->allocator,
                    "trunk_fd_cache", sizeof(TrunkFDCacheEntry),
                    alloc_elements_once, 0, NULL, NULL, false)) != 0)
    {
        return result;
    }

    cache_ctx->lru.count = 0;
    cache_ctx->lru.capacity = capacity;
    FC_INIT_LIST_HEAD(&cache_ctx->lru.head);
    return 0;
}

int trunk_fd_cache_get(TrunkFDCacheContext *cache_ctx,
        const int64_t trunk_id)
{
    TrunkFDCacheEntry **bucket;
    TrunkFDCacheEntry *entry;

    bucket = cache_ctx->htable.buckets + trunk_id % cache_ctx->htable.size;
    if (*bucket == NULL) {
        return -1;
    }
    if ((*bucket)->pair.trunk_id == trunk_id) {
        entry = *bucket;
    } else {
        entry = (*bucket)->next;
        while (entry != NULL) {
            if (entry->pair.trunk_id == trunk_id) {
                break;
            }

            entry = entry->next;
        }
    }

    if (entry != NULL) {
        fc_list_move_tail(&entry->dlink, &cache_ctx->lru.head);
        return entry->pair.fd;
    } else {
        return -1;
    }
}

int trunk_fd_cache_add(TrunkFDCacheContext *cache_ctx,
        const int64_t trunk_id, const int fd)
{
    TrunkFDCacheEntry **bucket;
    TrunkFDCacheEntry *entry;

    if (cache_ctx->lru.count >= cache_ctx->lru.capacity) {
        entry = fc_list_entry(cache_ctx->lru.head.next,
                TrunkFDCacheEntry, dlink);
        trunk_fd_cache_delete(cache_ctx, entry->pair.trunk_id);
    }

    entry = (TrunkFDCacheEntry *)fast_mblock_alloc_object(
            &cache_ctx->allocator);
    if (entry == NULL) {
        return ENOMEM;
    }

    entry->pair.trunk_id = trunk_id;
    entry->pair.fd = fd;

    bucket = cache_ctx->htable.buckets + trunk_id % cache_ctx->htable.size;
    entry->next = *bucket;
    *bucket = entry;

    fc_list_add_tail(&entry->dlink, &cache_ctx->lru.head);
    cache_ctx->lru.count++;
    return 0;
}

int trunk_fd_cache_delete(TrunkFDCacheContext *cache_ctx,
        const int64_t trunk_id)
{
    TrunkFDCacheEntry **bucket;
    TrunkFDCacheEntry *previous;
    TrunkFDCacheEntry *entry;

    bucket = cache_ctx->htable.buckets + trunk_id % cache_ctx->htable.size;
    if (*bucket == NULL) {
        return ENOENT;
    }

    previous = NULL;
    entry = *bucket;
    while (entry != NULL) {
        if (entry->pair.trunk_id == trunk_id) {
            break;
        }

        previous = entry;
        entry = entry->next;
    }
    if (entry == NULL) {
        return ENOENT;
    }

    if (previous == NULL) {
        *bucket = entry->next;
    } else {
        previous->next = entry->next;
    }

    close(entry->pair.fd);
    entry->pair.fd = -1;

    fc_list_del_init(&entry->dlink);
    fast_mblock_free_object(&cache_ctx->allocator, entry);
    cache_ctx->lru.count--;
    return 0;
}
