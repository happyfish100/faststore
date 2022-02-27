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
#include "timeout_handler.h"
#include "combine_handler.h"
#include "oid_htable.h"

typedef struct oid_htable_insert_block_context {
    FSAPIBlockEntry *block;
} OIDHtableInsertBlockContext;

typedef OIDHtableInsertBlockContext OIDHtableDeleteBlockContext;

static SFHtableShardingContext oid_ctx;

static int oid_htable_insert_callback(SFShardingHashEntry *he,
        void *arg, const bool new_create)
{
    FSWCombineOIDEntry *entry;
    OIDHtableInsertBlockContext *ictx;

    entry = (FSWCombineOIDEntry *)he;
    ictx = (OIDHtableInsertBlockContext *)arg;
    if (new_create) {
        FC_INIT_LIST_HEAD(&entry->writing_head);
    }

    fc_list_add_tail(&ictx->block->dlink, &entry->writing_head);
    return 0;
}

static int realloc_id_array(FSWCombineIDArray *array)
{
    int64_t *elts;
    int alloc;

    alloc = array->alloc * 2;
    elts = fc_malloc(sizeof(int64_t) * alloc);
    if (elts == NULL) {
        return ENOMEM;
    }

    memcpy(elts, array->elts, sizeof(int64_t) * array->count);
    if (array->elts != array->fixed) {
        free(array->elts);
    }

    array->elts = elts;
    array->alloc = alloc;
    return 0;
}

static void *oid_htable_find_callback(SFShardingHashEntry *he, void *arg)
{
    FSWCombineOIDEntry *entry;
    FSAPIBlockEntry *block;
    FSWCombineIDArray *array;

    entry = (FSWCombineOIDEntry *)he;
    array = (FSWCombineIDArray *)arg;
    fc_list_for_each_entry(block, &entry->writing_head, dlink) {
        if (array->count >= array->alloc) {
            if (realloc_id_array(array) != 0) {
                return NULL;
            }
        }
        array->elts[array->count++] = block->hentry.key.bid;
    }

    return entry;
}

static bool oid_htable_delete_callback(SFShardingHashEntry *he, void *arg)
{
    FSWCombineOIDEntry *entry;
    OIDHtableInsertBlockContext *ictx;

    entry = (FSWCombineOIDEntry *)he;
    ictx = (OIDHtableDeleteBlockContext *)arg;
    fc_list_del_init(&ictx->block->dlink);
    return fc_list_empty(&entry->writing_head);
}

static bool oid_htable_accept_reclaim_callback(SFShardingHashEntry *he)
{
    return fc_list_empty(&((FSWCombineOIDEntry *)he)->writing_head);
}

int wcombine_oid_htable_init(const int sharding_count,
        const int64_t htable_capacity,
        const int allocator_count, int64_t element_limit,
        const int64_t min_ttl_ms, const int64_t max_ttl_ms,
        const double low_water_mark_ratio)
{
    return sf_sharding_htable_init_ex(&oid_ctx,
            sf_sharding_htable_key_ids_one,
            oid_htable_insert_callback, oid_htable_find_callback,
            oid_htable_delete_callback, oid_htable_accept_reclaim_callback,
            sharding_count, htable_capacity, allocator_count,
            sizeof(FSWCombineOIDEntry), element_limit, min_ttl_ms,
            max_ttl_ms, low_water_mark_ratio);
}

int wcombine_oid_htable_insert(FSAPIBlockEntry *block)
{
    SFTwoIdsHashKey key;
    OIDHtableInsertBlockContext ictx;

    key.oid = block->hentry.key.oid;
    ictx.block = block;
    return sf_sharding_htable_insert(&oid_ctx, &key, &ictx);
}

int wcombine_oid_htable_find(const int64_t oid, FSWCombineIDArray *array)
{
    SFTwoIdsHashKey key;

    key.oid = oid;
    return sf_sharding_htable_find(&oid_ctx, &key, array) != NULL ? 0 : ENOENT;
}

int wcombine_oid_htable_delete(FSAPIBlockEntry *block)
{
    SFTwoIdsHashKey key;
    OIDHtableDeleteBlockContext ictx;

    key.oid = block->hentry.key.oid;
    ictx.block = block;
    return sf_sharding_htable_delete(&oid_ctx, &key, &ictx);
}
