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
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fc_memory.h"
#include "sf/sf_global.h"
#include "../server_types.h"
#include "../server_global.h"
#include "storage_allocator.h"

static FSStorageAllocatorManager allocator_mgr;
FSStorageAllocatorManager *g_allocator_mgr = &allocator_mgr;

static int init_allocator_context(FSStorageAllocatorContext *allocator_ctx,
        FSStoragePathArray *parray)
{
    int result;
    int bytes;
    FSStoragePathInfo *path;
    FSStoragePathInfo *end;
    FSTrunkAllocator *pallocator;
    FSTrunkAllocator **ppallocator;

    if (parray->count == 0) {
        return 0;
    }

    bytes = sizeof(FSTrunkAllocator) * parray->count;
    allocator_ctx->all.allocators = (FSTrunkAllocator *)fc_malloc(bytes);
    if (allocator_ctx->all.allocators == NULL) {
        return ENOMEM;
    }
    memset(allocator_ctx->all.allocators, 0, bytes);

    bytes = sizeof(FSTrunkAllocator *) * parray->count;
    allocator_ctx->avail.allocators = (FSTrunkAllocator **)fc_malloc(bytes);
    if (allocator_ctx->avail.allocators == NULL) {
        return ENOMEM;
    }

    end = parray->paths + parray->count;
    for (path=parray->paths,pallocator=allocator_ctx->all.allocators,
            ppallocator=allocator_ctx->avail.allocators; path<end;
            path++, pallocator++, ppallocator++)
    {
        if ((result=trunk_allocator_init_instance(pallocator, path)) != 0) {
            return result;
        }

        *ppallocator = pallocator;
        g_allocator_mgr->allocator_ptr_array.allocators
            [path->store.index] = pallocator;
    }
    allocator_ctx->all.count = parray->count;
    allocator_ctx->avail.count = parray->count;
    return 0;
}

int storage_allocator_init()
{
    int result;

    memset(g_allocator_mgr, 0, sizeof(FSStorageAllocatorManager));
    g_allocator_mgr->allocator_ptr_array.count = STORAGE_CFG.
        max_store_path_index + 1;
    g_allocator_mgr->allocator_ptr_array.allocators = (FSTrunkAllocator **)
        fc_calloc(g_allocator_mgr->allocator_ptr_array.count,
                sizeof(FSTrunkAllocator *));
    if (g_allocator_mgr->allocator_ptr_array.allocators == NULL) {
        return ENOMEM;
    }

    if ((result=trunk_allocator_init()) != 0) {
        return result;
    }

    if ((result=init_allocator_context(&g_allocator_mgr->write_cache,
                    &STORAGE_CFG.write_cache)) != 0)
    {
        return result;
    }
    if ((result=init_allocator_context(&g_allocator_mgr->store_path,
                    &STORAGE_CFG.store_path)) != 0)
    {
        return result;
    }

    if (g_allocator_mgr->write_cache.avail.count > 0) {
        g_allocator_mgr->current = &g_allocator_mgr->write_cache;
    } else {
        g_allocator_mgr->current = &g_allocator_mgr->store_path;
    }

    return trunk_id_info_init();
}

static int prealloc_trunk_freelist(FSStorageAllocatorContext *allocator_ctx)
{
    FSTrunkAllocator *allocator;
    FSTrunkAllocator *end;

    end = allocator_ctx->all.allocators + allocator_ctx->all.count;
    for (allocator=allocator_ctx->all.allocators; allocator<end; allocator++) {
        trunk_allocator_deal_on_ready(allocator);
        trunk_allocator_prealloc_trunks(allocator);

        /*
        logInfo("path index: %d, total: %"PRId64" MB, used: %"PRId64" MB, "
                "avail: %"PRId64" MB", allocator->path_info->store.index,
                allocator->path_info->trunk_stat.total / (1024 * 1024),
                allocator->path_info->trunk_stat.used / (1024 * 1024),
                allocator->path_info->trunk_stat.avail / (1024 * 1024));
                */
    }

    return 0;
}

int storage_allocator_prealloc_trunk_freelists()
{
    int result;

    g_trunk_allocator_vars.data_load_done = true;
    ob_index_enable_modify_used_space();

    if ((result=prealloc_trunk_freelist(&g_allocator_mgr->write_cache)) != 0) {
        return result;
    }

    if ((result=prealloc_trunk_freelist(&g_allocator_mgr->store_path)) != 0) {
        return result;
    }

    return 0;
}
