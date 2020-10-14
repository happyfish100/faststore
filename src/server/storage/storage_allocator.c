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
        if ((result=trunk_allocator_init(pallocator, path)) != 0) {
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

/*
static void log_trunk_ptr_array(const FSTrunkInfoPtrArray *trunk_ptr_array)
{
    FSTrunkFileInfo **pp;
    FSTrunkFileInfo **end;

    end = trunk_ptr_array->trunks + trunk_ptr_array->count;
    for (pp=trunk_ptr_array->trunks; pp<end; pp++) {
        trunk_allocator_log_trunk_info(*pp);
    }
}
*/

static int64_t sum_trunk_avail_space(const FSTrunkInfoPtrArray *trunk_ptr_array)
{
    FSTrunkFileInfo **pp;
    FSTrunkFileInfo **end;
    int64_t avail;

    avail = 0;
    end = trunk_ptr_array->trunks + trunk_ptr_array->count;
    for (pp=trunk_ptr_array->trunks; pp<end; pp++) {
        avail += FS_TRUNK_AVAIL_SPACE(*pp);
    }

    return avail;
}

static int prealloc_trunk_freelist(FSStorageAllocatorContext *allocator_ctx)
{
    FSTrunkAllocator *allocator;
    FSTrunkAllocator *end;
    int n;
    const FSTrunkInfoPtrArray *trunk_ptr_array;

    end = allocator_ctx->all.allocators + allocator_ctx->all.count;
    for (allocator=allocator_ctx->all.allocators; allocator<end; allocator++) {
        trunk_allocator_trunk_stat(allocator,
                &allocator->path_info->trunk_stat);

        n = allocator->path_info->write_thread_count *
            (2 + allocator->path_info->prealloc_trunks);
        trunk_ptr_array = trunk_allocator_free_size_top_n(allocator, n);

        allocator->path_info->trunk_stat.avail =
            sum_trunk_avail_space(trunk_ptr_array);

        //logInfo("top n: %d, trunk_ptr_array count: %d", n, trunk_ptr_array->count);
        //log_trunk_ptr_array(trunk_ptr_array);

        trunk_allocator_array_to_freelists(allocator, trunk_ptr_array);
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

    if ((result=prealloc_trunk_freelist(&g_allocator_mgr->write_cache)) != 0) {
        return result;
    }

    if ((result=prealloc_trunk_freelist(&g_allocator_mgr->store_path)) != 0) {
        return result;
    }

    ob_index_enable_modify_used_space();
    return 0;
}
