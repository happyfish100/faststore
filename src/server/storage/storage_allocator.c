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

static int prealloc_trunk_freelist(FSStorageAllocatorContext *allocator_ctx)
{
    FSTrunkAllocator *allocator;
    FSTrunkAllocator *end;
    const FSTrunkInfoPtrArray *trunk_ptr_array;

    end = allocator_ctx->all.allocators + allocator_ctx->all.count;
    for (allocator=allocator_ctx->all.allocators; allocator<end; allocator++) {
        trunk_ptr_array = trunk_allocator_free_size_top_n(
                allocator, allocator->path_info->write_thread_count *
                (2 + allocator->path_info->prealloc_trunks));

        logInfo("trunk_ptr_array count: %d", trunk_ptr_array->count);
        trunk_allocator_array_to_freelists(allocator, trunk_ptr_array);
        trunk_allocator_prealloc_trunks(allocator);
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
    return 0;
}
