#include <limits.h>
#include <sys/stat.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "sf/sf_global.h"
#include "../server_types.h"
#include "../server_global.h"
#include "storage_allocator.h"

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
    allocator_ctx->all.allocators = (FSTrunkAllocator *)malloc(bytes);
    if (allocator_ctx->all.allocators == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }

    bytes = sizeof(FSTrunkAllocator *) * parray->count;
    allocator_ctx->avail.allocators = (FSTrunkAllocator **)malloc(bytes);
    if (allocator_ctx->avail.allocators == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
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
    }
    allocator_ctx->all.count = parray->count;
    allocator_ctx->avail.count = parray->count;
    return 0;
}

int storage_allocator_init(FSStorageAllocatorManager *allocator_mgr)
{
    int result;

    memset(allocator_mgr, 0, sizeof(FSStorageAllocatorManager));
    if ((result=init_allocator_context(&allocator_mgr->write_cache,
                    &STORAGE_CFG.write_cache)) != 0)
    {
        return result;
    }
    if ((result=init_allocator_context(&allocator_mgr->store_path,
                    &STORAGE_CFG.store_path)) != 0)
    {
        return result;
    }

    if (allocator_mgr->write_cache.avail.count > 0) {
        allocator_mgr->current = &allocator_mgr->write_cache;
    } else {
        allocator_mgr->current = &allocator_mgr->store_path;
    }

    return 0;
}

int storage_allocator_alloc(FSStorageAllocatorManager *allocator_mgr,
        const uint32_t blk_hc, const int size, FSTrunkSpaceInfo *space_info)
{
    FSTrunkAllocator **allocator;

    if (allocator_mgr->current->avail.count == 0) {
        return ENOENT;
    }

    allocator = allocator_mgr->current->avail.allocators +
        blk_hc % allocator_mgr->current->avail.count;
    return trunk_allocator_alloc(*allocator, blk_hc, size, space_info);
}
