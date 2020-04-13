
#ifndef _STORAGE_ALLOCATOR_H
#define _STORAGE_ALLOCATOR_H

#include "../../common/fs_types.h"
#include "trunk_allocator.h"

typedef struct {
    FSTrunkAllocator *allocators;
    int count;
} FSTrunkAllocatorArray;

typedef struct {
    FSTrunkAllocator **allocators;
    int count;
} FSTrunkAllocatorPtrArray;

typedef struct {
    FSTrunkAllocatorArray all;
    FSTrunkAllocatorPtrArray avail;
} FSStorageAllocatorContext;

typedef struct {
    FSStorageAllocatorContext write_cache;
    FSStorageAllocatorContext store_path;
    FSStorageAllocatorContext *current;
} FSStorageAllocatorManager;

#ifdef __cplusplus
extern "C" {
#endif

    int storage_allocator_init(FSStorageAllocatorManager *allocator_mgr);

    int storage_allocator_alloc(FSStorageAllocatorManager *allocator_mgr,
            const uint32_t blk_hc, const int size, FSTrunkSpaceInfo *space_info,
            int *count);

    int storage_allocator_free(FSStorageAllocatorManager *allocator_mgr,
            const uint32_t blk_hc, const int id, const int size);

#ifdef __cplusplus
}
#endif

#endif
