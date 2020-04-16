
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
    FSTrunkAllocatorPtrArray allocator_ptr_array; //by store path index
    int64_t current_trunk_id;
} FSStorageAllocatorManager;

#ifdef __cplusplus
extern "C" {
#endif

    extern FSStorageAllocatorManager *g_allocator_mgr;

    int storage_allocator_init();

    static inline int storage_allocator_add_trunk(const int path_index,
            const FSTrunkIdInfo *id_info, const int64_t size)
    {
        return trunk_allocator_add(g_allocator_mgr->allocator_ptr_array.
                allocators[path_index], id_info, size);
    }

    static inline int storage_allocator_delete_trunk(const int path_index,
            const int64_t id)
    {
        return trunk_allocator_delete(g_allocator_mgr->allocator_ptr_array.
                allocators[path_index], id);
    }

    int storage_allocator_alloc(const uint32_t blk_hc, const int size,
            FSTrunkSpaceInfo *space_info, int *count);

    int storage_allocator_free(FSStorageAllocatorManager *allocator_mgr,
            const uint32_t blk_hc, const int id, const int size);

#ifdef __cplusplus
}
#endif

#endif
