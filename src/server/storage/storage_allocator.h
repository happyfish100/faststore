
#ifndef _STORAGE_ALLOCATOR_H
#define _STORAGE_ALLOCATOR_H

#include "../common/fs_types.h"
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
    FSStorageAllocatorContext store_path;
    FSStorageAllocatorContext write_cache;
} FSStorageAllocatorManager;

#ifdef __cplusplus
extern "C" {
#endif

    int storage_allocator_init(FSStorageAllocatorContext *allocator_ctx);

#ifdef __cplusplus
}
#endif

#endif
