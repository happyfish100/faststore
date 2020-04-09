
#ifndef _TRUNK_ALLOCATOR_H
#define _TRUNK_ALLOCATOR_H

#include "../../common/fs_types.h"
#include "storage_config.h"

typedef struct {
    int64_t id;
    int subdir;      //in which subdir
    int refer_count;
    int64_t used;    //used bytes
    int64_t size;    //file size
    int64_t offset;  //free space offset
} FSTrunkFileInfo;

typedef struct {
    int alloc;
    int count;
    FSTrunkFileInfo **files;
} FSTrunkFileArray;

typedef struct {
    FSStoragePathInfo *path_info;
    FSTrunkFileArray trunks;
} FSTrunkAllocator;

#ifdef __cplusplus
extern "C" {
#endif

    int trunk_allocator_init(FSTrunkAllocator *allocator,
            FSStoragePathInfo *path_info);

    int trunk_allocator_add(FSTrunkAllocator *allocator,
            const int64_t id, const int subdir, const int64_t size);

    int trunk_allocator_delete(FSTrunkAllocator *allocator, const int64_t id);

#ifdef __cplusplus
}
#endif

#endif
