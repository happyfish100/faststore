
#ifndef _TRUNK_ALLOCATOR_H
#define _TRUNK_ALLOCATOR_H

#include "fastcommon/uniq_skiplist.h"
#include "fastcommon/multi_skiplist.h"
#include "../../common/fs_types.h"
#include "storage_config.h"

#define FS_TRUNK_SKIPLIST_INIT_LEVEL_COUNT      10
#define FS_TRUNK_SKIPLIST_MAX_LEVEL_COUNT       16
#define FS_TRUNK_SKIPLIST_DELAY_FREE_SECONDS   600

typedef struct {
    int64_t id;
    int subdir;      //in which subdir
    int last_alloc_time;
    struct {
        volatile int count;
        volatile int64_t bytes;
    } used;
    int64_t size;        //file size
    int64_t free_start;  //free space offset
} FSTrunkFileInfo;

/*
typedef struct fs_trunk_free_node {
    FSTrunkFileInfo *trunk_info;
    struct fs_trunk_free_node *next;
} FSTrunkFreeNode;
*/

typedef struct {
    FSStoragePathInfo *path_info;
    UniqSkiplist *sl_trunks;
    MultiSkiplist *free_list;  //for space left trunk files
    FSTrunkFileInfo **current; //current allocator one to one map to disk thread
    pthread_mutex_t lock;
} FSTrunkAllocator;

#ifdef __cplusplus
extern "C" {
#endif

    int trunk_allocator_init(FSTrunkAllocator *allocator,
            FSStoragePathInfo *path_info);

    int trunk_allocator_add(FSTrunkAllocator *allocator,
            const int64_t id, const int subdir, const int64_t size);

    int trunk_allocator_delete(FSTrunkAllocator *allocator, const int64_t id);

    int trunk_allocator_alloc(FSTrunkAllocator *allocator,
            const int size, FSTrunkSpaceInfo *space_info);

    int trunk_allocator_free(FSTrunkAllocator *allocator,
            const int id, const int size);

#ifdef __cplusplus
}
#endif

#endif
