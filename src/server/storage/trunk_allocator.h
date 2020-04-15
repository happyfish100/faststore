
#ifndef _TRUNK_ALLOCATOR_H
#define _TRUNK_ALLOCATOR_H

#include "fastcommon/uniq_skiplist.h"
#include "fastcommon/multi_skiplist.h"
#include "../../common/fs_types.h"
#include "storage_config.h"

#define FS_TRUNK_SKIPLIST_INIT_LEVEL_COUNT      10
#define FS_TRUNK_SKIPLIST_MAX_LEVEL_COUNT       16
#define FS_TRUNK_SKIPLIST_DELAY_FREE_SECONDS   600


#define FS_TRUNK_STATUS_NONE        0
#define FS_TRUNK_STATUS_ALLOCING    1
#define FS_TRUNK_STATUS_RECLAIMING  2

typedef struct {
    FSTrunkIdInfo id_info;
    int last_alloc_time;
    int status;
    struct {
        volatile int count;  //slice count
        volatile int64_t bytes;
    } used;
    int64_t size;        //file size
    int64_t free_start;  //free space offset
} FSTrunkFileInfo;

typedef struct fs_trunk_free_node {
    FSTrunkFileInfo *trunk_info;
    struct fs_trunk_free_node *next;
} FSTrunkFreeNode;

typedef struct {
    int count;
    FSTrunkFreeNode *head;  //allocate from head
    FSTrunkFreeNode *tail;  //push to tail
} FSTrunkFreelist;

typedef struct {
    FSStoragePathInfo *path_info;
    UniqSkiplist *sl_trunks;   //all trunks
    FSTrunkFreelist *freelists; //current allocator map to disk write threads
    pthread_mutex_t lock;
    pthread_cond_t cond;
} FSTrunkAllocator;

#ifdef __cplusplus
extern "C" {
#endif

    int trunk_allocator_init(FSTrunkAllocator *allocator,
            FSStoragePathInfo *path_info);

    int trunk_allocator_add(FSTrunkAllocator *allocator,
            const FSTrunkIdInfo *id_info, const int64_t size);

    int trunk_allocator_delete(FSTrunkAllocator *allocator, const int64_t id);

    int trunk_allocator_alloc(FSTrunkAllocator *allocator,
            const uint32_t blk_hc, const int size,
            FSTrunkSpaceInfo *spaces, int *count);

    int trunk_allocator_free(FSTrunkAllocator *allocator,
            const int id, const int size);

#ifdef __cplusplus
}
#endif

#endif
