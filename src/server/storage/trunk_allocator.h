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


#ifndef _TRUNK_ALLOCATOR_H
#define _TRUNK_ALLOCATOR_H

#include "fastcommon/uniq_skiplist.h"
#include "fastcommon/fc_list.h"
#include "../../common/fs_types.h"
#include "storage_config.h"
#include "object_block_index.h"

#define FS_TRUNK_SKIPLIST_INIT_LEVEL_COUNT      10
#define FS_TRUNK_SKIPLIST_MAX_LEVEL_COUNT       16
#define FS_TRUNK_SKIPLIST_DELAY_FREE_SECONDS   600

#define FS_TRUNK_STATUS_NONE        0
#define FS_TRUNK_STATUS_ALLOCING    1
#define FS_TRUNK_STATUS_RECLAIMING  2

#define FS_TRUNK_UTIL_EVENT_NONE      0
#define FS_TRUNK_UTIL_EVENT_CREATE   'C'
#define FS_TRUNK_UTIL_EVENT_UPDATE   'U'

#define FS_TRUNK_AVAIL_SPACE(trunk) ((trunk)->size - (trunk)->free_start)

typedef struct {
    int count;
    int water_mark_trunks;
    FSTrunkFileInfo *head;  //allocate from head
    FSTrunkFileInfo *tail;  //push to tail
} FSTrunkFreelist;

typedef struct {
    FSTrunkFreelist normal;   //general purpose
    FSTrunkFreelist reclaim;  //special purpose for reclaiming
} FSTrunkFreelistPair;

typedef struct {
    int alloc;
    int count;
    FSTrunkFileInfo **trunks;
} FSTrunkInfoPtrArray;

typedef struct fs_trunk_allocator {
    FSStoragePathInfo *path_info;
    struct {
        UniqSkiplist *by_id;   //order by id
        UniqSkiplist *by_size; //order by used size and id
    } trunks;
    FSTrunkFreelistPair freelist; //trunk freelist pool
    struct {
        int waiting_count;
        pthread_lock_cond_pair_t lcp;  //for lock and notify
    } allocate; //for allacate space

    struct {
        int result;  //deal result
        time_t last_deal_time;
        struct fc_queue queue;  //trunk event queue for nodify
        struct fs_trunk_allocator *next; //for event notify queue
    } reclaim; //for trunk reclaim
} FSTrunkAllocator;

typedef struct {
    bool data_load_done;
    struct fast_mblock_man trunk_allocator;
    struct {
        UniqSkiplistFactory by_id;
        UniqSkiplistFactory by_size;
    } skiplist_factories;
} TrunkAllocatorGlobalVars;

#ifdef __cplusplus
extern "C" {
#endif

#define G_TRUNK_ALLOCATOR     g_trunk_allocator_vars.trunk_allocator

#define free_trunk_file_info_ptr(trunk_info) \
        fast_mblock_free_object(&G_TRUNK_ALLOCATOR, trunk_info)

    extern TrunkAllocatorGlobalVars g_trunk_allocator_vars;

    int trunk_allocator_init();

    int trunk_allocator_init_instance(FSTrunkAllocator *allocator,
            FSStoragePathInfo *path_info);

    int trunk_allocator_add(FSTrunkAllocator *allocator,
            const FSTrunkIdInfo *id_info, const int64_t size,
            FSTrunkFileInfo **pp_trunk);

    int trunk_allocator_delete(FSTrunkAllocator *allocator, const int64_t id);

    int trunk_allocator_normal_alloc(FSTrunkAllocator *allocator,
            const uint32_t blk_hc, const int size,
            FSTrunkSpaceInfo *spaces, int *count);

    int trunk_allocator_reclaim_alloc(FSTrunkAllocator *allocator,
            const uint32_t blk_hc, const int size,
            FSTrunkSpaceInfo *spaces, int *count);

    int trunk_allocator_free(FSTrunkAllocator *allocator,
            const int id, const int size);

    int trunk_allocator_add_slice(FSTrunkAllocator *allocator,
            OBSliceEntry *slice);

    int trunk_allocator_delete_slice(FSTrunkAllocator *allocator,
            OBSliceEntry *slice);

    int trunk_allocator_add_to_freelist(FSTrunkAllocator *allocator,
            FSTrunkFileInfo *trunk_info);

    void trunk_allocator_keep_water_mark(FSTrunkAllocator *allocator);

    void trunk_allocator_deal_on_ready(FSTrunkAllocator *allocator);

    void trunk_allocator_log_trunk_info(FSTrunkFileInfo *trunk_info);

    int compare_trunk_by_size_id(const FSTrunkFileInfo *t1,
            const FSTrunkFileInfo *t2);

    static inline int trunk_allocator_get_freelist_count(
            FSTrunkAllocator *allocator)
    {
        int count;
        PTHREAD_MUTEX_LOCK(&allocator->allocate.lcp.lock);
        count = allocator->freelist.normal.count;
        PTHREAD_MUTEX_UNLOCK(&allocator->allocate.lcp.lock);
        return count;
    }

#ifdef __cplusplus
}
#endif

#endif
