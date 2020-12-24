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
#include "../server_global.h"
#include "trunk_freelist.h"
#include "storage_config.h"
#include "object_block_index.h"

#define FS_TRUNK_SKIPLIST_INIT_LEVEL_COUNT       6
#define FS_TRUNK_SKIPLIST_MAX_LEVEL_COUNT       12
#define FS_TRUNK_SKIPLIST_DELAY_FREE_SECONDS   600

#define FS_TRUNK_STATUS_NONE        0
#define FS_TRUNK_STATUS_REPUSH      1  //intermediate state
#define FS_TRUNK_STATUS_ALLOCING    2
#define FS_TRUNK_STATUS_RECLAIMING  3

#define FS_TRUNK_UTIL_EVENT_NONE      0
#define FS_TRUNK_UTIL_EVENT_CREATE   'C'
#define FS_TRUNK_UTIL_EVENT_UPDATE   'U'

#define FS_TRUNK_AVAIL_SPACE(trunk) ((trunk)->size - (trunk)->free_start)

typedef enum {
    fs_freelist_type_none,
    fs_freelist_type_normal,
    fs_freelist_type_reclaim
} FSTrunkFreelistType;

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
        pthread_mutex_t lock;
    } trunks;
    FSTrunkFreelist freelist; //trunk freelist pool

    struct {
        time_t last_trigger_time; //caller trigger create trunk
        int creating_trunks;  //counter for creating (prealloc or reclaim) trunk
        int waiting_callers;  //caller count for waiting available trunk
    } allocate; //for allocate space

    struct {
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

    int trunk_allocator_free(FSTrunkAllocator *allocator,
            const int id, const int size);

    int trunk_allocator_add_slice(FSTrunkAllocator *allocator,
            OBSliceEntry *slice);

    int trunk_allocator_delete_slice(FSTrunkAllocator *allocator,
            OBSliceEntry *slice);

    FSTrunkFreelistType trunk_allocator_add_to_freelist(
            FSTrunkAllocator *allocator, FSTrunkFileInfo *trunk_info);

    void trunk_allocator_deal_on_ready(FSTrunkAllocator *allocator);

    static inline bool trunk_allocator_is_available(FSTrunkAllocator *allocator)
    {
        return allocator->freelist.count >=
            allocator->freelist.water_mark_trunks;
    }

    void trunk_allocator_log_trunk_info(FSTrunkFileInfo *trunk_info);

    static inline int fs_compare_trunk_by_size_id(const FSTrunkFileInfo *t1,
            const int64_t last_used_bytes2, const int64_t id2)
    {
        int sub;

        if ((sub=fc_compare_int64(t1->util.last_used_bytes,
                        last_used_bytes2)) != 0)
        {
            return sub;
        }

        return fc_compare_int64(t1->id_info.id, id2);
    }

    int compare_trunk_by_size_id(const FSTrunkFileInfo *t1,
            const FSTrunkFileInfo *t2);

    static inline int trunk_allocator_get_freelist_count(
            FSTrunkAllocator *allocator)
    {
        int count;
        PTHREAD_MUTEX_LOCK(&allocator->freelist.lcp.lock);
        count = allocator->freelist.count;
        PTHREAD_MUTEX_UNLOCK(&allocator->freelist.lcp.lock);
        return count;
    }

    static inline void fs_set_trunk_status(FSTrunkFileInfo *trunk,
            const int new_status)
    {
        int old_status;

        old_status = __sync_add_and_fetch(&trunk->status, 0);
        while (new_status != old_status) {
            if (__sync_bool_compare_and_swap(&trunk->status,
                        old_status, new_status))
            {
                break;
            }
            old_status = __sync_add_and_fetch(&trunk->status, 0);
        }
    }

    static inline void trunk_allocator_before_make_trunk(
            FSTrunkAllocator *allocator, const bool need_lock)
    {
        if (need_lock) {
            PTHREAD_MUTEX_LOCK(&allocator->freelist.lcp.lock);
        }
        allocator->allocate.creating_trunks++;
        if (need_lock) {
            PTHREAD_MUTEX_UNLOCK(&allocator->freelist.lcp.lock);
        }
    }

    static inline void trunk_allocator_after_make_trunk(
            FSTrunkAllocator *allocator)
    {
        PTHREAD_MUTEX_LOCK(&allocator->freelist.lcp.lock);
        allocator->allocate.creating_trunks--;
        if (allocator->allocate.waiting_callers > 0) {
            pthread_cond_broadcast(&allocator->freelist.lcp.cond);
        }
        PTHREAD_MUTEX_UNLOCK(&allocator->freelist.lcp.lock);
    }

    static inline double trunk_allocator_calc_reclaim_ratio_thredhold(
            FSTrunkAllocator *allocator)
    {
        double used_ratio;
        used_ratio = allocator->path_info->space_stat.used_ratio
            + allocator->path_info->reserved_space.ratio;
        if (used_ratio >= 1.00) {
            return STORAGE_CFG.never_reclaim_on_trunk_usage;
        } else {
            return STORAGE_CFG.never_reclaim_on_trunk_usage *
                (used_ratio - STORAGE_CFG.reclaim_trunks_on_path_usage) /
                (1.00 -  STORAGE_CFG.reclaim_trunks_on_path_usage);
        }
    }

#ifdef __cplusplus
}
#endif

#endif
