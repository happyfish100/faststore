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


#ifndef _STORAGE_ALLOCATOR_H
#define _STORAGE_ALLOCATOR_H

#include "trunk_id_info.h"
#include "trunk_freelist.h"
#include "trunk_allocator.h"

typedef struct {
    int count;
    FSTrunkAllocator *allocators;
} FSTrunkAllocatorArray;

typedef struct {
    int count;
    int alloc;
    FSTrunkAllocator **allocators;
} FSTrunkAllocatorPtrArray;

typedef struct {
    FSTrunkAllocatorArray all;
    volatile FSTrunkAllocatorPtrArray *full;
    volatile FSTrunkAllocatorPtrArray *avail;
} FSStorageAllocatorContext;

typedef struct {
    FSStorageAllocatorContext write_cache;
    FSStorageAllocatorContext store_path;
    FSTrunkFreelist reclaim_freelist;  //special purpose for reclaiming
    FSTrunkAllocatorPtrArray allocator_ptr_array; //by store path index
    struct fast_mblock_man aptr_array_allocator;
    pthread_mutex_t lock;
    int64_t current_trunk_id;
} FSStorageAllocatorManager;

#ifdef __cplusplus
extern "C" {
#endif

    extern FSStorageAllocatorManager *g_allocator_mgr;

    int storage_allocator_init();

    int storage_allocator_prealloc_trunk_freelists();

    static inline int storage_allocator_add_trunk_ex(const int path_index,
            const DATrunkIdInfo *id_info, const int64_t size,
            FSTrunkFileInfo **pp_trunk)
    {
        int result;

        if ((result=trunk_id_info_add(path_index, id_info)) != 0) {
            return result;
        }

        return trunk_allocator_add(g_allocator_mgr->allocator_ptr_array.
                allocators[path_index], id_info, size, pp_trunk);
    }

    static inline int storage_allocator_add_trunk(const int path_index,
            const DATrunkIdInfo *id_info, const int64_t size)
    {
        return storage_allocator_add_trunk_ex(path_index, id_info, size, NULL);
    }

    static inline int storage_allocator_delete_trunk(const int path_index,
            const DATrunkIdInfo *id_info)
    {
        int result;
        if ((result=trunk_id_info_delete(path_index, id_info)) != 0) {
            return result;
        }
        return trunk_allocator_delete(g_allocator_mgr->allocator_ptr_array.
                allocators[path_index], id_info->id);
    }

    int storage_allocator_dump_trunks_to_file(const char *filename,
            int64_t *total_trunk_count);

    static inline int storage_allocator_normal_alloc_ex(
            const uint32_t blk_hc, const int size,
            DATrunkSpaceWithVersion *spaces,
            int *count, const bool is_normal)
    {
        FSTrunkAllocatorPtrArray *avail_array;
        FSTrunkAllocator **allocator;
        int result;

        do {
            avail_array = (FSTrunkAllocatorPtrArray *)
                g_allocator_mgr->store_path.avail;
            if (avail_array->count == 0) {
                result = ENOSPC;
                break;
            }

            allocator = avail_array->allocators +
                blk_hc % avail_array->count;
            result = trunk_freelist_alloc_space(*allocator,
                    &(*allocator)->freelist, blk_hc, size,
                    spaces, count, is_normal);
        } while ((result == ENOSPC || result == EAGAIN) && is_normal);

        return result;
    }

    static inline int storage_allocator_reclaim_alloc(const uint32_t blk_hc,
            const int size, DATrunkSpaceWithVersion *spaces, int *count)
    {
        const bool is_normal = false;
        int result;

        if ((result=storage_allocator_normal_alloc_ex(blk_hc,
                        size, spaces, count, is_normal)) == 0)
        {
            return result;
        }

        return trunk_freelist_alloc_space(NULL,
                &g_allocator_mgr->reclaim_freelist, blk_hc,
                size, spaces, count, is_normal);
    }

#define storage_allocator_normal_alloc(blk_hc, size, spaces, count) \
    storage_allocator_normal_alloc_ex(blk_hc, size, spaces, count, true)

    static inline int storage_allocator_add_slice(OBSliceEntry *slice,
            const bool modify_used_space)
    {
        FSTrunkAllocator *allocator;

        allocator = g_allocator_mgr->allocator_ptr_array.
            allocators[slice->space.store->index];
        if (modify_used_space) {
            __sync_add_and_fetch(&allocator->path_info->
                    trunk_stat.used, slice->space.size);
        }
        return trunk_allocator_add_slice(allocator, slice);
    }

    static inline int storage_allocator_delete_slice(OBSliceEntry *slice,
            const bool modify_used_space)
    {
        FSTrunkAllocator *allocator;

        allocator = g_allocator_mgr->allocator_ptr_array.
            allocators[slice->space.store->index];
        if (modify_used_space) {
            __sync_sub_and_fetch(&allocator->path_info->
                    trunk_stat.used, slice->space.size);
        }
        return trunk_allocator_delete_slice(allocator, slice);
    }

    int fs_move_allocator_ptr_array(FSTrunkAllocatorPtrArray **src_array,
            FSTrunkAllocatorPtrArray **dest_array, FSTrunkAllocator *allocator);

    static inline int fs_add_to_avail_aptr_array(FSStorageAllocatorContext
            *allocator_ctx, FSTrunkAllocator *allocator)
    {
        int result;
        if ((result=fs_move_allocator_ptr_array((FSTrunkAllocatorPtrArray **)
                        &allocator_ctx->full, (FSTrunkAllocatorPtrArray **)
                        &allocator_ctx->avail, allocator)) == 0)
        {
            logInfo("file: "__FILE__", line: %d, "
                    "path: %s is available", __LINE__,
                    allocator->path_info->store.path.str);
        } else {
            logWarning("file: "__FILE__", line: %d, "
                    "path: %s set available fail, errno: %d, "
                    "error info: %s", __LINE__, allocator->path_info->
                    store.path.str,  result, STRERROR(result));
        }

        return result;
    }

    static inline int fs_remove_from_avail_aptr_array(FSStorageAllocatorContext
            *allocator_ctx, FSTrunkAllocator *allocator)
    {
        int result;
        if ((result=fs_move_allocator_ptr_array((FSTrunkAllocatorPtrArray **)
                        &allocator_ctx->avail, (FSTrunkAllocatorPtrArray **)
                        &allocator_ctx->full, allocator)) == 0)
        {
            allocator->path_info->trunk_stat.last_used = __sync_add_and_fetch(
                    &allocator->path_info->trunk_stat.used, 0);
            logWarning("file: "__FILE__", line: %d, "
                    "path: %s is full", __LINE__,
                    allocator->path_info->store.path.str);
        } else {
            logWarning("file: "__FILE__", line: %d, "
                    "path: %s set full fail, errno: %d, "
                    "error info: %s", __LINE__, allocator->path_info->
                    store.path.str,  result, STRERROR(result));
        }

        return result;
    }

    static inline int storage_allocator_avail_count()
    {
        return g_allocator_mgr->store_path.avail->count;
    }

#ifdef __cplusplus
}
#endif

#endif
