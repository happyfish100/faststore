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

#include <limits.h>
#include <sys/stat.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fc_memory.h"
#include "sf/sf_global.h"
#include "../server_types.h"
#include "../server_global.h"
#include "trunk_maker.h"
#include "storage_allocator.h"

static FSStorageAllocatorManager allocator_mgr;
FSStorageAllocatorManager *g_allocator_mgr = &allocator_mgr;
static int check_trunk_avail_func(void *args);

static int init_allocator_context(FSStorageAllocatorContext *allocator_ctx,
        FSStoragePathArray *parray)
{
    int result;
    int bytes;
    FSStoragePathInfo *path;
    FSStoragePathInfo *end;
    FSTrunkAllocator *pallocator;

    if (parray->count == 0) {
        return 0;
    }

    bytes = sizeof(FSTrunkAllocator) * parray->count;
    allocator_ctx->all.allocators = (FSTrunkAllocator *)fc_malloc(bytes);
    if (allocator_ctx->all.allocators == NULL) {
        return ENOMEM;
    }
    memset(allocator_ctx->all.allocators, 0, bytes);

    end = parray->paths + parray->count;
    for (path=parray->paths,pallocator=allocator_ctx->all.allocators;
            path<end; path++, pallocator++)
    {
        if ((result=trunk_allocator_init_instance(pallocator, path)) != 0) {
            return result;
        }

        g_allocator_mgr->allocator_ptr_array.allocators
            [path->store.index] = pallocator;
    }
    allocator_ctx->all.count = parray->count;
    return 0;
}

int aptr_array_alloc_init(void *element, void *args)
{
    FSTrunkAllocatorPtrArray *aptr_array;
    aptr_array = (FSTrunkAllocatorPtrArray *)element;
    aptr_array->allocators = (FSTrunkAllocator **)(aptr_array + 1);
    aptr_array->alloc = (long)args;
    return 0;
}

int storage_allocator_init()
{
    int result;
    int bytes;
    int count;
    int element_size;

    memset(g_allocator_mgr, 0, sizeof(FSStorageAllocatorManager));
    count = STORAGE_CFG.max_store_path_index + 1;
    bytes = sizeof(FSTrunkAllocator *) * count;
    g_allocator_mgr->allocator_ptr_array.allocators =
        (FSTrunkAllocator **)fc_malloc(bytes);
    if (g_allocator_mgr->allocator_ptr_array.allocators == NULL) {
        return ENOMEM;
    }
    memset(g_allocator_mgr->allocator_ptr_array.allocators, 0, bytes);
    g_allocator_mgr->allocator_ptr_array.count = count;

    if ((result=init_pthread_lock(&g_allocator_mgr->lock)) != 0) {
        return result;
    }

    if ((result=trunk_freelist_init(&g_allocator_mgr->
                    reclaim_freelist)) != 0)
    {
        return result;
    }

    if ((result=trunk_allocator_init()) != 0) {
        return result;
    }

    if ((result=init_allocator_context(&g_allocator_mgr->write_cache,
                    &STORAGE_CFG.write_cache)) != 0)
    {
        return result;
    }
    if ((result=init_allocator_context(&g_allocator_mgr->store_path,
                    &STORAGE_CFG.store_path)) != 0)
    {
        return result;
    }

    count = FC_MAX(g_allocator_mgr->write_cache.all.count,
            g_allocator_mgr->store_path.all.count);
    element_size = sizeof(FSTrunkAllocatorPtrArray) +
        sizeof(FSTrunkAllocator *) * count;
    if ((result=fast_mblock_init_ex1(&g_allocator_mgr->aptr_array_allocator,
                    "aptr_array", element_size, 64, 0, aptr_array_alloc_init,
                    (void *)(long)count, true)) != 0)
    {
        return result;
    }

    return trunk_id_info_init();
}

static int deal_allocator_on_ready(FSStorageAllocatorContext *allocator_ctx)
{
    FSTrunkAllocator *allocator;
    FSTrunkAllocator *end;

    end = allocator_ctx->all.allocators + allocator_ctx->all.count;
    for (allocator=allocator_ctx->all.allocators; allocator<end; allocator++) {
        trunk_allocator_deal_on_ready(allocator);

        /*
        logInfo("path index: %d, total: %"PRId64" MB, used: %"PRId64" MB, "
                "avail: %"PRId64" MB", allocator->path_info->store.index,
                allocator->path_info->trunk_stat.total / (1024 * 1024),
                allocator->path_info->trunk_stat.used / (1024 * 1024),
                allocator->path_info->trunk_stat.avail / (1024 * 1024));
                */
    }

    return 0;
}

static int prealloc_trunk_freelist(FSStorageAllocatorContext *allocator_ctx)
{
    FSTrunkAllocator *allocator;
    FSTrunkAllocator *end;

    end = allocator_ctx->all.allocators + allocator_ctx->all.count;
    for (allocator=allocator_ctx->all.allocators; allocator<end; allocator++) {
        trunk_freelist_keep_water_mark(allocator);
    }

    return 0;
}

static void wait_allocator_available()
{
    FSTrunkAllocatorPtrArray *avail_array;
    int count;
    int i;

    i = 0;
    while (g_allocator_mgr->store_path.full->count > 0 && i < 10) {
        fc_sleep_ms(100);
        if (i % 5 == 0) {
            check_trunk_avail_func(NULL);
        }
        ++i;
    }

    i = 0;
    while (g_allocator_mgr->store_path.avail->count == 0 && i++ < 10) {
        fc_sleep_ms(100);
    }

    logInfo("file: "__FILE__", line: %d, "
            "waiting count: %d, path stat {avail count: %d, "
            "full count: %d}, reclaim freelist count: %d", __LINE__,
            i, g_allocator_mgr->store_path.avail->count,
            g_allocator_mgr->store_path.full->count,
            g_allocator_mgr->reclaim_freelist.count);

    count = g_allocator_mgr->reclaim_freelist.water_mark_trunks -
        g_allocator_mgr->reclaim_freelist.count;
    if (count <= 0) {
        return;
    }

    avail_array = (FSTrunkAllocatorPtrArray *)
        g_allocator_mgr->store_path.avail;
    for (i=0; i<avail_array->count; i++) {
        trunk_maker_allocate_ex(avail_array->allocators[i],
                true, true, NULL, NULL);
    }
}

#define CMP_APTR_BY_PINDEX(a1, a2) \
    ((int)(a1)->path_info->store.index - \
     (int)(a2)->path_info->store.index)

static int compare_allocator_by_path_index(
        FSTrunkAllocator **pp1,
        FSTrunkAllocator **pp2)
{
    return CMP_APTR_BY_PINDEX(*pp1, *pp2);
}

static inline int add_to_aptr_array(FSTrunkAllocatorPtrArray
        *aptr_array, FSTrunkAllocator *allocator)
{
    FSTrunkAllocator **pos;
    FSTrunkAllocator **end;
    int r;

    end = aptr_array->allocators + aptr_array->count;
    for (pos=aptr_array->allocators; pos<end; pos++) {
        r = CMP_APTR_BY_PINDEX(allocator, *pos);
        if (r < 0) {
            break;
        } else if (r == 0) {
            return EEXIST;
        }
    }

    if (aptr_array->count >= aptr_array->alloc) {
        return ENOSPC;
    }

    if (pos < end) {
        FSTrunkAllocator **pp;

        for (pp=end; pp>pos; pp--) {
            *pp = *(pp - 1);
        }
    }

    *pos = allocator;
    aptr_array->count++;
    return 0;
}

static inline int remove_from_aptr_array(FSTrunkAllocatorPtrArray *aptr_array,
        FSTrunkAllocator *allocator)
{
    FSTrunkAllocator **pp;
    FSTrunkAllocator **end;

    end = aptr_array->allocators + aptr_array->count;
    for (pp=aptr_array->allocators; pp<end; pp++) {
        if (CMP_APTR_BY_PINDEX(*pp, allocator) == 0) {
            break;
        }
    }
    if (pp == end) {
        return ENOENT;
    }

    for (pp=pp+1; pp<end; pp++) {
        *(pp - 1) = *pp;
    }
    aptr_array->count--;
    return 0;
}

int init_allocator_ptr_array(FSStorageAllocatorContext *allocator_ctx)
{
    FSTrunkAllocator *allocator;
    FSTrunkAllocator *end;
    FSTrunkAllocatorPtrArray *aptr_array;

    allocator_ctx->full = (FSTrunkAllocatorPtrArray *)
        fast_mblock_alloc_object(&g_allocator_mgr->aptr_array_allocator);
    if (allocator_ctx->full == NULL) {
        return ENOMEM;
    }

    allocator_ctx->avail = (FSTrunkAllocatorPtrArray *)
        fast_mblock_alloc_object(&g_allocator_mgr->aptr_array_allocator);
    if (allocator_ctx->avail == NULL) {
        return ENOMEM;
    }

    allocator_ctx->full->count = allocator_ctx->avail->count = 0;
    end = allocator_ctx->all.allocators + allocator_ctx->all.count;
    for (allocator=allocator_ctx->all.allocators; allocator<end; allocator++) {
        if (trunk_allocator_is_available(allocator)) {
            aptr_array = (FSTrunkAllocatorPtrArray *)allocator_ctx->avail;
        } else {
            allocator->path_info->trunk_stat.last_used = __sync_add_and_fetch(
                    &allocator->path_info->trunk_stat.used, 0) +
                STORAGE_CFG.trunk_file_size;  //for trigger check trunk avail
            aptr_array = (FSTrunkAllocatorPtrArray *)allocator_ctx->full;
        }
        add_to_aptr_array(aptr_array, allocator);

        /*
        logInfo("path index: %d, avail count: %d, full count: %d",
                allocator->path_info->store.index,
                allocator_ctx->avail->count,
                allocator_ctx->full->count);
                */
    }

    return 0;
}

static int check_trunk_avail(FSStorageAllocatorContext *allocator_ctx)
{
    FSTrunkAllocatorPtrArray *aptr_array;
    FSTrunkAllocator **pp;
    FSTrunkAllocator **end;
    int64_t used_bytes;
    int r;
    int result;

    result = 0;
    aptr_array = (FSTrunkAllocatorPtrArray *)allocator_ctx->full;
    end = aptr_array->allocators + aptr_array->count;
    for (pp=aptr_array->allocators; pp<end; pp++) {
        if (trunk_allocator_is_available(*pp)) {
            if ((r=fs_add_to_avail_aptr_array(allocator_ctx, *pp)) != 0) {
                result = r;
            }
        } else {
            used_bytes = __sync_add_and_fetch(&(*pp)->
                    path_info->trunk_stat.used, 0);
            if ((*pp)->path_info->trunk_stat.last_used - used_bytes >=
                    STORAGE_CFG.trunk_file_size)
            {
                (*pp)->path_info->trunk_stat.last_used = used_bytes;
                trunk_maker_allocate_ex(*pp, true, true, NULL, NULL);
            } else {
                storage_config_calc_path_avail_space((*pp)->path_info);
                if ((*pp)->path_info->space_stat.avail - STORAGE_CFG.
                        trunk_file_size > (*pp)->path_info->reserved_space.value)
                {
                    trunk_maker_allocate(*pp);
                }
            }
        }
    }

    return result;
}

static int check_trunk_avail_func(void *args)
{
    int result;
    static bool in_progress = false;

    if (in_progress) {
        return EINPROGRESS;
    }

    in_progress = true;
    if (g_allocator_mgr->store_path.full->count > 0) {
        logInfo("store_path full count: %d",
                g_allocator_mgr->store_path.full->count);
        result = check_trunk_avail(&g_allocator_mgr->store_path);
    } else {
        result = 0;
    }
    in_progress = false;
    return result;
}

static int setup_check_trunk_avail_schedule()
{
    ScheduleArray scheduleArray;
    ScheduleEntry scheduleEntry;

    INIT_SCHEDULE_ENTRY(scheduleEntry, sched_generate_next_id(),
           TIME_NONE, TIME_NONE, TIME_NONE, 10,
            check_trunk_avail_func, NULL);
    scheduleArray.entries = &scheduleEntry;
    scheduleArray.count = 1;
    return sched_add_entries(&scheduleArray);
}

int storage_allocator_prealloc_trunk_freelists()
{
    int result;

    g_trunk_allocator_vars.data_load_done = true;
    ob_index_enable_modify_used_space();

    if ((result=deal_allocator_on_ready(&g_allocator_mgr->write_cache)) != 0) {
        return result;
    }

    if ((result=deal_allocator_on_ready(&g_allocator_mgr->store_path)) != 0) {
        return result;
    }

    if ((result=init_allocator_ptr_array(&g_allocator_mgr->write_cache)) != 0) {
        return result;
    }

    if ((result=init_allocator_ptr_array(&g_allocator_mgr->store_path)) != 0) {
        return result;
    }

    if ((result=prealloc_trunk_freelist(&g_allocator_mgr->write_cache)) != 0) {
        return result;
    }

    if ((result=prealloc_trunk_freelist(&g_allocator_mgr->store_path)) != 0) {
        return result;
    }

    if ((result=setup_check_trunk_avail_schedule()) != 0) {
        return result;
    }
    wait_allocator_available();
    return 0;
}

static inline FSTrunkAllocatorPtrArray *duplicate_aptr_array(
        FSTrunkAllocatorPtrArray *aptr_array)
{
    FSTrunkAllocatorPtrArray *new_array;

    new_array = (FSTrunkAllocatorPtrArray *)fast_mblock_alloc_object(
            &g_allocator_mgr->aptr_array_allocator);
    if (new_array == NULL) {
        return NULL;
    }

    if (aptr_array->count > 0) {
        memcpy(new_array->allocators, aptr_array->allocators,
                sizeof(FSTrunkAllocator *) * aptr_array->count);
    }

    new_array->count = aptr_array->count;
    return new_array;
}

static inline void switch_aptr_array(FSTrunkAllocatorPtrArray **aptr_array,
        FSTrunkAllocatorPtrArray *new_array)
{
    FSTrunkAllocatorPtrArray *old_array;

    old_array = *aptr_array;
    *aptr_array = new_array;
    fast_mblock_delay_free_object(&g_allocator_mgr->
            aptr_array_allocator, old_array, 60);
}

int fs_move_allocator_ptr_array(FSTrunkAllocatorPtrArray **src_array,
        FSTrunkAllocatorPtrArray **dest_array, FSTrunkAllocator *allocator)
{
    int result;
    FSTrunkAllocatorPtrArray *new_sarray;
    FSTrunkAllocatorPtrArray *new_darray;

    new_sarray = new_darray = NULL;
    PTHREAD_MUTEX_LOCK(&g_allocator_mgr->lock);
    do {
        if (bsearch(&allocator, (*src_array)->allocators,
                    (*src_array)->count, sizeof(FSTrunkAllocator *),
                    (int (*)(const void *, const void *))
                    compare_allocator_by_path_index) == NULL)
        {
            result = ENOENT;
            break;
        }

        if ((new_sarray=duplicate_aptr_array(*src_array)) == NULL) {
            result = ENOMEM;
            break;
        }
        if ((new_darray=duplicate_aptr_array(*dest_array)) == NULL) {
            result = ENOMEM;
            break;
        }

        if ((result=remove_from_aptr_array(new_sarray, allocator)) != 0) {
            break;
        }
        switch_aptr_array(src_array, new_sarray);
        new_sarray = NULL;

        if ((result=add_to_aptr_array(new_darray, allocator)) != 0) {
            break;
        }
        switch_aptr_array(dest_array, new_darray);
        new_darray = NULL;
    } while (0);
    PTHREAD_MUTEX_UNLOCK(&g_allocator_mgr->lock);

    if (new_sarray != NULL) {
        fast_mblock_free_object(&g_allocator_mgr->
                aptr_array_allocator, new_sarray);
    }
    if (new_darray != NULL) {
        fast_mblock_free_object(&g_allocator_mgr->
                aptr_array_allocator, new_darray);
    }

    return result;
}
