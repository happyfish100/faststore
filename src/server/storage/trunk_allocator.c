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
#include <assert.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fast_mblock.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "../binlog/trunk_binlog.h"
#include "../dio/trunk_write_thread.h"
#include "../server_global.h"
#include "trunk_maker.h"
#include "storage_allocator.h"
#include "trunk_allocator.h"

TrunkAllocatorGlobalVars g_trunk_allocator_vars;

static int compare_trunk_by_id(const FSTrunkFileInfo *t1,
        const FSTrunkFileInfo *t2)
{
    return fc_compare_int64(t1->id_info.id, t2->id_info.id);
}

int compare_trunk_by_size_id(const FSTrunkFileInfo *t1,
        const FSTrunkFileInfo *t2)
{
    return fs_compare_trunk_by_size_id(t1, t2->util.
            last_used_bytes, t2->id_info.id);
}

static void trunk_free_func(void *ptr, const int delay_seconds)
{
    FSTrunkFileInfo *trunk_info;
    trunk_info = (FSTrunkFileInfo *)ptr;

    if (delay_seconds > 0) {
        fast_mblock_delay_free_object(&G_TRUNK_ALLOCATOR, trunk_info,
                delay_seconds);
    } else {
        fast_mblock_free_object(&G_TRUNK_ALLOCATOR, trunk_info);
    }
}

static inline void push_trunk_util_change_event(FSTrunkAllocator *allocator,
        FSTrunkFileInfo *trunk, const int event)
{
    if (__sync_bool_compare_and_swap(&trunk->util.event,
                FS_TRUNK_UTIL_EVENT_NONE, event))
    {
        fc_queue_push(&allocator->reclaim.queue, trunk);
    }
}

int trunk_allocator_init()
{
    int result;

    if ((result=fast_mblock_init_ex1(&G_TRUNK_ALLOCATOR,
                    "trunk_file_info", sizeof(FSTrunkFileInfo),
                    16384, 0, NULL, NULL, true)) != 0)
    {
        return result;
    }

    return 0;
}

int trunk_allocator_init_instance(FSTrunkAllocator *allocator,
        FSStoragePathInfo *path_info)
{
    const int min_alloc_elements_once = 4;
    const int delay_free_seconds = 0;
    const bool bidirection = true;
    int result;

    if ((result=trunk_freelist_init(&allocator->freelist)) != 0) {
        return result;
    }

    if ((result=init_pthread_lock(&(allocator->trunks.lock))) != 0) {
        return result;
    }

    if ((result=uniq_skiplist_init_pair(&allocator->trunks.by_id,
                    FS_TRUNK_SKIPLIST_INIT_LEVEL_COUNT,
                    FS_TRUNK_SKIPLIST_MAX_LEVEL_COUNT,
                    (skiplist_compare_func)compare_trunk_by_id,
                    trunk_free_func, min_alloc_elements_once,
                    FS_TRUNK_SKIPLIST_DELAY_FREE_SECONDS)) != 0)
    {
        return result;
    }

    if ((result=uniq_skiplist_init_pair_ex(&allocator->trunks.by_size,
                    FS_TRUNK_SKIPLIST_INIT_LEVEL_COUNT,
                    FS_TRUNK_SKIPLIST_MAX_LEVEL_COUNT, (skiplist_compare_func)
                    compare_trunk_by_size_id, NULL, min_alloc_elements_once,
                    delay_free_seconds, bidirection)) != 0)
    {
        return result;
    }

    if ((result=fc_queue_init(&allocator->reclaim.queue, (long)
                    (&((FSTrunkFileInfo *)NULL)->util.next))) != 0)
    {
        return result;
    }

    allocator->path_info = path_info;
    return 0;
}

int trunk_allocator_add(FSTrunkAllocator *allocator,
        const DATrunkIdInfo *id_info, const int64_t size,
        FSTrunkFileInfo **pp_trunk)
{
    FSTrunkFileInfo *trunk_info;
    int result;

    trunk_info = (FSTrunkFileInfo *)fast_mblock_alloc_object(
            &G_TRUNK_ALLOCATOR);
    if (trunk_info == NULL) {
        if (pp_trunk != NULL) {
            *pp_trunk = NULL;
        }
        return ENOMEM;
    }

    fs_set_trunk_status(trunk_info, FS_TRUNK_STATUS_NONE);

    PTHREAD_MUTEX_LOCK(&allocator->freelist.lcp.lock);
    trunk_info->allocator = allocator;
    trunk_info->id_info = *id_info;
    trunk_info->size = size;
    trunk_info->used.bytes = 0;
    trunk_info->used.count = 0;
    trunk_info->free_start = 0;
    PTHREAD_MUTEX_UNLOCK(&allocator->freelist.lcp.lock);

    PTHREAD_MUTEX_LOCK(&allocator->trunks.lock);
    FC_INIT_LIST_HEAD(&trunk_info->used.slice_head);
    result = uniq_skiplist_insert(allocator->
            trunks.by_id.skiplist, trunk_info);
    PTHREAD_MUTEX_UNLOCK(&allocator->trunks.lock);

    if (result != 0) {
        logError("file: "__FILE__", line: %d, "
                "add trunk fail, path index: %d, subdir: %u, "
                "trunk id: %"PRId64", errno: %d, error info: %s",
                __LINE__, allocator->path_info->store.index,
                id_info->subdir, id_info->id, result, STRERROR(result));
        fast_mblock_free_object(&G_TRUNK_ALLOCATOR, trunk_info);
        trunk_info = NULL;
    }
    if (pp_trunk != NULL) {
        *pp_trunk = trunk_info;
    }
    return result;
}

int trunk_allocator_delete(FSTrunkAllocator *allocator, const int64_t id)
{
    FSTrunkFileInfo target;
    int result;

    target.id_info.id = id;
    PTHREAD_MUTEX_LOCK(&allocator->trunks.lock);
    result = uniq_skiplist_delete(allocator->trunks.by_id.skiplist, &target);
    PTHREAD_MUTEX_UNLOCK(&allocator->trunks.lock);

    return result;
}

FSTrunkFreelistType trunk_allocator_add_to_freelist(
        FSTrunkAllocator *allocator, FSTrunkFileInfo *trunk_info)
{
    FSTrunkFreelist *freelist;

    PTHREAD_MUTEX_LOCK(&g_allocator_mgr->reclaim_freelist.lcp.lock);
    if (g_allocator_mgr->reclaim_freelist.count < g_allocator_mgr->
            reclaim_freelist.water_mark_trunks)
    {
        freelist = &g_allocator_mgr->reclaim_freelist;
    } else {
        freelist = &allocator->freelist;
    }
    PTHREAD_MUTEX_UNLOCK(&g_allocator_mgr->reclaim_freelist.lcp.lock);

    trunk_freelist_add(freelist, trunk_info);
    return (freelist == &allocator->freelist) ? fs_freelist_type_normal :
        fs_freelist_type_reclaim;
}

int trunk_allocator_add_slice(FSTrunkAllocator *allocator, OBSliceEntry *slice)
{
    int result;
    FSTrunkFileInfo target;
    FSTrunkFileInfo *trunk_info;

    target.id_info.id = slice->space.id_info.id;
    PTHREAD_MUTEX_LOCK(&allocator->trunks.lock);
    if ((trunk_info=(FSTrunkFileInfo *)uniq_skiplist_find(allocator->
                    trunks.by_id.skiplist, &target)) == NULL)
    {
        logError("file: "__FILE__", line: %d, "
                "store path index: %d, trunk id: %"PRId64" not exist",
                __LINE__, allocator->path_info->store.index,
                slice->space.id_info.id);
        result = ENOENT;
    } else {
        trunk_freelist_decrease_reffer_count(trunk_info);
        trunk_info->used.bytes += slice->space.size;
        trunk_info->used.count++;
        fc_list_add_tail(&slice->dlink, &trunk_info->used.slice_head);
        result = 0;
    }
    PTHREAD_MUTEX_UNLOCK(&allocator->trunks.lock);

    return result;
}

int trunk_allocator_batch_add_slices(OBSliceEntry **slices,
        const int64_t count)
{
    int result;
    FSTrunkAllocator *allocator;
    OBSliceEntry **slice;
    OBSliceEntry **end;
    FSTrunkFileInfo target;
    FSTrunkFileInfo *trunk_info;

    if (count <= 0) {
        return 0;
    }

    allocator = g_allocator_mgr->allocator_ptr_array.
        allocators[slices[0]->space.store->index];
    target.id_info.id = slices[0]->space.id_info.id;
    PTHREAD_MUTEX_LOCK(&allocator->trunks.lock);
    if ((trunk_info=(FSTrunkFileInfo *)uniq_skiplist_find(allocator->
                    trunks.by_id.skiplist, &target)) == NULL)
    {
        logError("file: "__FILE__", line: %d, "
                "store path index: %d, trunk id: %"PRId64" not exist",
                __LINE__, allocator->path_info->store.index,
                slices[0]->space.id_info.id);
        result = ENOENT;
    } else {
        end = slices + count;
        for (slice=slices; slice<end; slice++) {
            /* for loading slice binlog */
            if ((*slice)->space.offset + (*slice)->space.size >
                    trunk_info->free_start)
            {
                trunk_info->free_start = (*slice)->space.offset +
                    (*slice)->space.size;
            }

            trunk_info->used.bytes += (*slice)->space.size;
            trunk_info->used.count++;
            fc_list_add_tail(&(*slice)->dlink, &trunk_info->used.slice_head);
        }

        result = 0;
    }
    PTHREAD_MUTEX_UNLOCK(&allocator->trunks.lock);

    return result;
}

int trunk_allocator_delete_slice(FSTrunkAllocator *allocator,
        OBSliceEntry *slice)
{
    int result;
    FSTrunkFileInfo target;
    FSTrunkFileInfo *trunk_info;

    target.id_info.id = slice->space.id_info.id;
    PTHREAD_MUTEX_LOCK(&allocator->trunks.lock);
    if ((trunk_info=(FSTrunkFileInfo *)uniq_skiplist_find(allocator->
                    trunks.by_id.skiplist, &target)) == NULL)
    {
        logError("file: "__FILE__", line: %d, "
                "store path index: %d, trunk id: %"PRId64" not exist",
                __LINE__, allocator->path_info->store.index,
                slice->space.id_info.id);
        result = ENOENT;
    } else {
        __sync_fetch_and_sub(&trunk_info->used.bytes, slice->space.size);
        trunk_info->used.count--;
        fc_list_del_init(&slice->dlink);

        push_trunk_util_change_event(allocator, trunk_info,
                FS_TRUNK_UTIL_EVENT_UPDATE);
        result = 0;
    }
    PTHREAD_MUTEX_UNLOCK(&allocator->trunks.lock);

    return result;
}

static bool can_add_to_freelist(FSTrunkFileInfo *trunk_info)
{
    int64_t remain_size;
    double ratio_thredhold;

    /*
    logInfo("file: "__FILE__", line: %d, "
            "path index: %d, trunk id: %"PRId64", "
            "used bytes: %"PRId64", free start: %"PRId64,
            __LINE__, trunk_info->allocator->path_info->store.index,
            trunk_info->id_info.id, trunk_info->used.bytes,
            trunk_info->free_start);
            */

    if (trunk_info->free_start == 0) {
        return true;
    } else if (trunk_info->used.bytes == 0) {
        trunk_info->free_start = 0;
        return true;
    }

    remain_size = FS_TRUNK_AVAIL_SPACE(trunk_info);
    if (remain_size < FS_FILE_BLOCK_SIZE) {
        return false;
    }

    if (trunk_info->allocator->path_info->space_stat.used_ratio <=
            STORAGE_CFG.reclaim_trunks_on_path_usage)
    {
        return ((double)trunk_info->free_start / (double)trunk_info->size
                <= (1.00 -  STORAGE_CFG.reclaim_trunks_on_path_usage));
    }

    if ((double)remain_size / (double)trunk_info->size >=
            (1.00 - STORAGE_CFG.reclaim_trunks_on_path_usage))
    {
        return true;
    }

    ratio_thredhold = trunk_allocator_calc_reclaim_ratio_thredhold(
            trunk_info->allocator);
    return ((double)trunk_info->used.bytes / (double)
            trunk_info->free_start > ratio_thredhold);
}

void trunk_allocator_deal_on_ready(FSTrunkAllocator *allocator)
{
    UniqSkiplistIterator it;
    FSTrunkFileInfo *trunk_info;

    uniq_skiplist_iterator(allocator->trunks.by_id.skiplist, &it);
    while ((trunk_info=uniq_skiplist_next(&it)) != NULL) {
        allocator->path_info->trunk_stat.total += trunk_info->size;
        allocator->path_info->trunk_stat.used += trunk_info->used.bytes;

        if (can_add_to_freelist(trunk_info)) {
            if (trunk_info->free_start == 0) { //whole trunk is available
                trunk_allocator_add_to_freelist(allocator, trunk_info);
            } else {
                trunk_freelist_add(&allocator->freelist, trunk_info);
            }
        } else {
            push_trunk_util_change_event(allocator, trunk_info,
                    FS_TRUNK_UTIL_EVENT_CREATE);
        }
    }
}

void trunk_allocator_log_trunk_info(FSTrunkFileInfo *trunk_info)
{
    logInfo("trunk id: %"PRId64", subdir: %u, status: %d, slice count: %d, "
            "used bytes: %"PRId64", trunk size: %"PRId64", "
            "free start: %"PRId64", remain bytes: %"PRId64,
            trunk_info->id_info.id, trunk_info->id_info.subdir,
            trunk_info->status, trunk_info->used.count, trunk_info->used.bytes,
            trunk_info->size, trunk_info->free_start,
            FS_TRUNK_AVAIL_SPACE(trunk_info));
}

int trunk_allocator_dump_trunks_to_file(FSTrunkAllocator *allocator,
        SFBufferedWriter *writer, int64_t *trunk_count)
{
    int result;
    UniqSkiplistIterator it;
    FSTrunkFileInfo *trunk_info;

    *trunk_count = 0;
    uniq_skiplist_iterator(allocator->trunks.by_id.skiplist, &it);
    while ((trunk_info=uniq_skiplist_next(&it)) != NULL) {
        if (SF_BUFFERED_WRITER_REMAIN(*writer) <
                FS_TRUNK_BINLOG_MAX_RECORD_SIZE)
        {
            if ((result=sf_buffered_writer_save(writer)) != 0) {
                return result;
            }
        }

        writer->buffer.current += trunk_binlog_log_to_buff(
                FS_IO_TYPE_CREATE_TRUNK, allocator->path_info->
                store.index, &trunk_info->id_info, trunk_info->
                size, writer->buffer.current);
        (*trunk_count)++;
    }

    return 0;
}
