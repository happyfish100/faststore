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
#include "../server_global.h"
#include "trunk_prealloc.h"
#include "trunk_util_man.h"
#include "trunk_allocator.h"

TrunkAllocatorGlobalVars g_trunk_allocator_vars = {false};

#define G_ID_SKIPLIST_FACTORY   g_trunk_allocator_vars.skiplist_factories.by_id
#define G_SIZE_SKIPLIST_FACTORY g_trunk_allocator_vars.skiplist_factories.by_size

#define PUSH_TO_TRUNK_UTIL_MAN_QUEUE(trunk_info, event) \
    do {  \
        if (g_trunk_allocator_vars.data_load_done) { \
            trunk_util_man_push(trunk_info, event);  \
        } \
    } while (0)

static int compare_trunk_by_id(const FSTrunkFileInfo *t1,
        const FSTrunkFileInfo *t2)
{
    return fc_compare_int64(t1->id_info.id, t2->id_info.id);
}

static int compare_trunk_by_size_id(const FSTrunkFileInfo *t1,
        const FSTrunkFileInfo *t2)
{
    int sub;

    if ((sub=fc_compare_int64(t1->util.last_used_bytes,
                    t2->util.last_used_bytes)) != 0)
    {
        return sub;
    }

    return fc_compare_int64(t1->id_info.id, t2->id_info.id);
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

static void init_freelists(FSTrunkAllocator *allocator)
{
/*
    FSTrunkFreelistPair *pair;
    FSTrunkFreelistPair *end;

    end = allocator->freelists + allocator->path_info->write_thread_count;
    for (pair=allocator->freelists; pair<end; pair++) {
        pair->normal.prealloc_trunks = allocator->path_info->prealloc_trunks;
        pair->reclaim.prealloc_trunks = 2;
    }
*/

    allocator->freelist.normal.prealloc_trunks =
        allocator->freelist.reclaim.prealloc_trunks = 2;
}

int trunk_allocator_init()
{
    int alloc_skiplist_once;
    const int min_alloc_elements_once = 4;
    const int delay_free_seconds = 0;
    const bool bidirection = true;
    int result;

    if ((result=fast_mblock_init_ex1(&G_TRUNK_ALLOCATOR,
                    "trunk_file_info", sizeof(FSTrunkFileInfo),
                    16384, 0, NULL, NULL, true)) != 0)
    {
        return result;
    }

    alloc_skiplist_once = STORAGE_CFG.store_path.count +
        STORAGE_CFG.write_cache.count;
    if ((result=uniq_skiplist_init_ex(&G_ID_SKIPLIST_FACTORY,
                    FS_TRUNK_SKIPLIST_MAX_LEVEL_COUNT,
                    (skiplist_compare_func)compare_trunk_by_id,
                    trunk_free_func, alloc_skiplist_once,
                    min_alloc_elements_once,
                    FS_TRUNK_SKIPLIST_DELAY_FREE_SECONDS)) != 0)
    {
        return result;
    }

    if ((result=uniq_skiplist_init_ex2(&G_SIZE_SKIPLIST_FACTORY,
                    FS_TRUNK_SKIPLIST_MAX_LEVEL_COUNT, (skiplist_compare_func)
                    compare_trunk_by_size_id, NULL,
                    alloc_skiplist_once, min_alloc_elements_once,
                    delay_free_seconds, bidirection)) != 0)
    {
        return result;
    }

    return 0;
}

int trunk_allocator_init_instance(FSTrunkAllocator *allocator,
        FSStoragePathInfo *path_info)
{
    int result;

    if ((result=init_pthread_lock_cond_pair(&allocator->lcp)) != 0) {
        return result;
    }
        
    if ((allocator->trunks.by_id=uniq_skiplist_new(&G_ID_SKIPLIST_FACTORY,
            FS_TRUNK_SKIPLIST_INIT_LEVEL_COUNT)) == NULL)
    {
        return ENOMEM;
    }

    if ((result=fc_queue_init(&allocator->event_queue, (long)
                    (&((FSTrunkFileInfo *)NULL)->util.next))) != 0)
    {
        return result;
    }

    if ((allocator->trunks.by_size=uniq_skiplist_new(&G_SIZE_SKIPLIST_FACTORY,
                    FS_TRUNK_SKIPLIST_INIT_LEVEL_COUNT)) == NULL)
    {
        return ENOMEM;
    }

    allocator->path_info = path_info;
    init_freelists(allocator);
    return 0;
}

int trunk_allocator_add(FSTrunkAllocator *allocator,
        const FSTrunkIdInfo *id_info, const int64_t size,
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

    trunk_info->status = FS_TRUNK_STATUS_NONE;
    trunk_info->id_info = *id_info;
    trunk_info->size = size;
    trunk_info->used.bytes = 0;
    trunk_info->used.count = 0;
    trunk_info->free_start = 0;
    FC_INIT_LIST_HEAD(&trunk_info->used.slice_head);

    PTHREAD_MUTEX_LOCK(&allocator->lcp.lock);
    result = uniq_skiplist_insert(allocator->trunks.by_id, trunk_info);
    PTHREAD_MUTEX_UNLOCK(&allocator->lcp.lock);

    if (result == 0) {
        PUSH_TO_TRUNK_UTIL_MAN_QUEUE(trunk_info, TRUNK_UTIL_EVENT_CREATE);
    } else {
        logError("file: "__FILE__", line: %d, "
                "add trunk fail, trunk id: %"PRId64", "
                "errno: %d, error info: %s", __LINE__,
                id_info->id, result, STRERROR(result));
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
    PTHREAD_MUTEX_LOCK(&allocator->lcp.lock);
    result = uniq_skiplist_delete(allocator->trunks.by_id, &target);
    PTHREAD_MUTEX_UNLOCK(&allocator->lcp.lock);

    return result;
}

#define TRUNK_ALLOC_SPACE(allocator, trunk_info, space_info, alloc_size) \
    do { \
        space_info->store = &allocator->path_info->store; \
        space_info->id_info = trunk_info->id_info;   \
        space_info->offset = trunk_info->free_start; \
        space_info->size = alloc_size;         \
        trunk_info->free_start += alloc_size;  \
        __sync_sub_and_fetch(&allocator->path_info-> \
                trunk_stat.avail, alloc_size);  \
    } while (0)

static void remove_trunk_from_freelist(FSTrunkAllocator *allocator,
        FSTrunkFreelist *freelist)
{
    FSTrunkFileInfo *trunk_info;

    trunk_info = freelist->head;
    trunk_info->status = FS_TRUNK_STATUS_NONE;
    freelist->head = freelist->head->alloc.next;
    if (freelist->head == NULL) {
        freelist->tail = NULL;
    }
    freelist->count--;

    trunk_prealloc_push(allocator, freelist, freelist->prealloc_trunks);
}

static void prealloc_trunks(FSTrunkAllocator *allocator,
        FSTrunkFreelist *freelist)
{
    int count;
    int i;

    count = freelist->prealloc_trunks - freelist->count;
    //logInfo("%s prealloc count: %d", allocator->path_info->store.path.str, count);
    for (i=0; i<count; i++) {
        trunk_prealloc_push(allocator, freelist, freelist->prealloc_trunks);
    }
}

void trunk_allocator_prealloc_trunks(FSTrunkAllocator *allocator)
{
    prealloc_trunks(allocator, &allocator->freelist.normal);
    prealloc_trunks(allocator, &allocator->freelist.reclaim);
}

static void add_to_freelist(FSTrunkAllocator *allocator,
        FSTrunkFreelist *freelist, FSTrunkFileInfo *trunk_info)
{
    bool notify;

    PTHREAD_MUTEX_LOCK(&allocator->lcp.lock);
    trunk_info->alloc.next = NULL;
    if (freelist->head == NULL) {
        freelist->head = trunk_info;
        notify = true;
    } else {
        freelist->tail->alloc.next = trunk_info;
        notify = false;
    }
    freelist->tail = trunk_info;

    freelist->count++;
    trunk_info->status = FS_TRUNK_STATUS_ALLOCING;
    PTHREAD_MUTEX_UNLOCK(&allocator->lcp.lock);

    __sync_add_and_fetch(&allocator->path_info->trunk_stat.avail,
        FS_TRUNK_AVAIL_SPACE(trunk_info));

    if (notify) {
        pthread_cond_signal(&allocator->lcp.cond);
    }
}

void trunk_allocator_add_to_freelist(FSTrunkAllocator *allocator,
        FSTrunkFileInfo *trunk_info)
{
    FSTrunkFreelist *freelist;

    if (allocator->freelist.reclaim.count < 2) {
        freelist = &allocator->freelist.reclaim;
    } else {
        freelist = &allocator->freelist.normal;
    }
    add_to_freelist(allocator, freelist, trunk_info);
}

static int alloc_space(FSTrunkAllocator *allocator, FSTrunkFreelist *freelist,
        const uint32_t blk_hc, const int size, FSTrunkSpaceInfo *spaces,
        int *count, const bool blocked)
{
    int aligned_size;
    int result;
    int remain_bytes;
    FSTrunkSpaceInfo *space_info;
    FSTrunkFileInfo *trunk_info;

    aligned_size = MEM_ALIGN(size);
    space_info = spaces;

    PTHREAD_MUTEX_LOCK(&allocator->lcp.lock);
    do {
        if (freelist->head != NULL) {
            trunk_info = freelist->head;
            remain_bytes = FS_TRUNK_AVAIL_SPACE(trunk_info);
            if (remain_bytes < aligned_size) {
                if (!blocked && freelist->count <= 1) {
                    result = EAGAIN;
                    break;
                }

                /*
                if (remain_bytes <= 0) {
                    logInfo("allocator: %p, trunk_info: %p, trunk size: %"PRId64", "
                            "free start: %"PRId64", remain_bytes: %d",
                            allocator, trunk_info, trunk_info->size,
                            trunk_info->free_start, remain_bytes);
                }
                assert(remain_bytes > 0);
                */

                TRUNK_ALLOC_SPACE(allocator, trunk_info,
                        space_info, remain_bytes);
                space_info++;

                aligned_size -= remain_bytes;
                remove_trunk_from_freelist(allocator, freelist);
            }
        }

        if (freelist->head == NULL) {
            if (!blocked) {
                result = EAGAIN;
                break;
            }
            pthread_cond_wait(&allocator->lcp.cond, &allocator->lcp.lock);
        }

        if (freelist->head == NULL) {
            result = EINTR;
            break;
        }

        trunk_info = freelist->head;
        TRUNK_ALLOC_SPACE(allocator, trunk_info, space_info, aligned_size);
        space_info++;
        if (FS_TRUNK_AVAIL_SPACE(trunk_info) <
                STORAGE_CFG.discard_remain_space_size)
        {
            remove_trunk_from_freelist(allocator, freelist);
            __sync_sub_and_fetch(&allocator->path_info->trunk_stat.avail,
                    FS_TRUNK_AVAIL_SPACE(trunk_info));
        }
        result = 0;
    } while (0);
    PTHREAD_MUTEX_UNLOCK(&allocator->lcp.lock);

    *count = space_info - spaces;
    return result;
}

int trunk_allocator_normal_alloc(FSTrunkAllocator *allocator,
        const uint32_t blk_hc, const int size,
        FSTrunkSpaceInfo *spaces, int *count)
{
    FSTrunkFreelist *freelist;

    freelist = &allocator->freelist.normal;
    return alloc_space(allocator, freelist, blk_hc, size, spaces,
            count, true);
}

int trunk_allocator_reclaim_alloc(FSTrunkAllocator *allocator,
        const uint32_t blk_hc, const int size,
        FSTrunkSpaceInfo *spaces, int *count)
{
    int result;
    FSTrunkFreelist *freelist;

    freelist = &allocator->freelist.normal;
    if ((result=alloc_space(allocator, freelist, blk_hc, size, spaces,
                    count, false)) == 0)
    {
        return 0;
    }

    freelist = &allocator->freelist.reclaim;
    return alloc_space(allocator, freelist, blk_hc, size, spaces,
            count, false);
}

int trunk_allocator_add_slice(FSTrunkAllocator *allocator, OBSliceEntry *slice)
{
    int result;
    FSTrunkFileInfo target;
    FSTrunkFileInfo *trunk_info;

    target.id_info.id = slice->space.id_info.id;
    PTHREAD_MUTEX_LOCK(&allocator->lcp.lock);
    if ((trunk_info=(FSTrunkFileInfo *)uniq_skiplist_find(
                    allocator->trunks.by_id, &target)) == NULL)
    {
        logError("file: "__FILE__", line: %d, "
                "store path index: %d, trunk id: %"PRId64" not exist",
                __LINE__, allocator->path_info->store.index,
                slice->space.id_info.id);
        result = ENOENT;
    } else {
        /* for loading slice binlog */
        if (slice->space.offset + slice->space.size > trunk_info->free_start) {
            trunk_info->free_start = slice->space.offset + slice->space.size;
        }

        trunk_info->used.bytes += slice->space.size;
        trunk_info->used.count++;
        fc_list_add_tail(&slice->dlink, &trunk_info->used.slice_head);
        result = 0;
    }
    PTHREAD_MUTEX_UNLOCK(&allocator->lcp.lock);

    return result;
}

int trunk_allocator_delete_slice(FSTrunkAllocator *allocator,
        OBSliceEntry *slice)
{
    int result;
    FSTrunkFileInfo target;
    FSTrunkFileInfo *trunk_info;

    target.id_info.id = slice->space.id_info.id;
    PTHREAD_MUTEX_LOCK(&allocator->lcp.lock);
    if ((trunk_info=(FSTrunkFileInfo *)uniq_skiplist_find(
                    allocator->trunks.by_id, &target)) == NULL)
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

        PUSH_TO_TRUNK_UTIL_MAN_QUEUE(trunk_info, TRUNK_UTIL_EVENT_UPDATE);
        result = 0;
    }
    PTHREAD_MUTEX_UNLOCK(&allocator->lcp.lock);

    return result;
}

static bool can_add_to_freelist(FSTrunkAllocator *allocator,
        FSTrunkFileInfo *trunk_info)
{
    int64_t remain_size;
    double ratio;

    remain_size = FS_TRUNK_AVAIL_SPACE(trunk_info);
    if (remain_size < FS_FILE_BLOCK_SIZE) {
        return false;
    }

    if (trunk_info->used.bytes == 0) {
        if (trunk_info->free_start != 0) {
            trunk_info->free_start = 0;
        }
        return true;
    }

    if (allocator->path_info->space_stat.used_ratio <=
            STORAGE_CFG.reclaim_trunks_on_path_usage)
    {
        return ((double)trunk_info->free_start / (double)trunk_info->size
                <= (1.00 -  STORAGE_CFG.reclaim_trunks_on_path_usage));
    }

    ratio = STORAGE_CFG.never_reclaim_on_trunk_usage *
        (allocator->path_info->space_stat.used_ratio -
         STORAGE_CFG.reclaim_trunks_on_path_usage) /
        (1.00 -  STORAGE_CFG.reclaim_trunks_on_path_usage);
    return ((double)trunk_info->used.bytes /
            (double)trunk_info->free_start > ratio);
}

void trunk_allocator_deal_on_ready(FSTrunkAllocator *allocator)
{
    UniqSkiplistIterator it;
    FSTrunkFreelist *freelist;
    FSTrunkFileInfo *trunk_info;

    uniq_skiplist_iterator(allocator->trunks.by_id, &it);
    while ((trunk_info=uniq_skiplist_next(&it)) != NULL) {
        allocator->path_info->trunk_stat.total += trunk_info->size;
        allocator->path_info->trunk_stat.used += trunk_info->used.bytes;

        if (can_add_to_freelist(allocator, trunk_info)) {
            if (trunk_info->free_start == 0 &&
                    allocator->freelist.reclaim.count < 2)
            {
                freelist = &allocator->freelist.reclaim;
            } else {
                freelist = &allocator->freelist.normal;
            }

            add_to_freelist(allocator, freelist, trunk_info);
        } else {
            PUSH_TO_TRUNK_UTIL_MAN_QUEUE(trunk_info, TRUNK_UTIL_EVENT_CREATE);
        }
    }

}

void trunk_allocator_log_trunk_info(FSTrunkFileInfo *trunk_info)
{
    logInfo("trunk id: %"PRId64", subdir: %"PRId64", status: %d, "
            "slice count: %d, used bytes: %"PRId64", trunk size: %"PRId64", "
            "free start: %"PRId64", remain bytes: %"PRId64,
            trunk_info->id_info.id, trunk_info->id_info.subdir,
            trunk_info->status, trunk_info->used.count, trunk_info->used.bytes,
            trunk_info->size, trunk_info->free_start,
            FS_TRUNK_AVAIL_SPACE(trunk_info));
}
