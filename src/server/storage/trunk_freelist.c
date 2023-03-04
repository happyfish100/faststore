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
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fast_mblock.h"
#include "fastcommon/sched_thread.h"
#include "../server_global.h"
#include "trunk_maker.h"
#include "storage_allocator.h"
#include "trunk_freelist.h"

int trunk_freelist_init(FSTrunkFreelist *freelist)
{
    int result;
    if ((result=init_pthread_lock_cond_pair(&freelist->lcp)) != 0) {
        return result;
    }

    freelist->water_mark_trunks = 2;
    return 0;
}

static inline void push_trunk_util_event_force(FSTrunkAllocator *allocator,
        FSTrunkFileInfo *trunk, const int event)
{
    int old_event;

    while (1) {
        old_event = __sync_add_and_fetch(&trunk->util.event, 0);
        if (event == old_event) {
            return;
        }

        if (__sync_bool_compare_and_swap(&trunk->util.event,
                    old_event, event))
        {
            if (old_event == FS_TRUNK_UTIL_EVENT_NONE) {
                fc_queue_push(&allocator->reclaim.queue, trunk);
            }
            return;
        }
    }
}

#define TRUNK_ALLOC_SPACE(trunk, space_info, alloc_size) \
    do { \
        space_info->space.store = &trunk->allocator->path_info->store; \
        space_info->space.id_info = trunk->id_info;   \
        space_info->space.offset = trunk->free_start; \
        space_info->space.size = alloc_size;          \
        space_info->version = __sync_add_and_fetch(&trunk->  \
                allocator->allocate.current_version, 1); \
        trunk->free_start += alloc_size;  \
        __sync_sub_and_fetch(&trunk->allocator->path_info-> \
                trunk_stat.avail, alloc_size);  \
        FC_ATOMIC_INC(trunk->reffer_count);  \
    } while (0)

void trunk_freelist_keep_water_mark(struct fs_trunk_allocator
        *allocator)
{
    int count;
    int i;

    count = allocator->freelist.water_mark_trunks - allocator->freelist.count;
    if (count <= 0) {
        logInfo("file: "__FILE__", line: %d, "
                "path: %s, freelist count: %d, water_mark count: %d",
                __LINE__, allocator->path_info->store.path.str,
                allocator->freelist.count,
                allocator->freelist.water_mark_trunks);
        return;
    }

    logInfo("file: "__FILE__", line: %d, "
            "path: %s, freelist count: %d, water_mark count: %d, "
            "should allocate: %d trunks", __LINE__, allocator->
            path_info->store.path.str, allocator->freelist.count,
            allocator->freelist.water_mark_trunks, count);
    for (i=0; i<count; i++) {
        trunk_maker_allocate(allocator);
    }
}

void trunk_freelist_add(FSTrunkFreelist *freelist,
        FSTrunkFileInfo *trunk_info)
{
    int64_t avail_bytes;

    PTHREAD_MUTEX_LOCK(&freelist->lcp.lock);
    trunk_info->alloc.next = NULL;
    if (freelist->head == NULL) {
        freelist->head = trunk_info;
    } else {
        freelist->tail->alloc.next = trunk_info;
    }
    freelist->tail = trunk_info;

    freelist->count++;
    fs_set_trunk_status(trunk_info, FS_TRUNK_STATUS_ALLOCING);
    avail_bytes = FS_TRUNK_AVAIL_SPACE(trunk_info);
    PTHREAD_MUTEX_UNLOCK(&freelist->lcp.lock);

    __sync_add_and_fetch(&trunk_info->allocator->path_info->
            trunk_stat.avail, avail_bytes);
}

#define PUSH_TRUNK_UTIL_EVEENT_QUEUE(trunk_info) \
    do { \
        fs_set_trunk_status(trunk_info, FS_TRUNK_STATUS_REPUSH); \
        push_trunk_util_event_force(trunk_info->allocator, \
                trunk_info, FS_TRUNK_UTIL_EVENT_CREATE);   \
        fs_set_trunk_status(trunk_info, FS_TRUNK_STATUS_NONE); \
    } while (0)

void trunk_freelist_decrease_reffer_count_ex(FSTrunkFileInfo *trunk,
        const int dec_count)
{
    if (__sync_sub_and_fetch(&trunk->reffer_count, dec_count) == 0) {
        if (FC_ATOMIC_GET(trunk->status) == FS_TRUNK_STATUS_NONE) {
            PUSH_TRUNK_UTIL_EVEENT_QUEUE(trunk);
        }
    }
}

static void trunk_freelist_remove(FSTrunkFreelist *freelist)
{
    FSTrunkFileInfo *trunk_info;

    trunk_info = freelist->head;
    freelist->head = freelist->head->alloc.next;
    if (freelist->head == NULL) {
        freelist->tail = NULL;
    }
    freelist->count--;

    if (FC_ATOMIC_GET(trunk_info->reffer_count) == 0) {
        PUSH_TRUNK_UTIL_EVEENT_QUEUE(trunk_info);
    } else {
        fs_set_trunk_status(trunk_info, FS_TRUNK_STATUS_NONE);
    }
    if (freelist->count < freelist->water_mark_trunks) {
        trunk_maker_allocate_ex(trunk_info->allocator,
                true, false, NULL, NULL);
    }
}

static int waiting_avail_trunk(struct fs_trunk_allocator *allocator,
        FSTrunkFreelist *freelist)
{
    int result;
    int i;

    result = 0;
    for (i=0; i<10; i++) {
        if (allocator->allocate.creating_trunks == 0 && (g_current_time -
                    allocator->allocate.last_trigger_time > 0 || i > 0))
        {
            allocator->allocate.last_trigger_time = g_current_time;
            if ((result=trunk_maker_allocate_ex(allocator,
                            true, false, NULL, NULL)) != 0)
            {
                break;
            }
        }

        allocator->allocate.waiting_callers++;
        while (allocator->allocate.creating_trunks > 0 &&
                freelist->head == NULL && SF_G_CONTINUE_FLAG)
        {
            pthread_cond_wait(&freelist->lcp.cond,
                    &freelist->lcp.lock);
        }
        allocator->allocate.waiting_callers--;

        if (freelist->head != NULL || allocator->reclaim.last_errno != 0) {
            break;
        }
    }

    return result;
}

int trunk_freelist_alloc_space(struct fs_trunk_allocator *allocator,
        FSTrunkFreelist *freelist, const uint32_t blk_hc, const int size,
        DATrunkSpaceWithVersion *spaces, int *count, const bool is_normal)
{
    int aligned_size;
    int result;
    int remain_bytes;
    DATrunkSpaceWithVersion *space_info;
    FSTrunkFileInfo *trunk_info;

    aligned_size = MEM_ALIGN_CEIL(size, FS_SPACE_ALIGN_SIZE);
    space_info = spaces;

    PTHREAD_MUTEX_LOCK(&freelist->lcp.lock);
    do {
        if (freelist->head != NULL) {
            trunk_info = freelist->head;
            remain_bytes = FS_TRUNK_AVAIL_SPACE(trunk_info);
            if (remain_bytes < aligned_size) {
                if (!is_normal && freelist->count <= 1) {
                    result = EAGAIN;
                    break;
                }
                
                if (remain_bytes <= 0) {
                    logInfo("allocator: %p, trunk_info: %p, trunk id: "
                            "%"PRId64", trunk size: %"PRId64", free start: "
                            "%"PRId64", remain_bytes: %d", trunk_info->
                            allocator, trunk_info, trunk_info->id_info.id,
                            trunk_info->size, trunk_info->free_start,
                            remain_bytes);
                    abort();
                }

                TRUNK_ALLOC_SPACE(trunk_info, space_info, remain_bytes);
                space_info++;

                aligned_size -= remain_bytes;
                trunk_freelist_remove(freelist);
            }
        }

        if (freelist->head == NULL) {
            if (!is_normal) {
                result = EAGAIN;
                break;
            }

            if ((result=waiting_avail_trunk(allocator, freelist)) != 0) {
                break;
            }
        }

        if (freelist->head == NULL) {
            result = (SF_G_CONTINUE_FLAG ? ENOSPC : EINTR);
            break;
        }

        trunk_info = freelist->head;
        if (aligned_size > FS_TRUNK_AVAIL_SPACE(trunk_info)) {
            result = EAGAIN;
            break;
        }

        TRUNK_ALLOC_SPACE(trunk_info, space_info, aligned_size);
        space_info++;
        if (FS_TRUNK_AVAIL_SPACE(trunk_info) <
                STORAGE_CFG.discard_remain_space_size)
        {
            trunk_freelist_remove(freelist);
            __sync_sub_and_fetch(&trunk_info->allocator->path_info->
                    trunk_stat.avail, FS_TRUNK_AVAIL_SPACE(trunk_info));
        }

        result = 0;
        *count = space_info - spaces;
    } while (0);

    if (result == ENOSPC && is_normal) {
        fs_remove_from_avail_aptr_array(&g_allocator_mgr->
                store_path, allocator);
    }
    PTHREAD_MUTEX_UNLOCK(&freelist->lcp.lock);

    return result;
}
