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
        space_info->store = &trunk->allocator->path_info->store; \
        space_info->id_info = trunk->id_info;   \
        space_info->offset = trunk->free_start; \
        space_info->size = alloc_size;         \
        trunk->free_start += alloc_size;  \
        __sync_sub_and_fetch(&trunk->allocator->path_info-> \
                trunk_stat.avail, alloc_size);  \
    } while (0)

void trunk_freelist_keep_water_mark(struct fs_trunk_allocator
        *allocator)
{
    int count;
    int i;

    logInfo("file: "__FILE__", line: %d, func: %s, "
            "%s freelist count: %d, water_mark count: %d",
            __LINE__, __FUNCTION__, allocator->path_info->store.path.str,
            allocator->freelist.count, allocator->freelist.water_mark_trunks);

    count = allocator->freelist.water_mark_trunks - allocator->freelist.count;
    for (i=0; i<count; i++) {
        trunk_maker_allocate(allocator);
    }
}

void trunk_freelist_add(FSTrunkFreelist *freelist,
        FSTrunkFileInfo *trunk_info)
{
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
    PTHREAD_MUTEX_UNLOCK(&freelist->lcp.lock);

    __sync_add_and_fetch(&trunk_info->allocator->path_info->
            trunk_stat.avail, FS_TRUNK_AVAIL_SPACE(trunk_info));
}

static void trunk_freelist_remove(FSTrunkAllocator *allocator,
        FSTrunkFreelist *freelist)
{
    FSTrunkFileInfo *trunk_info;

    trunk_info = freelist->head;
    freelist->head = freelist->head->alloc.next;
    if (freelist->head == NULL) {
        freelist->tail = NULL;
    }
    freelist->count--;

    fs_set_trunk_status(trunk_info, FS_TRUNK_STATUS_REPUSH);
    push_trunk_util_event_force(allocator, trunk_info,
            FS_TRUNK_UTIL_EVENT_CREATE);
    fs_set_trunk_status(trunk_info, FS_TRUNK_STATUS_NONE);

    if (freelist->count < freelist->water_mark_trunks) {
        trunk_maker_allocate_ex(allocator, true, false, NULL, NULL);
    }
}

int trunk_freelist_alloc_space(FSTrunkFreelist *freelist,
        const uint32_t blk_hc, const int size,
        FSTrunkSpaceInfo *spaces, int *count, const bool is_normal)
{
    int aligned_size;
    int result;
    int remain_bytes;
    FSTrunkSpaceInfo *space_info;
    FSTrunkFileInfo *trunk_info;

    aligned_size = MEM_ALIGN(size);
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
                
                /*
                if (remain_bytes <= 0) {
                    logInfo("allocator: %p, trunk_info: %p, "
                            "trunk size: %"PRId64", free start: %"PRId64
                            ", remain_bytes: %d", trunk_info->allocator,
                            trunk_info, trunk_info->size,
                            trunk_info->free_start, remain_bytes);
                }
                */

                TRUNK_ALLOC_SPACE(trunk_info, space_info, remain_bytes);
                space_info++;

                aligned_size -= remain_bytes;
                trunk_freelist_remove(trunk_info->allocator, freelist);
            }
        }

        if (freelist->head == NULL) {
            if (!is_normal) {
                result = EAGAIN;
                break;
            }

            if (trunk_info->allocator->allocate.creating_trunks == 0 &&
                    g_current_time - trunk_info->allocator->allocate.
                    last_trigger_time > 0)
            {
                trunk_info->allocator->allocate.last_trigger_time = g_current_time;
                if ((result=trunk_maker_allocate_ex(trunk_info->allocator,
                                true, false, NULL, NULL)) != 0)
                {
                    break;
                }
            }

            trunk_info->allocator->allocate.waiting_callers++;
            while (trunk_info->allocator->allocate.creating_trunks > 0 &&
                    freelist->head == NULL && SF_G_CONTINUE_FLAG)
            {
                pthread_cond_wait(&freelist->lcp.cond,
                        &freelist->lcp.lock);
            }
            trunk_info->allocator->allocate.waiting_callers--;
        }

        if (freelist->head == NULL) {
            result = SF_G_CONTINUE_FLAG ? ENOSPC : EINTR;
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
            trunk_freelist_remove(trunk_info->allocator, freelist);
            __sync_sub_and_fetch(&trunk_info->allocator->path_info->
                    trunk_stat.avail, FS_TRUNK_AVAIL_SPACE(trunk_info));
        }
        result = 0;
    } while (0);
    PTHREAD_MUTEX_UNLOCK(&freelist->lcp.lock);

    if (result == 0) {
        *count = space_info - spaces;
    } else if (result == ENOSPC && is_normal) {
        fs_remove_from_avail_aptr_array(&g_allocator_mgr->
                store_path, trunk_info->allocator);
    }

    return result;
}
