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
#include <sys/statvfs.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fast_mblock.h"
#include "fastcommon/fc_queue.h"
#include "fastcommon/common_blocked_queue.h"
#include "sf/sf_global.h"
#include "sf/sf_func.h"
#include "../server_global.h"
#include "../dio/trunk_io_thread.h"
#include "storage_allocator.h"
#include "trunk_reclaim.h"

typedef struct trunk_reclaim_thread_info {
    struct {
        bool finished;
        pthread_lock_cond_pair_t lcp; //for notify
    } allocate;
    FSTrunkAllocator *allocator;  //current pre-allocate / reclaim
    struct common_blocked_queue queue;
    pthread_t tid;
    bool running;
} TrunkReclaimThreadInfo;

typedef struct trunk_reclaim_thread_array {
    int count;
    TrunkReclaimThreadInfo *threads;
} TrunkReclaimThreadArray;

typedef struct trunk_reclaim_context {
    volatile int running_count;
    TrunkReclaimThreadArray thread_array;
} TrunkReclaimContext;

static TrunkReclaimContext reclaim_ctx;

static int deal_trunk_util_change_event(FSTrunkAllocator *allocator,
        FSTrunkFileInfo *trunk)
{
    UniqSkiplistNode *node;
    UniqSkiplistNode *previous;
    int event;
    int result;

    event = __sync_add_and_fetch(&trunk->util.event, 0);
    do {
        if (event == FS_TRUNK_UTIL_EVENT_CREATE) {
            trunk->util.last_used_bytes = __sync_fetch_and_add(
                    &trunk->used.bytes, 0);
            result = uniq_skiplist_insert(allocator->trunks.
                    by_size, trunk);
        } else {
            if ((node=uniq_skiplist_find_node(allocator->trunks.
                            by_size, trunk)) == NULL)
            {
                result = ENOENT;
                break;
            }

            result = 0;
            previous = UNIQ_SKIPLIST_LEVEL0_PREV_NODE(node);
            if (previous != allocator->trunks.by_size->top) {
                if (compare_trunk_by_size_id((FSTrunkFileInfo *)
                            previous->data, trunk) > 0)
                {
                    uniq_skiplist_delete_node(allocator->trunks.
                            by_size, previous, node);

                    trunk->util.last_used_bytes = __sync_fetch_and_add(
                            &trunk->used.bytes, 0);
                    result = uniq_skiplist_insert(allocator->trunks.
                            by_size, trunk);
                }
            }
        }
    } while (0);

    logInfo("event: %c, id: %"PRId64", status: %d, last_used_bytes: %"PRId64", "
            "current used: %"PRId64, event, trunk->id_info.id, trunk->status,
            trunk->util.last_used_bytes, trunk->used.bytes);

    __sync_bool_compare_and_swap(&trunk->util.event,
            event, FS_TRUNK_UTIL_EVENT_NONE);
    return result;
}

static void deal_trunk_util_change_events(FSTrunkAllocator *allocator)
{
    FSTrunkFileInfo *trunk;

    trunk = (FSTrunkFileInfo *)fc_queue_try_pop_all(&allocator->reclaim.queue);
    while (trunk != NULL && SF_G_CONTINUE_FLAG) {
        deal_trunk_util_change_event(allocator, trunk);
        trunk = trunk->util.next;
    }
}

static void create_trunk_done(struct trunk_io_buffer *record,
        const int result)
{
    TrunkReclaimThreadInfo *thread;
    FSTrunkAllocator *allocator;

    thread = (TrunkReclaimThreadInfo *)record->notify.arg;
    allocator = thread->allocator;
    if (result == 0) {
        FSTrunkFileInfo *trunk_info;
        time_t last_stat_time;

        __sync_add_and_fetch(&allocator->path_info->
                trunk_stat.total, record->space.size);

        allocator->reclaim.result = storage_allocator_add_trunk_ex(
                record->space.store->index, &record->space.id_info,
                record->space.size, &trunk_info);
        if (allocator->reclaim.result == 0) {
            allocator->reclaim.result = trunk_allocator_add_to_freelist(
                    allocator, trunk_info);
            __sync_add_and_fetch(&allocator->path_info->trunk_stat.avail,
                    record->space.size);
        }

        //trigger avail space stat
        last_stat_time = __sync_add_and_fetch(&allocator->path_info->
                space_stat.last_stat_time, 0);
        __sync_bool_compare_and_swap(&allocator->path_info->space_stat.
                last_stat_time, last_stat_time, 0);
    } else {
        allocator->reclaim.result = result;
    }

    PTHREAD_MUTEX_LOCK(&thread->allocate.lcp.lock);
    thread->allocate.finished = true;
    pthread_cond_signal(&thread->allocate.lcp.cond);
    PTHREAD_MUTEX_UNLOCK(&thread->allocate.lcp.lock);
}

static int do_prealloc_trunk(TrunkReclaimThreadInfo *thread)
{
    int result;
    FSTrunkSpaceInfo space;

    space.store = &thread->allocator->path_info->store;
    if ((result=trunk_id_info_generate(space.store->index,
                    &space.id_info)) != 0)
    {
        return result;
    }
    space.offset = 0;
    space.size = STORAGE_CFG.trunk_file_size;

    PTHREAD_MUTEX_LOCK(&thread->allocate.lcp.lock);
    thread->allocate.finished = false;
    if ((result=io_thread_push_trunk_op(FS_IO_TYPE_CREATE_TRUNK,
                    &space, create_trunk_done, thread)) == 0)
    {
        while (!thread->allocate.finished && SF_G_CONTINUE_FLAG) {
            pthread_cond_wait(&thread->allocate.lcp.cond,
                    &thread->allocate.lcp.lock);
        }

        if (!thread->allocate.finished) {
            result = EINTR;
        }
    }
    PTHREAD_MUTEX_UNLOCK(&thread->allocate.lcp.lock);

    return result;
}

static int do_reclaim_trunk(TrunkReclaimThreadInfo *thread)
{
    double ratio_thredhold;
    FSTrunkFileInfo *trunk;

    if (g_current_time - thread->allocator->reclaim.last_deal_time > 10) {
        thread->allocator->reclaim.last_deal_time = g_current_time;
        deal_trunk_util_change_events(thread->allocator);
    }

    if ((trunk=(FSTrunkFileInfo *)uniq_skiplist_get_first(
                    thread->allocator->trunks.by_size)) == NULL)
    {
        return ENOENT;
    }

    ratio_thredhold = STORAGE_CFG.never_reclaim_on_trunk_usage *
        (thread->allocator->path_info->space_stat.used_ratio -
         STORAGE_CFG.reclaim_trunks_on_path_usage) /
        (1.00 -  STORAGE_CFG.reclaim_trunks_on_path_usage);
    if ((double)__sync_fetch_and_add(&trunk->used.bytes, 0) /
            (double)trunk->size >= ratio_thredhold)
    {
        return ENOENT;
    }

    //TODO reclaim the trunk
    return 0;
}

static int do_allocate_trunk(TrunkReclaimThreadInfo *thread)
{
    int result;
    bool avail_enough;
    bool need_reclaim;

    if ((result=storage_config_calc_path_avail_space(thread->
                    allocator->path_info)) != 0)
    {
        return result;
    }

    avail_enough = thread->allocator->path_info->space_stat.avail -
        STORAGE_CFG.trunk_file_size > thread->allocator->
        path_info->reserved_space.value;
    if (thread->allocator->path_info->space_stat.used_ratio <=
            STORAGE_CFG.reclaim_trunks_on_path_usage)
    {
        need_reclaim = !avail_enough;
    } else {
        need_reclaim = true;
    }

    if (need_reclaim) {
        if ((result=do_reclaim_trunk(thread)) == 0) {
            return 0;
        }
    }

    if (avail_enough) {
        return do_prealloc_trunk(thread);
    } else {
        return ENOSPC;
    }
}

static void deal_allocate_request(TrunkReclaimThreadInfo *thread,
        struct common_blocked_node *head)
{
    while (head != NULL && SF_G_CONTINUE_FLAG) {
        thread->allocator = (FSTrunkAllocator *)head->data;
        do_allocate_trunk(thread);

        head = head->next;
    }
}

static void *trunk_reclaim_thread_func(void *arg)
{
    TrunkReclaimThreadInfo *thread;
    struct common_blocked_node *head;

    thread = (TrunkReclaimThreadInfo *)arg;
    thread->running = true;
    while (SF_G_CONTINUE_FLAG) {
        head = common_blocked_queue_pop_all_nodes(&thread->queue);
        if (head == NULL) {
            continue;
        }

        deal_allocate_request(thread, head);
        common_blocked_queue_free_all_nodes(&thread->queue, head);
    }

    thread->running = false;
    return NULL;
}

int trunk_reclaim_init()
{
    int result;
    int bytes;
    const int alloc_elements_once = 4096;
    TrunkReclaimThreadInfo *thread;
    TrunkReclaimThreadInfo *end;

    reclaim_ctx.thread_array.count = STORAGE_CFG.trunk_prealloc_threads;
    bytes = sizeof(TrunkReclaimThreadInfo) * reclaim_ctx.thread_array.count;
    reclaim_ctx.thread_array.threads =
        (TrunkReclaimThreadInfo *)fc_malloc(bytes);
    if (reclaim_ctx.thread_array.threads == NULL) {
        return ENOMEM;
    }
    memset(reclaim_ctx.thread_array.threads, 0, bytes);

    end = reclaim_ctx.thread_array.threads +
        reclaim_ctx.thread_array.count;
    for (thread=reclaim_ctx.thread_array.threads; thread<end; thread++) {
        if ((result=init_pthread_lock_cond_pair(&thread->allocate.lcp)) != 0) {
            return result;
        }
        if ((result=common_blocked_queue_init_ex(&thread->queue,
                        alloc_elements_once)) != 0)
        {
            return result;
        }

        if ((result=fc_create_thread(&thread->tid, trunk_reclaim_thread_func,
                        thread, SF_G_THREAD_STACK_SIZE)) != 0)
        {
            return result;
        }
    }

    return 0;
}

int trunk_allocate(FSTrunkAllocator *allocator)
{
    int result;

    TrunkReclaimThreadInfo *thread;
    thread = reclaim_ctx.thread_array.threads + allocator->path_info->
        store.index % reclaim_ctx.thread_array.count;
    PTHREAD_MUTEX_LOCK(&allocator->reclaim.lcp.lock);
    allocator->reclaim.finished = false;
    allocator->reclaim.result = EINTR;
    if ((result=common_blocked_queue_push(&thread->queue, allocator)) == 0) {
        while (!allocator->reclaim.finished && SF_G_CONTINUE_FLAG) {
            pthread_cond_wait(&allocator->reclaim.lcp.cond,
                    &allocator->reclaim.lcp.lock);
        }
        result = allocator->reclaim.result;
    }
    PTHREAD_MUTEX_UNLOCK(&allocator->reclaim.lcp.lock);

    return result;
}
