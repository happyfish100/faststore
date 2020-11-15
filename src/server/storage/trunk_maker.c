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
#include "trunk_maker.h"

struct trunk_maker_thread_info;
typedef struct trunk_maker_task {
    bool urgent;
    FSTrunkAllocator *allocator;
    struct {
        trunk_allocate_done_callback callback;
        void *arg;
    } notify;
    struct trunk_maker_thread_info *thread;
    struct trunk_maker_task *next;
} TrunkMakerTask;

typedef struct trunk_maker_thread_info {
    struct {
        int result;
        pthread_lock_cond_pair_t lcp; //for notify
    } allocate;
    TrunkReclaimContext reclaim_ctx;
    struct fast_mblock_man task_allocator;
    struct fc_queue queue;
    pthread_t tid;
    bool running;
} TrunkMakerThreadInfo;

typedef struct trunk_maker_thread_array {
    int count;
    TrunkMakerThreadInfo *threads;
} TrunkMakerThreadArray;

typedef struct trunk_maker_context {
    volatile int running_count;
    TrunkMakerThreadArray thread_array;
} TrunkMakerContext;

static TrunkMakerContext tmaker_ctx;

static int deal_trunk_util_change_event(FSTrunkAllocator *allocator,
        FSTrunkFileInfo *trunk)
{
    UniqSkiplistNode *node;
    UniqSkiplistNode *previous;
    int status;
    int event;
    int result;

    event = __sync_add_and_fetch(&trunk->util.event, 0);
    while (1) {
        status = __sync_add_and_fetch(&trunk->status, 0);
        if (status == FS_TRUNK_STATUS_NONE) {  //accept
            break;
        } else if (status == FS_TRUNK_STATUS_REPUSH) {
            fc_queue_push(&allocator->reclaim.queue, trunk); //repush
            return EAGAIN;
        }

        if (__sync_bool_compare_and_swap(&trunk->util.event,
                    event, FS_TRUNK_UTIL_EVENT_NONE))
        {
            return EAGAIN;  //refuse
        }
        event = __sync_add_and_fetch(&trunk->util.event, 0);
    }

    switch (event) {
        case FS_TRUNK_UTIL_EVENT_CREATE:
            trunk->util.last_used_bytes = __sync_fetch_and_add(
                    &trunk->used.bytes, 0);
            result = uniq_skiplist_insert(allocator->trunks.
                    by_size, trunk);
            break;
        case FS_TRUNK_UTIL_EVENT_UPDATE:
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
            break;
        default:
            result = 0;
            break;
    }

    logInfo("event: %c, id: %"PRId64", status: %d, last_used_bytes: %"PRId64", "
            "current used: %"PRId64", result: %d", event, trunk->id_info.id,
            trunk->status, trunk->util.last_used_bytes, trunk->used.bytes, result);

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
    TrunkMakerThreadInfo *thread;

    thread = (TrunkMakerThreadInfo *)record->notify.arg;
    PTHREAD_MUTEX_LOCK(&thread->allocate.lcp.lock);
    thread->allocate.result = result >= 0 ? result : -1 * result;
    pthread_cond_signal(&thread->allocate.lcp.cond);
    PTHREAD_MUTEX_UNLOCK(&thread->allocate.lcp.lock);
}

static int prealloc_trunk_finish(FSTrunkAllocator *allocator,
        FSTrunkSpaceInfo *space)
{
    int result;
    time_t last_stat_time;
    FSTrunkFileInfo *trunk_info;

    result = storage_allocator_add_trunk_ex(space->store->index,
            &space->id_info, space->size, &trunk_info);
    if (result == 0) {
        trunk_allocator_add_to_freelist(allocator, trunk_info);
    }

    __sync_add_and_fetch(&allocator->path_info->
            trunk_stat.total, space->size);

    //trigger avail space stat
    last_stat_time = __sync_add_and_fetch(&allocator->path_info->
            space_stat.last_stat_time, 0);
    __sync_bool_compare_and_swap(&allocator->path_info->space_stat.
            last_stat_time, last_stat_time, 0);
    return result;
}

static int do_prealloc_trunk(TrunkMakerThreadInfo *thread,
        TrunkMakerTask *task)
{
    int result;
    FSTrunkSpaceInfo space;

    space.store = &task->allocator->path_info->store;
    if ((result=trunk_id_info_generate(space.store->index,
                    &space.id_info)) != 0)
    {
        return result;
    }
    space.offset = 0;
    space.size = STORAGE_CFG.trunk_file_size;

    if ((result=io_thread_push_trunk_op(FS_IO_TYPE_CREATE_TRUNK,
                    &space, create_trunk_done, thread)) == 0)
    {
        PTHREAD_MUTEX_LOCK(&thread->allocate.lcp.lock);
        while (thread->allocate.result == -1 && SF_G_CONTINUE_FLAG) {
            pthread_cond_wait(&thread->allocate.lcp.cond,
                    &thread->allocate.lcp.lock);
        }
        result = thread->allocate.result;
        thread->allocate.result = -1;  /* reset for next */
        PTHREAD_MUTEX_UNLOCK(&thread->allocate.lcp.lock);

        if (result == -1) {
            return EINTR;
        }
    }

    if (result != 0) {
        return result;
    }

    return prealloc_trunk_finish(task->allocator, &space);
}

static int do_reclaim_trunk(TrunkMakerThreadInfo *thread,
        TrunkMakerTask *task)
{
    double ratio_thredhold;
    FSTrunkFileInfo *trunk;
    int64_t used_bytes;
    int result;

    if (task->urgent || g_current_time - task->allocator->
            reclaim.last_deal_time > 10)
    {
        task->allocator->reclaim.last_deal_time = g_current_time;
        deal_trunk_util_change_events(task->allocator);
    }

    if ((trunk=(FSTrunkFileInfo *)uniq_skiplist_get_first(
                    task->allocator->trunks.by_size)) == NULL)
    {
        return ENOENT;
    }

    used_bytes = __sync_fetch_and_add(&trunk->used.bytes, 0);
    if (trunk->size - used_bytes < FS_FILE_BLOCK_SIZE) {
        return ENOENT;
    }

    ratio_thredhold = trunk_allocator_calc_reclaim_ratio_thredhold(
            task->allocator);

    logInfo("file: "__FILE__", line: %d, "
            "path index: %d, trunk id: %"PRId64", "
            "usage ratio: %.2f%%, ratio_thredhold: %.2f%%",
            __LINE__, task->allocator->path_info->store.index,
            trunk->id_info.id, 100.00 * (double)used_bytes /
            (double)trunk->size, 100.00 * ratio_thredhold);

    if ((double)used_bytes / (double)trunk->size >= ratio_thredhold) {
        return ENOENT;
    }

    if (used_bytes > 0) {
        fs_set_trunk_status(trunk, FS_TRUNK_STATUS_RECLAIMING);
        result = trunk_reclaim(task->allocator, trunk,
                &thread->reclaim_ctx);
    } else {
        result = 0;
    }

    logInfo("file: "__FILE__", line: %d, "
            "path index: %d, reclaiming trunk id: %"PRId64", "
            "last used bytes: %"PRId64", current used bytes: %"PRId64", "
            "result: %d", __LINE__, task->allocator->path_info->store.index,
            trunk->id_info.id, used_bytes, trunk->used.bytes, result);

    if (result == 0) {
        PTHREAD_MUTEX_LOCK(&task->allocator->freelist.lcp.lock);
        trunk->free_start = 0;
        PTHREAD_MUTEX_UNLOCK(&task->allocator->freelist.lcp.lock);
        trunk_allocator_add_to_freelist(task->allocator, trunk);
        uniq_skiplist_delete(task->allocator->trunks.by_size, trunk);
    } else {
        fs_set_trunk_status(trunk, FS_TRUNK_STATUS_NONE); //rollback status
    }
    return result;
}

static int do_allocate_trunk(TrunkMakerThreadInfo *thread,
        TrunkMakerTask *task, bool *is_new_trunk)
{
    int result;
    bool avail_enough;
    bool need_reclaim;

    *is_new_trunk = false;
    if ((result=storage_config_calc_path_avail_space(task->
                    allocator->path_info)) != 0)
    {
        return result;
    }

    avail_enough = task->allocator->path_info->space_stat.avail -
        STORAGE_CFG.trunk_file_size > task->allocator->
        path_info->reserved_space.value;
    if (task->allocator->path_info->space_stat.used_ratio <=
            STORAGE_CFG.reclaim_trunks_on_path_usage)
    {
        need_reclaim = !avail_enough;
    } else {
        need_reclaim = true;
    }

    if (need_reclaim) {
        if ((result=do_reclaim_trunk(thread, task)) == 0) {
            return 0;
        }
    }

    if (avail_enough) {
        *is_new_trunk = true;
        return do_prealloc_trunk(thread, task);
    } else {
        return ENOSPC;
    }
}

static void deal_allocate_request(TrunkMakerThreadInfo *thread,
        TrunkMakerTask *head)
{
    TrunkMakerTask *task;
    int result;
    bool is_new_trunk;

    while (head != NULL && SF_G_CONTINUE_FLAG) {
        task = head;
        head = head->next;

        result = do_allocate_trunk(thread, task, &is_new_trunk);
        trunk_allocator_after_make_trunk(task->allocator);
        if (task->notify.callback != NULL) {
            task->notify.callback(task->allocator,
                    result, is_new_trunk, task->notify.arg);
        }
        fast_mblock_free_object(&thread->task_allocator, task);
    }
}

static void *trunk_maker_thread_func(void *arg)
{
    TrunkMakerThreadInfo *thread;
    TrunkMakerTask *head;

    thread = (TrunkMakerThreadInfo *)arg;
    thread->running = true;
    while (SF_G_CONTINUE_FLAG) {
        head = (TrunkMakerTask *)fc_queue_pop_all(&thread->queue);
        if (head == NULL) {
            continue;
        }

        deal_allocate_request(thread, head);
    }

    thread->running = false;
    return NULL;
}

static int maker_task_alloc_init(void *element, void *args)
{
    ((TrunkMakerTask *)element)->thread = (TrunkMakerThreadInfo *)args;
    return 0;
}

int trunk_maker_init()
{
    int result;
    int bytes;
    TrunkMakerThreadInfo *thread;
    TrunkMakerThreadInfo *end;

    tmaker_ctx.thread_array.count = STORAGE_CFG.trunk_prealloc_threads;
    bytes = sizeof(TrunkMakerThreadInfo) * tmaker_ctx.thread_array.count;
    tmaker_ctx.thread_array.threads =
        (TrunkMakerThreadInfo *)fc_malloc(bytes);
    if (tmaker_ctx.thread_array.threads == NULL) {
        return ENOMEM;
    }
    memset(tmaker_ctx.thread_array.threads, 0, bytes);

    end = tmaker_ctx.thread_array.threads +
        tmaker_ctx.thread_array.count;
    for (thread=tmaker_ctx.thread_array.threads; thread<end; thread++) {
        thread->allocate.result = -1;
        if ((result=init_pthread_lock_cond_pair(&thread->allocate.lcp)) != 0) {
            return result;
        }

        if ((result=fast_mblock_init_ex1(&thread->task_allocator,
                        "maker_task", sizeof(TrunkMakerTask), 1024, 0,
                        maker_task_alloc_init, thread, true)) != 0)
        {
            return result;
        }
        if ((result=fc_queue_init(&thread->queue, (long)
                        (&((TrunkMakerTask *)NULL)->next))) != 0)
        {
            return result;
        }

        if ((result=trunk_reclaim_init_ctx(&thread->reclaim_ctx)) != 0) {
            return result;
        }

        if ((result=fc_create_thread(&thread->tid, trunk_maker_thread_func,
                        thread, SF_G_THREAD_STACK_SIZE)) != 0)
        {
            return result;
        }
    }

    return 0;
}

int trunk_maker_allocate_ex(FSTrunkAllocator *allocator, const bool urgent,
        const bool need_lock, trunk_allocate_done_callback callback, void *arg)
{
    TrunkMakerThreadInfo *thread;
    TrunkMakerTask *task;

    thread = tmaker_ctx.thread_array.threads + allocator->path_info->
        store.index % tmaker_ctx.thread_array.count;
    if ((task=(TrunkMakerTask *)fast_mblock_alloc_object(
                    &thread->task_allocator)) == NULL)
    {
        return ENOMEM;
    }

    task->urgent = urgent;
    task->allocator = allocator;
    task->notify.callback = callback;
    task->notify.arg = arg;
    trunk_allocator_before_make_trunk(allocator, need_lock);
    fc_queue_push(&thread->queue, task);
    return 0;
}
