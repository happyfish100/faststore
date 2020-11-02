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
#include "trunk_util_man.h"

typedef struct trunk_reclaim_thread_info {
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

/*
static int trunk_util_man_deal_event(FSTrunkFileInfo *trunk)
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
            result = uniq_skiplist_insert(reclaim_thread_ctx.
                    sl_trunks, trunk);
        } else {
            if ((node=uniq_skiplist_find_node(reclaim_thread_ctx.
                            sl_trunks, trunk)) == NULL)
            {
                result = ENOENT;
                break;
            }

            result = 0;
            previous = UNIQ_SKIPLIST_LEVEL0_PREV_NODE(node);
            if (previous != reclaim_thread_ctx.sl_trunks->top) {
                if (compare_trunk_by_size_id((FSTrunkFileInfo *)
                            previous->data, trunk) > 0)
                {
                    uniq_skiplist_delete_node(reclaim_thread_ctx.
                            sl_trunks, previous, node);

                    trunk->util.last_used_bytes = __sync_fetch_and_add(
                            &trunk->used.bytes, 0);
                    result = uniq_skiplist_insert(reclaim_thread_ctx.
                            sl_trunks, trunk);
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
*/

static void deal_allocate_request(struct common_blocked_node *head)
{
    FSTrunkAllocator *allocator;
    //int result;

    while (head != NULL && SF_G_CONTINUE_FLAG) {
        allocator = (FSTrunkAllocator *)head->data;
        head = head->next;
    }
}

static void *trunk_util_man_thread_func(void *arg)
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

        deal_allocate_request(head);
        common_blocked_queue_free_all_nodes(&thread->queue, head);
    }

    thread->running = false;
    return NULL;
}

static int init_thread_ctx_array()
{
    int result;
    int bytes;
    const int alloc_elements_once = 4096;
    TrunkReclaimThreadInfo *thread;
    TrunkReclaimThreadInfo *end;

    reclaim_ctx.thread_array.count = STORAGE_CFG.trunk_allocator_threads;
    bytes = sizeof(TrunkReclaimThreadInfo) * reclaim_ctx.thread_array.count;
    reclaim_ctx.thread_array.threads =
        (TrunkReclaimThreadInfo *)fc_malloc(bytes);
    if (reclaim_ctx.thread_array.threads == NULL) {
        return ENOMEM;
    }

    end = reclaim_ctx.thread_array.threads +
        reclaim_ctx.thread_array.count;
    for (thread=reclaim_ctx.thread_array.threads; thread<end; thread++) {
        if ((result=common_blocked_queue_init_ex(&thread->queue,
                        alloc_elements_once)) != 0)
        {
            return result;
        }

        if ((result=fc_create_thread(&thread->tid, trunk_util_man_thread_func,
                        thread, SF_G_THREAD_STACK_SIZE)) != 0)
        {
            return result;
        }
    }

    return 0;
}

int trunk_util_man_init()
{
    return init_thread_ctx_array();
}

int trunk_allocate(FSTrunkAllocator *allocator)
{
    int result;

    TrunkReclaimThreadInfo *thread;
    thread = reclaim_ctx.thread_array.threads + allocator->path_info->
        store.index % reclaim_ctx.thread_array.count;
    PTHREAD_MUTEX_LOCK(&allocator->reclaim.lcp.lock);
    allocator->reclaim.finished = false;
    if ((result=common_blocked_queue_push(&thread->queue, allocator)) == 0) {
        while (!allocator->reclaim.finished && SF_G_CONTINUE_FLAG) {
            pthread_cond_wait(&allocator->reclaim.lcp.cond,
                    &allocator->reclaim.lcp.lock);
        }

        if (!allocator->reclaim.finished) {
            result = EINTR;
        }
    }
    PTHREAD_MUTEX_UNLOCK(&allocator->reclaim.lcp.lock);

    return result;
}
