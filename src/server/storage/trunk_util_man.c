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
#include "sf/sf_global.h"
#include "sf/sf_func.h"
#include "../server_global.h"
#include "trunk_util_man.h"

typedef struct trunk_util_man_thread_context {
    UniqSkiplistFactory skiplist_factory;
    UniqSkiplist *sl_trunks;   //order by used size and id
    struct fc_queue queue;
    pthread_t tid;
    bool running;
} TrunkReclaimThreadContext;

static TrunkReclaimThreadContext reclaim_thread_ctx;

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

static int trunk_util_man_deal_event(FSTrunkFileInfo *trunk)
{
    UniqSkiplistNode *node;
    UniqSkiplistNode *previous;
    int event;
    int result;

    event = __sync_add_and_fetch(&trunk->util.event, 0);
    do {
        if (event == TRUNK_UTIL_EVENT_CREATE) {
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
            event, TRUNK_UTIL_EVENT_NONE);
    return result;
}

static void *trunk_util_man_thread_func(void *arg)
{
    TrunkReclaimThreadContext *thread;
    FSTrunkFileInfo *trunk;
    int result;

    thread = (TrunkReclaimThreadContext *)arg;
    thread->running = true;
    while (SF_G_CONTINUE_FLAG) {
        trunk = (FSTrunkFileInfo *)fc_queue_pop(&thread->queue);
        if (trunk == NULL) {
            continue;
        }

        if ((result=trunk_util_man_deal_event(trunk)) != 0) {
            logCrit("file: "__FILE__", line: %d, "
                    "deal trunk event fail, errno: %d, error info: %s, "
                    "program exit!", __LINE__, result, STRERROR(result));
            sf_terminate_myself();
        }
    }

    thread->running = false;
    return NULL;
}

int trunk_util_man_init()
{
    const int init_level_count = 12;
    const int max_level_count = 20;
    const int alloc_skiplist_once = 1;
    const int min_alloc_elements_once = 2;
    const int delay_free_seconds = 0;
    const bool bidirection = true;
    int result;

    if ((result=fc_queue_init(&reclaim_thread_ctx.queue, (long)
                    (&((FSTrunkFileInfo *)NULL)->util.next))) != 0)
    {
        return result;
    }

    if ((result=uniq_skiplist_init_ex2(&reclaim_thread_ctx.skiplist_factory,
                    max_level_count, (skiplist_compare_func)
                    compare_trunk_by_size_id, NULL,
                    alloc_skiplist_once, min_alloc_elements_once,
                    delay_free_seconds, bidirection)) != 0)
    {
        return result;
    }

    if ((reclaim_thread_ctx.sl_trunks=uniq_skiplist_new(
                    &reclaim_thread_ctx.skiplist_factory,
                    init_level_count)) == NULL)
    {
        return ENOMEM;
    }

    return 0;
}

int trunk_util_man_start()
{
    return fc_create_thread(&reclaim_thread_ctx.tid,
            trunk_util_man_thread_func, &reclaim_thread_ctx,
            SF_G_THREAD_STACK_SIZE);
}

int trunk_util_man_push(FSTrunkFileInfo *trunk, const int event)
{
    if (!__sync_bool_compare_and_swap(&trunk->util.event,
                TRUNK_UTIL_EVENT_NONE, event))
    {
        return EEXIST;
    }

    fc_queue_push(&reclaim_thread_ctx.queue, trunk);
    return 0;
}
