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


#include "fastcommon/shared_func.h"
#include "fastcommon/fc_atomic.h"
#include "fastcommon/logger.h"
#include "fastcommon/sorted_queue.h"
#include "sf/sf_func.h"
#include "../server_global.h"
#include "../binlog/slice_binlog.h"
#include "event_dealer.h"
#include "change_notify.h"

typedef struct fs_change_notify_context {
    volatile int waiting_count;
    struct fast_mblock_man allocator;  //element: FSChangeNotifyEvent
    struct sorted_queue queue;
} FSchangeNotifyContext;

static FSchangeNotifyContext change_notify_ctx;

static inline int deal_events(struct fc_queue_info *qinfo)
{
    int result;
    int count;

    if ((result=event_dealer_do(qinfo->head, &count)) != 0) {
        return result;
    }

    sorted_queue_free_chain(&change_notify_ctx.queue,
            &change_notify_ctx.allocator, qinfo);
    __sync_sub_and_fetch(&change_notify_ctx.
            waiting_count, count);
    return 0;
}

static void *change_notify_func(void *arg)
{
    FSChangeNotifyEvent less_equal;
    struct fc_queue_info qinfo;
    time_t last_time;
    int wait_seconds;
    int waiting_count;
    int result;

#ifdef OS_LINUX
    prctl(PR_SET_NAME, "chg-notify");
#endif

    memset(&less_equal, 0, sizeof(less_equal));
    last_time = g_current_time;
    while (SF_G_CONTINUE_FLAG) {
        wait_seconds = (last_time + BATCH_STORE_INTERVAL + 1) - g_current_time;
        waiting_count = FC_ATOMIC_GET(change_notify_ctx.waiting_count);
        if (wait_seconds > 0 && waiting_count < BATCH_STORE_ON_MODIFIES) {
            lcp_timedwait_sec(&change_notify_ctx.queue.
                    queue.lc_pair, wait_seconds);
        }

        last_time = g_current_time;
        if (waiting_count == 0) {
            waiting_count = FC_ATOMIC_GET(change_notify_ctx.waiting_count);
            if (waiting_count == 0) {
                continue;
            }
        }

        if (SLICE_LOAD_DONE) {
            less_equal.sn = sf_binlog_writer_get_last_version(
                    slice_binlog_get_writer());
        } else {
            less_equal.sn = SLICE_LOAD_LAST_SN;
        }

        logInfo("file: "__FILE__", line: %d, "
                "SLICE_LOAD_DONE: %d, less than version: %"PRId64,
                __LINE__, SLICE_LOAD_DONE, less_equal.sn);

        sorted_queue_try_pop_to_queue(&change_notify_ctx.
                queue, &less_equal, &qinfo);
        if (qinfo.head != NULL) {
            if ((result=deal_events(&qinfo)) != 0) {
                logCrit("file: "__FILE__", line: %d, "
                        "deal notify events fail, error code: %d, "
                        "program exit!", __LINE__, result);
                sf_terminate_myself();
            }
        }
    }

    return NULL;
}

static int notify_event_compare(const FSChangeNotifyEvent *event1,
        const FSChangeNotifyEvent *event2)
{
    return fc_compare_int64(event1->sn, event2->sn);
}

int change_notify_init()
{
    int result;
    pthread_t tid;

    if ((result=fast_mblock_init_ex1(&change_notify_ctx.allocator,
                    "chg-event", sizeof(FSChangeNotifyEvent),
                    16 * 1024, 0, NULL, NULL, true)) != 0)
    {
        return result;
    }

    if ((result=sorted_queue_init(&change_notify_ctx.queue, (long)
                    (&((FSChangeNotifyEvent *)NULL)->next),
                    (int (*)(const void *, const void *))
                    notify_event_compare)) != 0)
    {
        return result;
    }

    return fc_create_thread(&tid, change_notify_func,
            NULL, SF_G_THREAD_STACK_SIZE);
}

void change_notify_destroy()
{
}

static inline void change_notify_push_to_queue(FSChangeNotifyEvent *event)
{
    bool notify;

    notify = __sync_add_and_fetch(&change_notify_ctx.
            waiting_count, 1) == BATCH_STORE_ON_MODIFIES;
    sorted_queue_push_silence(&change_notify_ctx.queue, event);
    if (notify) {
        pthread_cond_signal(&change_notify_ctx.queue.queue.lc_pair.cond);
    }
}

#define CHANGE_NOTIFY_SET_EVENT(event, _sn, _ob, _entry_type, _op_type) \
    FC_ATOMIC_INC(_ob->db_args->ref_count);  \
    event->sn = _sn;  \
    event->ob = _ob;  \
    event->entry_type = _entry_type;  \
    event->op_type = _op_type

int change_notify_push_add_slice(const int64_t sn, OBSliceEntry *slice)
{
    FSChangeNotifyEvent *event;

    if ((event=fast_mblock_alloc_object(&change_notify_ctx.
                    allocator)) == NULL)
    {
        return ENOMEM;
    }

    CHANGE_NOTIFY_SET_EVENT(event, sn, slice->ob,
            fs_change_entry_type_slice,
            da_binlog_op_type_create);
    event->slice.data_version = slice->data_version;
    event->slice.type = slice->type;
    event->slice.ssize = slice->ssize;
    event->slice.space = slice->space;
    change_notify_push_to_queue(event);
    return 0;
}

int change_notify_push_del_slice(const int64_t sn,
        OBEntry *ob, const FSSliceSize *ssize)
{
    FSChangeNotifyEvent *event;

    if ((event=fast_mblock_alloc_object(&change_notify_ctx.
                    allocator)) == NULL)
    {
        return ENOMEM;
    }

    CHANGE_NOTIFY_SET_EVENT(event, sn, ob,
            fs_change_entry_type_slice,
            da_binlog_op_type_remove);
    event->ssize = *ssize;
    change_notify_push_to_queue(event);
    return 0;
}

int change_notify_push_del_block(const int64_t sn, OBEntry *ob)
{
    FSChangeNotifyEvent *event;

    if ((event=fast_mblock_alloc_object(&change_notify_ctx.
                    allocator)) == NULL)
    {
        return ENOMEM;
    }

    CHANGE_NOTIFY_SET_EVENT(event, sn, ob,
            fs_change_entry_type_block,
            da_binlog_op_type_remove);
    change_notify_push_to_queue(event);
    return 0;
}

void change_notify_load_done_signal()
{
    int sleep_us;

    sleep_us = 100;
    while (FC_ATOMIC_GET(change_notify_ctx.waiting_count) > 0 &&
            SF_G_CONTINUE_FLAG)
    {
        pthread_cond_signal(&change_notify_ctx.queue.queue.lc_pair.cond);
        fc_sleep_us(sleep_us);
        if (sleep_us < 10 * 1000) {
            sleep_us *= 2;
        }
    }
}