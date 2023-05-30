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
    struct sorted_queue queue;
} FSchangeNotifyContext;

static FSchangeNotifyContext change_notify_ctx;

static inline int deal_events(struct fc_list_head *head)
{
    int result;
    int count;

    if ((result=event_dealer_do(head, &count)) != 0) {
        return result;
    }

    __sync_sub_and_fetch(&change_notify_ctx.
            waiting_count, count);
    return 0;
}

static void *change_notify_func(void *arg)
{
    FSChangeNotifyEvent less_equal;
    struct fc_list_head head;
    time_t last_time;
    int wait_seconds;
    int waiting_count;
    int result;

#ifdef OS_LINUX
    prctl(PR_SET_NAME, "ev-dispatcher");
#endif

    memset(&less_equal, 0, sizeof(less_equal));
    last_time = g_current_time;
    while (SF_G_CONTINUE_FLAG) {
        wait_seconds = (last_time + BATCH_STORE_INTERVAL + 1) - g_current_time;
        waiting_count = FC_ATOMIC_GET(change_notify_ctx.waiting_count);
        if (wait_seconds > 0 && waiting_count < BATCH_STORE_ON_MODIFIES) {
            lcp_timedwait_sec(&change_notify_ctx.queue.lcp, wait_seconds);
        }

        last_time = g_current_time;
        if (waiting_count == 0) {
            waiting_count = FC_ATOMIC_GET(change_notify_ctx.waiting_count);
            if (waiting_count == 0) {
                continue;
            }
        }

        switch (STORAGE_SN_TYPE) {
            case fs_sn_type_slice_loading:
                less_equal.sn = SLICE_LOAD_LAST_SN;
                break;
            case fs_sn_type_block_removing:
                less_equal.sn = FC_ATOMIC_GET(SLICE_BINLOG_SN);
                break;
            case fs_sn_type_slice_binlog:
                less_equal.sn = sf_binlog_writer_get_last_version_silence(
                        &SLICE_BINLOG_WRITER.writer);
                break;
        }

        /*
        logInfo("file: "__FILE__", line: %d, "
                "sn type: %d, less than version: %"PRId64,
                __LINE__, STORAGE_SN_TYPE, less_equal.sn);
                */

        sorted_queue_try_pop_to_chain(&change_notify_ctx.
                queue, &less_equal, &head);
        if (!fc_list_empty(&head)) {
            if ((result=deal_events(&head)) != 0) {
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
    int event_alloc_elements_once;
    int event_alloc_elements_limit;
    int limit;
    int max_threads;
    pthread_t tid;

    if (BATCH_STORE_ON_MODIFIES < 1000) {
        event_alloc_elements_once = 2 * 1024;
        limit = 64 * 1024;
    } else if (BATCH_STORE_ON_MODIFIES < 10 * 1000) {
        event_alloc_elements_once = 4 * 1024;
        limit = BATCH_STORE_ON_MODIFIES * 16;
    } else if (BATCH_STORE_ON_MODIFIES < 100 * 1000) {
        event_alloc_elements_once = 8 * 1024;
        limit = BATCH_STORE_ON_MODIFIES * 8;
    } else {
        event_alloc_elements_once = 16 * 1024;
        limit = BATCH_STORE_ON_MODIFIES * 4;
    }
    max_threads = FS_DATA_RECOVERY_THREADS_LIMIT * (2 +
            RECOVERY_THREADS_PER_DATA_GROUP +
            CLUSTER_SERVER_ARRAY.count) + DATA_THREAD_COUNT;
    limit += FS_CHANGE_NOTIFY_EVENT_TLS_BATCH_ALLOC *
        FC_MAX(max_threads, SYSTEM_CPU_COUNT);

    event_alloc_elements_limit = event_alloc_elements_once;
    while (event_alloc_elements_limit < limit) {
        event_alloc_elements_limit *= 2;
    }
    if ((result=fast_mblock_init_ex1(&STORAGE_EVENT_ALLOCATOR,
                    "chg-event", sizeof(FSChangeNotifyEvent),
                    event_alloc_elements_once, event_alloc_elements_limit,
                    NULL, NULL, true)) != 0)
    {
        return result;
    }
    fast_mblock_set_need_wait(&STORAGE_EVENT_ALLOCATOR,
                true, (bool *)&SF_G_CONTINUE_FLAG);

    if ((result=sorted_queue_init(&change_notify_ctx.queue, (long)
                    (&((FSChangeNotifyEvent *)NULL)->dlink),
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
        pthread_cond_signal(&change_notify_ctx.queue.lcp.cond);
    }
}

#define CHANGE_NOTIFY_SET_EVENT(event, _sn, _ob, _entry_type, _op_type) \
    FC_ATOMIC_INC(_ob->db_args->ref_count);  \
    event->sn = _sn;  \
    event->ob = _ob;  \
    event->entry_type = _entry_type;  \
    event->op_type = _op_type

void change_notify_push_add_slice(FSChangeNotifyEvent *event,
        const int64_t sn, OBSliceEntry *slice)
{
    CHANGE_NOTIFY_SET_EVENT(event, sn, slice->ob,
            fs_change_entry_type_slice,
            da_binlog_op_type_create);
    event->slice.data_version = slice->data_version;
    event->slice.type = slice->type;
    event->slice.ssize = slice->ssize;
    event->slice.space = slice->space;
    change_notify_push_to_queue(event);
}

void change_notify_push_del_slice(FSChangeNotifyEvent *event,
        const int64_t sn, OBEntry *ob, const FSSliceSize *ssize)
{
    CHANGE_NOTIFY_SET_EVENT(event, sn, ob,
            fs_change_entry_type_slice,
            da_binlog_op_type_remove);
    event->ssize = *ssize;
    change_notify_push_to_queue(event);
}

void change_notify_push_del_block(FSChangeNotifyEvent *event,
        const int64_t sn, OBEntry *ob)
{
    CHANGE_NOTIFY_SET_EVENT(event, sn, ob,
            fs_change_entry_type_block,
            da_binlog_op_type_remove);
    change_notify_push_to_queue(event);
}

void change_notify_load_done_signal()
{
    int sleep_us;

    sleep_us = 100;
    while (FC_ATOMIC_GET(change_notify_ctx.waiting_count) > 0 &&
            SF_G_CONTINUE_FLAG)
    {
        pthread_cond_signal(&change_notify_ctx.queue.lcp.cond);
        fc_sleep_us(sleep_us);
        if (sleep_us < 10 * 1000) {
            sleep_us *= 2;
        }
    }
}

void change_notify_signal_to_deal()
{
    pthread_cond_signal(&change_notify_ctx.queue.lcp.cond);
}
