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

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <pthread.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/common_blocked_queue.h"
#include "fastcommon/thread_pool.h"
#include "sf/sf_global.h"
#include "sf/sf_service.h"
#include "../../common/fs_proto.h"
#include "../server_global.h"
#include "../server_group_info.h"
#include "../cluster_relationship.h"
#include "data_recovery.h"
#include "recovery_thread.h"

typedef struct {
    pthread_t tid;
    struct common_blocked_queue queue;
    FCThreadPool tpool;
} RecoveryThreadContext;

static RecoveryThreadContext recovery_thread_ctx;

static void recovery_thread_run_task(void *arg, void *thread_data)
{
    int result;
    int old_status;
    int new_status;
    FSClusterDataServerInfo *ds;

    ds = (FSClusterDataServerInfo *)arg;
    old_status = __sync_fetch_and_add(&ds->status, 0);
    if (old_status == FS_SERVER_STATUS_INIT) {
        new_status = FS_SERVER_STATUS_REBUILDING;
    } else if (old_status == FS_SERVER_STATUS_OFFLINE) {
        new_status = FS_SERVER_STATUS_RECOVERING;
    } else {
        logInfo("file: "__FILE__", line: %d, "
                "data group id: %d, my status: %d (%s), "
                "skip data recovery", __LINE__,
                ds->dg->id, old_status,
                fs_get_server_status_caption(old_status));
        return;
    }

    if (!__sync_bool_compare_and_swap(&ds->recovery.in_progress, 0, 1)) {
        logInfo("file: "__FILE__", line: %d, "
                "data group id: %d, set recovery in progress fail, "
                "skip data recovery", __LINE__, ds->dg->id);
        return;
    }

    if (!cluster_relationship_swap_report_ds_status(ds,
                old_status, new_status, FS_EVENT_SOURCE_SELF_REPORT))
    {
        __sync_bool_compare_and_swap(&ds->recovery.in_progress, 1, 0);
        logWarning("file: "__FILE__", line: %d, "
                "data group id: %d, change my status to %d (%s) fail, "
                "skip data recovery", __LINE__, ds->dg->id, new_status,
                fs_get_server_status_caption(new_status));
        return;
    }

    result = data_recovery_start(ds);
    new_status = __sync_add_and_fetch(&ds->status, 0);
    __sync_bool_compare_and_swap(&ds->recovery.in_progress, 1, 0);

    if (result == 0) {
        ds->recovery.continuous_fail_count = 0;
    } else {
        bool recovery_again;

        ds->recovery.continuous_fail_count++;
        if (new_status == FS_SERVER_STATUS_REBUILDING ||
                new_status == FS_SERVER_STATUS_RECOVERING ||
                new_status == FS_SERVER_STATUS_ONLINE)
        {
            if (cluster_relationship_swap_report_ds_status(ds,
                        new_status, old_status, FS_EVENT_SOURCE_SELF_REPORT))
            {  //rollback status
                logWarning("file: "__FILE__", line: %d, "
                        "data group id: %d, data recovery continuous fail "
                        "count: %d, result: %d, rollback my status from "
                        "%d (%s) to %d (%s)", __LINE__, ds->dg->id, ds->
                        recovery.continuous_fail_count, result, new_status,
                        fs_get_server_status_caption(new_status),
                        old_status, fs_get_server_status_caption(old_status));

                if (ds->recovery.continuous_fail_count > 1) {
                    sleep(1);
                }
                recovery_again = false;
            } else {
                recovery_again = true;
            }
        } else {
            recovery_again = true;
        }

        if (recovery_again) {
            int status;
            status = __sync_add_and_fetch(&ds->status, 0);
            if (status == FS_SERVER_STATUS_INIT ||
                    status == FS_SERVER_STATUS_OFFLINE)
            {
                sleep(1);
                recovery_thread_push_to_queue(ds);
            }
        }
    }

    /*
    logInfo("====file: "__FILE__", line: %d, func: %s, "
            "do recovery, data group id: %d, result: %d, done status: %d, "
            "current status: %d =====", __LINE__, __FUNCTION__, ds->dg->id,
            result, new_status, __sync_add_and_fetch(&ds->status, 0));
            */
}

static void recovery_thread_deal(FSClusterDataServerInfo *ds)
{
    int status;
    bool notify;

    if (ds->cs != CLUSTER_MYSELF_PTR) {
        logWarning("file: "__FILE__", line: %d, "
                "i NOT belong to data group id: %d",
                __LINE__, ds->dg->id);
        return;
    }

    status = __sync_fetch_and_add(&ds->status, 0);
    switch (status) {
        case FS_SERVER_STATUS_REBUILDING:
        case FS_SERVER_STATUS_RECOVERING:
        case FS_SERVER_STATUS_ONLINE:
            logWarning("file: "__FILE__", line: %d, "
                    "data group id: %d, data recovery in progress",
                    __LINE__, ds->dg->id);
            break;
        case FS_SERVER_STATUS_ACTIVE:
            logWarning("file: "__FILE__", line: %d, "
                    "data group id: %d, status: %d (%s), "
                    "skip data recovery!", __LINE__, ds->dg->id,
                    status, fs_get_server_status_caption(status));
            break;
        case FS_SERVER_STATUS_INIT:
            if (fc_thread_pool_avail_count(&recovery_thread_ctx.tpool) <
                    DATA_RECOVERY_THREADS_LIMIT)
            {
                common_blocked_queue_push_ex(&recovery_thread_ctx.queue,
                        ds, &notify);
                sleep(1);
                break;
            }

        case FS_SERVER_STATUS_OFFLINE:
            fc_thread_pool_run(&recovery_thread_ctx.tpool,
                    recovery_thread_run_task, ds);
            if (status == FS_SERVER_STATUS_INIT) {
                fc_sleep_ms(500);
            }
            break;
        default:
            break;
    }
}

static void *recovery_thread_entrance(void *arg)
{
    FSClusterDataServerInfo *ds;

    while (SF_G_CONTINUE_FLAG) {
        ds = (FSClusterDataServerInfo *)common_blocked_queue_pop(
                &recovery_thread_ctx.queue);
        if (ds != NULL) {
            recovery_thread_deal(ds);
        }
    }

    return NULL;
}

int recovery_thread_init()
{
    const int alloc_elements_once = 256;
    const int limit = DATA_RECOVERY_THREADS_LIMIT;
    const int max_idle_time = 60;
    const int min_idle_count = 0;
    int result;

    if ((result=common_blocked_queue_init_ex(
                    &recovery_thread_ctx.queue,
                    alloc_elements_once)) != 0)
    {
        return result;
    }

    if ((result=fc_thread_pool_init(&recovery_thread_ctx.tpool,
                    "data recovery", limit, SF_G_THREAD_STACK_SIZE,
                    max_idle_time, min_idle_count, (bool *)
                    &SF_G_CONTINUE_FLAG)) != 0)
    {
        return result;
    }

    return fc_create_thread(&recovery_thread_ctx.tid,
            recovery_thread_entrance, NULL, SF_G_THREAD_STACK_SIZE);
}

void recovery_thread_destroy()
{
    common_blocked_queue_destroy(&recovery_thread_ctx.queue);
    fc_thread_pool_destroy(&recovery_thread_ctx.tpool);
}

int recovery_thread_push_to_queue(FSClusterDataServerInfo *ds)
{
    return common_blocked_queue_push(&recovery_thread_ctx.queue, ds);
}
