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

#define RECOVERY_THREADS_LIMIT  2

typedef struct {
    pthread_t tid;
    struct common_blocked_queue queue;
    FCThreadPool tpool;
} RecoveryThreadContext;

static RecoveryThreadContext recovery_thread_ctx;

static void recovery_thread_run_task(void *arg)
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

    if (!cluster_relationship_set_my_status(ds,
                old_status, new_status, true))
    {
        logInfo("file: "__FILE__", line: %d, "
                "data group id: %d, change my status to %d (%s) fail, "
                "skip data recovery", __LINE__, ds->dg->id, new_status,
                fs_get_server_status_caption(new_status));
        return;
    }

    if ((result=data_recovery_start(ds->dg->id)) != 0) {
        cluster_relationship_set_my_status(ds, new_status, old_status, true);
    }

    sleep(5); //TODO
    logInfo("====file: "__FILE__", line: %d, func: %s, "
            "do recovery, data group: %d, result: %d, status: %d =====", __LINE__,
            __FUNCTION__, ds->dg->id, result, ds->status);
}

static void recovery_thread_deal(FSClusterDataServerInfo *ds)
{
    int status;
    bool notify;

    if (ds->cs != CLUSTER_MYSELF_PTR) {
        logInfo("file: "__FILE__", line: %d, "
                "i NOT belong to data group id: %d",
                __LINE__, ds->dg->id);
        return;
    }

    status = __sync_fetch_and_add(&ds->status, 0);
    if (status == FS_SERVER_STATUS_REBUILDING ||
            status == FS_SERVER_STATUS_RECOVERING)
    {
        return;
    }

    switch (status) {
        case FS_SERVER_STATUS_ONLINE:
        case FS_SERVER_STATUS_ACTIVE:
            return;
        case FS_SERVER_STATUS_INIT:
            if (fc_thread_pool_avail_count(&recovery_thread_ctx.tpool) <
                    RECOVERY_THREADS_LIMIT)
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
                usleep(500000);
            }
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
    const int limit = RECOVERY_THREADS_LIMIT;
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
                    limit, SF_G_THREAD_STACK_SIZE, max_idle_time,
                    min_idle_count, (bool *)&SF_G_CONTINUE_FLAG)) != 0)
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
