#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <fcntl.h>
#include <assert.h>
#include <pthread.h>
#include "fastcommon/logger.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/ioevent_loop.h"
#include "sf/sf_global.h"
#include "common/fs_proto.h"
#include "server_global.h"
#include "cluster_topology.h"

static FSClusterDataServerInfo *find_data_group_server(
        const int gindex, FSClusterServerInfo *cs)
{
    FSClusterDataServerArray *ds_array;
    FSClusterDataServerInfo *ds;
    FSClusterDataServerInfo *end;

    ds_array = &CLUSTER_DATA_RGOUP_ARRAY.groups[gindex].data_server_array;
    end = ds_array->servers + ds_array->count;
    for (ds=ds_array->servers; ds<end; ds++) {
        if (ds->cs == cs) {
            logInfo("data group index: %d, server id: %d", gindex, cs->server->id);
            return ds;
        }
    }

    return NULL;
}

int cluster_topology_init_notify_ctx(FSClusterTopologyNotifyContext *notify_ctx)
{
    int result;
    int count;
    int bytes;
    int index;
    int i;
    FSClusterServerInfo *cs;
    FSClusterServerInfo *end;

    /*
    if ((result=init_pthread_lock(&notify_ctx->lock)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "init_pthread_lock fail, errno: %d, error info: %s",
                __LINE__, result, STRERROR(result));
        return result;
    }
    */

    if ((result=fc_queue_init(&notify_ctx->queue, (long)
                    (&((FSDataServerChangeEvent *)NULL)->next))) != 0)
    {
        return result;
    }

    count = CLUSTER_DATA_RGOUP_ARRAY.count * CLUSTER_SERVER_ARRAY.count;
    bytes = sizeof(FSDataServerChangeEvent) * count;
    notify_ctx->events = (FSDataServerChangeEvent *)malloc(bytes);
    if (notify_ctx->events == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(notify_ctx->events, 0, bytes);

    logInfo("data group count: %d, server count: %d\n",
            CLUSTER_DATA_RGOUP_ARRAY.count, CLUSTER_SERVER_ARRAY.count);

    end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (i=0; i<CLUSTER_DATA_RGOUP_ARRAY.count; i++) {
        for (cs=CLUSTER_SERVER_ARRAY.servers; cs<end; cs++) {
            index = i * CLUSTER_SERVER_ARRAY.count + cs->server_index;
            notify_ctx->events[index].data_server =
                find_data_group_server(i, cs);
        }
    }

    return 0;
}

int cluster_topology_data_server_chg_notify(FSClusterDataServerInfo *
        data_server, const bool notify_self)
{
    int data_group_index;
    FSClusterServerInfo *cs;
    FSClusterServerInfo *end;
    FSDataServerChangeEvent *event;
    bool notify;

    data_group_index = data_server->dg->index;
    end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (cs=CLUSTER_SERVER_ARRAY.servers; cs<end; cs++) {
        if (cs->is_leader || (!notify_self && data_server->cs == cs) ||
                (__sync_fetch_and_add(&cs->active, 0) == 0))
        {
            continue;
        }

        event = cs->notify_ctx.events + (data_group_index *
                CLUSTER_SERVER_ARRAY.count + data_server->cs->server_index);
        assert(event->data_server == data_server);

        if (__sync_bool_compare_and_swap(&event->in_queue, 0, 1)) {
            fc_queue_push_ex(&cs->notify_ctx.queue, event, &notify);
            if (notify) {
                iovent_notify_thread((struct nio_thread_data *)
                        cs->notify_ctx.thread_data);
            }
        }
    }

    return 0;
}
