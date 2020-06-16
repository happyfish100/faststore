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
#include <pthread.h>
#include "fastcommon/logger.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "common/fs_proto.h"
#include "server_global.h"
#include "cluster_topology.h"

int cluster_topology_init_notify_ctx(FSClusterTopologyNotifyContext *notify_ctx)
{
    int result;
    int count;
    int bytes;

    if ((result=init_pthread_lock(&notify_ctx->lock)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "init_pthread_lock fail, errno: %d, error info: %s",
                __LINE__, result, STRERROR(result));
        return result;
    }

    if ((result=fc_queue_init(&notify_ctx->queue, (long)
                    (&((FSDataServerChangeEvent *)NULL)->next))) != 0)
    {
        return result;
    }

    count = CLUSTER_DATA_RGOUP_ARRAY.count * CLUSTER_SERVER_ARRAY.count;
    bytes = sizeof(FSDataServerChangeEvent ) * count;
    notify_ctx->events = (FSDataServerChangeEvent *)malloc(bytes);
    if (notify_ctx->events == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(notify_ctx->events, 0, bytes);

    return 0;
}

int cluster_topology_data_server_chg_notify(const int data_group_id,
        const int server_index)
{
    return 0;
}
