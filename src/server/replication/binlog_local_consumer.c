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
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/ioevent_loop.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "../server_group_info.h"
#include "binlog_replication.h"
#include "push_result_ring.h"
#include "binlog_local_consumer.h"

typedef struct {
    FSReplicationArray repl_array;
    pthread_mutex_t lock;
} BinlogReplicationContext;

static BinlogReplicationContext repl_ctx;

static void set_server_link_index_for_replication()
{
    FSClusterServerInfo *cs;
    FSClusterServerInfo *end;
    int link_index;

    link_index = 0;
    end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (cs = CLUSTER_SERVER_ARRAY.servers; cs<end; cs++) {
        if (cs != CLUSTER_MYSELF_PTR) {
            cs->link_index = link_index++;

            logInfo("%d. server id: %d, link_index: %d",
                    (int)((cs - CLUSTER_SERVER_ARRAY.servers) + 1),
                    cs->server->id, cs->link_index);
        }
    }
}

static int init_binlog_replication(FSReplication *replication)
{
    int result;
    int alloc_size;

    replication->connection_info.conn.sock = -1;
    if ((result=fc_queue_init(&replication->context.queue,
                    (long)(&((ServerBinlogRecordBuffer *)NULL)->nexts) +
                    sizeof(void *) * replication->peer->link_index)) != 0)
    {
        return result;
    }

    alloc_size = 4 * g_sf_global_vars.min_buff_size /
        FS_REPLICA_BINLOG_MAX_RECORD_SIZE;
    if ((result=push_result_ring_check_init(&replication->
                    context.push_result_ctx, alloc_size)) != 0)
    {
        return result;
    }

    logInfo("file: "__FILE__", line: %d, "
            "replication: %d, thread_index: %d",
            __LINE__, (int)(replication - repl_ctx.repl_array.replications),
            replication->thread_index);

    return 0;
}

static int init_binlog_local_consumer_array()
{
    int result;
    int repl_server_count;
    int bytes;
    int offset;
    int i;
    FSClusterServerInfo *cs;
    FSClusterServerInfo *end;
    FSReplication *replication;

    repl_server_count = CLUSTER_SERVER_ARRAY.count - 1;
    if (repl_server_count == 0) {
        repl_ctx.repl_array.count = repl_server_count;
        return 0;
    }

    bytes = sizeof(FSReplication) * (repl_server_count *
            REPLICA_CHANNELS_BETWEEN_TWO_SERVERS);
    repl_ctx.repl_array.replications = (FSReplication *)
        malloc(bytes);
    if (repl_ctx.repl_array.replications == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(repl_ctx.repl_array.replications, 0, bytes);

    replication = repl_ctx.repl_array.replications;
    cs = CLUSTER_SERVER_ARRAY.servers;
    end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    while (cs < end) {
        if (cs == CLUSTER_MYSELF_PTR) {
            ++cs;   //skip myself
            continue;
        }

        offset = fs_get_server_pair_base_offset(CLUSTER_MYSELF_PTR->
                server->id, cs->server->id);
        if (offset < 0) {
            logError("file: "__FILE__", line: %d, "
                    "invalid server pair! server ids: %d and %d",
                    __LINE__, CLUSTER_MYSELF_PTR->server->id,
                    cs->server->id);
            return ENOENT;
        }

        logInfo("file: "__FILE__", line: %d, "
                "server pair ids: [%d, %d], offset: %d, link_index: %d",
                __LINE__, CLUSTER_MYSELF_PTR->server->id,
                cs->server->id, offset, cs->link_index);

        for (i=0; i<REPLICA_CHANNELS_BETWEEN_TWO_SERVERS; i++) {
            replication->peer = cs;
            replication->thread_index = offset + i;
            if ((result=init_binlog_replication(replication)) != 0) {
                return result;
            }

            replication++;
        }
        ++cs;
    }

    repl_ctx.repl_array.count = replication -
        repl_ctx.repl_array.replications;
    return 0;
}

int binlog_local_consumer_init()
{
    int result;

    set_server_link_index_for_replication();
    if ((result=init_binlog_local_consumer_array()) != 0) {
        return result;
    }

    if ((result=init_pthread_lock(&repl_ctx.lock)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "init_pthread_lock fail, errno: %d, error info: %s",
                __LINE__, result, STRERROR(result));
        return result;
    }

    return 0;
}

int binlog_local_consumer_start()
{
    int result;
    FSReplication *replication;
    FSReplication *end;

    end = repl_ctx.repl_array.replications + repl_ctx.repl_array.count;
    for (replication=repl_ctx.repl_array.replications; replication<end;
            replication++)
    {
        if (CLUSTER_MYSELF_PTR->server->id < replication->peer->server->id) {
            replication->is_client = true;
            if ((result=binlog_replication_bind_thread(replication)) != 0) {
                return result;
            }
        }
    }

    return 0;
}

void binlog_local_consumer_destroy()
{
    FSReplication *replication;
    FSReplication *end;
    if (repl_ctx.repl_array.replications == NULL) {
        return;
    }

    end = repl_ctx.repl_array.replications + repl_ctx.repl_array.count;
    for (replication=repl_ctx.repl_array.replications; replication<end;
            replication++)
    {
        fc_queue_destroy(&replication->context.queue);
    }
    free(repl_ctx.repl_array.replications);
    repl_ctx.repl_array.replications = NULL;
}

void binlog_local_consumer_terminate()
{
    FSReplication *replication;
    FSReplication *end;

    end = repl_ctx.repl_array.replications + repl_ctx.repl_array.count;
    for (replication=repl_ctx.repl_array.replications; replication<end;
            replication++) {
        iovent_notify_thread(replication->task->thread_data);
    }
}

static inline void push_to_slave_replica_queues(FSReplication *replication,
        ServerBinlogRecordBuffer *rbuffer)
{
    bool notify;

    fc_queue_push_ex(&replication->context.queue, rbuffer, &notify);
    if (notify) {
        iovent_notify_thread(replication->task->thread_data);
    }
}

int binlog_local_consumer_push_to_queues(const int data_group_index,
        ServerBinlogRecordBuffer *rbuffer)
{
    FSReplication *replication;
    FSReplication *end;
    //int result;

    if (repl_ctx.repl_array.count == 0) {
        return 0;
    }

    __sync_add_and_fetch(&rbuffer->reffer_count,
            repl_ctx.repl_array.count + 1);

    __sync_add_and_fetch(&((FSServerTaskArg *)rbuffer->task->arg)->context.
            service.waiting_rpc_count, repl_ctx.repl_array.count);

    end = repl_ctx.repl_array.replications + repl_ctx.repl_array.count;
    for (replication=repl_ctx.repl_array.replications; replication<end;
            replication++) {
        push_to_slave_replica_queues(replication, rbuffer);
    }

    return 0;
}

int fs_get_replication_count()
{
    return repl_ctx.repl_array.count;
}

FSReplication *fs_get_idle_replication_by_peer(const int peer_id)
{
    FSReplication *replication;
    FSReplication *end;

    PTHREAD_MUTEX_LOCK(&repl_ctx.lock);
    end = repl_ctx.repl_array.replications + repl_ctx.repl_array.count;
    for (replication=repl_ctx.repl_array.replications; replication<end;
            replication++)
    {
        if (peer_id == replication->peer->server->id &&
                replication->stage == FS_REPLICATION_STAGE_NONE)
        {
            replication->stage = FS_REPLICATION_STAGE_INITED;
            break;
        }
    }
    PTHREAD_MUTEX_UNLOCK(&repl_ctx.lock);

    return (replication < end) ? replication : NULL;
}
