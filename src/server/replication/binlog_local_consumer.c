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
#include "binlog_local_consumer.h"

static FSSlaveReplicationArray slave_replication_array;

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

static int init_binlog_local_consumer_array()
{
    int result;
    int repl_server_count;
    int bytes;
    int offset;
    int i;
    FSClusterServerInfo *cs;
    FSClusterServerInfo *end;
    FSSlaveReplication *replication;

    repl_server_count = CLUSTER_SERVER_ARRAY.count - 1;
    if (repl_server_count == 0) {
        slave_replication_array.count = repl_server_count;
        return 0;
    }

    bytes = sizeof(FSSlaveReplication) * (repl_server_count *
            REPLICA_CHANNELS_BETWEEN_TWO_SERVERS);
    slave_replication_array.replications = (FSSlaveReplication *)
        malloc(bytes);
    if (slave_replication_array.replications == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(slave_replication_array.replications, 0, bytes);

    replication = slave_replication_array.replications;
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
            replication->connection_info.conn.sock = -1;
            if ((result=fc_queue_init(&replication->context.queue,
                            (long)(&((ServerBinlogRecordBuffer *)NULL)->nexts) +
                            sizeof(void *) * replication->peer->link_index)) != 0)
            {
                return result;
            }

            logInfo("file: "__FILE__", line: %d, "
                    "replication: %d, thread_index: %d",
                    __LINE__, (int)(replication - slave_replication_array.replications),
                    replication->thread_index);

            replication++;
        }
        ++cs;
    }

    slave_replication_array.count = replication -
        slave_replication_array.replications;
    return 0;
}

int binlog_local_consumer_init()
{
    int result;

    set_server_link_index_for_replication();
    if ((result=init_binlog_local_consumer_array()) != 0) {
        return result;
    }

    return 0;
}

int binlog_local_consumer_start()
{
    int result;
    FSSlaveReplication *replication;
    FSSlaveReplication *end;

    end = slave_replication_array.replications + slave_replication_array.count;
    for (replication=slave_replication_array.replications; replication<end;
            replication++)
    {
        if (CLUSTER_MYSELF_PTR->server->id < replication->peer->server->id) {
            if ((result=binlog_replication_bind_thread(replication)) != 0) {
                return result;
            }
        }
    }

    return 0;
}

void binlog_local_consumer_destroy()
{
    FSSlaveReplication *replication;
    FSSlaveReplication *end;
    if (slave_replication_array.replications == NULL) {
        return;
    }

    end = slave_replication_array.replications + slave_replication_array.count;
    for (replication=slave_replication_array.replications; replication<end;
            replication++)
    {
        fc_queue_destroy(&replication->context.queue);
    }
    free(slave_replication_array.replications);
    slave_replication_array.replications = NULL;
}

void binlog_local_consumer_terminate()
{
    FSSlaveReplication *replication;
    FSSlaveReplication *end;

    end = slave_replication_array.replications + slave_replication_array.count;
    for (replication=slave_replication_array.replications; replication<end;
            replication++) {
        iovent_notify_thread(replication->task->thread_data);
    }
}

static void push_to_slave_replica_queues(FSSlaveReplication *replication,
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
    FSSlaveReplication *replication;
    FSSlaveReplication *end;
    //int result;

    if (slave_replication_array.count == 0) {
        return 0;
    }

    __sync_add_and_fetch(&rbuffer->reffer_count,
            slave_replication_array.count + 1);

    __sync_add_and_fetch(&((FSServerTaskArg *)rbuffer->task->arg)->context.
            service.waiting_rpc_count, slave_replication_array.count);

    end = slave_replication_array.replications + slave_replication_array.count;
    for (replication=slave_replication_array.replications; replication<end;
            replication++) {
        push_to_slave_replica_queues(replication, rbuffer);
    }

    return 0;
}
