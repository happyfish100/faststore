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
#include "binlog_replication.h"
#include "binlog_local_consumer.h"

static FSSlaveReplicationArray slave_replication_array;

static int init_binlog_local_consumer_array()
{
    static bool inited = false;
    int result;
    int count;
    int bytes;
    FSClusterServerInfo *server;
    FSSlaveReplication *replication;
    FSSlaveReplication *end;

    if (inited) {
        return 0;
    }

    count = CLUSTER_SERVER_ARRAY.count - 1;
    if (count == 0) {
        slave_replication_array.count = count;
        return 0;
    }

    bytes = sizeof(FSSlaveReplication) * count;
    slave_replication_array.replications = (FSSlaveReplication *)
        malloc(bytes);
    if (slave_replication_array.replications == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(slave_replication_array.replications, 0, bytes);

    server = CLUSTER_SERVER_ARRAY.servers;
    end = slave_replication_array.replications + count;
    for (replication=slave_replication_array.replications; replication<end;
            replication++)
    {
        if (server == CLUSTER_MYSELF_PTR) {
            ++server;   //skip myself
        }

        //TODO
        replication->peer = server++;
        replication->thread_index = replication - slave_replication_array.replications;
        replication->peer->link_index = replication - slave_replication_array.replications;
        replication->connection_info.conn.sock = -1;
        if ((result=init_pthread_lock(&replication->context.queue.lock)) != 0) {
            logError("file: "__FILE__", line: %d, "
                    "init_pthread_lock fail, errno: %d, error info: %s",
                    __LINE__, result, STRERROR(result));
            return result;
        }
    }

    slave_replication_array.count = count;
    inited = true;
    return 0;
}

int binlog_local_consumer_init()
{
    return 0;
}

int binlog_local_consumer_replication_start()
{
    int result;
    FSSlaveReplication *replication;
    FSSlaveReplication *end;

    if ((result=init_binlog_local_consumer_array()) != 0) {
        return result;
    }

    end = slave_replication_array.replications + slave_replication_array.count;
    for (replication=slave_replication_array.replications; replication<end;
            replication++)
    {
        if ((result=binlog_replication_bind_thread(replication)) != 0) {
            return result;
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
        pthread_mutex_destroy(&replication->context.queue.lock);
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

    rbuffer->nexts[replication->peer->link_index] = NULL;
    PTHREAD_MUTEX_LOCK(&replication->context.queue.lock);
    if (replication->context.queue.tail == NULL) {
        replication->context.queue.head = rbuffer;
        notify = true;
    } else {
        //TODO
        //replication->context.queue.tail->nexts[replication->peer->link_index] = rbuffer;
        notify = false;
    }

    replication->context.queue.tail = rbuffer;
    PTHREAD_MUTEX_UNLOCK(&replication->context.queue.lock);

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
