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
#include "replication_processor.h"
#include "push_result_ring.h"
#include "replication_producer.h"

typedef struct {
    FSReplicationArray repl_array;
    pthread_mutex_t lock;
    struct fast_mblock_man rb_allocator;
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

static int init_replication_processor(FSReplication *replication)
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

static int init_replication_producer_array()
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

        bytes = sizeof(FSReplication *) * REPLICA_CHANNELS_BETWEEN_TWO_SERVERS;
        cs->repl_ptr_array.replications = (FSReplication **)malloc(bytes);
        if (cs->repl_ptr_array.replications == NULL) {
            logError("file: "__FILE__", line: %d, "
                    "malloc %d bytes fail", __LINE__, bytes);
            return ENOMEM;
        }

        logInfo("file: "__FILE__", line: %d, "
                "server pair ids: [%d, %d], offset: %d, link_index: %d",
                __LINE__, FC_MIN(CLUSTER_MYSELF_PTR->server->id,
                cs->server->id), FC_MAX(CLUSTER_MYSELF_PTR->server->id,
                cs->server->id), offset, cs->link_index);

        for (i=0; i<REPLICA_CHANNELS_BETWEEN_TWO_SERVERS; i++) {
            replication->peer = cs;
            replication->thread_index = offset + i;
            if ((result=init_replication_processor(replication)) != 0) {
                return result;
            }

            cs->repl_ptr_array.replications[i] = replication;
            replication++;
        }

        cs->repl_ptr_array.count = REPLICA_CHANNELS_BETWEEN_TWO_SERVERS;
        ++cs;
    }

    repl_ctx.repl_array.count = replication - repl_ctx.repl_array.replications;
    return 0;
}

int replication_producer_init()
{
    int result;
    int element_size;

    set_server_link_index_for_replication();
    if ((result=init_replication_producer_array()) != 0) {
        return result;
    }

    element_size = sizeof(ServerBinlogRecordBuffer) +
        sizeof(struct server_binlog_record_buffer *) *
        CLUSTER_SERVER_ARRAY.count;
    if ((result=fast_mblock_init_ex2(&repl_ctx.rb_allocator,
                    "record_buffer", element_size, 1024,
                    NULL, NULL, true, NULL, NULL, NULL)) != 0)
    {
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

int replication_producer_start()
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
            if ((result=replication_processor_bind_thread(replication)) != 0) {
                return result;
            }
        }
    }

    return 0;
}

void replication_producer_destroy()
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

void replication_producer_terminate()
{
    FSReplication *replication;
    FSReplication *end;

    end = repl_ctx.repl_array.replications + repl_ctx.repl_array.count;
    for (replication=repl_ctx.repl_array.replications; replication<end;
            replication++) {
        iovent_notify_thread(replication->task->thread_data);
    }
}

static inline ServerBinlogRecordBuffer *replication_producer_alloc_rbuffer()
{
    ServerBinlogRecordBuffer *rbuffer;

    rbuffer = (ServerBinlogRecordBuffer *)fast_mblock_alloc_object(
            &repl_ctx.rb_allocator);
    if (rbuffer == NULL) {
        return NULL;
    }

    return rbuffer;
}

void replication_producer_release_rbuffer(ServerBinlogRecordBuffer *rbuffer)
{
    if (__sync_sub_and_fetch(&rbuffer->reffer_count, 1) == 0) {
        /*
        logInfo("file: "__FILE__", line: %d, "
                "free record buffer: %p", __LINE__, rbuffer);
                */
        fast_mblock_free_object(&repl_ctx.rb_allocator, rbuffer);
    }
}

static inline void push_to_slave_replica_queue(FSReplication *replication,
        ServerBinlogRecordBuffer *rbuffer)
{
    bool notify;

    fc_queue_push_ex(&replication->context.queue, rbuffer, &notify);
    if (notify) {
        iovent_notify_thread(replication->task->thread_data);
    }
}

static int push_to_slave_queues(FSClusterDataGroupInfo *group,
        const uint32_t hash_code, ServerBinlogRecordBuffer *rbuffer)
{
    FSClusterDataServerInfo **ds;
    FSClusterDataServerInfo **end;
    FSReplication *replication;
    int inactive_count;

    __sync_add_and_fetch(&rbuffer->reffer_count,
            group->slave_ds_array.count);

    __sync_add_and_fetch(&((FSServerTaskArg *)rbuffer->task->arg)->context.
            service.waiting_rpc_count, group->slave_ds_array.count);

    inactive_count = 0;
    end = group->slave_ds_array.servers + group->slave_ds_array.count;
    for (ds=group->slave_ds_array.servers; ds<end; ds++) {
        if (__sync_fetch_and_add(&(*ds)->status, 0) != FS_SERVER_STATUS_ACTIVE) {
            inactive_count++;
            continue;
        }

        replication = (*ds)->cs->repl_ptr_array.replications[hash_code %
            (*ds)->cs->repl_ptr_array.count];
        if (replication->task == NULL) {
            inactive_count++;
            continue;
        }

        push_to_slave_replica_queue(replication, rbuffer);
    }

    if (inactive_count > 0) {
        __sync_sub_and_fetch(&rbuffer->reffer_count, inactive_count);
    }

    if (__sync_sub_and_fetch(&((FSServerTaskArg *)rbuffer->task->arg)->
                context.service.waiting_rpc_count, inactive_count) == 0)
    {
        return 0;
    } else {
        return TASK_STATUS_CONTINUE;
    }
}

int replication_producer_push_to_slave_queues(struct fast_task_info *task)
{
    FSClusterDataGroupInfo *group;
    ServerBinlogRecordBuffer *rbuffer;
    int result;

    if ((group=fs_get_data_group(TASK_CTX.data_group_id)) == NULL) {
        return ENOENT;
    }

    if (group->slave_ds_array.count == 0) {
        return 0;
    }

    if ((rbuffer=replication_producer_alloc_rbuffer()) == NULL) {
        return ENOMEM;
    }

    rbuffer->task = task;
    rbuffer->task_version = __sync_add_and_fetch(&((FSServerTaskArg *)
                task->arg)->task_version, 0);
    if ((result=push_to_slave_queues(group, TASK_CTX.bs_key.block.hash_code,
                    rbuffer)) != TASK_STATUS_CONTINUE)
    {
        fast_mblock_free_object(&repl_ctx.rb_allocator, rbuffer);
    }
    return result;
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

