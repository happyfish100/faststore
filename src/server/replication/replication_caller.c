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
#include "rpc_result_ring.h"
#include "replication_caller.h"

typedef struct {
    struct fast_mblock_man rpc_allocator;
} ReplicationMasterContext;

static ReplicationMasterContext repl_mctx;

int replication_caller_init()
{
    int result;
    int element_size;

    element_size = sizeof(ReplicationRPCEntry) +
        sizeof(ReplicationRPCEntry *) * CLUSTER_SERVER_ARRAY.count;
    if ((result=fast_mblock_init_ex2(&repl_mctx.rpc_allocator,
                    "rpc_entry", element_size, 1024, NULL, NULL,
                    true, NULL, NULL, NULL)) != 0)
    {
        return result;
    }

    return 0;
}

void replication_caller_destroy()
{
}

static inline ReplicationRPCEntry *replication_caller_alloc_rpc_entry()
{
    ReplicationRPCEntry *rpc;

    rpc = (ReplicationRPCEntry *)fast_mblock_alloc_object(
            &repl_mctx.rpc_allocator);
    if (rpc == NULL) {
        return NULL;
    }

    return rpc;
}

void replication_caller_release_rpc_entry(ReplicationRPCEntry *rpc)
{
    if (__sync_sub_and_fetch(&rpc->reffer_count, 1) == 0) {
        logInfo("file: "__FILE__", line: %d, "
                "free record buffer: %p", __LINE__, rpc);
        fast_mblock_free_object(&repl_mctx.rpc_allocator, rpc);
    }
}

static inline void push_to_slave_replica_queue(FSReplication *replication,
        ReplicationRPCEntry *rpc)
{
    bool notify;

    fc_queue_push_ex(&replication->context.caller.rpc_queue, rpc, &notify);
    if (notify) {
        iovent_notify_thread(replication->task->thread_data);
    }
}

static int push_to_slave_queues(FSClusterDataGroupInfo *group,
        const uint32_t hash_code, ReplicationRPCEntry *rpc)
{
    FSClusterDataServerInfo **ds;
    FSClusterDataServerInfo **end;
    FSReplication *replication;
    int inactive_count;

    __sync_add_and_fetch(&rpc->reffer_count,
            group->slave_ds_array.count);

    __sync_add_and_fetch(&((FSServerTaskArg *)rpc->task->arg)->context.
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

        push_to_slave_replica_queue(replication, rpc);
    }

    if (inactive_count > 0) {
        __sync_sub_and_fetch(&rpc->reffer_count, inactive_count);
    }

    if (__sync_sub_and_fetch(&((FSServerTaskArg *)rpc->task->arg)->
                context.service.waiting_rpc_count, inactive_count) == 0)
    {
        return 0;
    } else {
        return TASK_STATUS_CONTINUE;
    }
}

int replication_caller_push_to_slave_queues(struct fast_task_info *task)
{
    FSClusterDataGroupInfo *group;
    ReplicationRPCEntry *rpc;
    int result;

    if ((group=fs_get_data_group(OP_CTX_INFO.data_group_id)) == NULL) {
        return ENOENT;
    }

    if (group->slave_ds_array.count == 0) {
        return 0;
    }

    if ((rpc=replication_caller_alloc_rpc_entry()) == NULL) {
        return ENOMEM;
    }

    rpc->task = task;
    rpc->task_version = __sync_add_and_fetch(&((FSServerTaskArg *)
                task->arg)->task_version, 0);
    if ((result=push_to_slave_queues(group, OP_CTX_INFO.bs_key.block.hash_code,
                    rpc)) != TASK_STATUS_CONTINUE)
    {
        fast_mblock_free_object(&repl_mctx.rpc_allocator, rpc);
    }
    return result;
}
