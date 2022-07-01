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
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/ioevent_loop.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "../server_group_info.h"
#include "../cluster_relationship.h"
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
    if ((result=fast_mblock_init_ex1(&repl_mctx.rpc_allocator,
                    "rpc_entry", element_size, 1024, 0, NULL,
                    NULL, true)) != 0)
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
        /*
        logInfo("file: "__FILE__", line: %d, "
                "free record buffer: %p", __LINE__, rpc);
                */
        fast_mblock_free_object(&repl_mctx.rpc_allocator, rpc);
    }
}

static inline void push_to_slave_replica_queue(FSReplication *replication,
        ReplicationRPCEntry *rpc)
{
    bool notify;

    fc_queue_push_ex(&replication->context.caller.rpc_queue, rpc, &notify);
    if (notify) {
        ioevent_notify_thread(replication->task->thread_data);
    }
}

static int push_to_slave_queues(FSClusterDataGroupInfo *group,
        const uint32_t hash_code, ReplicationRPCEntry *rpc,
        FSDataOperation *op)
{
    FSClusterDataServerInfo **ds;
    FSClusterDataServerInfo **end;
    FSReplication *replication;
    int status;
    int inactive_count;

    __sync_add_and_fetch(&rpc->reffer_count,
            group->slave_ds_array.count);

    __sync_add_and_fetch(&((FSServerTaskArg *)rpc->task->arg)->context.
            service.rpc.waiting_count, group->slave_ds_array.count);

    inactive_count = 0;
    end = group->slave_ds_array.servers + group->slave_ds_array.count;
    for (ds=group->slave_ds_array.servers; ds<end; ds++) {
        status = __sync_fetch_and_add(&(*ds)->status, 0);
        if (status == FS_DS_STATUS_ONLINE) {
            log_data_update(op);  //log before RPC for slave fetching binlog
        } else if (status != FS_DS_STATUS_ACTIVE) {
            inactive_count++;
            continue;
        }

        replication = (*ds)->cs->repl_ptr_array.replications[hash_code %
            (*ds)->cs->repl_ptr_array.count];
        if (!replication_channel_is_ready(replication)) {
            int64_t data_version;

            if (status == FS_DS_STATUS_ACTIVE) {
                cluster_relationship_swap_report_ds_status(*ds,
                        FS_DS_STATUS_ACTIVE, FS_DS_STATUS_OFFLINE,
                        FS_EVENT_SOURCE_MASTER_REPORT);
            }
            data_version = ((FSServerTaskArg *)rpc->task->arg)->
                context.slice_op_ctx.info.data_version;
            logWarning("file: "__FILE__", line: %d, "
                    "the replica connection for peer id %d %s:%u "
                    "NOT established, skip the RPC call: %"PRId64, __LINE__,
                    (*ds)->cs->server->id, REPLICA_GROUP_ADDRESS_FIRST_IP(
                        (*ds)->cs->server), REPLICA_GROUP_ADDRESS_FIRST_PORT(
                            (*ds)->cs->server), data_version);

            inactive_count++;
            continue;
        }

        push_to_slave_replica_queue(replication, rpc);
    }

    if (inactive_count > 0) {
        int result;
        if (__sync_sub_and_fetch(&((FSServerTaskArg *)rpc->task->arg)->
                    context.service.rpc.waiting_count, inactive_count) == 0)
        {
            result = 0;  //rpc finished
        } else {
            result = TASK_STATUS_CONTINUE;
        }

        if (__sync_sub_and_fetch(&rpc->reffer_count, inactive_count) == 0) {
            fast_mblock_free_object(&repl_mctx.rpc_allocator, rpc);
        }

        return result;
    } else {
        return TASK_STATUS_CONTINUE;
    }
}

int replication_caller_push_to_slave_queues(FSDataOperation *op)
{
    FSClusterDataGroupInfo *group;
    ReplicationRPCEntry *rpc;
    uint32_t hash_code;

    if ((group=fs_get_data_group(op->ctx->info.data_group_id)) == NULL) {
        return ENOENT;
    }

    if (group->slave_ds_array.count == 0) {
        return 0;
    }

    if ((rpc=replication_caller_alloc_rpc_entry()) == NULL) {
        return ENOMEM;
    }

    rpc->task = (struct fast_task_info *)op->arg;
    rpc->body_offset = op->ctx->info.body - rpc->task->data;
    rpc->body_length = op->ctx->info.body_len;
    hash_code = op->ctx->info.data_group_id;
    return push_to_slave_queues(group, hash_code, rpc, op);
}
