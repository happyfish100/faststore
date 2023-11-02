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

#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/ioevent_loop.h"
#include "sf/sf_global.h"
#include "../../common/fs_proto.h"
#include "../server_global.h"
#include "../server_group_info.h"
#include "replication_processor.h"
#include "replication_common.h"

typedef struct {
    FSReplicationArray repl_array;
    pthread_mutex_t lock;
} ReplicationCommonContext;

static ReplicationCommonContext repl_ctx;

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

            logDebug("%d. server id: %d, link_index: %d",
                    (int)((cs - CLUSTER_SERVER_ARRAY.servers) + 1),
                    cs->server->id, cs->link_index);
        }
    }
}

static int init_replication_context(FSReplication *replication)
{
    int result;
    int bytes;
    int alloc_size;

    if ((result=fc_queue_init(&replication->context.caller.rpc_queue,
                    (long)(&((ReplicationRPCEntry *)NULL)->nexts) +
                    sizeof(void *) * replication->peer->link_index)) != 0)
    {
        return result;
    }

    alloc_size = 4 * g_sf_global_vars.max_pkg_size /
        FS_REPLICA_BINLOG_MAX_RECORD_SIZE;
    bytes = sizeof(FSReplicaRPCResultEntry) * alloc_size;
    if ((replication->context.caller.rpc_result_array.results=
                fc_malloc(bytes)) == NULL)
    {
        return ENOMEM;
    }
    replication->context.caller.rpc_result_array.alloc = alloc_size;

    bytes = sizeof(FSProtoReplicaRPCReqBodyPart) * (IOV_MAX / 2);
    if ((replication->rpc.body_parts=fc_malloc(bytes)) == NULL) {
        return ENOMEM;
    }

    bytes = sizeof(struct iovec) * IOV_MAX;
    if ((replication->rpc.io_vecs=fc_malloc(bytes)) == NULL) {
        return ENOMEM;
    }

    logDebug("file: "__FILE__", line: %d, "
            "replication: %d, thread_index: %d", __LINE__,
            (int)(replication - repl_ctx.repl_array.replications),
            replication->thread_index);

    return 0;
}

static int init_replication_common_array()
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
        fc_malloc(bytes);
    if (repl_ctx.repl_array.replications == NULL) {
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
        cs->repl_ptr_array.replications = (FSReplication **)fc_malloc(bytes);
        if (cs->repl_ptr_array.replications == NULL) {
            return ENOMEM;
        }

        logDebug("file: "__FILE__", line: %d, "
                "server pair ids: [%d, %d], offset: %d, link_index: %d",
                __LINE__, FC_MIN(CLUSTER_MYSELF_PTR->server->id,
                cs->server->id), FC_MAX(CLUSTER_MYSELF_PTR->server->id,
                cs->server->id), offset, cs->link_index);

        for (i=0; i<REPLICA_CHANNELS_BETWEEN_TWO_SERVERS; i++) {
            replication->peer = cs;
            replication->thread_index = offset + i;
            if ((result=init_replication_context(replication)) != 0) {
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

int replication_common_init()
{
    int result;

    set_server_link_index_for_replication();
    if ((result=init_replication_common_array()) != 0) {
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

int replication_common_start()
{
    int result;
    FSReplication *replication;
    FSReplication *end;

    result = 0;
    PTHREAD_MUTEX_LOCK(&repl_ctx.lock);
    end = repl_ctx.repl_array.replications + repl_ctx.repl_array.count;
    for (replication=repl_ctx.repl_array.replications; replication<end;
            replication++)
    {
        if ((result=replication_processor_bind_thread(replication)) != 0) {
            break;
        }
    }
    PTHREAD_MUTEX_UNLOCK(&repl_ctx.lock);

    return result;
}

void replication_common_destroy()
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
        fc_queue_destroy(&replication->context.caller.rpc_queue);
    }
    free(repl_ctx.repl_array.replications);
    repl_ctx.repl_array.replications = NULL;
}

void replication_common_terminate()
{
    FSReplication *replication;
    FSReplication *end;

    end = repl_ctx.repl_array.replications + repl_ctx.repl_array.count;
    for (replication=repl_ctx.repl_array.replications; replication<end;
            replication++) {
        if (replication->task != NULL) {
            ioevent_notify_thread(replication->task->thread_data);
        }
    }
}

int fs_get_replication_count()
{
    return repl_ctx.repl_array.count;
}
