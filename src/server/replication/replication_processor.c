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
#include "fastcommon/sched_thread.h"
#include "fastcommon/ioevent_loop.h"
#include "sf/sf_func.h"
#include "sf/sf_nio.h"
#include "sf/sf_service.h"
#include "../../common/fs_proto.h"
#include "../server_global.h"
#include "../server_group_info.h"
#include "../data_thread.h"
#include "../binlog/binlog_reader.h"
#include "replication_common.h"
#include "replication_caller.h"
#include "replication_callee.h"
#include "replication_processor.h"

static void replication_queue_discard_all(FSReplication *replication);

static int alloc_replication_ptr_array(FSReplicationPtrArray *array)
{
    int bytes;

    bytes = sizeof(FSReplication *) * fs_get_replication_count();
    array->replications = (FSReplication **)fc_malloc(bytes);
    if (array->replications == NULL) {
        return ENOMEM;
    }
    memset(array->replications, 0, bytes);
    return 0;
}

int replication_alloc_connection_ptr_arrays(FSServerContext *server_context)
{
    int result;
    if ((result=alloc_replication_ptr_array(&server_context->
                    replica.connectings)) != 0)
    {
        return result;
    }
    if ((result=alloc_replication_ptr_array(&server_context->
                    replica.connected)) != 0)
    {
        return result;
    }

    return 0;
}

static void add_to_replication_ptr_array(FSReplicationPtrArray *
        array, FSReplication *replication)
{
    array->replications[array->count++] = replication;
}

static int remove_from_replication_ptr_array(FSReplicationPtrArray *
        array, FSReplication *replication)
{
    int i;

    for (i=0; i<array->count; i++) {
        if (array->replications[i] == replication) {
            break;
        }
    }

    if (i == array->count) {
        logError("file: "__FILE__", line: %d, "
                "can't found replication server id: %d",
                __LINE__, replication->peer->server->id);
        return ENOENT;
    }

    for (i=i+1; i<array->count; i++) {
        array->replications[i - 1] = array->replications[i];
    }
    array->count--;
    return 0;
}

#define REPLICATION_BIND_TASK(replication, task) \
    do { \
        replication->task = task;  \
        SERVER_TASK_TYPE = FS_SERVER_TASK_TYPE_REPLICATION; \
        REPLICA_REPLICATION = replication;  \
    } while (0)

void replication_processor_bind_task(FSReplication *replication,
        struct fast_task_info *task)
{
    FSServerContext *server_ctx;

    set_replication_stage(replication, FS_REPLICATION_STAGE_SYNCING);
    REPLICATION_BIND_TASK(replication, task);

    server_ctx = (FSServerContext *)task->thread_data->arg;
    add_to_replication_ptr_array(&server_ctx->
            replica.connected, replication);
}

int replication_processor_bind_thread(FSReplication *replication)
{
    struct fast_task_info *task;
    FSServerContext *server_ctx;

    if ((task=sf_alloc_init_task(&REPLICA_SF_CTX, -1)) == NULL) {
        return ENOMEM;
    }

    task->thread_data = REPLICA_SF_CTX.thread_data +
        replication->thread_index % REPLICA_SF_CTX.work_threads;

    set_replication_stage(replication, FS_REPLICATION_STAGE_INITED);
    REPLICATION_BIND_TASK(replication, task);

    server_ctx = (FSServerContext *)task->thread_data->arg;
    replication->connection_info.conn.sock = -1;
    add_to_replication_ptr_array(&server_ctx->
            replica.connectings, replication);
    return 0;
}

int replication_processor_unbind(FSReplication *replication)
{
    int result;
    FSServerContext *server_ctx;

    server_ctx = (FSServerContext *)replication->task->thread_data->arg;
    if ((result=remove_from_replication_ptr_array(&server_ctx->
                replica.connected, replication)) == 0)
    {
        replication_queue_discard_all(replication);
        rpc_result_ring_clear_all(&replication->
                context.caller.rpc_result_ctx);
        if (replication->is_client) {
            result = replication_processor_bind_thread(replication);
        } else {
            set_replication_stage(replication, FS_REPLICATION_STAGE_NONE);
            fs_server_release_replication(replication);
        }
    }

    return result;
}

static void calc_next_connect_time(FSReplication *replication)
{
    int interval;

    switch (replication->connection_info.fail_count) {
        case 0:
            interval = 1;
            break;
        case 1:
            interval = 2;
            break;
        case 2:
            interval = 4;
            break;
        case 3:
            interval = 8;
            break;
        case 4:
            interval = 16;
            break;
        default:
            interval = 32;
            break;
    }

    replication->connection_info.next_connect_time = g_current_time + interval;
}

static int check_and_make_replica_connection(FSReplication *replication)
{
    int result;
    int polled;
    socklen_t len;
    struct pollfd pollfds;

    if (replication->connection_info.conn.sock < 0) {
        FCAddressPtrArray *addr_array;
        FCAddressInfo *addr;

        if (replication->connection_info.next_connect_time > g_current_time) {
            return EAGAIN;
        }

        addr_array = &REPLICA_GROUP_ADDRESS_ARRAY(replication->peer->server);
        addr = addr_array->addrs[replication->conn_index++];
        if (replication->conn_index >= addr_array->count) {
            replication->conn_index = 0;
        }

        replication->connection_info.start_time = g_current_time;
        replication->connection_info.conn = addr->conn;
        calc_next_connect_time(replication);
        if ((result=conn_pool_async_connect_server(&replication->
                        connection_info.conn)) == 0)
        {
            return 0;
        }
        if (result != EINPROGRESS) {
            return result;
        }
    }

    pollfds.fd = replication->connection_info.conn.sock;
    pollfds.events = POLLIN | POLLOUT;
    polled = poll(&pollfds, 1, 0);
    if (polled == 0) {  //timeout
        if (g_current_time - replication->connection_info.start_time >
                SF_G_CONNECT_TIMEOUT)
        {
            result = ETIMEDOUT;
        } else {
            result = EINPROGRESS;
        }
    } else if (polled < 0) {   //error
        result = errno != 0 ? errno : EIO;
    } else {
        len = sizeof(result);
        if (getsockopt(replication->connection_info.conn.sock, SOL_SOCKET,
                    SO_ERROR, &result, &len) < 0)
        {
            result = errno != 0 ? errno : EACCES;
        }
    }

    /*
    logInfo("file: "__FILE__", line: %d, func: %s, "
            "replication: %p, peer id: %d, sock: %d, result: %d",
            __LINE__, __FUNCTION__, replication, replication->peer->server->id,
            replication->connection_info.conn.sock, result);
            */

    if (!(result == 0 || result == EINPROGRESS)) {
        close(replication->connection_info.conn.sock);
        replication->connection_info.conn.sock = -1;
    }
    return result;
}

static int send_join_server_package(FSReplication *replication)
{
	int result;
	FSProtoHeader *header;
    FSProtoJoinServerReq *req;
	char out_buff[sizeof(FSProtoHeader) + sizeof(FSProtoJoinServerReq)];

    header = (FSProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, FS_REPLICA_PROTO_JOIN_SERVER_REQ,
            sizeof(out_buff) - sizeof(FSProtoHeader));

    req = (FSProtoJoinServerReq *)(out_buff + sizeof(FSProtoHeader));
    int2buff(CLUSTER_MY_SERVER_ID, req->server_id);
    int2buff(replication->task->size, req->buffer_size);
    int2buff(REPLICA_CHANNELS_BETWEEN_TWO_SERVERS,
            req->replica_channels_between_two_servers);
    memcpy(req->config_signs.servers, SERVERS_CONFIG_SIGN_BUF,
            SERVERS_CONFIG_SIGN_LEN);
    memcpy(req->config_signs.cluster, CLUSTER_CONFIG_SIGN_BUF,
            CLUSTER_CONFIG_SIGN_LEN);
    if ((result=tcpsenddata_nb(replication->connection_info.conn.sock,
                    out_buff, sizeof(out_buff), SF_G_NETWORK_TIMEOUT)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "send data to server %s:%u fail, "
                "errno: %d, error info: %s", __LINE__,
                replication->connection_info.conn.ip_addr,
                replication->connection_info.conn.port,
                result, STRERROR(result));
        close(replication->connection_info.conn.sock);
        replication->connection_info.conn.sock = -1;
    } else {
        replication->last_net_comm_time = g_current_time;
    }

    return result;
}

static void decrease_task_waiting_rpc_count(ReplicationRPCEntry *rb)
{
    FSServerTaskArg *task_arg;

    task_arg = (FSServerTaskArg *)rb->task->arg;
    if (__sync_sub_and_fetch(&task_arg->context.service.
                waiting_rpc_count, 1) == 0)
    {
        data_thread_notify((FSDataThreadContext *)
                task_arg->context.slice_op_ctx.arg);
    }
}

static void discard_queue(FSReplication *replication,
        ReplicationRPCEntry *head)
{
    ReplicationRPCEntry *rb;
    while (head != NULL) {
        rb = head;
        head = head->nexts[replication->peer->link_index];

        decrease_task_waiting_rpc_count(rb);
        replication_caller_release_rpc_entry(rb);
    }
}

static void replication_queue_discard_all(FSReplication *replication)
{
    struct fc_queue_info qinfo;

    fc_queue_pop_to_queue(&replication->context.caller.rpc_queue, &qinfo);
    if (qinfo.head != NULL) {
        discard_queue(replication, (ReplicationRPCEntry *)qinfo.head);
    }
}

static int deal_connecting_replication(FSReplication *replication)
{
    int result;

    result = check_and_make_replica_connection(replication);
    if (result == 0) {
        result = send_join_server_package(replication);
    }

    return result;
}

static int deal_replication_connectings(FSServerContext *server_ctx)
{
#define SUCCESS_ARRAY_ELEMENT_MAX  64

    int result;
    int i;
    char prompt[128];
    struct {
        int count;
        FSReplication *replications[SUCCESS_ARRAY_ELEMENT_MAX];
    } success_array;
    FSReplication *replication;

    if (server_ctx->replica.connectings.count == 0) {
        return 0;
    }

    success_array.count = 0;
    for (i=0; i<server_ctx->replica.connectings.count; i++) {
        replication = server_ctx->replica.connectings.replications[i];
        result = deal_connecting_replication(replication);
        if (result == 0) {
            if (success_array.count < SUCCESS_ARRAY_ELEMENT_MAX) {
                success_array.replications[success_array.count++] = replication;
            }
            set_replication_stage(replication,
                    FS_REPLICATION_STAGE_WAITING_JOIN_RESP);
        } else if (result == EINPROGRESS) {
            set_replication_stage(replication,
                    FS_REPLICATION_STAGE_CONNECTING);
        } else if (result != EAGAIN) {
            if (result != replication->connection_info.last_errno
                    || replication->connection_info.fail_count % 100 == 0)
            {
                replication->connection_info.last_errno = result;
                logError("file: "__FILE__", line: %d, "
                        "%dth connect to replication peer: %d, %s:%u fail, "
                        "time used: %ds, errno: %d, error info: %s",
                        __LINE__, replication->connection_info.fail_count + 1,
                        replication->peer->server->id,
                        replication->connection_info.conn.ip_addr,
                        replication->connection_info.conn.port, (int)
                        (g_current_time - replication->connection_info.start_time),
                        result, STRERROR(result));
            }
            replication->connection_info.fail_count++;
        }
    }

    for (i=0; i<success_array.count; i++) {
        replication = success_array.replications[i];

        if (replication->connection_info.fail_count > 0) {
            sprintf(prompt, " after %d retries",
                    replication->connection_info.fail_count);
        } else {
            *prompt = '\0';
        }
        logInfo("file: "__FILE__", line: %d, "
                "connect to replication peer id: %d, %s:%u successfully%s",
                __LINE__, replication->peer->server->id,
                replication->connection_info.conn.ip_addr,
                replication->connection_info.conn.port, prompt);

        if (remove_from_replication_ptr_array(&server_ctx->
                    replica.connectings, replication) == 0)
        {
            add_to_replication_ptr_array(&server_ctx->
                    replica.connected, replication);
        }

        replication->connection_info.fail_count = 0;
        replication->task->event.fd = replication->
            connection_info.conn.sock;
        snprintf(replication->task->client_ip,
                sizeof(replication->task->client_ip), "%s",
                replication->connection_info.conn.ip_addr);
        replication->task->port = replication->connection_info.conn.port;
        sf_nio_notify(replication->task, SF_NIO_STAGE_INIT);
    }
    return 0;
}

void clean_connected_replications(FSServerContext *server_ctx)
{
    FSReplication *replication;
    int i;

    for (i=0; i<server_ctx->replica.connected.count; i++) {
        replication = server_ctx->replica.connected.replications[i];
        ioevent_add_to_deleted_list(replication->task);
    }
}

static int replication_rpc_from_queue(FSReplication *replication)
{
    struct fc_queue_info qinfo;
    ReplicationRPCEntry *rb;
    ReplicationRPCEntry *deleted;
    struct fast_task_info *task;
    FSProtoReplicaRPCReqBodyHeader *body_header;
    FSProtoReplicaRPCReqBodyPart *body_part;
    uint64_t data_version;
    int data_group_id;
    int count;
    int body_len;
    int pkg_len;
    int result;

    fc_queue_pop_to_queue(&replication->context.caller.rpc_queue, &qinfo);
    if (qinfo.head == NULL) {
        return 0;
    }

    rb = (ReplicationRPCEntry *)qinfo.head;
    count = 0;
    task = replication->task;
    task->length = sizeof(FSProtoHeader) +
        sizeof(FSProtoReplicaRPCReqBodyHeader);
    do {
        body_part = (FSProtoReplicaRPCReqBodyPart *)(task->data +
                task->length);
        pkg_len = task->length + sizeof(*body_part) + rb->body_length;
        if (pkg_len > task->size) {
            bool notify;

            qinfo.head = rb;
            fc_queue_push_queue_to_head_ex(&replication->context.
                    caller.rpc_queue, &qinfo, &notify);
            break;
        }

        body_part->cmd = ((FSProtoHeader *)rb->task->data)->cmd;
        data_group_id = ((FSServerTaskArg *)rb->task->arg)->
            context.slice_op_ctx.info.data_group_id;
        data_version = ((FSServerTaskArg *)rb->task->arg)->
            context.slice_op_ctx.info.data_version;
        memcpy(body_part->body, rb->task->data +
                rb->body_offset, rb->body_length);

        ++count;
        task->length = pkg_len;
        long2buff(data_version, body_part->data_version);
        int2buff(rb->body_length, body_part->body_len);
        if ((result=rpc_result_ring_add(&replication->context.caller.
                        rpc_result_ctx, data_group_id, data_version,
                        rb->task)) != 0)
        {
            sf_terminate_myself();
            return result;
        }

        deleted = rb;
        rb = rb->nexts[replication->peer->link_index];

        replication_caller_release_rpc_entry(deleted);
    } while (rb != NULL);

    if (count == 0) {
        return 0;
    }

    body_header = (FSProtoReplicaRPCReqBodyHeader *)
        (task->data + sizeof(FSProtoHeader));
    body_len = task->length - sizeof(FSProtoHeader);
    int2buff(count, body_header->count);

    SF_PROTO_SET_HEADER((FSProtoHeader *)task->data,
            FS_REPLICA_PROTO_RPC_REQ, body_len);
    sf_send_add_event(task);

    if (replication->last_net_comm_time != g_current_time) {
        replication->last_net_comm_time = g_current_time;
    }
    return 0;
}

static inline void send_active_test_package(FSReplication *replication)
{
    replication->task->length = sizeof(FSProtoHeader);
    SF_PROTO_SET_HEADER((FSProtoHeader *)replication->task->data,
            SF_PROTO_ACTIVE_TEST_REQ, 0);
    sf_send_add_event(replication->task);
}

static int deal_connected_replication(FSReplication *replication)
{
    int stage;

    stage = __sync_add_and_fetch(&replication->stage, 0);
    /*
    logInfo("replication stage: %d, task offset: %d, length: %d",
            stage, replication->task->offset, replication->task->length);
            */

    if (stage != FS_REPLICATION_STAGE_SYNCING) {
        replication_queue_discard_all(replication);
    }

    if (stage == FS_REPLICATION_STAGE_WAITING_JOIN_RESP) {
        return 0;
    }

    if (!(replication->task->offset == 0 && replication->task->length == 0)) {
        return 0;
    }

    if (stage == FS_REPLICATION_STAGE_SYNCING) {
        if (rpc_result_ring_clear_timeouts(&replication->
                    context.caller.rpc_result_ctx) > 0)
        {
            return ETIMEDOUT;
        }
        return replication_rpc_from_queue(replication);
    }

    return 0;
}

static int deal_replication_connected(FSServerContext *server_ctx)
{
    FSReplication *replication;
    int result;
    int i;
    bool send_hb;

    /*
    static int count = 0;
    if (++count % 100 == 0) {
        logInfo("server_ctx %p, connected.count: %d", server_ctx,
                server_ctx->replica.connected.count);
    }
    */

    if (server_ctx->replica.connected.count == 0) {
        return 0;
    }

    for (i=0; i<server_ctx->replica.connected.count; i++) {
        replication = server_ctx->replica.connected.replications[i];
        if ((result=deal_connected_replication(replication)) == 0) {
            result = replication_callee_deal_rpc_result_queue(replication);
        }
        if (result != 0) {
            ioevent_add_to_deleted_list(replication->task);
            continue;
        }

        if (replication->is_client) {
            send_hb = g_current_time - replication->last_net_comm_time >=
                g_server_global_vars.replica.active_test_interval;
        } else {
            send_hb = __sync_add_and_fetch(&replication->reverse_hb, 0) == 1;
            if (send_hb) {
                __sync_bool_compare_and_swap(&replication->reverse_hb, 1, 0);
                logInfo("file: "__FILE__", line: %d, "
                        "reverse send active test to peer: %d, %s:%u",
                        __LINE__, replication->peer->server->id,
                        REPLICA_GROUP_ADDRESS_FIRST_IP(
                            replication->peer->server),
                        REPLICA_GROUP_ADDRESS_FIRST_PORT(
                            replication->peer->server));
            }
        }

        if (send_hb) {
            replication->last_net_comm_time = g_current_time;
            send_active_test_package(replication);
        }
    }

    return 0;
}

int replication_processor_process(FSServerContext *server_ctx)
{
    int result;

    /*
    static int count = 0;

    if (++count % 10000 == 0) {
        logInfo("file: "__FILE__", line: %d, count: %d, g_current_time: %d",
                __LINE__, count, (int)g_current_time);
    }
    */

    if ((result=deal_replication_connectings(server_ctx)) != 0) {
        return result;
    }

    return deal_replication_connected(server_ctx);
}
