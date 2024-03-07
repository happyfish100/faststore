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
        SERVER_TASK_TYPE = FS_SERVER_TASK_TYPE_REPLICATION_CLIENT; \
        if (SF_CTX->realloc_task_buffer) { \
            sf_set_task_send_max_buffer_size(task); \
            sf_set_task_recv_max_buffer_size(task); \
        } \
        REPLICA_REPLICATION = replication;      \
    } while (0)

int replication_processor_bind_thread(FSReplication *replication)
{
    struct nio_thread_data *thread_data;
    FSServerContext *server_ctx;

    set_replication_stage(replication, FS_REPLICATION_STAGE_INITED);
    thread_data = REPLICA_SF_CTX.thread_data + replication->
        thread_index % REPLICA_SF_CTX.work_threads;
    server_ctx = (FSServerContext *)thread_data->arg;
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
        return replication_processor_bind_thread(replication);
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

void replication_processor_connect_done(struct fast_task_info *task,
        const int err_no)
{
    FSReplication *replication;
    char formatted_ip[FORMATTED_IP_SIZE];

    if ((replication=REPLICA_REPLICATION) == NULL) {
        return;
    }

    if (err_no == 0) {
        return;
    }

    set_replication_stage(replication, FS_REPLICATION_STAGE_INITED);
    if (err_no != replication->connection_info.last_errno
            || replication->connection_info.fail_count % 10 == 0)
    {
        replication->connection_info.last_errno = err_no;
        format_ip_address(task->server_ip, formatted_ip);
        logError("file: "__FILE__", line: %d, "
                "%dth connect to replication peer: %d, %s:%u fail, "
                "time used: %ds, errno: %d, error info: %s",
                __LINE__, replication->connection_info.fail_count + 1,
                replication->peer->server->id, formatted_ip, task->port,
                (int)(g_current_time - replication->connection_info.
                    start_time), err_no, STRERROR(err_no));
    }
    replication->connection_info.fail_count++;
}

static void check_and_make_replica_connection(FSReplication *replication)
{
    struct fast_task_info *task;
    FCAddressPtrArray *addr_array;
    FCAddressInfo *addr;

    if (FC_ATOMIC_GET(replication->stage) != FS_REPLICATION_STAGE_INITED) {
        return;
    }

    if (replication->connection_info.next_connect_time > g_current_time) {
        return;
    }

    if ((task=sf_alloc_init_task(REPLICA_NET_HANDLER, -1)) == NULL) {
        return;
    }
    task->thread_data = REPLICA_SF_CTX.thread_data + replication->
        thread_index % REPLICA_SF_CTX.work_threads;
    REPLICATION_BIND_TASK(replication, task);

    addr_array = &REPLICA_GROUP_ADDRESS_ARRAY(replication->peer->server);
    addr = addr_array->addrs[replication->conn_index++];
    if (replication->conn_index >= addr_array->count) {
        replication->conn_index = 0;
    }

    replication->connection_info.start_time = g_current_time;
    calc_next_connect_time(replication);
    snprintf(task->server_ip, sizeof(task->server_ip),
            "%s", addr->conn.ip_addr);
    task->port = addr->conn.port;
    if (sf_nio_notify(task, SF_NIO_STAGE_CONNECT) == 0) {
        set_replication_stage(replication, FS_REPLICATION_STAGE_CONNECTING);
    }
}

static void on_connect_success(FSReplication *replication)
{
    char prompt[128];
    FSServerContext *server_ctx;
    char formatted_ip[FORMATTED_IP_SIZE];

    server_ctx = (FSServerContext *)replication->task->thread_data->arg;
    if (replication->connection_info.fail_count > 0) {
        sprintf(prompt, " after %d retries",
                replication->connection_info.fail_count);
    } else {
        *prompt = '\0';
    }
    format_ip_address(replication->task->server_ip, formatted_ip);
    logInfo("file: "__FILE__", line: %d, "
            "connect to replication peer id: %d, %s:%u successfully%s",
            __LINE__, replication->peer->server->id, formatted_ip,
            replication->task->port, prompt);

    if (remove_from_replication_ptr_array(&server_ctx->
                replica.connectings, replication) == 0)
    {
        set_replication_stage(replication,
                FS_REPLICATION_STAGE_WAITING_JOIN_RESP);
        add_to_replication_ptr_array(&server_ctx->
                replica.connected, replication);
    }

    replication->connection_info.fail_count = 0;
}

int replication_processor_join_server(struct fast_task_info *task)
{
    FSProtoJoinServerReq *req;
    FSReplication *replication;

    TASK_CTX.common.req_start_time = get_current_time_us();

    /* set magic number for the first request */
    SF_PROTO_SET_MAGIC(((FSProtoHeader *)task->send.ptr->data)->magic);

    req = (FSProtoJoinServerReq *)SF_PROTO_SEND_BODY(task);
    int2buff(CLUSTER_MY_SERVER_ID, req->server_id);
    req->auth_enabled = (AUTH_ENABLED ? 1 : 0);
    int2buff(task->send.ptr->size, req->buffer_size);
    int2buff(REPLICA_CHANNELS_BETWEEN_TWO_SERVERS,
            req->replica_channels_between_two_servers);
    memcpy(req->config_signs.servers, SERVERS_CONFIG_SIGN_BUF,
            SF_CLUSTER_CONFIG_SIGN_LEN);
    memcpy(req->config_signs.cluster, CLUSTER_CONFIG_SIGN_BUF,
            SF_CLUSTER_CONFIG_SIGN_LEN);

    RESPONSE.error.length = 0;
    RESPONSE.header.cmd = FS_REPLICA_PROTO_JOIN_SERVER_REQ;
    TASK_CTX.common.need_response = true;
    TASK_CTX.common.log_level = LOG_ERR;
    if ((replication=REPLICA_REPLICATION) == NULL) {
        TASK_CTX.common.response_done = false;
        return ENOENT;
    }

    on_connect_success(replication);
    RESPONSE.header.body_len = sizeof(FSProtoJoinServerReq);
    TASK_CTX.common.response_done = true;
    replication->last_net_comm_time = g_current_time;
    return 0;
}

static void decrease_task_waiting_rpc_count(ReplicationRPCEntry *rb)
{
    FSServerTaskArg *task_arg;

    task_arg = (FSServerTaskArg *)rb->task->arg;
    if (__sync_sub_and_fetch(&task_arg->context.service.
                rpc.waiting_count, 1) == 0)
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

    fc_queue_try_pop_to_queue(&replication->
            context.caller.rpc_queue, &qinfo);
    if (qinfo.head != NULL) {
        discard_queue(replication, (ReplicationRPCEntry *)qinfo.head);
    }
}

static int deal_replication_connectings(FSServerContext *server_ctx)
{
    int i;
    FSReplication *replication;

    if (server_ctx->replica.connectings.count == 0) {
        return 0;
    }

    for (i=0; i<server_ctx->replica.connectings.count; i++) {
        replication = server_ctx->replica.connectings.replications[i];
        check_and_make_replica_connection(replication);
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

static inline void send_active_test_package(struct fast_task_info *task)
{
    ++TASK_PENDING_SEND_COUNT;
    task->send.ptr->length = sizeof(FSProtoHeader);
    SF_PROTO_SET_HEADER((FSProtoHeader *)task->send.ptr->data,
            SF_PROTO_ACTIVE_TEST_REQ, 0);
    sf_send_add_event(task);
}

static int replication_rpc_from_queue(FSReplication *replication)
{
    struct fc_queue_info qinfo;
    ReplicationRPCEntry *rb;
    ReplicationRPCEntry *deleted;
    FSReplicaRPCResultEntry *rentry;
    struct fast_task_info *task;
    struct iovec *iov;
    FSProtoReplicaRPCReqBodyHeader *body_header;
    FSProtoReplicaRPCReqBodyPart *body_part;
    int body_len;
    int pkg_len;
    bool notify;

    task = replication->task;
    if (task->canceled || TASK_PENDING_SEND_COUNT > 0) {
        return 0;
    }

    fc_queue_try_pop_to_queue(&replication->
            context.caller.rpc_queue, &qinfo);
    if (qinfo.head == NULL) {
        if (g_current_time - replication->last_net_comm_time >=
                g_server_global_vars->replica.active_test_interval)
        {
            replication->last_net_comm_time = g_current_time;
            send_active_test_package(task);
        }

        return 0;
    }

    ++TASK_PENDING_SEND_COUNT;
    rentry = replication->context.caller.rpc_result_array.results;
    body_part = replication->rpc.body_parts;
    task->send.ptr->length = sizeof(FSProtoHeader) +
        sizeof(FSProtoReplicaRPCReqBodyHeader);
    iov = replication->rpc.io_vecs;
    FC_SET_IOVEC(*iov, task->send.ptr->data, task->send.ptr->length);
    ++iov;

    rb = (ReplicationRPCEntry *)qinfo.head;
    do {
        pkg_len = task->send.ptr->length + sizeof(*body_part) +
            rb->op_ctx->info.body_len;
        if (pkg_len > task->send.ptr->size || iov - replication->rpc.io_vecs >
                IOV_MAX - 2 || rentry - replication->context.caller.
                rpc_result_array.results == replication->context.
                caller.rpc_result_array.alloc)
        {
            qinfo.head = rb;
            fc_queue_push_queue_to_head_ex(&replication->context.
                    caller.rpc_queue, &qinfo, &notify);
            break;
        }

        FC_SET_IOVEC(*iov, (char *)body_part, sizeof(*body_part));
        ++iov;

        FC_SET_IOVEC(*iov, rb->op_ctx->info.body,
                rb->op_ctx->info.body_len);
        ++iov;

        body_part->cmd = ((FSProtoHeader *)rb->task->send.ptr->data)->cmd;

        if (((FSServerTaskArg *)rb->task->arg)->context.service.
                idempotency_request != NULL)
        {
            long2buff(((FSServerTaskArg *)rb->task->arg)->context.service.
                    idempotency_request->req_id, body_part->req_id);
            int2buff(rb->op_ctx->update.space_changed, body_part->inc_alloc);
        } else {
            long2buff(0, body_part->req_id);
            int2buff(0, body_part->inc_alloc);
        }

        task->send.ptr->length = pkg_len;
        long2buff(rb->op_ctx->info.data_version, body_part->data_version);
        int2buff(rb->op_ctx->info.body_len, body_part->body_len);
        ++body_part;

        rentry->data_group_id = rb->op_ctx->info.data_group_id;
        rentry->data_version = rb->op_ctx->info.data_version;
        rentry->waiting_task = rb->task;
        ++rentry;

        deleted = rb;
        rb = rb->nexts[replication->peer->link_index];

        replication_caller_release_rpc_entry(deleted);
    } while (rb != NULL);

    replication->context.caller.rpc_result_array.count = rentry -
        replication->context.caller.rpc_result_array.results;

    task->iovec_array.iovs = replication->rpc.io_vecs;
    task->iovec_array.count = iov - replication->rpc.io_vecs;
    body_header = (FSProtoReplicaRPCReqBodyHeader *)
        (task->send.ptr->data + sizeof(FSProtoHeader));
    body_len = task->send.ptr->length - sizeof(FSProtoHeader);
    int2buff(body_part - replication->rpc.body_parts, body_header->count);

    if (replication->last_net_comm_time != g_current_time) {
        replication->last_net_comm_time = g_current_time;
    }

    SF_PROTO_SET_HEADER((FSProtoHeader *)task->send.ptr->data,
            FS_REPLICA_PROTO_RPC_CALL_REQ, body_len);
    sf_send_add_event(task);
    return 0;
}

static int deal_connected_replication(FSReplication *replication)
{
    int stage;

    stage = FC_ATOMIC_GET(replication->stage);
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

    if (stage == FS_REPLICATION_STAGE_SYNCING) {
        return replication_rpc_from_queue(replication);
    }

    return 0;
}

static int deal_replication_connected(FSServerContext *server_ctx)
{
    FSReplication *replication;
    int i;

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
        if (deal_connected_replication(replication) != 0) {
            ioevent_add_to_deleted_list(replication->task);
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
