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

//cluster_handler.c

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/ioevent_loop.h"
#include "fastcommon/json_parser.h"
#include "sf/sf_util.h"
#include "sf/sf_func.h"
#include "sf/sf_nio.h"
#include "sf/sf_global.h"
#include "common/fs_proto.h"
#include "server_global.h"
#include "server_func.h"
#include "server_group_info.h"
#include "cluster_topology.h"
#include "cluster_relationship.h"
#include "common_handler.h"
#include "cluster_handler.h"

int cluster_handler_init()
{
    return 0;
}

int cluster_handler_destroy()
{   
    return 0;
}

int cluster_recv_timeout_callback(struct fast_task_info *task)
{
    if (SERVER_TASK_TYPE == FS_SERVER_TASK_TYPE_RELATIONSHIP &&
            CLUSTER_PEER != NULL)
    {
        logError("file: "__FILE__", line: %d, "
                "client ip: %s, server id: %d, recv timeout",
                __LINE__, task->client_ip, CLUSTER_PEER->server->id);
        return ETIMEDOUT;
    }

    return 0;
}

void cluster_task_finish_cleanup(struct fast_task_info *task)
{
    FSServerContext *server_ctx;

    switch (SERVER_TASK_TYPE) {
        case FS_SERVER_TASK_TYPE_RELATIONSHIP:
            if (CLUSTER_PEER != NULL) {
                server_ctx = (FSServerContext *)task->thread_data->arg;
                cluster_topology_remove_notify_ctx(&server_ctx->cluster.
                        notify_ctx_ptr_array, &CLUSTER_PEER->notify_ctx);

                if (FC_ATOMIC_GET(CLUSTER_PEER->notify_ctx.task) == task) {
                    if (cluster_topology_deactivate_server(CLUSTER_PEER)) {
                        logWarning("file: "__FILE__", line: %d, "
                                "deactivate peer server id: %d!",
                                __LINE__, CLUSTER_PEER->server->id);
                    }

                    __sync_bool_compare_and_swap(&CLUSTER_PEER->
                            notify_ctx.task, task, NULL);
                }
                CLUSTER_PEER = NULL;
            } else {
                logError("file: "__FILE__", line: %d, "
                        "mistake happen! SERVER_TASK_TYPE: %d, CLUSTER_PEER: %p",
                        __LINE__, SERVER_TASK_TYPE, CLUSTER_PEER);
            }
            SERVER_TASK_TYPE = SF_SERVER_TASK_TYPE_NONE;
            break;
        default:
            break;
    }

    if (CLUSTER_PEER != NULL) {
        logError("file: "__FILE__", line: %d, "
                "mistake happen! SERVER_TASK_TYPE: %d, CLUSTER_PEER: %p",
                __LINE__, SERVER_TASK_TYPE, CLUSTER_PEER);
    }

    sf_task_finish_clean_up(task);
}

static int cluster_deal_get_server_status(struct fast_task_info *task)
{
    int result;
    int server_id;
    FSProtoGetServerStatusReq *req;
    FSProtoGetServerStatusResp *resp;

    if ((result=server_expect_body_length(task,
                    sizeof(FSProtoGetServerStatusReq))) != 0)
    {
        return result;
    }

    req = (FSProtoGetServerStatusReq *)REQUEST.body;
    server_id = buff2int(req->server_id);
    if ((result=handler_check_config_signs(task, server_id,
                    &req->config_signs)) != 0)
    {
        return result;
    }

    resp = (FSProtoGetServerStatusResp *)REQUEST.body;
    if (CLUSTER_MYSELF_PTR == CLUSTER_LEADER_ATOM_PTR) {
        if (req->is_leader) {
            FSClusterServerInfo *peer;

            peer = fs_get_server_by_id(server_id);
            if (peer == NULL) {
                RESPONSE.error.length = sprintf(
                        RESPONSE.error.message,
                        "peer server id: %d not exist", server_id);
                return ENOENT;
            }

            logError("file: "__FILE__", line: %d, "
                    "two leaders occurs, anonther leader id: %d, ip %s:%u, "
                    "trigger re-select leader ...", __LINE__, server_id,
                    CLUSTER_GROUP_ADDRESS_FIRST_IP(peer->server),
                    CLUSTER_GROUP_ADDRESS_FIRST_PORT(peer->server));
            cluster_relationship_trigger_reselect_leader();
        }
        resp->is_leader = 1;
    } else {
        resp->is_leader = 0;
    }

    resp->leader_hint = MYSELF_IS_LEADER;
    int2buff(CLUSTER_MY_SERVER_ID, resp->server_id);
    int2buff(g_sf_global_vars.up_time, resp->up_time);
    int2buff(fs_get_last_shutdown_time(), resp->last_shutdown_time);
    long2buff(CLUSTER_CURRENT_VERSION, resp->version);

    RESPONSE.header.body_len = sizeof(FSProtoGetServerStatusResp);
    RESPONSE.header.cmd = FS_CLUSTER_PROTO_GET_SERVER_STATUS_RESP;
    TASK_ARG->context.response_done = true;
    return 0;
}

static int cluster_deal_join_leader(struct fast_task_info *task)
{
    int result;
    int server_id;
    FSProtoJoinLeaderReq *req;
    FSProtoJoinLeaderResp *resp;
    FSClusterServerInfo *peer;
    FSServerContext *server_ctx;

    if ((result=server_expect_body_length(task,
                    sizeof(FSProtoJoinLeaderReq))) != 0)
    {
        return result;
    }

    req = (FSProtoJoinLeaderReq *)REQUEST.body;
    server_id = buff2int(req->server_id);
    peer = fs_get_server_by_id(server_id);
    if (peer == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "peer server id: %d not exist", server_id);
        return ENOENT;
    }
    if (peer == CLUSTER_MYSELF_PTR) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "can't join self");
        return EINVAL;
    }

    if ((result=handler_check_config_signs(task, server_id,
                    &req->config_signs)) != 0)
    {
        return result;
    }

    if (CLUSTER_MYSELF_PTR != CLUSTER_LEADER_ATOM_PTR) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "i am not leader");
        return EINVAL;
    }

    if (CLUSTER_PEER != NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "peer server id: %d already joined", server_id);
        return EEXIST;
    }

    if (!__sync_bool_compare_and_swap(&peer->notify_ctx.task, NULL, task)) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "peer server id: %d already joined", server_id);
        return EEXIST;
    }

    server_ctx = (FSServerContext *)task->thread_data->arg;
    if ((result=cluster_topology_add_notify_ctx(&server_ctx->cluster.
                    notify_ctx_ptr_array, &peer->notify_ctx)) != 0)
    {
        __sync_bool_compare_and_swap(&peer->notify_ctx.task, task, NULL);
        return result;
    }

    SERVER_TASK_TYPE = FS_SERVER_TASK_TYPE_RELATIONSHIP;
    CLUSTER_PEER = peer;

    if (FC_ATOMIC_GET(peer->status) == FS_SERVER_STATUS_ACTIVE) {
        logWarning("file: "__FILE__", line: %d, "
                "peer server id: %d, unexpect server status: %d(ACTIVE), "
                "set to %d(OFFLINE)", __LINE__, server_id,
                FS_SERVER_STATUS_ACTIVE, FS_SERVER_STATUS_OFFLINE);
        cluster_relationship_set_server_status(
                peer, FS_SERVER_STATUS_OFFLINE);
    }
    cluster_topology_sync_all_data_servers(peer);

    resp = (FSProtoJoinLeaderResp *)REQUEST.body;
    long2buff(CLUSTER_MYSELF_PTR->leader_version, resp->leader_version);
    RESPONSE.header.body_len = sizeof(FSProtoJoinLeaderResp);
    RESPONSE.header.cmd = FS_CLUSTER_PROTO_JOIN_LEADER_RESP;
    TASK_ARG->context.response_done = true;
    return 0;
}

static bool set_ds_status_and_data_version(FSClusterDataServerInfo *ds,
        const int status, const uint64_t data_version, const bool notify_self,
        const int source)
{
    int event_type;
    if ((event_type=cluster_relationship_set_ds_status_and_dv(ds,
                status, data_version)) != 0)
    {
        cluster_topology_data_server_chg_notify(ds, source,
                event_type, notify_self);
        return true;
    } else {
        return false;
    }
}

static int process_ping_leader_req(struct fast_task_info *task)
{
    FSProtoPingLeaderReqHeader *req_header;
    FSProtoPingLeaderReqBodyPart *body_part;
    FSClusterDataServerInfo *ds;
    int data_group_count;
    int expect_body_length;
    int data_group_id;
    int64_t leader_version;
    uint64_t data_version;
    int i;
    int change_count;

    req_header = (FSProtoPingLeaderReqHeader *)REQUEST.body;
    leader_version = buff2long(req_header->leader_version);
    if (leader_version != CLUSTER_MYSELF_PTR->leader_version) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "peer id: %d, leader version: %"PRId64" != mine: %"PRId64,
                CLUSTER_PEER->server->id, leader_version,
                CLUSTER_MYSELF_PTR->leader_version);
        TASK_ARG->context.log_level = LOG_WARNING;
        return SF_CLUSTER_ERROR_LEADER_VERSION_INCONSISTENT;
    }

    data_group_count = buff2int(req_header->data_group_count);
    expect_body_length = sizeof(FSProtoPingLeaderReqHeader) +
        sizeof(FSProtoPingLeaderReqBodyPart) * data_group_count;
    if (REQUEST.header.body_len != expect_body_length) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "request body length: %d != %d",
                REQUEST.header.body_len, expect_body_length);
        return EINVAL;
    }

    if (data_group_count == 0) {
        return 0;
    }

    change_count = 0;
    body_part = (FSProtoPingLeaderReqBodyPart *)(req_header + 1);
    for (i=0; i<data_group_count; i++, body_part++) {
        data_group_id = buff2int(body_part->data_group_id);
        if ((ds=fs_get_data_server(data_group_id,
                        CLUSTER_PEER->server->id)) != NULL)
        {
            data_version = buff2long(body_part->data_version);
            if (set_ds_status_and_data_version(ds, body_part->status,
                        data_version, false, FS_EVENT_SOURCE_SELF_PING))
            {
                ++change_count;
            }
        }
    }

    if (change_count > 0) {
        __sync_add_and_fetch(&CLUSTER_CURRENT_VERSION, 1);

        /*
        logInfo("peer id: %d, data_group_count: %d, current_version: %"PRId64,
                CLUSTER_PEER->server->id, data_group_count,
                __sync_add_and_fetch(&CLUSTER_CURRENT_VERSION, 0));
                */
    }

    return 0;
}

static int cluster_deal_ping_leader(struct fast_task_info *task)
{
    int result;
    FSClusterServerInfo *peer;

    RESPONSE.header.cmd = FS_CLUSTER_PROTO_PING_LEADER_RESP;
    if ((result=server_check_min_body_length(task,
                    sizeof(FSProtoPingLeaderReqHeader))) != 0)
    {
        return result;
    }

    if (((peer=CLUSTER_PEER) == NULL) || (SERVER_TASK_TYPE !=
                FS_SERVER_TASK_TYPE_RELATIONSHIP))
    {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "please join first");
        return EINVAL;
    }

    if (CLUSTER_MYSELF_PTR != CLUSTER_LEADER_ATOM_PTR) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am not leader");
        return EINVAL;
    }

    if ((result=process_ping_leader_req(task)) == 0) {
        peer->last_ping_time = g_current_time;
    }
    return result;
}

static int cluster_deal_active_server(struct fast_task_info *task)
{
    int result;

    if ((result=cluster_deal_ping_leader(task)) == 0) {
        if (cluster_topology_activate_server(CLUSTER_PEER)) {
            logInfo("file: "__FILE__", line: %d, "
                    "activate peer server id: %d.",
                    __LINE__, CLUSTER_PEER->server->id);
        } else {
            RESPONSE.error.length = sprintf(
                    RESPONSE.error.message,
                    "i am not leader");
            return EINVAL;
        }
    }

    return result;
}

static int cluster_deal_report_ds_status(struct fast_task_info *task)
{
    int result;
    FSProtoReportDSStatusReq *req;
    FSClusterDataServerInfo *ds;
    int my_server_id;
    int ds_server_id;
    int data_group_id;
    int old_status;
    int source;
    bool notify_self;

    RESPONSE.header.cmd = FS_CLUSTER_PROTO_REPORT_DS_STATUS_RESP;
    if ((result=server_expect_body_length(task,
                    sizeof(FSProtoReportDSStatusReq))) != 0)
    {
        return result;
    }

    if (CLUSTER_MYSELF_PTR != CLUSTER_LEADER_ATOM_PTR) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am not leader");
        return EINVAL;
    }

    req = (FSProtoReportDSStatusReq *)REQUEST.body;
    my_server_id = buff2int(req->my_server_id);
    ds_server_id = buff2int(req->ds_server_id);
    data_group_id = buff2int(req->data_group_id);
    if ((ds=fs_get_data_server(data_group_id, ds_server_id)) == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data_group_id: %d, ds_server_id: %d not exist",
                data_group_id, ds_server_id);
        return ENOENT;
    }

    old_status = __sync_fetch_and_add(&ds->status, 0);
    if (my_server_id == ds_server_id) {
        source = FS_EVENT_SOURCE_SELF_REPORT;
        notify_self = false;
    } else {
        FSClusterDataServerInfo *master;

        if ((master=fs_get_data_server(data_group_id, my_server_id)) == NULL) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "data_group_id: %d, my_server_id: %d not exist",
                    data_group_id, my_server_id);
            return ENOENT;
        }
        if (!__sync_add_and_fetch(&master->is_master, 0)) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "data_group_id: %d, my_server_id: %d is not master",
                    data_group_id, my_server_id);
            return EPERM;
        }

        if (old_status != FS_DS_STATUS_ACTIVE) {
            if (old_status == req->status) {  //just ignore
                return 0;
            } else {
                RESPONSE.error.length = sprintf(RESPONSE.error.message,
                        "data_group_id: %d, my_server_id: %d, "
                        "ds_server_id: %d, invalid old status: %d (%s)",
                        data_group_id, my_server_id, ds_server_id,
                        old_status, fs_get_server_status_caption(old_status));
                TASK_ARG->context.log_level = LOG_WARNING;
                return EINVAL;
            }
        }

        if (req->status != FS_DS_STATUS_OFFLINE) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "data_group_id: %d, my_server_id: %d, ds_server_id: %d, "
                    "invalid dest status: %d", data_group_id, my_server_id,
                    ds_server_id, req->status);
            return EINVAL;
        }

        source = FS_EVENT_SOURCE_MASTER_REPORT;
        notify_self = true;
    }

    if (cluster_relationship_set_ds_status_ex(ds, old_status, req->status)) {
        cluster_topology_data_server_chg_notify(ds, source,
                FS_EVENT_TYPE_STATUS_CHANGE, notify_self);
        __sync_add_and_fetch(&CLUSTER_CURRENT_VERSION, 1);
    }
    return 0;
}

static int cluster_deal_report_disk_space(struct fast_task_info *task)
{
    int result;
    FSProtoReportDiskSpaceReq *req;
    FSClusterServerSpaceStat stat;

    RESPONSE.header.cmd = FS_CLUSTER_PROTO_REPORT_DISK_SPACE_RESP;
    if ((result=server_expect_body_length(task,
                    sizeof(FSProtoReportDiskSpaceReq))) != 0)
    {
        return result;
    }

    if (CLUSTER_PEER == NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "please join first");
        return EINVAL;
    }

    if (CLUSTER_MYSELF_PTR != CLUSTER_LEADER_ATOM_PTR) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am not leader");
        return EINVAL;
    }

    req = (FSProtoReportDiskSpaceReq *)REQUEST.body;
    stat.total = buff2long(req->total);
    stat.avail = buff2long(req->avail);
    stat.used = buff2long(req->used);

    CLUSTER_PEER->space_stat = stat;
    return 0;
}

static int cluster_deal_next_leader(struct fast_task_info *task)
{
    int result;
    int leader_id;
    FSClusterServerInfo *leader;

    if ((result=server_expect_body_length(task, 4)) != 0) {
        return result;
    }

    if (CLUSTER_MYSELF_PTR == CLUSTER_LEADER_ATOM_PTR) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am already leader");
        cluster_relationship_trigger_reselect_leader();
        return EEXIST;
    }

    leader_id = buff2int(REQUEST.body);
    leader = fs_get_server_by_id(leader_id);
    if (leader == NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "leader server id: %d not exist", leader_id);
        return ENOENT;
    }

    if (REQUEST.header.cmd == FS_CLUSTER_PROTO_PRE_SET_NEXT_LEADER) {
        return cluster_relationship_pre_set_leader(leader);
    } else {
        return cluster_relationship_commit_leader(leader);
    }
}

int cluster_deal_task_partly(struct fast_task_info *task, const int stage)
{
    handler_init_task_context(task);
    switch (REQUEST.header.cmd) {
        case SF_PROTO_ACTIVE_TEST_REQ:
            RESPONSE.header.cmd = SF_PROTO_ACTIVE_TEST_RESP;
            RESPONSE_STATUS = sf_proto_deal_active_test(task, &REQUEST, &RESPONSE);
            break;
        default:
            RESPONSE_STATUS = SF_ERROR_EOPNOTSUPP;
            break;
    }

    return handler_deal_task_done(task);
}

int cluster_deal_task_fully(struct fast_task_info *task, const int stage)
{
    int result;

    if (stage == SF_NIO_STAGE_CONTINUE) {
        if (task->continue_callback != NULL) {
            result = task->continue_callback(task);
        } else {
            result = RESPONSE_STATUS;
            if (result == TASK_STATUS_CONTINUE) {
                logError("file: "__FILE__", line: %d, "
                        "unexpect status: %d", __LINE__, result);
                result = EBUSY;
            }
        }
    } else {
        handler_init_task_context(task);

        switch (REQUEST.header.cmd) {
            case SF_PROTO_ACTIVE_TEST_REQ:
                RESPONSE.header.cmd = SF_PROTO_ACTIVE_TEST_RESP;
                result = sf_proto_deal_active_test(task, &REQUEST, &RESPONSE);
                break;
            case SF_PROTO_ACK:
                result = sf_proto_deal_ack(task, &REQUEST, &RESPONSE);
                TASK_ARG->context.need_response = false;
                break;
            case FS_CLUSTER_PROTO_GET_SERVER_STATUS_REQ:
                result = cluster_deal_get_server_status(task);
                break;
            case FS_CLUSTER_PROTO_REPORT_DS_STATUS_REQ:
                result = cluster_deal_report_ds_status(task);
                break;
            case FS_CLUSTER_PROTO_PRE_SET_NEXT_LEADER:
            case FS_CLUSTER_PROTO_COMMIT_NEXT_LEADER:
                result = cluster_deal_next_leader(task);
                break;
            case FS_CLUSTER_PROTO_JOIN_LEADER_REQ:
                result = cluster_deal_join_leader(task);
                break;
            case FS_CLUSTER_PROTO_ACTIVATE_SERVER:
                result = cluster_deal_active_server(task);
                break;
            case FS_CLUSTER_PROTO_PING_LEADER_REQ:
                result = cluster_deal_ping_leader(task);
                break;
            case FS_CLUSTER_PROTO_REPORT_DISK_SPACE_REQ:
                result = cluster_deal_report_disk_space(task);
                break;
            default:
                RESPONSE.error.length = sprintf(RESPONSE.error.message,
                        "unkown cmd: %d", REQUEST.header.cmd);
                result = EINVAL;
                break;
        }
    }

    if (result == TASK_STATUS_CONTINUE) {
        return 0;
    } else {
        RESPONSE_STATUS = (result > 0 ? -1 * result : result);
        return handler_deal_task_done(task);
    }
}

static int alloc_notify_ctx_ptr_array(FSClusterNotifyContextPtrArray *array)
{
    int bytes;

    bytes = sizeof(FSClusterTopologyNotifyContext *) *
        CLUSTER_SERVER_ARRAY.count;
    array->contexts = (FSClusterTopologyNotifyContext **)fc_malloc(bytes);
    if (array->contexts == NULL) {
        return ENOMEM;
    }
    memset(array->contexts, 0, bytes);
    array->alloc = CLUSTER_SERVER_ARRAY.count;

    return 0;
}

void *cluster_alloc_thread_extra_data(const int thread_index)
{
    FSServerContext *server_context;

    server_context = (FSServerContext *)fc_malloc(sizeof(FSServerContext));
    if (server_context == NULL) {
        return NULL;
    }
    memset(server_context, 0, sizeof(FSServerContext));

    if (alloc_notify_ctx_ptr_array(&server_context->
                cluster.notify_ctx_ptr_array) != 0)
    {
        return NULL;
    }

    return server_context;
}

int cluster_thread_loop_callback(struct nio_thread_data *thread_data)
{
    FSServerContext *server_ctx;

    server_ctx = (FSServerContext *)thread_data->arg;
    if (CLUSTER_MYSELF_PTR == CLUSTER_LEADER_ATOM_PTR) {
        /*
        static int count = 0;
        if (count++ % 100 == 0) {
            logInfo("thread index: %d, notify context count: %d",
                    SF_THREAD_INDEX(CLUSTER_SF_CTX, thread_data),
                    server_ctx->cluster.notify_ctx_ptr_array.count);
        }
        */

        cluster_topology_process_notify_events(
                &server_ctx->cluster.notify_ctx_ptr_array);
    }
    return 0;
}
