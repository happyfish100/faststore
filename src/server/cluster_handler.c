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
#include "replication/replication_quorum.h"
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

    server_ctx = (FSServerContext *)task->thread_data->arg;
    switch (SERVER_TASK_TYPE) {
        case FS_SERVER_TASK_TYPE_CLUSTER_PUSH:
            if (CLUSTER_PEER != NULL) {
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
            } else {
                logError("file: "__FILE__", line: %d, "
                        "mistake happen! SERVER_TASK_TYPE: %d, CLUSTER_PEER: %p",
                        __LINE__, SERVER_TASK_TYPE, CLUSTER_PEER);
            }
        case FS_SERVER_TASK_TYPE_RELATIONSHIP:
            CLUSTER_PEER = NULL;
            SERVER_TASK_TYPE = SF_SERVER_TASK_TYPE_NONE;
            break;
        default:
            break;
    }

    if (TASK_PENDING_SEND_COUNT > 0) {
        remove_from_pending_send_queue(&server_ctx->pending_send_head, task);
    }

    sf_task_finish_clean_up(task);
}

static int cluster_deal_get_server_status(struct fast_task_info *task,
        char *resp_body)
{
    int result;
    int server_id;
    FSProtoGetServerStatusReq *req;
    FSProtoGetServerStatusResp *resp;

    if ((result=server_expect_body_length(sizeof(
                        FSProtoGetServerStatusReq))) != 0)
    {
        return result;
    }

    req = (FSProtoGetServerStatusReq *)REQUEST.body;
    server_id = buff2int(req->server_id);
    if ((result=handler_check_config_signs(task, server_id,
                    req->auth_enabled, &req->config_signs)) != 0)
    {
        return result;
    }

    resp = (FSProtoGetServerStatusResp *)resp_body;
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

    resp->leader_hint = (MYSELF_IS_LEADER ? 1 : 0);
    resp->force_election = (FORCE_LEADER_ELECTION ? 1 : 0);
    int2buff(CLUSTER_MY_SERVER_ID, resp->server_id);
    int2buff(g_sf_global_vars.up_time, resp->up_time);
    int2buff(CLUSTER_LAST_SHUTDOWN_TIME, resp->last_shutdown_time);
    int2buff(CLUSTER_LAST_HEARTBEAT_TIME, resp->last_heartbeat_time);
    long2buff(CLUSTER_CURRENT_VERSION, resp->version);

    RESPONSE.header.body_len = sizeof(FSProtoGetServerStatusResp);
    RESPONSE.header.cmd = FS_CLUSTER_PROTO_GET_SERVER_STATUS_RESP;
    TASK_CTX.common.response_done = true;
    return 0;
}

static int cluster_deal_join_leader(struct fast_task_info *task,
        char *resp_body)
{
    int result;
    int server_id;
    bool server_push;
    int64_t key;
    FSProtoJoinLeaderReq *req;
    FSProtoJoinLeaderResp *resp;
    FSClusterServerInfo *peer;

    if ((result=server_expect_body_length(sizeof(
                        FSProtoJoinLeaderReq))) != 0)
    {
        return result;
    }

    req = (FSProtoJoinLeaderReq *)REQUEST.body;
    server_id = buff2int(req->server_id);
    key = buff2long(req->key);
    server_push = (req->server_push != 0);
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
                    req->auth_enabled, &req->config_signs)) != 0)
    {
        return result;
    }

    if (CLUSTER_MYSELF_PTR != CLUSTER_LEADER_ATOM_PTR) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "i am not leader");
        return SF_CLUSTER_ERROR_NOT_LEADER;
    }

    if (CLUSTER_PEER != NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "peer server id: %d already joined, server_push: %d",
                server_id, server_push);
        return EEXIST;
    }

    if (SF_CTX->realloc_task_buffer) {
        bool set_resp_body;

        set_resp_body = (resp_body == SF_PROTO_SEND_BODY(task));
        if ((result=sf_set_task_send_max_buffer_size(task)) != 0) {
            return result;
        }

        if (set_resp_body) {
            resp_body = SF_PROTO_SEND_BODY(task);
        }
    }

    if (server_push) {
        FSServerContext *server_ctx;

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
        SERVER_TASK_TYPE = FS_SERVER_TASK_TYPE_CLUSTER_PUSH;
        TASK_PUSH_EVENT_INPROGRESS = false;
    } else {
        SERVER_TASK_TYPE = FS_SERVER_TASK_TYPE_RELATIONSHIP;
    }

    peer->key = key;
    CLUSTER_PEER = peer;

    if (FC_ATOMIC_GET(peer->status) == FS_SERVER_STATUS_ACTIVE) {
        logWarning("file: "__FILE__", line: %d, "
                "peer server id: %d, unexpect server status: %d(ACTIVE), "
                "set to %d(OFFLINE)", __LINE__, server_id,
                FS_SERVER_STATUS_ACTIVE, FS_SERVER_STATUS_OFFLINE);
        cluster_relationship_set_server_status(
                peer, FS_SERVER_STATUS_OFFLINE);
    }

    if (server_push) {
        cluster_topology_sync_all_data_servers(peer);
    }

    resp = (FSProtoJoinLeaderResp *)resp_body;
    long2buff(CLUSTER_MYSELF_PTR->leader_version, resp->leader_version);
    RESPONSE.header.body_len = sizeof(FSProtoJoinLeaderResp);
    RESPONSE.header.cmd = FS_CLUSTER_PROTO_JOIN_LEADER_RESP;
    TASK_CTX.common.response_done = true;
    return 0;
}

static bool set_ds_status_and_dv(FSClusterDataServerInfo *ds, const int status,
        const FSClusterDataVersionPair *data_versions)
{
    const bool notify_self = false;
    const int source = FS_EVENT_SOURCE_SELF_PING;
    int event_type;

    if ((event_type=cluster_relationship_set_ds_status_and_dv(ds,
                status, data_versions)) != 0)
    {
        cluster_topology_data_server_chg_notify(ds, source,
                event_type, notify_self);

        if ((event_type & FS_EVENT_TYPE_CONFIRMED_DV_CHANGE) != 0 &&
                ds->dg->myself == FC_ATOMIC_GET(ds->dg->master) &&
                FC_ATOMIC_GET(ds->dg->replica_quorum.need_majority))
        {
            replication_quorum_deal_version_change(&ds->dg->
                    repl_quorum_ctx, data_versions->confirmed);
        }
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
    FSClusterDataVersionPair data_versions;
    int i;
    int change_count;

    req_header = (FSProtoPingLeaderReqHeader *)REQUEST.body;
    leader_version = buff2long(req_header->leader_version);
    if (leader_version != CLUSTER_MYSELF_PTR->leader_version) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "peer id: %d, leader version: %"PRId64" != mine: %"PRId64,
                CLUSTER_PEER->server->id, leader_version,
                CLUSTER_MYSELF_PTR->leader_version);
        TASK_CTX.common.log_level = LOG_WARNING;
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
            data_versions.current = buff2long(
                    body_part->data_versions.current);
            data_versions.confirmed = buff2long(
                    body_part->data_versions.confirmed);
            if (set_ds_status_and_dv(ds, body_part->status, &data_versions)) {
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

static int proto_deal_ping_leader(struct fast_task_info *task,
        const int expect_server_type)
{
    int result;
    FSClusterServerInfo *peer;

    if ((result=server_check_min_body_length(sizeof(
                        FSProtoPingLeaderReqHeader))) != 0)
    {
        return result;
    }

    if (((peer=CLUSTER_PEER) == NULL) || (SERVER_TASK_TYPE !=
                expect_server_type))
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "please join first, server type: %d != expected: %d",
                SERVER_TASK_TYPE, expect_server_type);
        return EINVAL;
    }

    if (CLUSTER_MYSELF_PTR != CLUSTER_LEADER_ATOM_PTR) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am not leader");
        return SF_CLUSTER_ERROR_NOT_LEADER;
    }

    if ((result=process_ping_leader_req(task)) == 0) {
        peer->last_ping_time = g_current_time;
    }
    return result;
}

static inline int cluster_deal_ping_leader(struct fast_task_info *task)
{
    return proto_deal_ping_leader(task, FS_SERVER_TASK_TYPE_RELATIONSHIP);
}

static int cluster_deal_active_server(struct fast_task_info *task)
{
    int result;

    result = proto_deal_ping_leader(task, FS_SERVER_TASK_TYPE_CLUSTER_PUSH);
    if (result != 0) {
        return result;
    }

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

    return 0;
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
    if ((result=server_expect_body_length(sizeof(
                        FSProtoReportDSStatusReq))) != 0)
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

        if ((master=fs_get_data_server(data_group_id,
                        my_server_id)) == NULL)
        {
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
                TASK_CTX.common.log_level = LOG_WARNING;
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

static int cluster_deal_reselect_master(struct fast_task_info *task)
{
    int result;
    FSProtoReselectMasterReq *req;
    FSClusterDataServerInfo *ds;
    int server_id;
    int data_group_id;

    RESPONSE.header.cmd = FS_CLUSTER_PROTO_RESELECT_MASTER_RESP;
    if ((result=server_expect_body_length(sizeof(
                        FSProtoReselectMasterReq))) != 0)
    {
        return result;
    }

    if (CLUSTER_MYSELF_PTR != CLUSTER_LEADER_ATOM_PTR) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am not leader");
        return EINVAL;
    }

    req = (FSProtoReselectMasterReq *)REQUEST.body;
    data_group_id = buff2int(req->data_group_id);
    server_id = buff2int(req->server_id);
    if ((ds=fs_get_data_server(data_group_id, server_id)) == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data_group_id: %d, server_id: %d not exist",
                data_group_id, server_id);
        return ENOENT;
    }

    if (!__sync_add_and_fetch(&ds->is_master, 0)) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data_group_id: %d, server_id: %d is not master",
                data_group_id, server_id);
        return EPERM;
    }

    cluster_relationship_trigger_reselect_master(ds->dg);
    return 0;
}

static int cluster_deal_report_disk_space(struct fast_task_info *task)
{
    int result;
    FSProtoReportDiskSpaceReq *req;
    FSClusterServerSpaceStat stat;

    RESPONSE.header.cmd = FS_CLUSTER_PROTO_REPORT_DISK_SPACE_RESP;
    if ((result=server_expect_body_length(sizeof(
                        FSProtoReportDiskSpaceReq))) != 0)
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

    if ((result=server_expect_body_length(4)) != 0) {
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

static int cluster_deal_unset_master(struct fast_task_info *task)
{
    int result;
    int data_group_id;
    int leader_id;
    int64_t key;
    FSProtoUnsetMasterReq *req;
    FSClusterServerInfo *leader;
    FSClusterDataServerInfo *ds;

    if ((result=server_expect_body_length(sizeof(
                        FSProtoUnsetMasterReq))) != 0)
    {
        return result;
    }

    if ((leader=CLUSTER_LEADER_ATOM_PTR) == NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "leader not exist");
        return EINVAL;
    }
    if (CLUSTER_MYSELF_PTR == leader) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "can't request myself");
        return EINVAL;
    }

    req = (FSProtoUnsetMasterReq *)REQUEST.body;
    data_group_id = buff2int(req->data_group_id);
    leader_id = buff2int(req->leader_id);
    key = buff2long(req->key);
    if (leader_id != leader->server->id) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "invalid leader server id: %d != %d",
                leader_id, leader->server->id);
        return EINVAL;
    }
    if (key != CLUSTER_MYSELF_PTR->key) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "key: %"PRId64" != mine: %"PRId64,
                key, CLUSTER_MYSELF_PTR->key);
        return EPERM;
    }

    if ((ds=fs_get_my_data_server(data_group_id)) == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data group id: %d NOT belongs to me", data_group_id);
        return ENOENT;
    }
    if (!FC_ATOMIC_GET(ds->is_master)) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data_group_id: %d, i am NOT master", data_group_id);
        return EINVAL;
    }

    __sync_bool_compare_and_swap(&ds->dg->master_swapping, 0, 1);
    RESPONSE.header.cmd = FS_CLUSTER_PROTO_UNSET_MASTER_RESP;
    return 0;
}

static int cluster_deal_get_ds_status(struct fast_task_info *task,
        char *resp_body)
{
    int result;
    int data_group_id;
    FSProtoGetDSStatusReq *req;
    FSProtoGetDSStatusResp *resp;
    FSClusterDataServerInfo *ds;

    if ((result=server_expect_body_length(sizeof(
                        FSProtoGetDSStatusReq))) != 0)
    {
        return result;
    }

    req = (FSProtoGetDSStatusReq *)REQUEST.body;
    data_group_id = buff2int(req->data_group_id);
    if ((ds=fs_get_my_data_server(data_group_id)) == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data group id: %d NOT belongs to me", data_group_id);
        return ENOENT;
    }

    resp = (FSProtoGetDSStatusResp *)resp_body;
    resp->is_master = FC_ATOMIC_GET(ds->is_master);
    resp->status = FC_ATOMIC_GET(ds->status);
    int2buff(FC_ATOMIC_GET(ds->master_dealing_count),
            resp->master_dealing_count);
    long2buff(FC_ATOMIC_GET(ds->data.current_version), resp->data_version);

    RESPONSE.header.body_len = sizeof(FSProtoGetDSStatusResp);
    RESPONSE.header.cmd = FS_CLUSTER_PROTO_GET_DS_STATUS_RESP;
    TASK_CTX.common.response_done = true;
    return 0;
}

int cluster_deal_task_partly(struct fast_task_info *task, const int stage)
{
    sf_proto_init_task_context(task, &TASK_CTX.common);
    switch (REQUEST.header.cmd) {
        case SF_PROTO_ACTIVE_TEST_REQ:
            RESPONSE.header.cmd = SF_PROTO_ACTIVE_TEST_RESP;
            RESPONSE_STATUS = sf_proto_deal_active_test(task, &REQUEST, &RESPONSE);
            break;
        default:
            RESPONSE_STATUS = SF_ERROR_EOPNOTSUPP;
            break;
    }

    return sf_proto_deal_task_done(task, "cluster-partly", &TASK_CTX.common);
}

int cluster_deal_task_fully(struct fast_task_info *task, const int stage)
{
    int result;
    bool immediate_send;
    FSPendingSendBuffer *pb;
    char *resp_body;

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
        sf_proto_init_task_context(task, &TASK_CTX.common);
        if (task->handler->comm_type == fc_comm_type_rdma &&
                task->send.ptr->length > 0)
        {
            pb = fast_mblock_alloc_object(&PENDING_SEND_ALLOCATOR);
            if (pb == NULL) {
                return -ENOMEM;
            }
            immediate_send = false;
            resp_body = pb->data + sizeof(FSProtoHeader);
        } else {
            pb = NULL;
            immediate_send = true;
            resp_body = SF_PROTO_SEND_BODY(task);
        }

        switch (REQUEST.header.cmd) {
            case SF_PROTO_ACTIVE_TEST_REQ:
                RESPONSE.header.cmd = SF_PROTO_ACTIVE_TEST_RESP;
                result = sf_proto_deal_active_test(task, &REQUEST, &RESPONSE);
                break;
            case SF_PROTO_ACK:
                result = sf_proto_deal_ack(task, &REQUEST, &RESPONSE);
                TASK_CTX.common.need_response = false;
                break;
            case FS_CLUSTER_PROTO_GET_SERVER_STATUS_REQ:
                result = cluster_deal_get_server_status(task, resp_body);
                break;
            case FS_CLUSTER_PROTO_REPORT_DS_STATUS_REQ:
                result = cluster_deal_report_ds_status(task);
                break;
            case FS_CLUSTER_PROTO_RESELECT_MASTER_REQ:
                result = cluster_deal_reselect_master(task);
                break;
            case FS_CLUSTER_PROTO_PRE_SET_NEXT_LEADER:
            case FS_CLUSTER_PROTO_COMMIT_NEXT_LEADER:
                result = cluster_deal_next_leader(task);
                break;
            case FS_CLUSTER_PROTO_UNSET_MASTER_REQ:
                result = cluster_deal_unset_master(task);
                break;
            case FS_CLUSTER_PROTO_GET_DS_STATUS_REQ:
                result = cluster_deal_get_ds_status(task, resp_body);
                break;
            case FS_CLUSTER_PROTO_JOIN_LEADER_REQ:
                result = cluster_deal_join_leader(task, resp_body);
                break;
            case FS_CLUSTER_PROTO_ACTIVATE_SERVER_REQ:
                RESPONSE.header.cmd = FS_CLUSTER_PROTO_ACTIVATE_SERVER_RESP;
                result = cluster_deal_active_server(task);
                break;
            case FS_CLUSTER_PROTO_PING_LEADER_REQ:
                RESPONSE.header.cmd = FS_CLUSTER_PROTO_PING_LEADER_RESP;
                result = cluster_deal_ping_leader(task);
                break;
            case FS_CLUSTER_PROTO_REPORT_DISK_SPACE_REQ:
                result = cluster_deal_report_disk_space(task);
                break;
            case FS_CLUSTER_PROTO_PUSH_DS_STATUS_RESP:
                result = 0;
                TASK_PUSH_EVENT_INPROGRESS = false;
                TASK_CTX.common.need_response = false;
                break;
            default:
                RESPONSE.error.length = sprintf(RESPONSE.error.message,
                        "unkown cmd: %d", REQUEST.header.cmd);
                result = EINVAL;
                break;
        }

        if (!immediate_send) {
            if (TASK_CTX.common.need_response && result == 0) {
                push_to_pending_send_queue(&SERVER_PENDING_SEND_HEAD, task, pb);
                return sf_set_read_event(task);
            }

            fast_mblock_free_object(&PENDING_SEND_ALLOCATOR, pb);
        }
    }

    if (result == TASK_STATUS_CONTINUE) {
        return 0;
    } else {
        RESPONSE_STATUS = (result > 0 ? -1 * result : result);
        return sf_proto_deal_task_done(task, "cluster", &TASK_CTX.common);
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
    FSServerContext *server_ctx;

    server_ctx = (FSServerContext *)fc_malloc(sizeof(FSServerContext));
    if (server_ctx == NULL) {
        return NULL;
    }
    memset(server_ctx, 0, sizeof(FSServerContext));

    if (alloc_notify_ctx_ptr_array(&server_ctx->
                cluster.notify_ctx_ptr_array) != 0)
    {
        return NULL;
    }

    FC_INIT_LIST_HEAD(&server_ctx->pending_send_head);
    return server_ctx;
}

int cluster_thread_loop_callback(struct nio_thread_data *thread_data)
{
    FSServerContext *server_ctx;

    server_ctx = (FSServerContext *)thread_data->arg;
    if (!fc_list_empty(&server_ctx->pending_send_head)) {
        process_pending_send_queue(&server_ctx->pending_send_head);
    }
    if (CLUSTER_MYSELF_PTR == CLUSTER_LEADER_ATOM_PTR) {
        /*
           static int count = 0;
           if (count++ % 100 == 0) {
           logInfo("thread index: %d, notify context count: %d",
           SF_THREAD_INDEX(CLUSTER_SF_CTX, thread_data),
           server_ctx->cluster.notify_ctx_ptr_array.count);
           }
         */

        if (server_ctx->cluster.notify_ctx_ptr_array.count > 0) {
            cluster_topology_process_notify_events(
                    &server_ctx->cluster.notify_ctx_ptr_array);
        }
    }

    return 0;
}
