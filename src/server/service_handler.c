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

//service_handler.c

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
#include "sf/sf_util.h"
#include "sf/sf_func.h"
#include "sf/sf_nio.h"
#include "sf/sf_service.h"
#include "sf/sf_global.h"
#include "sf/sf_configs.h"
#include "sf/idempotency/server/server_channel.h"
#include "sf/idempotency/server/server_handler.h"
#include "fastcfs/auth/fcfs_auth_for_server.h"
#include "common/fs_proto.h"
#include "common/fs_func.h"
#include "binlog/replica_binlog.h"
#include "replication/replication_common.h"
#include "replication/replication_quorum.h"
#include "server_global.h"
#include "server_func.h"
#include "server_group_info.h"
#include "server_storage.h"
#include "server_binlog.h"
#include "data_thread.h"
#include "common_handler.h"
#include "data_update_handler.h"
#include "service_handler.h"

typedef int (*deal_task_func)(struct fast_task_info *task,
        FSSliceOpContext *op_ctx);

int service_handler_init()
{
    int result;

    if ((result=idempotency_channel_init(SF_IDEMPOTENCY_MAX_CHANNEL_ID,
                    SF_IDEMPOTENCY_DEFAULT_REQUEST_HINT_CAPACITY,
                    SF_IDEMPOTENCY_DEFAULT_CHANNEL_RESERVE_INTERVAL,
                    SF_IDEMPOTENCY_DEFAULT_CHANNEL_SHARED_LOCK_COUNT)) != 0)
    {
        return result;
    }

    return replication_quorum_init();
}

int service_handler_destroy()
{
    return 0;
}

void service_task_finish_cleanup(struct fast_task_info *task)
{
    switch (SERVER_TASK_TYPE) {
        case SF_SERVER_TASK_TYPE_CHANNEL_HOLDER:
        case SF_SERVER_TASK_TYPE_CHANNEL_USER:
            if (IDEMPOTENCY_CHANNEL != NULL) {
                idempotency_channel_release(IDEMPOTENCY_CHANNEL,
                        SERVER_TASK_TYPE == SF_SERVER_TASK_TYPE_CHANNEL_HOLDER);
                IDEMPOTENCY_CHANNEL = NULL;
            } else {
                logError("file: "__FILE__", line: %d, "
                        "mistake happen! task: %p, SERVER_TASK_TYPE: %d, "
                        "IDEMPOTENCY_CHANNEL is NULL", __LINE__, task,
                        SERVER_TASK_TYPE);
            }
            SERVER_TASK_TYPE = SF_SERVER_TASK_TYPE_NONE;
            break;
        default:
            break;
    }

    if (IDEMPOTENCY_CHANNEL != NULL) {
        logError("file: "__FILE__", line: %d, "
                "mistake happen! task: %p, SERVER_TASK_TYPE: %d, "
                "IDEMPOTENCY_CHANNEL: %p != NULL", __LINE__, task,
                SERVER_TASK_TYPE, IDEMPOTENCY_CHANNEL);
    }

    sf_task_finish_clean_up(task);
}

static int service_deal_service_stat(struct fast_task_info *task)
{
    int result;
    int data_group_id;
    int64_t current_version;
    int64_t ob_count;
    int64_t slice_count;
    FSBinlogWriterStat writer_stat;
    FSClusterDataGroupInfo *group;
    FSProtoServiceStatReq *req;
    FSProtoServiceStatResp *stat_resp;

    if ((result=server_expect_body_length(sizeof(
                        FSProtoServiceStatReq))) != 0)
    {
        return result;
    }

    req = (FSProtoServiceStatReq *)REQUEST.body;
    data_group_id = buff2int(req->data_group_id);
    if (data_group_id == 0) {
        current_version = FC_ATOMIC_GET(SLICE_BINLOG_SN);
        slice_binlog_writer_stat(&writer_stat);
    } else {
        if ((group=fs_get_data_group(data_group_id)) == NULL ||
                group->myself == NULL)
        {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "data_group_id: %d not exist", data_group_id);
            return ENOENT;
        }

        current_version = FC_ATOMIC_GET(group->myself->data.current_version);
        replica_binlog_writer_stat(data_group_id, &writer_stat);
    }
    ob_index_get_ob_and_slice_counts(&ob_count, &slice_count);

    stat_resp = (FSProtoServiceStatResp *)SF_PROTO_RESP_BODY(task);
    stat_resp->is_leader  = CLUSTER_MYSELF_PTR == CLUSTER_LEADER_PTR ? 1 : 0;
    int2buff(CLUSTER_MYSELF_PTR->server->id, stat_resp->server_id);
    int2buff(SF_G_CONN_CURRENT_COUNT, stat_resp->connection.current_count);
    int2buff(SF_G_CONN_MAX_COUNT, stat_resp->connection.max_count);

    long2buff(current_version, stat_resp->binlog.current_version);
    long2buff(writer_stat.total_count, stat_resp->binlog.writer.total_count);
    long2buff(writer_stat.next_version, stat_resp->binlog.writer.next_version);
    int2buff(writer_stat.waiting_count, stat_resp->binlog.writer.waiting_count);
    int2buff(writer_stat.max_waitings, stat_resp->binlog.writer.max_waitings);

    long2buff(ob_count, stat_resp->data.ob_count);
    long2buff(slice_count, stat_resp->data.slice_count);

    RESPONSE.header.body_len = sizeof(FSProtoServiceStatResp);
    RESPONSE.header.cmd = FS_SERVICE_PROTO_SERVICE_STAT_RESP;
    TASK_CTX.common.response_done = true;
    return 0;
}

static int service_deal_slice_read(struct fast_task_info *task)
{
    int result;
    FSProtoServiceSliceReadReq *req;

    OP_CTX_INFO.deal_done = false;
    OP_CTX_INFO.is_update = false;
    RESPONSE.header.cmd = FS_SERVICE_PROTO_SLICE_READ_RESP;
    if ((result=server_expect_body_length(sizeof(
                        FSProtoServiceSliceReadReq))) != 0)
    {
        return result;
    }

    req = (FSProtoServiceSliceReadReq *)REQUEST.body;
    if ((result=du_handler_parse_check_readable_block_slice(
                    task, &req->bs)) != 0)
    {
        return result;
    }

    if (OP_CTX_INFO.bs_key.slice.length > task->size - sizeof(FSProtoHeader)) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "read slice length: %d > task buffer size: %d",
                OP_CTX_INFO.bs_key.slice.length, (int)(
                    task->size - sizeof(FSProtoHeader)));
        return EOVERFLOW;
    }

    sf_hold_task(task);
    OP_CTX_INFO.source = BINLOG_SOURCE_RPC_MASTER;
    OP_CTX_INFO.buff = SF_PROTO_RESP_BODY(task);
    SLICE_OP_CTX.rw_done_callback = (fs_rw_done_callback_func)
        du_handler_slice_read_done_callback;
    SLICE_OP_CTX.arg = task;
    if ((result=fs_slice_read(&SLICE_OP_CTX)) != 0) {
        TASK_CTX.common.log_level = result == ENOENT ? LOG_DEBUG : LOG_ERR;
        du_handler_set_slice_op_error_msg(task, &SLICE_OP_CTX,
                "slice read", result);
        sf_release_task(task);
        return result;
    }

    return TASK_STATUS_CONTINUE;
}

static int service_deal_get_master(struct fast_task_info *task)
{
    int result;
    int data_group_id;
    FSClusterDataGroupInfo *group;
    FSProtoGetServerResp *resp;
    FSClusterDataServerInfo *master;
    const FCAddressInfo *addr;

    if ((result=server_expect_body_length(4)) != 0) {
        return result;
    }

    data_group_id = buff2int(REQUEST.body);
    if ((group=fs_get_data_group(data_group_id)) == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data_group_id: %d not exist", data_group_id);
        return ENOENT;
    }
    master = (FSClusterDataServerInfo *)FC_ATOMIC_GET(group->master);
    if (master == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data group id: %d, the master NOT exist", data_group_id);
        return SF_RETRIABLE_ERROR_NO_SERVER;
    }

    resp = (FSProtoGetServerResp *)SF_PROTO_RESP_BODY(task);
    addr = fc_server_get_address_by_peer(&SERVICE_GROUP_ADDRESS_ARRAY(
                master->cs->server), task->client_ip);

    int2buff(master->cs->server->id, resp->server_id);
    snprintf(resp->ip_addr, sizeof(resp->ip_addr), "%s",
            addr->conn.ip_addr);
    short2buff(addr->conn.port, resp->port);

    RESPONSE.header.body_len = sizeof(FSProtoGetServerResp);
    RESPONSE.header.cmd = FS_SERVICE_PROTO_GET_MASTER_RESP;
    TASK_CTX.common.response_done = true;
    return 0;
}

static int service_deal_get_leader(struct fast_task_info *task)
{
    int result;
    FSProtoGetServerResp *resp;
    FSClusterServerInfo *leader;
    const FCAddressInfo *addr;

    if ((result=server_expect_body_length(0)) != 0) {
        return result;
    }

    leader = CLUSTER_LEADER_ATOM_PTR;
    if (leader == NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "the leader NOT exist");
        return SF_RETRIABLE_ERROR_NO_SERVER;
    }

    resp = (FSProtoGetServerResp *)SF_PROTO_RESP_BODY(task);
    addr = fc_server_get_address_by_peer(&SERVICE_GROUP_ADDRESS_ARRAY(
                leader->server), task->client_ip);

    int2buff(leader->server->id, resp->server_id);
    snprintf(resp->ip_addr, sizeof(resp->ip_addr), "%s",
            addr->conn.ip_addr);
    short2buff(addr->conn.port, resp->port);

    RESPONSE.header.body_len = sizeof(FSProtoGetServerResp);
    RESPONSE.header.cmd = SF_SERVICE_PROTO_GET_LEADER_RESP;
    TASK_CTX.common.response_done = true;
    return 0;
}


static int service_deal_cluster_stat(struct fast_task_info *task)
{
    int result;
    int status;
    bool is_master;
    FSClusterStatFilter filter;
    FSProtoClusterStatReq *req;
    FSProtoClusterStatRespBodyHeader *body_header;
    FSProtoClusterStatRespBodyPart *part_start;
    FSProtoClusterStatRespBodyPart *body_part;
    FSClusterDataGroupInfo *group;
    FSClusterDataGroupInfo *gstart;
    FSClusterDataGroupInfo *gend;
    FSClusterDataServerInfo *ds;
    FSClusterDataServerInfo *dend;
    char *p;
    const FCAddressInfo *addr;

    if ((result=server_expect_body_length(sizeof(
                        FSProtoClusterStatReq))) != 0)
    {
        return result;
    }

    req = (FSProtoClusterStatReq *)REQUEST.body;
    filter.filter_by = req->filter_by;
    filter.op_type = req->op_type;
    filter.status = req->status;
    filter.is_master = req->is_master;
    if ((filter.filter_by & FS_CLUSTER_STAT_FILTER_BY_GROUP) == 0) {
        gstart = CLUSTER_DATA_RGOUP_ARRAY.groups;
        gend = CLUSTER_DATA_RGOUP_ARRAY.groups +
            CLUSTER_DATA_RGOUP_ARRAY.count;
    } else {
        filter.filter_by &= ~FS_CLUSTER_STAT_FILTER_BY_GROUP;
        filter.data_group_id = buff2int(req->data_group_id);
        if ((gstart=fs_get_data_group(filter.data_group_id)) == NULL) {
            return ENOENT;
        }
        gend = gstart + 1;
    }

    body_header = (FSProtoClusterStatRespBodyHeader *)SF_PROTO_RESP_BODY(task);
    p = (char *)(body_header + 1);
    for (group=gstart; group<gend; group++) {
        int2buff(group->id, p);
        p += 4;
    }

    part_start = (FSProtoClusterStatRespBodyPart *)p;
    body_part = part_start;
    for (group=gstart; group<gend; group++) {
        dend = group->data_server_array.servers +
            group->data_server_array.count;
        for (ds=group->data_server_array.servers; ds<dend; ds++) {
            status = FC_ATOMIC_GET(ds->status);
            is_master = FC_ATOMIC_GET(ds->is_master);
            if (filter.filter_by > 0) {
                if ((filter.filter_by & FS_CLUSTER_STAT_FILTER_BY_IS_MASTER)) {
                    if (is_master != filter.is_master) {
                        continue;
                    }
                }

                if ((filter.filter_by & FS_CLUSTER_STAT_FILTER_BY_STATUS)) {
                    if (filter.op_type == '=') {
                        if (status != filter.status) {
                            continue;
                        }
                    } else if (filter.op_type == '!') {
                        if (status == filter.status) {
                            continue;
                        }
                    } else {
                        RESPONSE.error.length = sprintf(
                                RESPONSE.error.message,
                                "unkown op_type: %d", filter.op_type);
                        return EINVAL;
                    }
                }
            }

            addr = fc_server_get_address_by_peer(&SERVICE_GROUP_ADDRESS_ARRAY(
                        ds->cs->server), task->client_ip);

            int2buff(ds->dg->id, body_part->data_group_id);
            int2buff(ds->cs->server->id, body_part->server_id);
            snprintf(body_part->ip_addr, sizeof(body_part->ip_addr),
                    "%s", addr->conn.ip_addr);
            short2buff(addr->conn.port, body_part->port);
            body_part->is_preseted = ds->is_preseted;
            body_part->is_master = is_master;
            body_part->status = status;
            long2buff(ds->data.current_version, body_part->data_version);
            body_part++;
        }
    }

    int2buff(gend - gstart, body_header->dg_count);
    int2buff(body_part - part_start, body_header->ds_count);
    RESPONSE.header.body_len = (char *)body_part - SF_PROTO_RESP_BODY(task);
    RESPONSE.header.cmd = FS_SERVICE_PROTO_CLUSTER_STAT_RESP;
    TASK_CTX.common.response_done = true;
    return 0;
}

static int service_deal_disk_space_stat(struct fast_task_info *task)
{
    int result;
    FSProtoDiskSpaceStatRespBodyHeader *body_header;
    FSProtoDiskSpaceStatRespBodyPart *part_start;
    FSProtoDiskSpaceStatRespBodyPart *body_part;
    FSClusterServerInfo *cs;
    FSClusterServerInfo *end;

    if ((result=server_expect_body_length(0)) != 0) {
        return result;
    }

    body_header = (FSProtoDiskSpaceStatRespBodyHeader *)
        SF_PROTO_RESP_BODY(task);
    part_start = (FSProtoDiskSpaceStatRespBodyPart *)(body_header + 1);
    body_part = part_start;

	end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (cs=CLUSTER_SERVER_ARRAY.servers; cs<end; cs++, body_part++) {
        int2buff(cs->server->id, body_part->server_id);
        long2buff(cs->space_stat.total, body_part->total);
        long2buff(cs->space_stat.avail, body_part->avail);
        long2buff(cs->space_stat.used, body_part->used);

        /*
        logInfo("server id: %d, space_stat total: %"PRId64" MB, "
                "avail: %"PRId64" MB, used: %"PRId64 "MB", cs->server->id,
                cs->space_stat.total / (1024 * 1024), cs->space_stat.avail /
                (1024 * 1024), cs->space_stat.used / (1024 * 1024));
                */
    }

    int2buff(body_part - part_start, body_header->count);
    RESPONSE.header.body_len = (char *)body_part - SF_PROTO_RESP_BODY(task);
    RESPONSE.header.cmd = FS_SERVICE_PROTO_DISK_SPACE_STAT_RESP;
    TASK_CTX.common.response_done = true;
    return 0;
}

static int service_update_prepare_and_check(struct fast_task_info *task,
        const int resp_cmd)
{
    OP_CTX_INFO.deal_done = false;
    OP_CTX_INFO.is_update = true;

    if (SERVER_TASK_TYPE == SF_SERVER_TASK_TYPE_CHANNEL_USER &&
            IDEMPOTENCY_CHANNEL != NULL)
    {
        IdempotencyRequest *request;
        int result;

        request = sf_server_update_prepare_and_check(
                &REQUEST, &SERVER_CTX->service.request_allocator,
                IDEMPOTENCY_CHANNEL, &RESPONSE, &result);
        if (request != NULL) {
            if (result != 0) {
                if (result == EEXIST) { //found
                    result = request->output.result;
                    if (result == 0) {
                        du_handler_fill_slice_update_response(task,
                                ((FSUpdateOutput *)request->output.
                                 response)->inc_alloc);
                        RESPONSE.header.cmd = resp_cmd;
                    } else {
                        TASK_CTX.common.log_level = LOG_WARNING;
                    }
                }

                fast_mblock_free_object(request->allocator, request);
                OP_CTX_INFO.deal_done = true;
                return result;
            }
        } else {
            OP_CTX_INFO.deal_done = true;
            if (result == SF_RETRIABLE_ERROR_CHANNEL_INVALID) {
                TASK_CTX.common.log_level = LOG_DEBUG;
            }
            return result;
        }

        IDEMPOTENCY_REQUEST = request;
        REQUEST.body += sizeof(SFProtoIdempotencyAdditionalHeader);
        REQUEST.header.body_len -= sizeof(SFProtoIdempotencyAdditionalHeader);
    }

    OP_CTX_INFO.body = REQUEST.body;
    OP_CTX_INFO.body_len = REQUEST.header.body_len;

    TASK_CTX.which_side = FS_WHICH_SIDE_MASTER;
    OP_CTX_INFO.source = BINLOG_SOURCE_RPC_MASTER;
    OP_CTX_INFO.data_version = 0;
    SLICE_OP_CTX.update.space_changed = 0;

    return 0;
}

static inline int service_process_update(struct fast_task_info *task,
                deal_task_func real_update_func, const int resp_cmd)
{
    int result;

    result = service_update_prepare_and_check(task, resp_cmd);
    if (result != 0 || OP_CTX_INFO.deal_done) {
        return result;
    }

    if ((result=real_update_func(task, &SLICE_OP_CTX)) !=
            TASK_STATUS_CONTINUE)
    {
        du_handler_idempotency_request_finish(task, result);
    }

    return result;
}

static int service_check_priv(struct fast_task_info *task)
{
    FCFSAuthValidatePriviledgeType priv_type;
    int64_t the_priv;

    switch (REQUEST.header.cmd) {
            case SF_PROTO_ACTIVE_TEST_REQ:
            case FS_COMMON_PROTO_CLIENT_JOIN_REQ:
            case FS_SERVICE_PROTO_GET_MASTER_REQ:
            case FS_COMMON_PROTO_GET_READABLE_SERVER_REQ:
            case SF_SERVICE_PROTO_SETUP_CHANNEL_REQ:
            case SF_SERVICE_PROTO_CLOSE_CHANNEL_REQ:
            case SF_SERVICE_PROTO_REPORT_REQ_RECEIPT_REQ:
            case SF_SERVICE_PROTO_REBIND_CHANNEL_REQ:
            case SF_SERVICE_PROTO_GET_LEADER_REQ:
            case SF_SERVICE_PROTO_GET_GROUP_SERVERS_REQ:
            case SF_SERVER_TASK_TYPE_CHANNEL_HOLDER:
            case SF_SERVER_TASK_TYPE_CHANNEL_USER:
                return 0;

            case FS_SERVICE_PROTO_SLICE_WRITE_REQ:
            case FS_SERVICE_PROTO_SLICE_ALLOCATE_REQ:
            case FS_SERVICE_PROTO_SLICE_DELETE_REQ:
            case FS_SERVICE_PROTO_BLOCK_DELETE_REQ:
                priv_type = fcfs_auth_validate_priv_type_pool_fstore;
                the_priv = FCFS_AUTH_POOL_ACCESS_WRITE;
                break;

            case FS_SERVICE_PROTO_SLICE_READ_REQ:
                priv_type = fcfs_auth_validate_priv_type_pool_fstore;
                the_priv = FCFS_AUTH_POOL_ACCESS_READ;
                break;

            case FS_SERVICE_PROTO_SERVICE_STAT_REQ:
            case FS_SERVICE_PROTO_CLUSTER_STAT_REQ:
            case FS_SERVICE_PROTO_DISK_SPACE_STAT_REQ:
                priv_type = fcfs_auth_validate_priv_type_user;
                the_priv = FCFS_AUTH_USER_PRIV_MONITOR_CLUSTER;
                break;
            default:
                RESPONSE.error.length = sprintf(RESPONSE.error.message,
                        "unkown cmd: %d", REQUEST.header.cmd);
                return -EINVAL;
    }

    return fcfs_auth_for_server_check_priv(AUTH_CLIENT_CTX,
            &REQUEST, &RESPONSE, priv_type, the_priv);
}

static int service_process(struct fast_task_info *task)
{
    int result;

    switch (REQUEST.header.cmd) {
        case SF_PROTO_ACTIVE_TEST_REQ:
            RESPONSE.header.cmd = SF_PROTO_ACTIVE_TEST_RESP;
            return sf_proto_deal_active_test(task, &REQUEST, &RESPONSE);
        case FS_COMMON_PROTO_CLIENT_JOIN_REQ:
            return du_handler_deal_client_join(task);
        case FS_SERVICE_PROTO_SERVICE_STAT_REQ:
            return service_deal_service_stat(task);
        case FS_SERVICE_PROTO_SLICE_WRITE_REQ:
            return service_process_update(task, du_handler_deal_slice_write,
                    FS_SERVICE_PROTO_SLICE_WRITE_RESP);
        case FS_SERVICE_PROTO_SLICE_ALLOCATE_REQ:
            return service_process_update(task, du_handler_deal_slice_allocate,
                    FS_SERVICE_PROTO_SLICE_ALLOCATE_RESP);
        case FS_SERVICE_PROTO_SLICE_DELETE_REQ:
            return service_process_update(task, du_handler_deal_slice_delete,
                    FS_SERVICE_PROTO_SLICE_DELETE_RESP);
        case FS_SERVICE_PROTO_BLOCK_DELETE_REQ:
            return service_process_update(task, du_handler_deal_block_delete,
                    FS_SERVICE_PROTO_BLOCK_DELETE_RESP);
        case FS_SERVICE_PROTO_SLICE_READ_REQ:
            return service_deal_slice_read(task);
        case FS_SERVICE_PROTO_GET_MASTER_REQ:
            return service_deal_get_master(task);
        case FS_COMMON_PROTO_GET_READABLE_SERVER_REQ:
            return du_handler_deal_get_readable_server(task,
                    SERVICE_GROUP_INDEX);
        case SF_SERVICE_PROTO_GET_LEADER_REQ:
            return service_deal_get_leader(task);
        case FS_SERVICE_PROTO_CLUSTER_STAT_REQ:
            return service_deal_cluster_stat(task);
        case FS_SERVICE_PROTO_DISK_SPACE_STAT_REQ:
            return service_deal_disk_space_stat(task);
        case SF_SERVICE_PROTO_SETUP_CHANNEL_REQ:
            if ((result=sf_server_deal_setup_channel(task, &SERVER_TASK_TYPE,
                            CLUSTER_MY_SERVER_ID, &IDEMPOTENCY_CHANNEL,
                            &RESPONSE)) == 0)
            {
                TASK_CTX.common.response_done = true;
            }
            return result;
        case SF_SERVICE_PROTO_CLOSE_CHANNEL_REQ:
            return sf_server_deal_close_channel(task,
                    &SERVER_TASK_TYPE, &IDEMPOTENCY_CHANNEL, &RESPONSE);
        case SF_SERVICE_PROTO_REPORT_REQ_RECEIPT_REQ:
            return sf_server_deal_report_req_receipt(task,
                    SERVER_TASK_TYPE, IDEMPOTENCY_CHANNEL, &RESPONSE);
        case SF_SERVICE_PROTO_REBIND_CHANNEL_REQ:
            return sf_server_deal_rebind_channel(task,
                    &SERVER_TASK_TYPE, &IDEMPOTENCY_CHANNEL, &RESPONSE);
        case SF_SERVICE_PROTO_GET_GROUP_SERVERS_REQ:
            return du_handler_deal_get_group_servers(task);
        default:
            RESPONSE.error.length = sprintf(
                    RESPONSE.error.message,
                    "unkown cmd: %d", REQUEST.header.cmd);
            return -EINVAL;
    }
}

int service_deal_task(struct fast_task_info *task, const int stage)
{
    int result;

    /*
    logInfo("file: "__FILE__", line: %d, "
            "nio stage: %d, SF_NIO_STAGE_CONTINUE: %d", __LINE__,
            stage, SF_NIO_STAGE_CONTINUE);
            */

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
        if (AUTH_ENABLED) {
            if ((result=service_check_priv(task)) == 0) {
                result = service_process(task);
            }
        } else {
            result = service_process(task);
        }
    }

    if (result == TASK_STATUS_CONTINUE) {
        return 0;
    } else {
        RESPONSE_STATUS = result;
        return sf_proto_deal_task_done(task, &TASK_CTX.common);
    }
}

void *service_alloc_thread_extra_data(const int thread_index)
{
    FSServerContext *server_context;
    int element_size;

    if ((server_context=du_handler_alloc_server_context()) == NULL) {
        return NULL;
    }

    element_size = sizeof(IdempotencyRequest) + sizeof(FSUpdateOutput);
    if (fast_mblock_init_ex1(&server_context->service.request_allocator,
                "idempotency_request", element_size, 1024, 0,
                idempotency_request_alloc_init,
                &server_context->service.request_allocator, true) != 0)
    {
        return NULL;
    }
    return server_context;
}
