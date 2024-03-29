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

//replica_handler.c

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
#include "sf/sf_service.h"
#include "sf/sf_global.h"
#include "common/fs_proto.h"
#include "server_global.h"
#include "server_func.h"
#include "server_binlog.h"
#include "server_storage.h"
#include "server_group_info.h"
#include "server_replication.h"
#include "cluster_topology.h"
#include "cluster_relationship.h"
#include "data_update_handler.h"
#include "common_handler.h"
#include "replica_handler.h"

int replica_handler_init()
{
    const int master_side_timeout = 600;
    int process_interval_ms;
    FSIdArray *id_array;

    id_array = fs_cluster_cfg_get_my_data_group_ids(&CLUSTER_CONFIG_CTX,
            CLUSTER_MYSELF_PTR->server->id);
    process_interval_ms = 1000 / id_array->count;
    if (process_interval_ms == 0) {
        process_interval_ms = 2;
    } else if (process_interval_ms % 2 == 1) {
        process_interval_ms++;
    }

    return idempotency_request_metadata_start(
            process_interval_ms, master_side_timeout);
}

int replica_handler_destroy()
{   
    return 0;
}

static inline int replica_alloc_reader(struct fast_task_info *task,
        const int server_type)
{
    REPLICA_READER = fc_malloc(sizeof(ServerBinlogReader));
    if (REPLICA_READER == NULL) {
        return ENOMEM;
    }
    SERVER_TASK_TYPE = server_type;
    return 0;
}

static inline void replica_release_reader(struct fast_task_info *task,
        const bool reader_inited)
{
    if (REPLICA_READER != NULL) {
        if (reader_inited) {
            if (SERVER_TASK_TYPE == FS_SERVER_TASK_TYPE_FETCH_BINLOG &&
                    binlog_reader_get_writer(REPLICA_READER) == NULL)
            {
                fc_delete_file(REPLICA_READER->filename);
            }
            binlog_reader_destroy(REPLICA_READER);
        }
        free(REPLICA_READER);
        REPLICA_READER = NULL;
    }
    SERVER_TASK_TYPE = SF_SERVER_TASK_TYPE_NONE;
}

static inline void desc_task_waiting_rpc_count(
        FSReplicaRPCResultEntry *rentry, const int err_no)
{
    FSServerTaskArg *task_arg;

    task_arg = (FSServerTaskArg *)rentry->waiting_task->arg;
    if (err_no == 0 && FC_ATOMIC_GET(task_arg->context.slice_op_ctx.
                info.myself->dg->replica_quorum.need_majority))
    {
        FC_ATOMIC_INC(task_arg->context.service.rpc.success_count);
    }

    if (__sync_sub_and_fetch(&task_arg->context.
                service.rpc.waiting_count, 1) == 0)
    {
        data_thread_notify((FSDataThreadContext *)
                task_arg->context.slice_op_ctx.arg);
    }
}

static inline void desc_replication_waiting_rpc_count(
        FSReplication *replication, const int err_no)
{
    FSReplicaRPCResultEntry *rentry;
    FSReplicaRPCResultEntry *rend;

    if (replication->context.caller.rpc_result_array.count == 0) {
        return;
    }

    rend = replication->context.caller.rpc_result_array.results +
        replication->context.caller.rpc_result_array.count;
    for (rentry=replication->context.caller.rpc_result_array.results;
            rentry<rend; rentry++)
    {
        desc_task_waiting_rpc_count(rentry, err_no);
    }
    replication->context.caller.rpc_result_array.count = 0;
}

int replica_recv_timeout_callback(struct fast_task_info *task)
{
    char formatted_ip[FORMATTED_IP_SIZE];
    if (SERVER_TASK_TYPE == FS_SERVER_TASK_TYPE_REPLICATION_CLIENT &&
            REPLICA_REPLICATION != NULL)
    {
        format_ip_address(task->client_ip, formatted_ip);
        logError("file: "__FILE__", line: %d, "
                "server id: %d, %s:%u, sock: %d, recv timeout",
                __LINE__, REPLICA_REPLICATION->peer->server->id,
                formatted_ip, task->port, task->event.fd);
        desc_replication_waiting_rpc_count(REPLICA_REPLICATION, ETIMEDOUT);
        return ETIMEDOUT;
    } else if (SERVER_TASK_TYPE == FS_SERVER_TASK_TYPE_REPLICATION_SERVER) {
        format_ip_address(task->client_ip, formatted_ip);
        logError("file: "__FILE__", line: %d, "
                "client %s:%u, sock: %d, recv timeout", __LINE__,
                formatted_ip, task->port, task->event.fd);
        return ETIMEDOUT;
    }

    return 0;
}

static inline void replica_offline_slave_data_servers(FSClusterServerInfo *peer)
{
    int count;
    cluster_topology_offline_slave_data_servers(peer, &count);
    if (count > 0) {
        logDebug("file: "__FILE__", line: %d, "
                "peer server id: %d, offline slave data server count: %d",
                __LINE__, peer->server->id, count);
    }
}

void replica_task_finish_cleanup(struct fast_task_info *task)
{
    FSReplication *replication;
    char formatted_ip[FORMATTED_IP_SIZE];

    switch (SERVER_TASK_TYPE) {
        case FS_SERVER_TASK_TYPE_REPLICATION_CLIENT:
            replication = REPLICA_REPLICATION;
            if (replication != NULL) {
                desc_replication_waiting_rpc_count(replication, ENOTCONN);
                switch (FC_ATOMIC_GET(replication->stage)) {
                    case FS_REPLICATION_STAGE_WAITING_JOIN_RESP:
                    case FS_REPLICATION_STAGE_SYNCING:
                        replication_processor_unbind(replication);
                        replica_offline_slave_data_servers(replication->peer);

                        format_ip_address(REPLICA_GROUP_ADDRESS_FIRST_IP(
                                    replication->peer->server), formatted_ip);
                        logInfo("file: "__FILE__", line: %d, "
                                "replication peer id: %d, %s:%u disconnected",
                                __LINE__, replication->peer->server->id,
                                formatted_ip, REPLICA_GROUP_ADDRESS_FIRST_PORT(
                                    replication->peer->server));
                        break;
                    default:
                        break;
                }
                REPLICA_REPLICATION = NULL;
            } else {
                logError("file: "__FILE__", line: %d, "
                        "mistake happen! task: %p, SERVER_TASK_TYPE: %d, "
                        "REPLICA_REPLICATION is NULL", __LINE__, task,
                        SERVER_TASK_TYPE);
            }
            if (TASK_PENDING_SEND_COUNT != 0) {
                TASK_PENDING_SEND_COUNT = 0;
            }
            SERVER_TASK_TYPE = SF_SERVER_TASK_TYPE_NONE;
            break;
        case FS_SERVER_TASK_TYPE_REPLICATION_SERVER:
            SERVER_TASK_TYPE = SF_SERVER_TASK_TYPE_NONE;
            break;
        case FS_SERVER_TASK_TYPE_FETCH_BINLOG:
        case FS_SERVER_TASK_TYPE_SYNC_BINLOG:
            if (REPLICA_READER == NULL) {
                logError("file: "__FILE__", line: %d, "
                        "mistake happen! task: %p, SERVER_TASK_TYPE: %d, "
                        "REPLICA_READER is NULL", __LINE__, task,
                        SERVER_TASK_TYPE);
            }
            replica_release_reader(task, true);
            REPLICA_UNTIL_OFFSET = 0;
            break;
        default:
            break;
    }

    if (REPLICA_REPLICATION != NULL) {
        logError("file: "__FILE__", line: %d, "
                "mistake happen! task: %p, SERVER_TASK_TYPE: %d, "
                "REPLICA_REPLICATION: %p != NULL", __LINE__, task,
                SERVER_TASK_TYPE, REPLICA_REPLICATION);
    }

    if (task->recv_body != NULL) {
        sf_release_task_shared_mbuffer(task);
    }

    sf_task_finish_clean_up(task);
}

static int check_peer_slave(struct fast_task_info *task,
        const int data_group_id, const int server_id,
        FSClusterDataServerInfo **peer)
{
    if ((*peer=fs_get_data_server(data_group_id, server_id)) == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data group id: %d, server id: %d not exist",
                data_group_id, server_id);
        return ENOENT;
    }
    if (FC_ATOMIC_GET((*peer)->is_master)) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data group id: %d, server id: %d is master",
                data_group_id, server_id);
        return EINVAL;
    }

    return 0;
}

static int fetch_binlog_check_peer(struct fast_task_info *task,
        const int data_group_id, const int server_id,
        const int catch_up, FSClusterDataServerInfo **peer)
{
    int status;
    int result;

    if ((result=check_peer_slave(task, data_group_id,
                    server_id, peer)) != 0)
    {
        return result;
    }

    status = FC_ATOMIC_GET((*peer)->status);
    if ((status == FS_DS_STATUS_ACTIVE) ||
            (status == FS_DS_STATUS_ONLINE && !catch_up))
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data group id: %d, server id: %d, "
                "unexpect data server status: %d (%s) or "
                "catch up: %d", data_group_id, server_id, status,
                fs_get_server_status_caption(status), catch_up);
        TASK_CTX.common.log_level = LOG_DEBUG;
        return EAGAIN;
    }

    return 0;
}

static int check_myself_master(struct fast_task_info *task,
        const int data_group_id, FSClusterDataServerInfo **myself)
{
    *myself = fs_get_my_data_server(data_group_id);
    if (*myself == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data group id: %d NOT belongs to me", data_group_id);
        return ENOENT;
    }

    if (!FC_ATOMIC_GET((*myself)->is_master)) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data group id: %d, i am NOT master", data_group_id);
        return EINVAL;
    }

    return 0;
}

static int fetch_binlog_output(struct fast_task_info *task, char *buff,
        const int body_header_size, const int resp_cmd)
{
    int result;
    int size;
    int read_bytes;
    FSProtoReplicaFetchBinlogRespBodyHeader *bheader;

    bheader = (FSProtoReplicaFetchBinlogRespBodyHeader *)
        SF_PROTO_SEND_BODY(task);
    size = SF_SEND_BUFF_END(task) - buff;
    result = binlog_reader_integral_read(REPLICA_READER,
            buff, size, &read_bytes);
    if (!(result == 0 || result == ENOENT)) {
        return result;
    }

    int2buff(read_bytes, bheader->binlog_length);
    if (size - read_bytes < FS_REPLICA_BINLOG_MAX_RECORD_SIZE) {
        bheader->is_last = false;
    } else {
        bheader->is_last = binlog_reader_is_last_file(REPLICA_READER);
    }

    RESPONSE.header.cmd = resp_cmd;
    RESPONSE.header.body_len = body_header_size + read_bytes;
    TASK_CTX.common.response_done = true;
    return 0;
}

static int replica_fetch_binlog_first_output(struct fast_task_info *task,
        FSClusterDataServerInfo *myself, const bool is_full_dump,
        const bool is_online, const uint32_t repl_version,
        const uint64_t until_version)
{
    FSProtoReplicaFetchBinlogFirstRespBodyHeader *body_header;
    char *buff;
    int result;

    body_header = (FSProtoReplicaFetchBinlogFirstRespBodyHeader *)
        SF_PROTO_SEND_BODY(task);
    buff = (char *)(body_header + 1);
    if ((result=fetch_binlog_output(task, buff, sizeof(*body_header),
                    FS_REPLICA_PROTO_FETCH_BINLOG_FIRST_RESP)) != 0)
    {
        return result;
    }

    body_header->is_restore = 0;
    body_header->is_full_dump = (is_full_dump ? 1 : 0);
    body_header->is_online = (is_online ? 1 : 0);
    int2buff(repl_version, body_header->repl_version);
    long2buff(until_version, body_header->until_version);
    return 0;
}

static int replica_fetch_binlog_next_output(struct fast_task_info *task)
{
    int result;
    char *buff;

    if (task->send.ptr->size < SF_CTX->net_buffer_cfg.max_buff_size) {
        if ((result=sf_set_task_send_max_buffer_size(task)) != 0) {
            return result;
        }
    }

    buff = SF_PROTO_SEND_BODY(task) + sizeof(
            FSProtoReplicaFetchBinlogNextRespBodyHeader);
    return fetch_binlog_output(task, buff,
            sizeof(FSProtoReplicaFetchBinlogNextRespBodyHeader),
            FS_REPLICA_PROTO_FETCH_BINLOG_NEXT_RESP);
}

static int replica_fetch_binlog_inconsistent_output(struct fast_task_info
        *task, const int data_group_id, const uint64_t last_data_version)
{
    int result;
    int binlog_length;
    FSProtoReplicaFetchBinlogFirstRespBodyHeader *body_header;
    char *buff;

    if (task->send.ptr->size < SF_CTX->net_buffer_cfg.max_buff_size) {
        if ((result=sf_set_task_send_max_buffer_size(task)) != 0) {
            return result;
        }
    }

    body_header = (FSProtoReplicaFetchBinlogFirstRespBodyHeader *)
        SF_PROTO_SEND_BODY(task);
    buff = (char *)(body_header + 1);
    result = replica_binlog_load_until_dv(data_group_id, last_data_version,
            buff, SF_SEND_BUFF_END(task) - buff, &binlog_length);
    if (result != 0) {
        return result;
    }

    memset(body_header, 0, sizeof(*body_header));
    body_header->is_restore = 1;
    body_header->common.is_last = 1;
    int2buff(binlog_length, body_header->common.binlog_length);
    RESPONSE.header.cmd = FS_REPLICA_PROTO_FETCH_BINLOG_FIRST_RESP;
    RESPONSE.header.body_len = sizeof(*body_header) + binlog_length;
    TASK_CTX.common.response_done = true;
    return 0;
}

static int replica_deal_fetch_binlog_first(struct fast_task_info *task)
{
    FSProtoReplicaFetchBinlogFirstReqHeader *rheader;
    FSClusterDataServerInfo *myself;
    FSClusterDataServerInfo *slave;
    uint64_t last_data_version;
    uint64_t my_data_version;
    uint64_t until_version;
    uint64_t first_unmatched_dv;
    string_t binlog;
    uint32_t repl_version;
    int data_group_id;
    int server_id;
    int result;
    bool is_online;
    bool is_full_dump;

    if ((result=server_check_min_body_length(sizeof(*rheader))) != 0) {
        return result;
    }

    rheader = (FSProtoReplicaFetchBinlogFirstReqHeader *)REQUEST.body;
    last_data_version = buff2long(rheader->last_data_version);
    data_group_id = buff2int(rheader->data_group_id);
    server_id = buff2int(rheader->server_id);
    binlog.len = buff2int(rheader->binlog_length);
    if (REQUEST.header.body_len != sizeof(*rheader) + binlog.len) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "body length: %d != expected: %d", REQUEST.header.body_len,
                (int)(sizeof(*rheader) + binlog.len));
        return EINVAL;
    }

    if ((result=fetch_binlog_check_peer(task, data_group_id,
                    server_id, rheader->catch_up, &slave)) != 0)
    {
        return result;
    }
    if ((result=check_myself_master(task, data_group_id, &myself)) != 0) {
        return result;
    }

    if (SERVER_TASK_TYPE != SF_SERVER_TASK_TYPE_NONE ||
            REPLICA_READER != NULL)
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "already in progress. task type: %d, have reader: %d",
                SERVER_TASK_TYPE, REPLICA_READER != NULL ? 1 : 0);
        return EALREADY;
    }

    my_data_version = FC_ATOMIC_GET(myself->data.current_version);
    if (last_data_version > my_data_version) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data group id: %d, binlog consistency check fail, "
                "slave %d 's data version: %"PRId64" > master's data "
                "version: %"PRId64, data_group_id, server_id,
                last_data_version, my_data_version);
        cluster_relationship_report_reselect_master_to_leader(myself);
        return SF_CLUSTER_ERROR_BINLOG_INCONSISTENT;
    }

    binlog.str = rheader->binlog;
    if ((result=replica_binlog_master_check_consistency(data_group_id,
                    &binlog, &first_unmatched_dv)) != 0)
    {
        char prompt[128];
        if (result == SF_CLUSTER_ERROR_BINLOG_INCONSISTENT) {
            SFBinlogWriterInfo *writer;
            uint64_t binlog_last_dv;

            replica_binlog_get_last_dv(data_group_id, &binlog_last_dv);
            writer = replica_binlog_get_writer(data_group_id);
            logError("file: "__FILE__", line: %d, "
                    "data group id: %d, slave id: %d, first unmatched "
                    "data version: %"PRId64", my current data version: "
                    "%"PRId64", my last data version in binlog: %"PRId64", "
                    "binlog writer next version: %"PRId64", binlog writer "
                    "waiting count: %d, binlog thread waiting count: %d",
                    __LINE__, data_group_id, server_id, first_unmatched_dv,
                    my_data_version, binlog_last_dv,
                    sf_binlog_writer_get_next_version(writer),
                    sf_binlog_writer_get_waiting_count(writer),
                    sf_binlog_writer_get_thread_waiting_count(
                        &REPLICA_BINLOG_WRITER_THREAD));
            return replica_fetch_binlog_inconsistent_output(task,
                    data_group_id, last_data_version);
        } else if (result == SF_CLUSTER_ERROR_BINLOG_MISSED) {
            sprintf(prompt, "replica binlog missed");
        } else {
            sprintf(prompt, "some mistake happen, "
                    "error code is %d", result);
        }
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data group id: %d, slave server id: %d, "
                "binlog consistency check fail, %s",
                data_group_id, server_id, prompt);
        return result;
    }

    result = replica_alloc_reader(task, FS_SERVER_TASK_TYPE_FETCH_BINLOG);
    if (result != 0) {
        return result;
    }

    is_full_dump = false;
    if ((result=replica_binlog_reader_init(REPLICA_READER,
                    data_group_id, last_data_version)) != 0)
    {
        if (result == EOVERFLOW) {
            my_data_version = FC_ATOMIC_GET(myself->data.current_version);
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "data group id: %d, slave server id: %d 's last data "
                    "version: %"PRId64" larger than the last of my binlog "
                    "file, my current data version: %"PRId64, data_group_id,
                    server_id, last_data_version, my_data_version);
            TASK_CTX.common.log_level = LOG_WARNING;
        } else if (result == SF_CLUSTER_ERROR_BINLOG_MISSED) {
            if (last_data_version == 0) {  //accept
                if ((result=replica_binlog_init_dump_reader(data_group_id,
                                server_id, REPLICA_READER)) == 0)
                {
                    is_full_dump = true;
                } else if (!(result == EAGAIN || result == EINPROGRESS)) {
                    RESPONSE.error.length = sprintf(
                            RESPONSE.error.message,
                            "internal server error for "
                            "dump replica binlog");
                }
            } else {
                RESPONSE.error.length = sprintf(
                        RESPONSE.error.message,
                        "replica binlog missed");
            }
        }

        if (result != 0) {
            replica_release_reader(task, false);
            return result;
        }
    }

    if (rheader->catch_up) {
        int old_status;
        FSReplication *replication;

        replication = replication_channel_get(slave);
        repl_version = FC_ATOMIC_GET(replication->version);
        if (!replication_channel_is_ready(replication)) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "data group id: %d, slave id: %d, the replica connection "
                    "NOT established!", data_group_id, server_id);
            TASK_CTX.common.log_level = LOG_WARNING;
            return EBUSY;
        }

        old_status = FC_ATOMIC_GET(slave->status);
        if (old_status == FS_DS_STATUS_ONLINE) {
            is_online = true;
        } else if (old_status == FS_DS_STATUS_REBUILDING ||
                old_status == FS_DS_STATUS_RECOVERING)
        {
            if (cluster_relationship_set_ds_status_ex(slave,
                        old_status, FS_DS_STATUS_ONLINE))
            {
                is_online = true;
            } else {
                is_online = (FC_ATOMIC_GET(slave->status) ==
                        FS_DS_STATUS_ONLINE);
            }
        } else {
            is_online = false;
        }

        if (!is_online) {
            replica_release_reader(task, true);
            return EAGAIN;
        }
    } else {
        repl_version = 0;
        is_online = false;
    }

    if (is_online) {
        until_version = FC_ATOMIC_GET(myself->data.current_version);
    } else {
        until_version = 0;
    }
    return replica_fetch_binlog_first_output(task, myself, is_full_dump,
            is_online, repl_version, until_version);
}

static int replica_deal_fetch_binlog_next(struct fast_task_info *task)
{
    int result;

    if ((result=server_expect_body_length(0)) != 0) {
        return result;
    }

    if (!(SERVER_TASK_TYPE == FS_SERVER_TASK_TYPE_FETCH_BINLOG &&
            REPLICA_READER != NULL))
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "please send cmd %d (%s) first",
                FS_REPLICA_PROTO_FETCH_BINLOG_FIRST_REQ,
                fs_get_cmd_caption(FS_REPLICA_PROTO_FETCH_BINLOG_FIRST_REQ));
        return EINVAL;
    }

    return replica_fetch_binlog_next_output(task);
}

static int replica_deal_query_binlog_info(struct fast_task_info *task)
{
    FSProtoReplicaQueryBinlogInfoReq *req;
    FSProtoReplicaQueryBinlogInfoResp *resp;
    FSClusterDataServerInfo *myself;
    SFBinlogFilePosition position;
    int data_group_id;
    int server_id;
    int start_index;
    int last_index;
    int64_t until_version;
    int64_t current_version;
    int result;

    RESPONSE.header.cmd = FS_REPLICA_PROTO_QUERY_BINLOG_INFO_RESP;
    if ((result=server_expect_body_length(sizeof(
                        FSProtoReplicaQueryBinlogInfoReq))) != 0)
    {
        return result;
    }

    req = (FSProtoReplicaQueryBinlogInfoReq *)REQUEST.body;
    data_group_id = buff2int(req->data_group_id);
    server_id = buff2int(req->server_id);
    until_version = buff2long(req->until_version);
    if ((result=check_myself_master(task, data_group_id, &myself)) != 0) {
        return result;
    }

    current_version = FC_ATOMIC_GET(myself->data.current_version);
    if (until_version > current_version) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data group id: %d, slave id: %d 's until version: %"PRId64
                " > current data version: %"PRId64, data_group_id, server_id,
                until_version, current_version);
        TASK_CTX.common.log_level = LOG_WARNING;
        return EOVERFLOW;
    }

    if ((result=replica_binlog_get_binlog_indexes(data_group_id,
            &start_index, &last_index)) != 0)
    {
        return result;
    }

    if ((result=replica_binlog_get_position_by_dv(data_group_id,
                    until_version, &position, false)) != 0)
    {
        return result;
    }

    resp = (FSProtoReplicaQueryBinlogInfoResp *)SF_PROTO_SEND_BODY(task);
    int2buff(start_index, resp->start_index);
    int2buff(position.index, resp->last_index);
    long2buff(position.offset, resp->last_size);

    RESPONSE.header.body_len = sizeof(*resp);
    TASK_CTX.common.response_done = true;
    return 0;
}

static int sync_binlog_output(struct fast_task_info *task)
{
    int result;
    int size;
    int read_bytes;

    size = task->send.ptr->size - sizeof(FSProtoHeader);
    result = binlog_reader_integral_read(REPLICA_READER,
            SF_PROTO_SEND_BODY(task), size, &read_bytes);
    if (!(result == 0 || result == ENOENT)) {
        return result;
    }

    if (REPLICA_UNTIL_OFFSET > 0 && REPLICA_READER->
            position.offset >= REPLICA_UNTIL_OFFSET)
    {
        RESPONSE.header.body_len = read_bytes - (REPLICA_READER->
                position.offset - REPLICA_UNTIL_OFFSET);
        RESPONSE.header.flags = FS_COMMON_PROTO_FLAGS_LAST_PKG;
    } else {
        RESPONSE.header.body_len = read_bytes;
        RESPONSE.header.flags = (read_bytes > 0 ? 0 :
                FS_COMMON_PROTO_FLAGS_LAST_PKG);
    }
    RESPONSE.header.cmd = FS_REPLICA_PROTO_SYNC_BINLOG_RESP;
    TASK_CTX.common.response_done = true;
    return 0;
}

static int replica_deal_sync_binlog_first(struct fast_task_info *task)
{
    FSProtoReplicaSyncBinlogFirstReq *req;
    FSClusterDataServerInfo *myself;
    char subdir_name[64];
    SFBinlogFilePosition position;
    int64_t binlog_size;
    int data_group_id;
    int result;

    if ((result=server_expect_body_length(sizeof(*req))) != 0) {
        return result;
    }

    req = (FSProtoReplicaSyncBinlogFirstReq *)REQUEST.body;
    data_group_id = buff2int(req->data_group_id);
    position.index = buff2int(req->binlog_index);
    binlog_size = buff2long(req->binlog_size);
    if ((result=check_myself_master(task, data_group_id, &myself)) != 0) {
        return result;
    }

    if (SERVER_TASK_TYPE != SF_SERVER_TASK_TYPE_NONE) {
        if (SERVER_TASK_TYPE == FS_SERVER_TASK_TYPE_SYNC_BINLOG &&
                REPLICA_READER != NULL)
        {
            binlog_reader_destroy(REPLICA_READER);
        } else {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "already in progress. task type: %d, have reader: %d",
                    SERVER_TASK_TYPE, REPLICA_READER != NULL ? 1 : 0);
            return EALREADY;
        }
    } else {
        result = replica_alloc_reader(task, FS_SERVER_TASK_TYPE_SYNC_BINLOG);
        if (result != 0) {
            return result;
        }
    }

    position.offset = 0;
    replica_binlog_get_subdir_name(subdir_name, data_group_id);
    if ((result=binlog_reader_init1(REPLICA_READER, subdir_name,
                    position.index, &position)) != 0)
    {
        replica_release_reader(task, false);
        return result;
    }

    REPLICA_UNTIL_OFFSET = binlog_size;
    return sync_binlog_output(task);
}

static int replica_deal_sync_binlog_next(struct fast_task_info *task)
{
    int result;

    if ((result=server_expect_body_length(0)) != 0) {
        return result;
    }

    if (!(SERVER_TASK_TYPE == FS_SERVER_TASK_TYPE_SYNC_BINLOG &&
                REPLICA_READER != NULL))
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "please send cmd %d (%s) first",
                FS_REPLICA_PROTO_SYNC_BINLOG_FIRST_REQ,
                fs_get_cmd_caption(FS_REPLICA_PROTO_SYNC_BINLOG_FIRST_REQ));
        return EINVAL;
    }

    if (task->send.ptr->size < SF_CTX->net_buffer_cfg.max_buff_size) {
        if ((result=sf_set_task_send_max_buffer_size(task)) != 0) {
            return result;
        }
    }

    return sync_binlog_output(task);
}

static int replica_deal_active_confirm(struct fast_task_info *task)
{
    FSProtoReplicaActiveConfirmReq *req;
    FSClusterDataServerInfo *myself;
    FSClusterDataServerInfo *slave;
    FSReplication *replication;
    int data_group_id;
    int server_id;
    uint32_t repl_version;
    uint32_t current_version;
    int status;
    int result;

    RESPONSE.header.cmd = FS_REPLICA_PROTO_ACTIVE_CONFIRM_RESP;
    if ((result=server_expect_body_length(sizeof(
                        FSProtoReplicaActiveConfirmReq))) != 0)
    {
        return result;
    }

    req = (FSProtoReplicaActiveConfirmReq *)REQUEST.body;
    data_group_id = buff2int(req->data_group_id);
    server_id = buff2int(req->server_id);
    repl_version = buff2int(req->repl_version);

    if ((result=check_myself_master(task, data_group_id, &myself)) != 0) {
        return result;
    }
    if ((result=check_peer_slave(task, data_group_id,
                    server_id, &slave)) != 0)
    {
        return result;
    }

    status = FC_ATOMIC_GET(slave->status);
    if (status != FS_DS_STATUS_ONLINE) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data group id: %d, slave id: %d, "
                "unexpect data server status: %d (%s), "
                "expect status: %d", data_group_id, server_id,
                status, fs_get_server_status_caption(status),
                FS_DS_STATUS_ONLINE);
        return EINVAL;
    }

    replication = replication_channel_get(slave);
    if (!replication_channel_is_ready(replication)) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data group id: %d, slave id: %d, the replica connection "
                "NOT established!", data_group_id, server_id);
        TASK_CTX.common.log_level = LOG_WARNING;
        return EBUSY;
    }

    current_version = FC_ATOMIC_GET(replication->version);
    if (repl_version != current_version) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data group id: %d, slave id: %d, the replication channel "
                "changed from verson %u to %u", data_group_id, server_id,
                repl_version, current_version);
        TASK_CTX.common.log_level = LOG_WARNING;
        return EBUSY;
    }

    return 0;
}

static int replica_deal_join_server_req(struct fast_task_info *task)
{
    int result;
    int server_id;
    int buffer_size;
    int replica_channels_between_two_servers;
    FSProtoJoinServerReq *req;
    FSClusterServerInfo *peer;
    char formatted_ip[FORMATTED_IP_SIZE];

    if ((result=server_expect_body_length(sizeof(
                        FSProtoJoinServerReq))) != 0)
    {
        return result;
    }

    req = (FSProtoJoinServerReq *)REQUEST.body;
    server_id = buff2int(req->server_id);
    buffer_size = buff2int(req->buffer_size);
    replica_channels_between_two_servers = buff2int(
            req->replica_channels_between_two_servers);
    if (buffer_size != SF_CTX->net_buffer_cfg.max_buff_size) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "peer task buffer size: %d != mine: %d",
                buffer_size, SF_CTX->net_buffer_cfg.max_buff_size);
        return EINVAL;
    }
    if (replica_channels_between_two_servers !=
            REPLICA_CHANNELS_BETWEEN_TWO_SERVERS)
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "replica_channels_between_two_servers: %d != mine: %d",
                replica_channels_between_two_servers,
                REPLICA_CHANNELS_BETWEEN_TWO_SERVERS);
        return EINVAL;
    }

    if ((result=handler_check_config_signs(task, server_id,
                    req->auth_enabled, &req->config_signs)) != 0)
    {
        return result;
    }

    peer = fs_get_server_by_id(server_id);
    if (peer == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "peer server id: %d not exist", server_id);
        return ENOENT;
    }
    if (SERVER_TASK_TYPE == FS_SERVER_TASK_TYPE_REPLICATION_SERVER) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "server id: %d already joined", server_id);
        return EEXIST;
    }

    SERVER_TASK_TYPE = FS_SERVER_TASK_TYPE_REPLICATION_SERVER;
    RESPONSE.header.cmd = FS_REPLICA_PROTO_JOIN_SERVER_RESP;

    format_ip_address(REPLICA_GROUP_ADDRESS_FIRST_IP(
                peer->server), formatted_ip);
    logInfo("file: "__FILE__", line: %d, "
            "replication peer id: %d, %s:%u join in",
            __LINE__, server_id, formatted_ip,
            REPLICA_GROUP_ADDRESS_FIRST_PORT(peer->server));
    return 0;
}

static int replica_deal_join_server_resp(struct fast_task_info *task)
{
    if (!(SERVER_TASK_TYPE == FS_SERVER_TASK_TYPE_REPLICATION_CLIENT &&
                REPLICA_REPLICATION != NULL))
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "unexpect cmd: %d", REQUEST.header.cmd);
        return EINVAL;
    }

    set_replication_stage(REPLICA_REPLICATION, FS_REPLICATION_STAGE_SYNCING);
    return 0;
}

static inline int replica_check_replication_client(struct fast_task_info *task)
{
    if (SERVER_TASK_TYPE != FS_SERVER_TASK_TYPE_REPLICATION_CLIENT) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "invalid task type: %d != %d", SERVER_TASK_TYPE,
                FS_SERVER_TASK_TYPE_REPLICATION_CLIENT);
        return EINVAL;
    }

    if (REPLICA_REPLICATION == NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "replica replication ptr is null");
        return EINVAL;
    }

    if (REPLICA_REPLICATION->last_net_comm_time != g_current_time) {
        REPLICA_REPLICATION->last_net_comm_time = g_current_time;
    }

    --TASK_PENDING_SEND_COUNT;
    return 0;
}

static inline int replica_check_replication_server(struct fast_task_info *task)
{
    if (SERVER_TASK_TYPE != FS_SERVER_TASK_TYPE_REPLICATION_SERVER) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "invalid task type: %d != %d", SERVER_TASK_TYPE,
                FS_SERVER_TASK_TYPE_REPLICATION_SERVER);
        return EINVAL;
    }

    return 0;
}

static int handle_rpc_req(struct fast_task_info *task, const int count)
{
    SFSharedMBuffer *mbuffer;
    FSProtoReplicaRPCReqBodyPart *body_part;
    FSSliceOpBufferContext *op_buffer_ctx;
    FSSliceOpContext *op_ctx;
    SFRequestMetadata metadata;
    int inc_alloc;
    int current_len;
    int last_index;
    int blen;
    int finish_count;
    int i;
    int result;

    finish_count = 0;
    __sync_add_and_fetch(&REPLICA_RPC_WAITING_COUNT, count);
    mbuffer = fc_list_entry(task->recv_body, SFSharedMBuffer, buff);
    TASK_CTX.which_side = FS_WHICH_SIDE_SLAVE;
    last_index = count - 1;
    current_len = sizeof(FSProtoReplicaRPCReqBodyHeader);
    for (i=0; i<count; i++) {
        body_part = (FSProtoReplicaRPCReqBodyPart *)
            (REQUEST.body + current_len);
        blen = buff2int(body_part->body_len);
        if (blen <= 0) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "rpc body length: %d <= 0, rpc count: %d", blen, count);
            result = EINVAL;
            break;
        }
        current_len += sizeof(*body_part) + blen;
        if (i < last_index) {
            if (REQUEST.header.body_len < current_len) {
                RESPONSE.error.length = sprintf(RESPONSE.error.message,
                        "body length: %d < %d, rpc count: %d, current: %d",
                        REQUEST.header.body_len, current_len, count, i + 1);
                result = EINVAL;
                break;
            }
        } else {
            if (REQUEST.header.body_len != current_len) {
                RESPONSE.error.length = sprintf(RESPONSE.error.message,
                        "body length: %d != %d, rpc count: %d",
                        REQUEST.header.body_len, current_len, count);
                result = EINVAL;
                break;
            }
        }

        if ((op_buffer_ctx=replication_callee_alloc_op_buffer_ctx(
                        SERVER_CTX)) == NULL)
        {
            result = ENOMEM;
            break;
        }
        op_ctx = &op_buffer_ctx->op_ctx;

        if (body_part->cmd == FS_SERVICE_PROTO_SLICE_WRITE_REQ) {
            op_buffer_ctx->op_ctx.mbuffer = mbuffer;
            sf_shared_mbuffer_hold(mbuffer);
        }

        op_ctx->info.deal_done = false;
        op_ctx->info.is_update = true;
        op_ctx->info.source = BINLOG_SOURCE_RPC_SLAVE;
        op_ctx->info.data_version = buff2long(body_part->data_version);
        if (op_ctx->info.data_version <= 0) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "invalid data version: %"PRId64", rpc count: %d, "
                    "current: %d", op_ctx->info.data_version, count, i + 1);
            result = EINVAL;
            break;
        }

        op_ctx->info.body = (char *)(body_part + 1);
        op_ctx->info.body_len = blen;
        switch (body_part->cmd) {
            case FS_SERVICE_PROTO_SLICE_WRITE_REQ:
                result = du_handler_deal_slice_write(task, op_ctx);
                break;
            case FS_SERVICE_PROTO_SLICE_ALLOCATE_REQ:
                result = du_handler_deal_slice_allocate(task, op_ctx);
                break;
            case FS_SERVICE_PROTO_SLICE_DELETE_REQ:
                result = du_handler_deal_slice_delete(task, op_ctx);
                break;
            case FS_SERVICE_PROTO_BLOCK_DELETE_REQ:
                result = du_handler_deal_block_delete(task, op_ctx);
                break;
            default:
                RESPONSE.error.length = sprintf(RESPONSE.error.message,
                        "unkown cmd: %d", body_part->cmd);
                result = EINVAL;
                break;
        }

        if (result == TASK_STATUS_CONTINUE) {
            if ((metadata.req_id=buff2long(body_part->req_id)) != 0) {
                metadata.data_version = op_ctx->info.data_version;
                inc_alloc = buff2int(body_part->inc_alloc);
                if ((result=idempotency_request_metadata_add(&op_ctx->
                                info.myself->dg->req_meta_ctx, &metadata,
                                inc_alloc)) != 0)
                {
                    break;
                }
            } else {
                result = 0;
            }
        } else {
            ++finish_count;
            if (body_part->cmd == FS_SERVICE_PROTO_SLICE_WRITE_REQ) {
                sf_shared_mbuffer_release(op_buffer_ctx->op_ctx.mbuffer);
            }
            replication_callee_free_op_buffer_ctx(SERVER_CTX, op_buffer_ctx);
            if (result != 0) {
                break;
            }
        }
    }

    sf_shared_mbuffer_release(mbuffer);
    task->recv_body = NULL;
    if (result != 0) {
        FC_ATOMIC_SET(REPLICA_RPC_WAITING_COUNT, 0);
        return result;
    }

    if (finish_count > 0) {
        if (__sync_sub_and_fetch(&REPLICA_RPC_WAITING_COUNT,
                    finish_count) == 0)
        {
            return 0;
        }
    }
    return TASK_STATUS_CONTINUE;
}

static int replica_deal_rpc_req(struct fast_task_info *task)
{
    FSProtoReplicaRPCReqBodyHeader *body_header;
    int result;
    int min_body_len;
    int count;

    if ((result=replica_check_replication_server(task)) != 0) {
        return result;
    }

    if ((result=server_check_min_body_length(
                    sizeof(FSProtoReplicaRPCReqBodyHeader) +
                    sizeof(FSProtoReplicaRPCReqBodyPart))) != 0)
    {
        return result;
    }

    body_header = (FSProtoReplicaRPCReqBodyHeader *)REQUEST.body;
    count = buff2int(body_header->count);
    if (count <= 0) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "rpc count: %d <= 0", count);
        return EINVAL;
    }

    min_body_len = sizeof(FSProtoReplicaRPCReqBodyHeader) +
        sizeof(FSProtoReplicaRPCReqBodyPart) * count;
    if (REQUEST.header.body_len < min_body_len) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "body length: %d < min length: %d, rpc count: %d",
                REQUEST.header.body_len, min_body_len, count);
        return EINVAL;
    }

    return handle_rpc_req(task, count);
}

static int replica_deal_rpc_resp(struct fast_task_info *task)
{
    int result;

    if (REQUEST.header.status != 0) {
        sf_proto_deal_ack(task, &REQUEST, &RESPONSE);
    } else if ((result=server_expect_body_length(0)) != 0) {
        return result;
    }

    if ((result=replica_check_replication_client(task)) != 0) {
        return result;
    }

    desc_replication_waiting_rpc_count(REPLICA_REPLICATION,
            REQUEST.header.status);
    return REQUEST.header.status;
}

static int replica_deal_slice_read(struct fast_task_info *task)
{
    int result;
    int slave_id;
    bool direct_read;
    FSProtoReplicaSliceReadReq *req;

    OP_CTX_INFO.deal_done = false;
    OP_CTX_INFO.is_update = false;
    RESPONSE.header.cmd = FS_REPLICA_PROTO_SLICE_READ_RESP;
    if ((result=server_expect_body_length(sizeof(
                        FSProtoReplicaSliceReadReq))) != 0)
    {
        return result;
    }

    req = (FSProtoReplicaSliceReadReq *)REQUEST.body;
    slave_id = buff2int(req->slave_id);
    if ((result=du_handler_parse_check_readable_block_slice(
                    task, &req->bs)) != 0)
    {
        return result;
    }

    if ((result=du_handler_check_size_for_read(task)) != 0) {
        return result;
    }

    if (data_thread_is_blocked(OP_CTX_INFO.myself->dg->id)) {
        direct_read = true;
    } else if (slave_id == 0) {
        direct_read = false;
    } else if (FC_ATOMIC_GET(OP_CTX_INFO.myself->is_master)) {
        /*
        FSClusterDataServerInfo *slave;
        if ((slave=fs_get_data_server_ex(OP_CTX_INFO.
                        myself->dg, slave_id)) == NULL)
        {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "data group id: %d, server id: %d not exist",
                    OP_CTX_INFO.myself->dg->id, slave_id);
            return ENOENT;
        }
        */
        direct_read = true;
    } else {
        direct_read = false;
    }

    sf_hold_task(task);
    OP_CTX_INFO.source = BINLOG_SOURCE_RPC_MASTER;
    OP_CTX_INFO.buff = SF_PROTO_SEND_BODY(task);
    if (direct_read) {
        SLICE_OP_CTX.rw_done_callback = (fs_rw_done_callback_func)
            du_handler_slice_read_done_callback;
        SLICE_OP_CTX.arg = task;
        result = fs_slice_read(&SLICE_OP_CTX);
    } else {
        OP_CTX_NOTIFY_FUNC = du_handler_slice_read_done_notify;
        result = push_to_data_thread_queue(DATA_OPERATION_SLICE_READ,
                DATA_SOURCE_MASTER_SERVICE, task, &SLICE_OP_CTX);
    }

    if (result != 0) {
        TASK_CTX.common.log_level = (result == ENOENT ? LOG_DEBUG : LOG_ERR);
        du_handler_set_slice_op_error_msg(task, &SLICE_OP_CTX,
                "replica slice read", result);
        sf_release_task(task);
        return result;
    }

    return TASK_STATUS_CONTINUE;
}

int replica_deal_task(struct fast_task_info *task, const int stage)
{
    int result;

    if (stage == SF_NIO_STAGE_HANDSHAKE) {
        result = replication_processor_join_server(task);
    } else if (stage == SF_NIO_STAGE_CONTINUE) {
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
        switch (REQUEST.header.cmd) {
            case SF_PROTO_ACTIVE_TEST_REQ:
                RESPONSE.header.cmd = SF_PROTO_ACTIVE_TEST_RESP;
                result = sf_proto_deal_active_test(task, &REQUEST, &RESPONSE);
                break;
            case SF_PROTO_ACTIVE_TEST_RESP:
                result = replica_check_replication_client(task);
                TASK_CTX.common.need_response = false;
                break;
            case SF_PROTO_ACK:
                result = sf_proto_deal_ack(task, &REQUEST, &RESPONSE);
                TASK_CTX.common.need_response = false;
                break;
            case FS_REPLICA_PROTO_JOIN_SERVER_REQ:
                if ((result=replica_deal_join_server_req(task)) > 0) {
                    result *= -1;  //force close connection
                }
                break;
            case FS_REPLICA_PROTO_JOIN_SERVER_RESP:
                if ((result=replica_deal_join_server_resp(task)) > 0 ) {
                    result *= -1;  //force close connection
                }
                TASK_CTX.common.need_response = false;
                break;
            case FS_COMMON_PROTO_CLIENT_JOIN_REQ:
                if ((result=du_handler_deal_client_join(task)) > 0) {
                    result *= -1;  //force close connection
                }
                break;
            case FS_COMMON_PROTO_GET_READABLE_SERVER_REQ:
                result = du_handler_deal_get_readable_server(task,
                        REPLICA_GROUP_INDEX);
                break;
            case SF_SERVICE_PROTO_GET_GROUP_SERVERS_REQ:
                result = du_handler_deal_get_group_servers(task);
                break;
            case FS_REPLICA_PROTO_RPC_CALL_REQ:
                result = replica_deal_rpc_req(task);
                RESPONSE.header.cmd = FS_REPLICA_PROTO_RPC_CALL_RESP;
                if (!(result == 0 || result == TASK_STATUS_CONTINUE)) {
                    if (result > 0) {
                        result *= -1;  //force close connection
                    }
                }
                break;
            case FS_REPLICA_PROTO_RPC_CALL_RESP:
                if ((result=replica_deal_rpc_resp(task)) != 0) {
                    if (result > 0) {
                        result *= -1;  //force close connection
                    }
                }
                TASK_CTX.common.need_response = false;
                break;
            case FS_REPLICA_PROTO_FETCH_BINLOG_FIRST_REQ:
                result = replica_deal_fetch_binlog_first(task);
                break;
            case FS_REPLICA_PROTO_FETCH_BINLOG_NEXT_REQ:
                result = replica_deal_fetch_binlog_next(task);
                break;
            case FS_REPLICA_PROTO_ACTIVE_CONFIRM_REQ:
                result = replica_deal_active_confirm(task);
                break;
            case FS_REPLICA_PROTO_SLICE_READ_REQ:
                result = replica_deal_slice_read(task);
                break;
            case FS_REPLICA_PROTO_QUERY_BINLOG_INFO_REQ:
                result = replica_deal_query_binlog_info(task);
                break;
            case FS_REPLICA_PROTO_SYNC_BINLOG_FIRST_REQ:
                result = replica_deal_sync_binlog_first(task);
                break;
            case FS_REPLICA_PROTO_SYNC_BINLOG_NEXT_REQ:
                result = replica_deal_sync_binlog_next(task);
                break;
            default:
                RESPONSE.error.length = sprintf(RESPONSE.error.message,
                        "unkown cmd: %d", REQUEST.header.cmd);
                result = -EINVAL;
                break;
        }

        if (task->recv_body != NULL) {
            sf_release_task_shared_mbuffer(task);
        }
    }

    if (result == TASK_STATUS_CONTINUE) {
        return 0;
    } else {
        RESPONSE_STATUS = result;
        return sf_proto_deal_task_done(task, "replica", &TASK_CTX.common);
    }
}

void *replica_alloc_thread_extra_data(const int thread_index)
{
    FSServerContext *server_ctx;

    if ((server_ctx=du_handler_alloc_server_context()) == NULL) {
        return NULL;
    }

    if (replication_alloc_connection_ptr_arrays(server_ctx) != 0) {
        return NULL;
    }

    if (replication_callee_init_allocator(server_ctx) != 0) {
        return NULL;
    }

    return server_ctx;
}

int replica_thread_loop_callback(struct nio_thread_data *thread_data)
{
    FSServerContext *server_ctx;
    //static int count = 0;

    server_ctx = (FSServerContext *)thread_data->arg;
    /*
    if (count++ % 100 == 0) {
        logInfo("thread index: %d, connectings.count: %d, "
                "connected.count: %d",
                SF_THREAD_INDEX(REPLICA_SF_CTX, thread_data),
                server_ctx->replica.connectings.count,
                server_ctx->replica.connected.count);
    }
    */
    replication_processor_process(server_ctx);
    return 0;
}
