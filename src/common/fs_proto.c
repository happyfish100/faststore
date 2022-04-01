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


#include <errno.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/connection_pool.h"
#include "fastcommon/ini_file_reader.h"
#include "fs_types.h"
#include "fs_proto.h"

void fs_proto_init()
{
}

const char *fs_get_server_status_caption(const int status)
{
    switch (status) {
        case FS_DS_STATUS_INIT:
            return "INIT";
        case FS_DS_STATUS_REBUILDING:
            return "REBUILDING";
        case FS_DS_STATUS_OFFLINE:
            return "OFFLINE";
        case FS_DS_STATUS_RECOVERING:
            return "RECOVERING";
        case FS_DS_STATUS_ONLINE:
            return "ONLINE";
        case FS_DS_STATUS_ACTIVE:
            return "ACTIVE";
        default:
            return "UNKOWN";
    }
}

const char *fs_get_cmd_caption(const int cmd)
{
    switch (cmd) {
        case FS_COMMON_PROTO_CLIENT_JOIN_REQ:
            return "CLIENT_JOIN_REQ";
        case FS_COMMON_PROTO_CLIENT_JOIN_RESP:
            return "CLIENT_JOIN_RESP";
        case FS_SERVICE_PROTO_SERVICE_STAT_REQ:
            return "SERVICE_STAT_REQ";
        case FS_SERVICE_PROTO_SERVICE_STAT_RESP:
            return "SERVICE_STAT_RESP";
        case FS_SERVICE_PROTO_CLUSTER_STAT_REQ:
            return "CLUSTER_STAT_REQ";
        case FS_SERVICE_PROTO_CLUSTER_STAT_RESP:
            return "CLUSTER_STAT_RESP";
        case FS_SERVICE_PROTO_DISK_SPACE_STAT_REQ:
            return "DISK_SPACE_STAT_REQ";
        case FS_SERVICE_PROTO_DISK_SPACE_STAT_RESP:
            return "DISK_SPACE_STAT_RESP";
        case FS_SERVICE_PROTO_SLICE_WRITE_REQ:
            return "SLICE_WRITE_REQ";
        case FS_SERVICE_PROTO_SLICE_WRITE_RESP:
            return "SLICE_WRITE_RESP";
        case FS_SERVICE_PROTO_SLICE_READ_REQ:
            return "SLICE_READ_REQ";
        case FS_SERVICE_PROTO_SLICE_READ_RESP:
            return "SLICE_READ_RESP";
        case FS_SERVICE_PROTO_SLICE_ALLOCATE_REQ:
            return "SLICE_ALLOCATE_REQ";
        case FS_SERVICE_PROTO_SLICE_ALLOCATE_RESP:
            return "SLICE_ALLOCATE_RESP";
        case FS_SERVICE_PROTO_SLICE_DELETE_REQ:
            return "SLICE_DELETE_REQ";
        case FS_SERVICE_PROTO_SLICE_DELETE_RESP:
            return "SLICE_DELETE_RESP";
        case FS_SERVICE_PROTO_BLOCK_DELETE_REQ:
            return "BLOCK_DELETE_REQ";
        case FS_SERVICE_PROTO_BLOCK_DELETE_RESP:
            return "BLOCK_DELETE_RESP";
        case FS_SERVICE_PROTO_GET_MASTER_REQ:
            return "GET_MASTER_REQ";
        case FS_SERVICE_PROTO_GET_MASTER_RESP:
            return "GET_MASTER_RESP";
        case FS_COMMON_PROTO_GET_READABLE_SERVER_REQ:
            return "GET_READABLE_SERVER_REQ";
        case FS_COMMON_PROTO_GET_READABLE_SERVER_RESP:
            return "GET_READABLE_SERVER_RESP";
        case FS_CLUSTER_PROTO_GET_SERVER_STATUS_REQ:
            return "GET_SERVER_STATUS_REQ";
        case FS_CLUSTER_PROTO_GET_SERVER_STATUS_RESP:
            return "GET_SERVER_STATUS_RESP";
        case FS_CLUSTER_PROTO_REPORT_DS_STATUS_REQ:
            return "REPORT_DS_STATUS_REQ";
        case FS_CLUSTER_PROTO_REPORT_DS_STATUS_RESP:
            return "REPORT_DS_STATUS_RESP";
        case FS_CLUSTER_PROTO_JOIN_LEADER_REQ:
            return "JOIN_LEADER_REQ";
        case FS_CLUSTER_PROTO_JOIN_LEADER_RESP:
            return "JOIN_LEADER_RESP";
        case FS_CLUSTER_PROTO_PING_LEADER_REQ:
            return "PING_LEADER_REQ";
        case FS_CLUSTER_PROTO_PING_LEADER_RESP:
            return "PING_LEADER_RESP";
        case FS_CLUSTER_PROTO_REPORT_DISK_SPACE_REQ:
            return "REPORT_DISK_SPACE_REQ";
        case FS_CLUSTER_PROTO_REPORT_DISK_SPACE_RESP:
            return "REPORT_DISK_SPACE_RESP";
        case FS_CLUSTER_PROTO_ACTIVATE_SERVER:
            return "ACTIVATE_SERVER";
        case FS_CLUSTER_PROTO_PRE_SET_NEXT_LEADER:
            return "PRE_SET_NEXT_LEADER";
        case FS_CLUSTER_PROTO_COMMIT_NEXT_LEADER:
            return "COMMIT_NEXT_LEADER";
        case FS_CLUSTER_PROTO_UNSET_MASTER_REQ:
            return "UNSET_MASTER_REQ";
        case FS_CLUSTER_PROTO_UNSET_MASTER_RESP:
            return "UNSET_MASTER_RESP";
        case FS_CLUSTER_PROTO_GET_DS_STATUS_REQ:
            return "GET_DS_STATUS_REQ";
        case FS_CLUSTER_PROTO_GET_DS_STATUS_RESP:
            return "GET_DS_STATUS_RESP";
        case FS_CLUSTER_PROTO_PUSH_DATA_SERVER_STATUS:
            return "PUSH_DATA_SERVER_STATUS";
        case FS_REPLICA_PROTO_JOIN_SERVER_REQ:
            return "JOIN_SERVER_REQ";
        case FS_REPLICA_PROTO_JOIN_SERVER_RESP:
            return "JOIN_SERVER_RESP";
        case FS_REPLICA_PROTO_FETCH_BINLOG_FIRST_REQ:
            return "FETCH_BINLOG_FIRST_REQ";
        case FS_REPLICA_PROTO_FETCH_BINLOG_FIRST_RESP:
            return "FETCH_BINLOG_FIRST_RESP";
        case FS_REPLICA_PROTO_FETCH_BINLOG_NEXT_REQ:
            return "FETCH_BINLOG_NEXT_REQ";
        case FS_REPLICA_PROTO_FETCH_BINLOG_NEXT_RESP:
            return "FETCH_BINLOG_NEXT_RESP";
        case FS_REPLICA_PROTO_RPC_REQ:
            return "REPLICA_RPC_REQ";
        case FS_REPLICA_PROTO_RPC_RESP:
            return "REPLICA_RPC_RESP";
        case FS_REPLICA_PROTO_ACTIVE_CONFIRM_REQ:
            return "ACTIVE_CONFIRM_REQ";
        case FS_REPLICA_PROTO_ACTIVE_CONFIRM_RESP:
            return "ACTIVE_CONFIRM_RESP";
        case FS_REPLICA_PROTO_SLICE_READ_REQ:
            return "REPLICA_SLICE_READ_REQ";
        case FS_REPLICA_PROTO_SLICE_READ_RESP:
            return "REPLICA_SLICE_READ_RESP";
        default:
            return sf_get_cmd_caption(cmd);
    }
}
