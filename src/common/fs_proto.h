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

#ifndef _FS_PROTO_H
#define _FS_PROTO_H

#include "fastcommon/fast_task_queue.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/connection_pool.h"
#include "fastcommon/ini_file_reader.h"
#include "fastcommon/sockopt.h"
#include "sf/sf_proto.h"
#include "fs_types.h"

//service and replica commands
#define FS_COMMON_PROTO_CLIENT_JOIN_REQ           23
#define FS_COMMON_PROTO_CLIENT_JOIN_RESP          24
#define FS_COMMON_PROTO_GET_READABLE_SERVER_REQ   53
#define FS_COMMON_PROTO_GET_READABLE_SERVER_RESP  54

//service commands
#define FS_SERVICE_PROTO_SLICE_WRITE_REQ         25
#define FS_SERVICE_PROTO_SLICE_WRITE_RESP        26
#define FS_SERVICE_PROTO_SLICE_READ_REQ          27
#define FS_SERVICE_PROTO_SLICE_READ_RESP         28
#define FS_SERVICE_PROTO_SLICE_ALLOCATE_REQ      29
#define FS_SERVICE_PROTO_SLICE_ALLOCATE_RESP     30
#define FS_SERVICE_PROTO_SLICE_DELETE_REQ        31
#define FS_SERVICE_PROTO_SLICE_DELETE_RESP       32
#define FS_SERVICE_PROTO_BLOCK_DELETE_REQ        33
#define FS_SERVICE_PROTO_BLOCK_DELETE_RESP       34

#define FS_SERVICE_PROTO_SERVICE_STAT_REQ        41
#define FS_SERVICE_PROTO_SERVICE_STAT_RESP       42
#define FS_SERVICE_PROTO_CLUSTER_STAT_REQ        43
#define FS_SERVICE_PROTO_CLUSTER_STAT_RESP       44
#define FS_SERVICE_PROTO_DISK_SPACE_STAT_REQ     45
#define FS_SERVICE_PROTO_DISK_SPACE_STAT_RESP    46

#define FS_SERVICE_PROTO_GET_MASTER_REQ          51
#define FS_SERVICE_PROTO_GET_MASTER_RESP         52

//cluster commands
#define FS_CLUSTER_PROTO_GET_SERVER_STATUS_REQ  \
    SF_CLUSTER_PROTO_GET_SERVER_STATUS_REQ
#define FS_CLUSTER_PROTO_GET_SERVER_STATUS_RESP \
    SF_CLUSTER_PROTO_GET_SERVER_STATUS_RESP
#define FS_CLUSTER_PROTO_RESELECT_MASTER_REQ     61
#define FS_CLUSTER_PROTO_RESELECT_MASTER_RESP    62
#define FS_CLUSTER_PROTO_REPORT_DS_STATUS_REQ    63  //report data server status
#define FS_CLUSTER_PROTO_REPORT_DS_STATUS_RESP   64
#define FS_CLUSTER_PROTO_JOIN_LEADER_REQ         65
#define FS_CLUSTER_PROTO_JOIN_LEADER_RESP        66
#define FS_CLUSTER_PROTO_ACTIVATE_SERVER         67
#define FS_CLUSTER_PROTO_PING_LEADER_REQ         69
#define FS_CLUSTER_PROTO_PING_LEADER_RESP        70
#define FS_CLUSTER_PROTO_REPORT_DISK_SPACE_REQ   71
#define FS_CLUSTER_PROTO_REPORT_DISK_SPACE_RESP  72
#define FS_CLUSTER_PROTO_PRE_SET_NEXT_LEADER     73  //notify next leader to other servers
#define FS_CLUSTER_PROTO_COMMIT_NEXT_LEADER      74  //commit next leader to other servers
#define FS_CLUSTER_PROTO_UNSET_MASTER_REQ        75
#define FS_CLUSTER_PROTO_UNSET_MASTER_RESP       76
#define FS_CLUSTER_PROTO_GET_DS_STATUS_REQ       77
#define FS_CLUSTER_PROTO_GET_DS_STATUS_RESP      78
#define FS_CLUSTER_PROTO_PUSH_DS_STATUS_REQ      79
#define FS_CLUSTER_PROTO_PUSH_DS_STATUS_RESP     80

//replication commands
#define FS_REPLICA_PROTO_JOIN_SERVER_REQ         81
#define FS_REPLICA_PROTO_JOIN_SERVER_RESP        82

//slave -> master
#define FS_REPLICA_PROTO_FETCH_BINLOG_FIRST_REQ  83
#define FS_REPLICA_PROTO_FETCH_BINLOG_FIRST_RESP 84
#define FS_REPLICA_PROTO_FETCH_BINLOG_NEXT_REQ   85
#define FS_REPLICA_PROTO_FETCH_BINLOG_NEXT_RESP  86
#define FS_REPLICA_PROTO_ACTIVE_CONFIRM_REQ      87
#define FS_REPLICA_PROTO_ACTIVE_CONFIRM_RESP     88
#define FS_REPLICA_PROTO_SLICE_READ_REQ          89
#define FS_REPLICA_PROTO_SLICE_READ_RESP         90

#define FS_REPLICA_PROTO_QUERY_BINLOG_INFO_REQ   91
#define FS_REPLICA_PROTO_QUERY_BINLOG_INFO_RESP  92
#define FS_REPLICA_PROTO_SYNC_BINLOG_FIRST_REQ   93
#define FS_REPLICA_PROTO_SYNC_BINLOG_NEXT_REQ    95
#define FS_REPLICA_PROTO_SYNC_BINLOG_RESP        96

// master -> slave RPC
#define FS_REPLICA_PROTO_RPC_CALL_REQ            99
#define FS_REPLICA_PROTO_RPC_CALL_RESP          100

typedef SFCommonProtoHeader  FSProtoHeader;

typedef struct fs_proto_client_join_req {
    char data_group_count[4];
    char file_block_size[4];
    char flags[4];
    struct {
        char channel_id[4];
        char key[4];
    } idempotency;

    char auth_enabled;
    char padding[3];

    FSClusterMD5Digests cluster_cfg_signs;
} FSProtoClientJoinReq;

typedef struct fs_proto_client_join_resp {
    char buffer_size[4];
    char padding[4];
} FSProtoClientJoinResp;

typedef struct fs_proto_block_key {
    char oid[8];     //object id
    char offset[8];  //aligned by block size
} FSProtoBlockKey;

typedef struct fs_proto_slice_size {
    char offset[4];  //offset within the block
    char length[4];
} FSProtoSliceSize;

typedef struct fs_proto_block_slice {
    FSProtoBlockKey  bkey;
    FSProtoSliceSize slice_size;
} FSProtoBlockSlice;

typedef struct fs_proto_slice_write_req_header {
    FSProtoBlockSlice bs;
} FSProtoSliceWriteReqHeader;

typedef struct fs_proto_slice_update_resp {
    char inc_alloc[4];   //increase alloc space in bytes
    char padding[4];
} FSProtoSliceUpdateResp;

typedef struct fs_proto_slice_allocate_req {
    FSProtoBlockSlice bs;
} FSProtoSliceAllocateReq;

typedef struct fs_proto_slice_delete_req {
    FSProtoBlockSlice bs;
} FSProtoSliceDeleteReq;

typedef struct fs_proto_block_delete_req {
    FSProtoBlockKey bkey;
} FSProtoBlockDeleteReq;

typedef struct fs_proto_service_slice_read_req{
    FSProtoBlockSlice bs;
} FSProtoServiceSliceReadReq;

typedef struct fs_proto_replica_slice_read_req {
    char slave_id[4];
    char padding[4];
    FSProtoBlockSlice bs;
} FSProtoReplicaSliceReadReq;

typedef SFProtoConfigSigns FSProtoConfigSigns;

typedef SFProtoGetServerStatusReq FSProtoGetServerStatusReq;

typedef struct fs_proto_get_server_status_resp {
    char server_id[4];
    char up_time[4];
    char version[8];
    char last_heartbeat_time[4];
    char last_shutdown_time[4];
    char is_leader;
    char leader_hint;
    char force_election;
    char padding[1];
} FSProtoGetServerStatusResp;

typedef struct fs_proto_report_ds_status_req {
    char my_server_id[4];
    char ds_server_id[4];
    char data_group_id[4];
    char status;
    char padding[3];
} FSProtoReportDSStatusReq;

typedef struct fs_proto_reselect_master_req {
    char data_group_id[4];
    char server_id[4];
} FSProtoReselectMasterReq;

typedef struct fs_proto_service_stat_req {
    char data_group_id[4];   //0 for slice binlog
} FSProtoServiceStatReq;

typedef struct fs_proto_service_ob_slice_stat {
    char total_count[8];
    char cached_count[8];
    char element_used[8];
} FSProtoServiceOBSliceStat;

typedef struct fs_proto_service_stat_resp {
    char up_time[4];
    char server_id[4];
    char is_leader;
    char auth_enabled;
    char padding1[1];
    struct {
        char enabled;
        char current_version[8];
    } storage_engine;

    char padding2[5];
    struct {
        char len;
        char str[10];
    } version;

    struct {
        char current_count[4];
        char max_count[4];
    } connection;

    struct {
        char current_version[8];
        struct {
            char total_count[8];
            char next_version[8];
            char waiting_count[4];
            char max_waitings[4];
        } writer;
    } binlog;

    struct {
        FSProtoServiceOBSliceStat ob;
        FSProtoServiceOBSliceStat slice;
    } data;

} FSProtoServiceStatResp;

typedef struct fs_proto_cluster_stat_req {
    char data_group_id[4];  //0 for all data groups
    char filter_by;
    char op_type;  //including =, !
    char status;
    char is_master;
} FSProtoClusterStatReq;

typedef struct fs_proto_cluster_stat_resp_body_header {
    char dg_count[4];  //data group count
    char ds_count[4];  //data server count
} FSProtoClusterStatRespBodyHeader;

typedef struct fs_proto_cluster_stat_resp_body_part {
    char data_group_id[4];
    char server_id[4];
    struct {
        char current[8];
        char confirmed[8];
    } data_versions;
    char ip_addr[IP_ADDRESS_SIZE];
    char port[2];
    char is_preseted;
    char is_master;
    char status;
    char padding[3];
} FSProtoClusterStatRespBodyPart;

typedef struct fs_proto_disk_space_stat_resp_body_header {
    char count[4];
    char padding[4];
} FSProtoDiskSpaceStatRespBodyHeader;

typedef struct fs_proto_disk_space_stat_resp_body_part {
    char server_id[4];
    char padding[4];
    char total[8];
    char used[8];
    char avail[8];
} FSProtoDiskSpaceStatRespBodyPart;

typedef struct fs_proto_get_readable_server_req {
    char data_group_id[4];
    char read_rule;
    char padding[3];
} FSProtoGetReadableServerReq;

/* for FS_SERVICE_PROTO_GET_MASTER_RESP and
   FS_COMMON_PROTO_GET_READABLE_SERVER_RESP
   */
typedef struct fs_proto_get_server_resp {
    char ip_addr[IP_ADDRESS_SIZE];
    char server_id[4];
    char port[2];
    char padding[2];
} FSProtoGetServerResp;

typedef struct fs_proto_get_slaves_resp_body_header {
    char count[2];
    char padding[6];
} FSProtoGetSlavesRespBodyHeader;

typedef struct fs_proto_get_slaves_resp_body_part {
    char ip_addr[IP_ADDRESS_SIZE];
    char server_id[4];
    char port[2];
    char status;
    char padding[1];
} FSProtoGetSlavesRespBodyPart;

typedef struct fs_proto_join_leader_req {
    char server_id[4];   //the follower server id
    char auth_enabled;
    char padding[3];
    char key[8];         //for leader call follower to unset master
    FSProtoConfigSigns config_signs;
} FSProtoJoinLeaderReq;

typedef struct fs_proto_join_leader_resp {
    char leader_version[8];  //for check leader generation
} FSProtoJoinLeaderResp;

typedef struct fs_proto_join_server_req {
    char server_id[4];   //the server id
    char auth_enabled;
    char padding[3];
    char buffer_size[4]; //the task size
    char replica_channels_between_two_servers[4];
    FSProtoConfigSigns config_signs;
} FSProtoJoinServerReq;

typedef struct fs_proto_unset_master_req {
    char data_group_id[4];
    char leader_id[4];
    char key[8];
} FSProtoUnsetMasterReq;

typedef struct fs_proto_get_ds_status_req {
    char data_group_id[4];
} FSProtoGetDSStatusReq;

typedef struct fs_proto_get_ds_status_resp {
    char is_master;
    char status;
    char padding[2];
    char master_dealing_count[4];
    char data_version[8];
} FSProtoGetDSStatusResp;

typedef struct fs_proto_push_data_server_status_header  {
    char current_version[8];
    char data_server_count[4];
    char padding[4];
} FSProtoPushDataServerStatusHeader;

typedef struct fs_proto_push_data_server_status_body_part {
    char data_group_id[4];
    char server_id[4];
    struct {
        char current[8];
        char confirmed[8];
    } data_versions;
    char is_master;
    char status;
    char padding[6];
} FSProtoPushDataServerStatusBodyPart;

typedef struct fs_proto_ping_leader_req_header  {
    char leader_version[8];  //for check leader generation
    char data_group_count[4];
    char padding[4];
} FSProtoPingLeaderReqHeader;

typedef struct fs_proto_ping_leader_req_body_part {
    struct {
        char current[8];
        char confirmed[8];
    } data_versions;
    char data_group_id[4];
    char status;
    char padding[3];
} FSProtoPingLeaderReqBodyPart;

typedef struct fs_proto_report_disk_space_req {
    char total[8];
    char used[8];
    char avail[8];
} FSProtoReportDiskSpaceReq;

typedef struct fs_proto_replia_fetch_binlog_first_req_header {
    char last_data_version[8]; //NOT including
    char data_group_id[4];
    char server_id[4];
    char binlog_length[4]; //last N rows for consistency check
    char catch_up;         //tell master to ONLINE me
    char padding[3];
    char binlog[0];
} FSProtoReplicaFetchBinlogFirstReqHeader;

typedef struct fs_proto_replia_fetch_binlog_resp_body_header {
    char binlog_length[4]; //current binlog length
    char is_last;          //is the last package
} FSProtoReplicaFetchBinlogRespBodyHeader;

typedef struct fs_proto_replia_fetch_binlog_first_resp_body_header {
    FSProtoReplicaFetchBinlogRespBodyHeader common;
    char is_restore;        //binlog for restore when not consistent
    char is_full_dump;      //full dump because old binlogs be deleted
    char is_online;         //tell slave to ONLINE
    char repl_version[4];   //master replication version for check
    char padding[4];
    char until_version[8];  //for catch up master (including)
    char binlog[0];
} FSProtoReplicaFetchBinlogFirstRespBodyHeader;

typedef struct fs_proto_replia_fetch_binlog_next_resp_body_header {
    FSProtoReplicaFetchBinlogRespBodyHeader common;
    char padding[3];
    char binlog[0];
} FSProtoReplicaFetchBinlogNextRespBodyHeader;

typedef struct fs_proto_replia_query_binlog_info_req {
    char data_group_id[4];
    char server_id[4];
    char until_version[8];
} FSProtoReplicaQueryBinlogInfoReq;

typedef struct fs_proto_replia_query_binlog_info_resp {
    char start_index[4];
    char last_index[4];
    char last_size[8];
} FSProtoReplicaQueryBinlogInfoResp;

typedef struct fs_proto_replia_sync_binlog_first_req {
    char data_group_id[4];
    char binlog_index[4];
    char binlog_size[8];   //0 for end fo file
} FSProtoReplicaSyncBinlogFirstReq;

typedef struct fs_proto_replia_active_confirm_req {
    char data_group_id[4];
    char server_id[4];
    char repl_version[4];  //master replication version for check
} FSProtoReplicaActiveConfirmReq;

typedef struct fs_proto_replica_rpc_req_body_header {
    char count[4];
    char padding[4];
} FSProtoReplicaRPCReqBodyHeader;

typedef struct fs_proto_replica_rpc_req_body_part {
    char req_id[8];
    char data_version[8];
    char body_len[4];
    char inc_alloc[4];
    unsigned char cmd;
    char padding[7];
    char body[0];
} FSProtoReplicaRPCReqBodyPart;

typedef struct fs_proto_replica_rpc_resp_body_header {
    char count[4];
    char padding[4];
} FSProtoReplicaRPCRespBodyHeader;

typedef struct fs_proto_replica_rpc_resp_body_part {
    char data_version[8];
    char data_group_id[4];
    char err_no[2];
    char padding[2];
} FSProtoReplicaRPCRespBodyPart;

#define fs_log_network_error_ex(response, conn, result, log_level) \
    sf_log_network_error_ex(response, conn, "fstore", result, log_level)

#define fs_log_network_error(response, conn, result) \
    sf_log_network_error(response, conn, "fstore", result)

#define fs_log_network_error_for_update(response, conn, result) \
    sf_log_network_error_for_update(response, conn, "fstore", result)

#define fs_log_network_error_for_delete(response, \
        conn, result, enoent_log_level) \
    sf_log_network_error_for_delete(response, conn, \
            "fstore", result, enoent_log_level)

#ifdef __cplusplus
extern "C" {
#endif

void fs_proto_init();

const char *fs_get_server_status_caption(const int status);

const char *fs_get_cmd_caption(const int cmd);

#ifdef __cplusplus
}
#endif

#endif
