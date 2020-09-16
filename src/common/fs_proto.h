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

#define FS_STATUS_LEADER_INCONSISTENT     9999


#define FS_SERVICE_PROTO_CLIENT_JOIN_REQ         23
#define FS_SERVICE_PROTO_CLIENT_JOIN_RESP        24

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

#define FS_SERVICE_PROTO_GET_MASTER_REQ           45
#define FS_SERVICE_PROTO_GET_MASTER_RESP          46
#define FS_SERVICE_PROTO_GET_READABLE_SERVER_REQ  49
#define FS_SERVICE_PROTO_GET_READABLE_SERVER_RESP 50

//cluster commands
#define FS_CLUSTER_PROTO_GET_SERVER_STATUS_REQ   61
#define FS_CLUSTER_PROTO_GET_SERVER_STATUS_RESP  62
#define FS_CLUSTER_PROTO_REPORT_DS_STATUS_REQ    63  //report data server status
#define FS_CLUSTER_PROTO_REPORT_DS_STATUS_RESP   64
#define FS_CLUSTER_PROTO_JOIN_LEADER_REQ         65
#define FS_CLUSTER_PROTO_JOIN_LEADER_RESP        66
#define FS_CLUSTER_PROTO_ACTIVATE_SERVER         67
#define FS_CLUSTER_PROTO_PING_LEADER_REQ         69
#define FS_CLUSTER_PROTO_PING_LEADER_RESP        70
#define FS_CLUSTER_PROTO_PRE_SET_NEXT_LEADER     75  //notify next leader to other servers
#define FS_CLUSTER_PROTO_COMMIT_NEXT_LEADER      76  //commit next leader to other servers
#define FS_CLUSTER_PROTO_PUSH_DATA_SERVER_STATUS 79

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

// master -> slave RPC
#define FS_REPLICA_PROTO_RPC_REQ                 99
#define FS_REPLICA_PROTO_RPC_RESP               100

typedef SFCommonProtoHeader  FSProtoHeader;

typedef struct fs_proto_client_join_req {
    char data_group_count[4];
    char file_block_size[4];
    char flags[4];
    struct {
        char channel_id[4];
        char key[4];
    } idempotency;
    char padding[4];
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

typedef struct fs_proto_idempotency_additional_header {
    char req_id[8];
} FSProtoIdempotencyAdditionalHeader;

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

typedef struct fs_proto_slice_read_req_header {
    FSProtoBlockSlice bs;
} FSProtoSliceReadReqHeader;

typedef struct {
    unsigned char servers[16];
    unsigned char cluster[16];
} FSProtoConfigSigns;

typedef struct fs_proto_get_server_status_req {
    FSProtoConfigSigns config_signs;
    char server_id[4];
    char padding[4];
} FSProtoGetServerStatusReq;

typedef struct fs_proto_get_server_status_resp {
    char server_id[4];
    char up_time[4];
    char version[8];
    char last_shutdown_time[4];
    char is_leader;
    char padding[3];
} FSProtoGetServerStatusResp;

typedef struct fs_proto_report_ds_status_req {
    char my_server_id[4];
    char ds_server_id[4];
    char data_group_id[4];
    char status;
    char padding[3];
} FSProtoReportDSStatusReq;

typedef struct fs_proto_service_stat_resp {
    char server_id[4];
    char is_leader;

    struct {
        char current_count[4];
        char max_count[4];
    } connection;

} FSProtoServiceStatResp;

typedef struct fs_proto_cluster_stat_resp_body_header {
    char count[4];
} FSProtoClusterStatRespBodyHeader;

typedef struct fs_proto_cluster_stat_resp_body_part {
    char data_group_id[4];
    char server_id[4];
    char data_version[8];
    char ip_addr[IP_ADDRESS_SIZE];
    char port[2];
    char is_preseted;
    char is_master;
    char status;
    char padding[4];
} FSProtoClusterStatRespBodyPart;

typedef struct fs_proto_get_readable_server_req {
    char data_group_id[4];
    char read_rule;
    char padding[3];
} FSProtoGetReadableServerReq;

/* for FS_SERVICE_PROTO_GET_MASTER_RESP and
   FS_SERVICE_PROTO_GET_READABLE_SERVER_RESP
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
    char server_id[4];     //the slave server id
    FSProtoConfigSigns config_signs;
} FSProtoJoinLeaderReq;

typedef struct fs_proto_join_server_req {
    char server_id[4];   //the server id
    char buffer_size[4]; //the task size
    char replica_channels_between_two_servers[4];
    FSProtoConfigSigns config_signs;
} FSProtoJoinServerReq;

typedef struct fs_proto_join_server_resp {
} FSProtoJoinServerResp;

typedef struct fs_proto_push_data_server_status_header  {
    char current_version[8];
    char data_server_count[4];
    char padding[4];
} FSProtoPushDataServerStatusHeader;

typedef struct fs_proto_push_data_server_status_body_part {
    char data_group_id[4];
    char server_id[4];
    char data_version[8];
    char is_master;
    char status;
    char padding[6];
} FSProtoPushDataServerStatusBodyPart;

typedef struct fs_proto_ping_leader_req_header  {
    char data_group_count[4];
    char padding[4];
} FSProtoPingLeaderReqHeader;

typedef struct fs_proto_ping_leader_req_body_part {
    char data_version[8];
    char data_group_id[4];
    char status;
    char padding[3];
} FSProtoPingLeaderReqBodyPart;

typedef struct fs_proto_replia_fetch_binlog_first_req {
    char last_data_version[8]; //NOT including
    char data_group_id[4];
    char server_id[4];
    char catch_up;  //tell master to ONLINE me
    char padding[7];
} FSProtoReplicaFetchBinlogFirstReq;

typedef struct fs_proto_replia_fetch_binlog_resp_body_header {
    char binlog_length[4]; //current binlog length
    char is_last;          //is the last package
} FSProtoReplicaFetchBinlogRespBodyHeader;

typedef struct fs_proto_replia_fetch_binlog_first_resp_body_header {
    FSProtoReplicaFetchBinlogRespBodyHeader common;
    char is_online;        //tell slave to ONLINE
    char padding[2];
    char until_version[8];  // for catch up master (including)
    char binlog[0];
} FSProtoReplicaFetchBinlogFirstRespBodyHeader;

typedef struct fs_proto_replia_fetch_binlog_next_resp_body_header {
    FSProtoReplicaFetchBinlogRespBodyHeader common;
    char padding[3];
    char binlog[0];
} FSProtoReplicaFetchBinlogNextRespBodyHeader;

typedef struct fs_proto_replia_active_confirm_req {
    char data_group_id[4];
    char server_id[4];
} FSProtoReplicaActiveConfirmReq;

typedef struct fs_proto_replica_rpc_req_body_header {
    char count[4];
    char padding[4];
} FSProtoReplicaRPCReqBodyHeader;

typedef struct fs_proto_replica_rpc_req_body_part {
    char data_version[8];
    char body_len[4];
    unsigned char cmd;
    char padding[3];
    char body[0];
} FSProtoReplicaRPCReqBodyPart;

typedef struct fs_proto_replica_rpc_resp_body_header {
    char count[4];
    char padding[4];
} FSProtoReplicaRPCRespBodyHeader;

typedef struct fs_proto_replica_rpc_resp_body_part {
    char data_version[8];
    char err_no[2];
    char padding[6];
} FSProtoReplicaRPCRespBodyPart;

#ifdef __cplusplus
extern "C" {
#endif

void fs_proto_init();

int fs_active_test(ConnectionInfo *conn, SFResponseInfo *response,
        const int network_timeout);

const char *fs_get_server_status_caption(const int status);

const char *fs_get_cmd_caption(const int cmd);

#ifdef __cplusplus
}
#endif

#endif
