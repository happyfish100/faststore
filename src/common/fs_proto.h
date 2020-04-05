#ifndef _FS_PROTO_H
#define _FS_PROTO_H

#include "fastcommon/fast_task_queue.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/connection_pool.h"
#include "fastcommon/ini_file_reader.h"
#include "fs_types.h"

#define FS_STATUS_MASTER_INCONSISTENT     9999

#define FS_PROTO_ACK                      6

#define FS_PROTO_ACTIVE_TEST_REQ         21
#define FS_PROTO_ACTIVE_TEST_RESP        22

#define FS_SERVICE_PROTO_SERVICE_STAT_REQ        41
#define FS_SERVICE_PROTO_SERVICE_STAT_RESP       42
#define FS_SERVICE_PROTO_CLUSTER_STAT_REQ        43
#define FS_SERVICE_PROTO_CLUSTER_STAT_RESP       44

#define FS_SERVICE_PROTO_GET_MASTER_REQ           45
#define FS_SERVICE_PROTO_GET_MASTER_RESP          46
#define FS_SERVICE_PROTO_GET_SLAVES_REQ           47
#define FS_SERVICE_PROTO_GET_SLAVES_RESP          48
#define FS_SERVICE_PROTO_GET_READABLE_SERVER_REQ  49
#define FS_SERVICE_PROTO_GET_READABLE_SERVER_RESP 50

//cluster commands
#define FS_CLUSTER_PROTO_GET_SERVER_STATUS_REQ   61
#define FS_CLUSTER_PROTO_GET_SERVER_STATUS_RESP  62
#define FS_CLUSTER_PROTO_JOIN_MASTER             63  //slave  -> master
#define FS_CLUSTER_PROTO_PING_MASTER_REQ         65
#define FS_CLUSTER_PROTO_PING_MASTER_RESP        66
#define FS_CLUSTER_PROTO_PRE_SET_NEXT_MASTER     67  //notify next leader to other servers
#define FS_CLUSTER_PROTO_COMMIT_NEXT_MASTER      68  //commit next leader to other servers

//replication commands, master -> slave
#define FS_REPLICA_PROTO_JOIN_SLAVE_REQ          81
#define FS_REPLICA_PROTO_JOIN_SLAVE_RESP         82
#define FS_REPLICA_PROTO_PUSH_BINLOG_REQ         83
#define FS_REPLICA_PROTO_PUSH_BINLOG_RESP        84

#define FS_PROTO_MAGIC_CHAR        '#'
#define FS_PROTO_SET_MAGIC(m)   \
    m[0] = m[1] = m[2] = m[3] = FS_PROTO_MAGIC_CHAR

#define FS_PROTO_CHECK_MAGIC(m) \
    (m[0] == FS_PROTO_MAGIC_CHAR && m[1] == FS_PROTO_MAGIC_CHAR && \
     m[2] == FS_PROTO_MAGIC_CHAR && m[3] == FS_PROTO_MAGIC_CHAR)

#define FS_PROTO_MAGIC_FORMAT "0x%02X%02X%02X%02X"
#define FS_PROTO_MAGIC_EXPECT_PARAMS \
    FS_PROTO_MAGIC_CHAR, FS_PROTO_MAGIC_CHAR, \
    FS_PROTO_MAGIC_CHAR, FS_PROTO_MAGIC_CHAR

#define FS_PROTO_MAGIC_PARAMS(m) \
    m[0], m[1], m[2], m[3]

#define FS_PROTO_SET_HEADER(header, _cmd, _body_len) \
    do {  \
        FS_PROTO_SET_MAGIC((header)->magic);   \
        (header)->cmd = _cmd;      \
        (header)->status[0] = (header)->status[1] = 0; \
        int2buff(_body_len, (header)->body_len); \
    } while (0)

#define FS_PROTO_SET_RESPONSE_HEADER(proto_header, resp_header) \
    do {  \
        (proto_header)->cmd = (resp_header).cmd;       \
        short2buff((resp_header).status, (proto_header)->status);  \
        int2buff((resp_header).body_len, (proto_header)->body_len);\
    } while (0)

typedef struct fs_proto_header {
    unsigned char magic[4]; //magic number
    char body_len[4];       //body length
    char status[2];         //status to store errno
    char flags[2];
    unsigned char cmd;      //the command code
    char padding[3];
} FSProtoHeader;

typedef struct fs_proto_service_stat_resp {
    char server_id[4];
    char is_master;
    char status;

    struct {
        char current_count[4];
        char max_count[4];
    } connection;

    struct {
        char current_data_version[8];
        char current_inode_sn[8];
        struct {
            char ns[8];
            char dir[8];
            char file[8];
        } counters;
    } dentry;
} FSProtoServiceStatResp;

typedef struct fs_proto_cluster_stat_resp_body_header {
    char count[4];
} FSProtoClusterStatRespBodyHeader;

/* for FS_SERVICE_PROTO_GET_MASTER_RESP and
   FS_SERVICE_PROTO_GET_READABLE_SERVER_RESP
   */
typedef struct fs_proto_get_server_resp {
    char server_id[4];
    char ip_addr[IP_ADDRESS_SIZE];
    char port[2];
} FSProtoGetServerResp;

typedef struct fs_proto_get_slaves_resp_body_header {
    char count[2];
} FSProtoGetSlavesRespBodyHeader;

typedef struct fs_proto_get_slaves_resp_body_part {
    char server_id[4];
    char ip_addr[IP_ADDRESS_SIZE];
    char port[2];
    char status;
} FSProtoGetSlavesRespBodyPart;

typedef struct fs_proto_join_master_req {
    char cluster_id[4];    //the cluster id
    char server_id[4];     //the slave server id
    char config_sign[16];
    char key[FS_REPLICA_KEY_SIZE];   //the slave key used on JOIN_SLAVE
} FSProtoJoinMasterReq;

typedef struct fs_proto_join_slave_req {
    char cluster_id[4];  //the cluster id
    char server_id[4];   //the master server id
    char buffer_size[4];   //the master task task size
    char key[FS_REPLICA_KEY_SIZE];  //the slave key passed / set by JOIN_MASTER
} FSProtoJoinSlaveReq;

typedef struct fs_proto_join_slave_resp {
    struct {
        char index[4];   //binlog file index
        char offset[8];  //binlog file offset
    } binlog_pos_hint;
    char last_data_version[8];   //the slave's last data version
} FSProtoJoinSlaveResp;

typedef struct fs_proto_ping_master_resp_header {
    char inode_sn[8];  //current inode sn of master
    char server_count[4];
} FSProtoPingMasterRespHeader;

typedef struct fs_proto_ping_master_resp_body_part {
    char server_id[4];
    char status;
} FSProtoPingMasterRespBodyPart;

typedef struct fs_proto_push_binlog_req_body_header {
    char binlog_length[4];
    char last_data_version[8];
} FSProtoPushBinlogReqBodyHeader;

typedef struct fs_proto_push_binlog_resp_body_header {
    char count[4];
} FSProtoPushBinlogRespBodyHeader;

typedef struct fs_proto_push_binlog_resp_body_part {
    char data_version[8];
    char err_no[2];
} FSProtoPushBinlogRespBodyPart;

#ifdef __cplusplus
extern "C" {
#endif

void fs_proto_init();

int fs_proto_set_body_length(struct fast_task_info *task);

int fs_check_response(ConnectionInfo *conn, FSResponseInfo *response,
        const int network_timeout, const unsigned char expect_cmd);

int fs_send_and_recv_response_header(ConnectionInfo *conn, char *data,
        const int len, FSResponseInfo *response, const int network_timeout);

static inline int fs_send_and_check_response_header(ConnectionInfo *conn,
        char *data, const int len, FSResponseInfo *response,
        const int network_timeout,  const unsigned char expect_cmd)
{
    int result;

    if ((result=fs_send_and_recv_response_header(conn, data, len,
                    response, network_timeout)) != 0)
    {
        return result;
    }


    if ((result=fs_check_response(conn, response, network_timeout,
                    expect_cmd)) != 0)
    {
        return result;
    }

    return 0;
}

int fs_send_and_recv_response(ConnectionInfo *conn, char *send_data,
        const int send_len, FSResponseInfo *response,
        const int network_timeout, const unsigned char expect_cmd,
        char *recv_data, const int expect_body_len);

static inline int fs_send_and_recv_none_body_response(ConnectionInfo *conn,
        char *send_data, const int send_len, FSResponseInfo *response,
        const int network_timeout, const unsigned char expect_cmd)
{
    char *recv_data = NULL;
    const int expect_body_len = 0;

    return fs_send_and_recv_response(conn, send_data, send_len, response,
        network_timeout, expect_cmd, recv_data, expect_body_len);
}

static inline void fs_proto_extract_header(FSProtoHeader *header_proto,
        FSHeaderInfo *header_info)
{
    header_info->cmd = header_proto->cmd;
    header_info->body_len = buff2int(header_proto->body_len);
    header_info->flags = buff2short(header_proto->flags);
    header_info->status = buff2short(header_proto->status);
}

int fs_send_active_test_req(ConnectionInfo *conn, FSResponseInfo *response,
        const int network_timeout);

const char *fs_get_server_status_caption(const int status);

const char *fs_get_cmd_caption(const int cmd);

#ifdef __cplusplus
}
#endif

#endif
