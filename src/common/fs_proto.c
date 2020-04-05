
#include <errno.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/connection_pool.h"
#include "fastcommon/ini_file_reader.h"
#include "fastcommon/sockopt.h"
#include "fs_types.h"
#include "fs_proto.h"

void fs_proto_init()
{
}

int fs_proto_set_body_length(struct fast_task_info *task)
{
    FSProtoHeader *header;

    header = (FSProtoHeader *)task->data;
    if (!FS_PROTO_CHECK_MAGIC(header->magic)) {
        logError("file: "__FILE__", line: %d, "
                "client ip: %s, magic "FS_PROTO_MAGIC_FORMAT
                " is invalid, expect: "FS_PROTO_MAGIC_FORMAT,
                __LINE__, task->client_ip,
                FS_PROTO_MAGIC_PARAMS(header->magic),
                FS_PROTO_MAGIC_EXPECT_PARAMS);
        return EINVAL;
    }

    task->length = buff2int(header->body_len); //set body length
    return 0;
}

void fs_set_admin_header (FSProtoHeader *fs_header_proto,
        unsigned char cmd, int body_len)
{
    fs_header_proto->cmd = cmd;
    int2buff(body_len, fs_header_proto->body_len);
}

int fs_check_response(ConnectionInfo *conn, FSResponseInfo *response,
        const int network_timeout, const unsigned char expect_cmd)
{
    int result;

    if (response->header.status == 0) {
        if (response->header.cmd != expect_cmd) {
            response->error.length = sprintf(
                    response->error.message,
                    "response cmd: %d != expect: %d",
                    response->header.cmd, expect_cmd);
            return EINVAL;
        }

        return 0;
    }

    if (response->header.body_len > 0) {
        int recv_bytes;
        if (response->header.body_len >= sizeof(response->error.message)) {
            response->error.length = sizeof(response->error.message) - 1;
        } else {
            response->error.length = response->header.body_len;
        }

        if ((result=tcprecvdata_nb_ex(conn->sock, response->error.message,
                response->error.length, network_timeout, &recv_bytes)) == 0)
        {
            response->error.message[response->error.length] = '\0';
        } else {
            response->error.length = snprintf(response->error.message,
                    sizeof(response->error.message),
                    "recv error message fail, "
                    "recv bytes: %d, expect message length: %d, "
                    "errno: %d, error info: %s", recv_bytes,
                    response->error.length, result, STRERROR(result));
        }
    } else {
        response->error.length = 0;
        response->error.message[0] = '\0';
    }

    return response->header.status;
}

int fs_send_and_recv_response_header(ConnectionInfo *conn, char *data,
        const int len, FSResponseInfo *response, const int network_timeout)
{
    int result;
    FSProtoHeader header_proto;

    if ((result=tcpsenddata_nb(conn->sock, data, len, network_timeout)) != 0) {
        response->error.length = snprintf(response->error.message,
                sizeof(response->error.message),
                "send data fail, errno: %d, error info: %s",
                result, STRERROR(result));
        return result;
    }

    if ((result=tcprecvdata_nb(conn->sock, &header_proto,
            sizeof(FSProtoHeader), network_timeout)) != 0)
    {
        response->error.length = snprintf(response->error.message,
                sizeof(response->error.message),
                "recv data fail, errno: %d, error info: %s",
                result, STRERROR(result));
        return result;
    }

    fs_proto_extract_header(&header_proto, &response->header);
    return 0;
}

int fs_send_and_recv_response(ConnectionInfo *conn, char *send_data,
        const int send_len, FSResponseInfo *response,
        const int network_timeout, const unsigned char expect_cmd,
        char *recv_data, const int expect_body_len)
{
    int result;
    int recv_bytes;

    if ((result=fs_send_and_check_response_header(conn,
                    send_data, send_len, response,
                    network_timeout, expect_cmd)) != 0)
    {
        return result;
    }

    if (response->header.body_len != expect_body_len) {
        response->error.length = sprintf(response->error.message,
                "response body length: %d != %d",
                response->header.body_len,
                expect_body_len);
        return EINVAL;
    }
    if (expect_body_len == 0) {
        return 0;
    }

    if ((result=tcprecvdata_nb_ex(conn->sock, recv_data,
                    expect_body_len, network_timeout, &recv_bytes)) != 0)
    {
        response->error.length = snprintf(response->error.message,
                sizeof(response->error.message),
                "recv body fail, recv bytes: %d, expect body length: %d, "
                "errno: %d, error info: %s", recv_bytes,
                response->header.body_len,
                result, STRERROR(result));
    }
    return result;
}

int fs_send_active_test_req(ConnectionInfo *conn, FSResponseInfo *response,
        const int network_timeout)
{
    int ret;
    FSProtoHeader fs_header_proto;

    fs_set_admin_header(&fs_header_proto, FS_PROTO_ACTIVE_TEST_REQ,
            0);
    ret = fs_send_and_recv_response_header(conn, (char *)&fs_header_proto,
            sizeof(FSProtoHeader), response, network_timeout);
    if (ret == 0) {
        ret = fs_check_response(conn, response, network_timeout,
                FS_PROTO_ACTIVE_TEST_RESP);
    }

    return ret;
}

const char *fs_get_server_status_caption(const int status)
{

    switch (status) {
        case FS_SERVER_STATUS_INIT:
            return "INIT";
        case FS_SERVER_STATUS_BUILDING:
            return "BUILDING";
        case FS_SERVER_STATUS_DUMPING:
            return "DUMPING";
        case FS_SERVER_STATUS_OFFLINE:
            return "OFFLINE";
        case FS_SERVER_STATUS_SYNCING:
            return "SYNCING";
        case FS_SERVER_STATUS_ACTIVE:
            return "ACTIVE";
        default:
            return "UNKOWN";
    }
}

const char *fs_get_cmd_caption(const int cmd)
{
    switch (cmd) {
        case FS_PROTO_ACK:
            return "ACK";
        case FS_PROTO_ACTIVE_TEST_REQ:
            return "ACTIVE_TEST_REQ";
        case FS_PROTO_ACTIVE_TEST_RESP:
            return "ACTIVE_TEST_RESP";
        case FS_SERVICE_PROTO_SERVICE_STAT_REQ:
            return "SERVICE_STAT_REQ";
        case FS_SERVICE_PROTO_SERVICE_STAT_RESP:
            return "SERVICE_STAT_RESP";
        case FS_SERVICE_PROTO_CLUSTER_STAT_REQ:
            return "CLUSTER_STAT_REQ";
        case FS_SERVICE_PROTO_CLUSTER_STAT_RESP:
            return "CLUSTER_STAT_RESP";
        case FS_SERVICE_PROTO_GET_MASTER_REQ:
            return "GET_MASTER_REQ";
        case FS_SERVICE_PROTO_GET_MASTER_RESP:
            return "GET_MASTER_RESP";
        case FS_SERVICE_PROTO_GET_SLAVES_REQ:
            return "GET_SLAVE_REQ";
        case FS_SERVICE_PROTO_GET_SLAVES_RESP:
            return "GET_SLAVE_RESP";
        case FS_SERVICE_PROTO_GET_READABLE_SERVER_REQ:
            return "GET_READABLE_SERVER_REQ";
        case FS_SERVICE_PROTO_GET_READABLE_SERVER_RESP:
            return "GET_READABLE_SERVER_RESP";
        case FS_CLUSTER_PROTO_GET_SERVER_STATUS_REQ:
            return "GET_SERVER_STATUS_REQ";
        case FS_CLUSTER_PROTO_GET_SERVER_STATUS_RESP:
            return "GET_SERVER_STATUS_RESP";
        case FS_CLUSTER_PROTO_JOIN_MASTER:
            return "JOIN_MASTER";
        case FS_CLUSTER_PROTO_PING_MASTER_REQ:
            return "PING_MASTER_REQ";
        case FS_CLUSTER_PROTO_PING_MASTER_RESP:
            return "PING_MASTER_RESP";
        case FS_CLUSTER_PROTO_PRE_SET_NEXT_MASTER:
            return "PRE_SET_NEXT_MASTER";
        case FS_CLUSTER_PROTO_COMMIT_NEXT_MASTER:
            return "COMMIT_NEXT_MASTER";
        case FS_REPLICA_PROTO_JOIN_SLAVE_REQ:
            return "JOIN_SLAVE_REQ";
        case FS_REPLICA_PROTO_JOIN_SLAVE_RESP:
            return "JOIN_SLAVE_RESP";
        case FS_REPLICA_PROTO_PUSH_BINLOG_REQ:
            return "PUSH_BINLOG_REQ";
        case FS_REPLICA_PROTO_PUSH_BINLOG_RESP:
            return "PUSH_BINLOG_RESP";
        default:
            return "UNKOWN";
    }
}
