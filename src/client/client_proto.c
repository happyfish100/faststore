#include <sys/stat.h>
#include <limits.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/connection_pool.h"
#include "fs_proto.h"
#include "client_global.h"
#include "client_proto.h"

static inline void fs_client_release_connection(
        FSClientContext *client_ctx,
        ConnectionInfo *conn, const int result)
{
    if ((result == EINVAL) || (result != 0 && is_network_error(result))) {
        client_ctx->conn_manager.close_connection(client_ctx, conn);
    } else if (client_ctx->conn_manager.release_connection != NULL) {
        client_ctx->conn_manager.release_connection(client_ctx, conn);
    }
}

static inline void proto_pack_block_key(const FSBlockKey *
        bkey, FSProtoBlockKey *proto_bkey)
{
    long2buff(bkey->oid, proto_bkey->oid);
    long2buff(bkey->offset, proto_bkey->offset);
}

#define FS_CLIENT_DATA_GROUP_INDEX(hash_code) \
    (hash_code % FS_DATA_GROUP_COUNT(*client_ctx->cluster_cfg.ptr))

int fs_client_proto_slice_write(FSClientContext *client_ctx,
        const FSBlockSliceKeyInfo *bs_key, const char *data,
        int *write_bytes, int *inc_alloc)
{
    ConnectionInfo *conn;
    const FSConnectionParameters *connection_params;
    char out_buff[sizeof(FSProtoHeader) + sizeof(FSProtoSliceWriteReqHeader)];
    FSProtoHeader *proto_header;
    FSProtoSliceWriteReqHeader *req_header;
    FSResponseInfo response;
    FSProtoSliceUpdateResp resp;
    int result;
    int remain;
    int bytes;
    int i;

    *write_bytes = *inc_alloc = 0;
    proto_header = (FSProtoHeader *)out_buff;
    req_header = (FSProtoSliceWriteReqHeader *)(proto_header + 1);
    proto_pack_block_key(&bs_key->block, &req_header->bs.bkey);
    for (i=0; i<3; i++) {
        if ((conn=client_ctx->conn_manager.get_master_connection(client_ctx,
                        FS_CLIENT_DATA_GROUP_INDEX(bs_key->block.hash_code),
                        &result)) == NULL)
        {
            return result;
        }

        connection_params = client_ctx->conn_manager.get_connection_params(
                client_ctx, conn);
        response.error.length = 0;
        remain = bs_key->slice.length - *write_bytes;
        while (remain > 0) {
            if (remain <= connection_params->buffer_size) {
                bytes = remain;
            } else {
                bytes = connection_params->buffer_size;
            }

            FS_PROTO_SET_HEADER(proto_header, FS_SERVICE_PROTO_SLICE_WRITE_REQ,
                    sizeof(FSProtoSliceWriteReqHeader) + bytes);
            int2buff(bs_key->slice.offset + *write_bytes,
                    req_header->bs.slice_size.offset);
            int2buff(bytes, req_header->bs.slice_size.length);

            logInfo("sent: %d, current offset: %d, current length: %d",
                    *write_bytes, bs_key->slice.offset + *write_bytes, bytes);

            if ((result=tcpsenddata_nb(conn->sock, out_buff, sizeof(out_buff),
                            g_fs_client_vars.network_timeout)) != 0)
            {
                break;
            }

            if ((result=tcpsenddata_nb(conn->sock, (char *)data + *write_bytes,
                            bytes, g_fs_client_vars.network_timeout)) != 0)
            {
                break;
            }

            if ((result=fs_recv_response(conn, &response, g_fs_client_vars.
                            network_timeout, FS_SERVICE_PROTO_SLICE_WRITE_RESP,
                            (char *)&resp, sizeof(FSProtoSliceUpdateResp))) != 0)
            {
                break;
            }

            *inc_alloc += buff2int(resp.inc_alloc);
            *write_bytes += bytes;
            remain -= bytes;
        }

        fs_client_release_connection(client_ctx, conn, result);
        if (result != 0) {
            fs_log_network_error(&response, conn, result);
        }

        if (!(result != 0 && is_network_error(result))) {
            break;
        }
    }

    return result;
}

int fs_client_proto_slice_read(FSClientContext *client_ctx,
        const FSBlockSliceKeyInfo *bs_key, char *buff, int *read_bytes)
{
    ConnectionInfo *conn;
    const FSConnectionParameters *connection_params;
    char out_buff[sizeof(FSProtoHeader) + sizeof(FSProtoSliceReadReqHeader)];
    FSProtoHeader *proto_header;
    FSProtoSliceReadReqHeader *req_header;
    FSResponseInfo response;
    int remain;
    int curr_len;
    int bytes;
    int result;
    int i;

    *read_bytes = 0;
    proto_header = (FSProtoHeader *)out_buff;
    req_header = (FSProtoSliceReadReqHeader *)(proto_header + 1);
    FS_PROTO_SET_HEADER(proto_header, FS_SERVICE_PROTO_SLICE_READ_REQ,
            sizeof(FSProtoSliceReadReqHeader));
    proto_pack_block_key(&bs_key->block, &req_header->bs.bkey);
    for (i=0; i<3; i++) {
        if ((conn=client_ctx->conn_manager.get_readable_connection(client_ctx,
                        FS_CLIENT_DATA_GROUP_INDEX(bs_key->block.hash_code),
                        &result)) == NULL)
        {
            return result;
        }

        connection_params = client_ctx->conn_manager.get_connection_params(
                client_ctx, conn);
        response.error.length = 0;
        remain = bs_key->slice.length - *read_bytes;
        while (remain > 0) {
            if (remain <= connection_params->buffer_size) {
                curr_len = remain;
            } else {
                curr_len = connection_params->buffer_size;
            }

            int2buff(bs_key->slice.offset + *read_bytes,
                    req_header->bs.slice_size.offset);
            int2buff(curr_len, req_header->bs.slice_size.length);

            logInfo("recved: %d, current offset: %d, current length: %d",
                    *read_bytes, bs_key->slice.offset + *read_bytes, curr_len);

            if ((result=fs_send_and_recv_response_header(conn,
                            out_buff, sizeof(out_buff), &response,
                            g_fs_client_vars.network_timeout)) != 0)
            {
                break;
            }

            if ((result=fs_check_response(conn, &response,
                            g_fs_client_vars.network_timeout,
                            FS_SERVICE_PROTO_SLICE_READ_RESP)) != 0)
            {
                break;
            }

            if (response.header.body_len > curr_len) {
                response.error.length = sprintf(response.error.message,
                        "reponse body length: %d > slice length: %d",
                        response.header.body_len, curr_len);
                result = EINVAL;
                break;
            }

            if ((result=tcprecvdata_nb_ex(conn->sock, buff + *read_bytes,
                            response.header.body_len, g_fs_client_vars.
                            network_timeout, &bytes)) != 0)
            {
                response.error.length = snprintf(response.error.message,
                        sizeof(response.error.message),
                        "recv data fail, errno: %d, error info: %s",
                        result, STRERROR(result));
                break;
            }

            *read_bytes += bytes;
            remain -= bytes;
            //TODO
            if (curr_len > bytes) {
                break;
            }
        }

        fs_client_release_connection(client_ctx, conn, result);
        if (result != 0) {
            fs_log_network_error(&response, conn, result);
        }

        if (!(result != 0 && is_network_error(result))) {
            break;
        }
    }

    return result;
}

static int fs_client_proto_slice_operate(FSClientContext *client_ctx,
        const FSBlockSliceKeyInfo *bs_key, const int req_cmd,
        const int resp_cmd, int *inc_alloc)
{
    ConnectionInfo *conn;
    char out_buff[sizeof(FSProtoHeader) + sizeof(FSProtoSliceAllocateReq)];
    FSProtoHeader *proto_header;
    FSProtoSliceAllocateReq *req;
    FSResponseInfo response;
    FSProtoSliceUpdateResp resp;
    int result;
    int i;

    proto_header = (FSProtoHeader *)out_buff;
    req = (FSProtoSliceAllocateReq *)(proto_header + 1);
    proto_pack_block_key(&bs_key->block, &req->bs.bkey);
    for (i=0; i<3; i++) {
        if ((conn=client_ctx->conn_manager.get_master_connection(client_ctx,
                        FS_CLIENT_DATA_GROUP_INDEX(bs_key->block.hash_code),
                        &result)) == NULL)
        {
            return result;
        }

        response.error.length = 0;
        FS_PROTO_SET_HEADER(proto_header, req_cmd,
                sizeof(FSProtoSliceAllocateReq));
        int2buff(bs_key->slice.offset, req->bs.slice_size.offset);
        int2buff(bs_key->slice.length, req->bs.slice_size.length);
        if ((result=fs_send_and_recv_response(conn, out_buff,
                sizeof(out_buff), &response, g_fs_client_vars.
                network_timeout, resp_cmd, (char *)&resp,
                sizeof(FSProtoSliceUpdateResp))) == 0)
        {
            *inc_alloc = buff2int(resp.inc_alloc);
        } else {
            fs_log_network_error(&response, conn, result);
        }

        fs_client_release_connection(client_ctx, conn, result);
        if (!(result != 0 && is_network_error(result))) {
            break;
        }
    }

    return result;
}

int fs_client_proto_slice_allocate(FSClientContext *client_ctx,
        const FSBlockSliceKeyInfo *bs_key, int *inc_alloc)
{
    return fs_client_proto_slice_operate(client_ctx,
        bs_key, FS_SERVICE_PROTO_SLICE_ALLOCATE_REQ,
        FS_SERVICE_PROTO_SLICE_ALLOCATE_RESP, inc_alloc);
}

int fs_client_proto_slice_delete(FSClientContext *client_ctx,
        const FSBlockSliceKeyInfo *bs_key, int *dec_alloc)
{
    return fs_client_proto_slice_operate(client_ctx,
        bs_key, FS_SERVICE_PROTO_SLICE_DELETE_REQ,
        FS_SERVICE_PROTO_SLICE_DELETE_RESP, dec_alloc);
}

int fs_client_proto_block_delete(FSClientContext *client_ctx,
        const FSBlockKey *bkey, int *dec_alloc)
{
    ConnectionInfo *conn;
    char out_buff[sizeof(FSProtoHeader) + sizeof(FSProtoBlockDeleteReq)];
    FSProtoHeader *proto_header;
    FSProtoBlockDeleteReq *req;
    FSResponseInfo response;
    FSProtoSliceUpdateResp resp;
    int result;
    int i;

    proto_header = (FSProtoHeader *)out_buff;
    req = (FSProtoBlockDeleteReq *)(proto_header + 1);
    proto_pack_block_key(bkey, &req->bkey);
    for (i=0; i<3; i++) {
        if ((conn=client_ctx->conn_manager.get_master_connection(client_ctx,
                        FS_CLIENT_DATA_GROUP_INDEX(bkey->hash_code),
                        &result)) == NULL)
        {
            return result;
        }

        response.error.length = 0;
        FS_PROTO_SET_HEADER(proto_header, FS_SERVICE_PROTO_BLOCK_DELETE_REQ,
                sizeof(FSProtoBlockDeleteReq));
        if ((result=fs_send_and_recv_response(conn, out_buff,
                sizeof(out_buff), &response, g_fs_client_vars.
                network_timeout, FS_SERVICE_PROTO_BLOCK_DELETE_RESP,
                (char *)&resp, sizeof(FSProtoSliceUpdateResp))) == 0)
        {
            *dec_alloc = buff2int(resp.inc_alloc);
        } else {
            fs_log_network_error(&response, conn, result);
        }

        fs_client_release_connection(client_ctx, conn, result);
        if (!(result != 0 && is_network_error(result))) {
            break;
        }
    }

    return result;
}

int fs_client_proto_join_server(FSClientContext *client_ctx,
        ConnectionInfo *conn, FSConnectionParameters *conn_params)
{
    char out_buff[sizeof(FSProtoHeader) + sizeof(FSProtoClientJoinReq)];
    FSProtoHeader *proto_header;
    FSProtoClientJoinReq *req;
    FSProtoClientJoinResp join_resp;
    FSResponseInfo response;
    int result;

    proto_header = (FSProtoHeader *)out_buff;
    req = (FSProtoClientJoinReq *)(proto_header + 1);
    int2buff(FS_DATA_GROUP_COUNT(*client_ctx->cluster_cfg.ptr),
            req->data_group_count);
    FS_PROTO_SET_HEADER(proto_header, FS_SERVICE_PROTO_CLIENT_JOIN_REQ,
            sizeof(FSProtoClientJoinReq));
    if ((result=fs_send_and_recv_response(conn, out_buff, sizeof(out_buff),
                    &response, g_fs_client_vars.network_timeout,
                    FS_SERVICE_PROTO_CLIENT_JOIN_RESP, (char *)&join_resp,
                    sizeof(FSProtoClientJoinResp))) != 0)
    {
        fs_log_network_error(&response, conn, result);
    }

    conn_params->buffer_size = buff2int(join_resp.buffer_size);
    return result;
}

int fs_client_proto_get_master(FSClientContext *client_ctx,
        const int data_group_index, FSClientServerEntry *master)
{
    int result;
    ConnectionInfo *conn;
    FSProtoHeader *header;
    FSResponseInfo response;
    FSProtoGetServerResp server_resp;
    char out_buff[sizeof(FSProtoHeader) + 4];

    conn = client_ctx->conn_manager.get_connection(client_ctx,
            data_group_index, &result);
    if (conn == NULL) {
        return result;
    }

    header = (FSProtoHeader *)out_buff;
    int2buff(data_group_index + 1, (char *)(header + 1));
    FS_PROTO_SET_HEADER(header, FS_SERVICE_PROTO_GET_MASTER_REQ,
            sizeof(out_buff) - sizeof(FSProtoHeader));
    if ((result=fs_send_and_recv_response(conn, out_buff, sizeof(out_buff),
                    &response, g_fs_client_vars.network_timeout,
                    FS_SERVICE_PROTO_GET_MASTER_RESP,
                    (char *)&server_resp, sizeof(FSProtoGetServerResp))) != 0)
    {
        fs_log_network_error(&response, conn, result);
    } else {
        master->server_id = buff2int(server_resp.server_id);
        memcpy(master->conn.ip_addr, server_resp.ip_addr, IP_ADDRESS_SIZE);
        *(master->conn.ip_addr + IP_ADDRESS_SIZE - 1) = '\0';
        master->conn.port = buff2short(server_resp.port);
    }

    fs_client_release_connection(client_ctx, conn, result);
    return result;
}

int fs_client_proto_get_readable_server(FSClientContext *client_ctx,
        const int data_group_index, FSClientServerEntry *server)
{
    int result;
    ConnectionInfo *conn;
    FSProtoHeader *header;
    FSResponseInfo response;
    FSProtoGetServerResp server_resp;
    char out_buff[sizeof(FSProtoHeader) + 4];

    conn = client_ctx->conn_manager.get_connection(client_ctx,
            data_group_index, &result);
    if (conn == NULL) {
        return result;
    }

    header = (FSProtoHeader *)out_buff;
    int2buff(data_group_index + 1, (char *)(header + 1));
    FS_PROTO_SET_HEADER(header, FS_SERVICE_PROTO_GET_READABLE_SERVER_REQ,
            sizeof(out_buff) - sizeof(FSProtoHeader));
    if ((result=fs_send_and_recv_response(conn, out_buff, sizeof(out_buff),
                    &response, g_fs_client_vars.network_timeout,
                    FS_SERVICE_PROTO_GET_READABLE_SERVER_RESP,
                    (char *)&server_resp, sizeof(FSProtoGetServerResp))) != 0)
    {
        fs_log_network_error(&response, conn, result);
    } else {
        server->server_id = buff2int(server_resp.server_id);
        memcpy(server->conn.ip_addr, server_resp.ip_addr, IP_ADDRESS_SIZE);
        *(server->conn.ip_addr + IP_ADDRESS_SIZE - 1) = '\0';
        server->conn.port = buff2short(server_resp.port);
    }

    fs_client_release_connection(client_ctx, conn, result);
    return result;
}

int fs_client_proto_cluster_stat(FSClientContext *client_ctx,
        const ConnectionInfo *spec_conn, const int data_group_id,
        FSClientClusterStatEntry *stats, const int size, int *count)
{
    FSProtoHeader *header;
    FSProtoClusterStatRespBodyHeader *body_header;
    FSProtoClusterStatRespBodyPart *body_part;
    FSProtoClusterStatRespBodyPart *body_end;
    FSClientClusterStatEntry *stat;
    ConnectionInfo *conn;
    char out_buff[sizeof(FSProtoHeader) + 4];
    char fixed_buff[8 * 1024];
    char *in_buff;
    FSResponseInfo response;
    int result;
    int out_bytes;
    int calc_size;

    if ((conn=client_ctx->conn_manager.get_spec_connection(
                    client_ctx, spec_conn, &result)) == NULL)
    {
        return result;
    }

    header = (FSProtoHeader *)out_buff;
    if (data_group_id > 0) {
        out_bytes = sizeof(FSProtoHeader) + 4;
        int2buff(data_group_id, out_buff + sizeof(FSProtoHeader));
    } else {
        out_bytes = sizeof(FSProtoHeader);
    }
    FS_PROTO_SET_HEADER(header, FS_SERVICE_PROTO_CLUSTER_STAT_REQ,
            out_bytes - sizeof(FSProtoHeader));

    in_buff = fixed_buff;
    if ((result=fs_send_and_check_response_header(conn, out_buff,
                    out_bytes, &response, g_fs_client_vars.
                    network_timeout, FS_SERVICE_PROTO_CLUSTER_STAT_RESP)) == 0)
    {
        if (response.header.body_len > sizeof(fixed_buff)) {
            in_buff = (char *)fc_malloc(response.header.body_len);
            if (in_buff == NULL) {
                response.error.length = sprintf(response.error.message,
                        "malloc %d bytes fail", response.header.body_len);
                result = ENOMEM;
            }
        }

        if (result == 0) {
            result = tcprecvdata_nb(conn->sock, in_buff,
                    response.header.body_len, g_fs_client_vars.
                    network_timeout);
        }
    }

    body_header = (FSProtoClusterStatRespBodyHeader *)in_buff;
    body_part = (FSProtoClusterStatRespBodyPart *)(in_buff +
            sizeof(FSProtoClusterStatRespBodyHeader));
    if (result == 0) {
        *count = buff2int(body_header->count);

        calc_size = sizeof(FSProtoClusterStatRespBodyHeader) +
            (*count) * sizeof(FSProtoClusterStatRespBodyPart);
        if (calc_size != response.header.body_len) {
            response.error.length = sprintf(response.error.message,
                    "response body length: %d != calculate size: %d, "
                    "server count: %d", response.header.body_len,
                    calc_size, *count);
            result = EINVAL;
        } else if (size < *count) {
            response.error.length = sprintf(response.error.message,
                    "entry size %d too small < %d", size, *count);
            *count = 0;
            result = ENOSPC;
        }
    } else {
        *count = 0;
    }

    if (result != 0) {
        fs_log_network_error(&response, conn, result);
    } else {
        body_end = body_part + (*count);
        for (stat=stats; body_part<body_end; body_part++, stat++) {
            stat->data_group_id = buff2int(body_part->data_group_id);
            stat->server_id = buff2int(body_part->server_id);
            stat->is_master = body_part->is_master;
            stat->status = body_part->status;
            memcpy(stat->ip_addr, body_part->ip_addr, IP_ADDRESS_SIZE);
            *(stat->ip_addr + IP_ADDRESS_SIZE - 1) = '\0';
            stat->port = buff2short(body_part->port);
            stat->data_version = buff2long(body_part->data_version);
        }
    }

    fs_client_release_connection(client_ctx, conn, result);
    if (in_buff != fixed_buff) {
        if (in_buff != NULL) {
            free(in_buff);
        }
    }

    return result;
}
