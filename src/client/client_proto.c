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

#include <sys/stat.h>
#include <limits.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/connection_pool.h"
#include "sf/idempotency/client/client_channel.h"
#include "fs_proto.h"
#include "client_global.h"
#include "client_proto.h"

static inline void proto_pack_block_key(const FSBlockKey *
        bkey, FSProtoBlockKey *proto_bkey)
{
    long2buff(bkey->oid, proto_bkey->oid);
    long2buff(bkey->offset, proto_bkey->offset);
}

int fs_client_proto_slice_write(FSClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const FSBlockSliceKeyInfo *bs_key, const char *data,
        int *inc_alloc)
{
    char out_buff[sizeof(FSProtoHeader) +
        sizeof(SFProtoIdempotencyAdditionalHeader) +
        sizeof(FSProtoSliceWriteReqHeader)];
    FSProtoHeader *proto_header;
    FSProtoSliceWriteReqHeader *req_header;
    SFResponseInfo response;
    FSProtoSliceUpdateResp resp;
    int result;
    int body_front_len;

    proto_header = (FSProtoHeader *)out_buff;
    body_front_len = sizeof(FSProtoSliceWriteReqHeader);
    if (req_id > 0) {
        long2buff(req_id, ((SFProtoIdempotencyAdditionalHeader *)
                    (proto_header + 1))->req_id);
        body_front_len += sizeof(SFProtoIdempotencyAdditionalHeader);
        req_header = (FSProtoSliceWriteReqHeader *)((char *)(proto_header
                    + 1) + sizeof(SFProtoIdempotencyAdditionalHeader));
    } else {
        req_header = (FSProtoSliceWriteReqHeader *)(proto_header + 1);
    }
    proto_pack_block_key(&bs_key->block, &req_header->bs.bkey);

    response.error.length = 0;
    do {
        SF_PROTO_SET_HEADER(proto_header, FS_SERVICE_PROTO_SLICE_WRITE_REQ,
                body_front_len + bs_key->slice.length);
        int2buff(bs_key->slice.offset, req_header->bs.slice_size.offset);
        int2buff(bs_key->slice.length, req_header->bs.slice_size.length);

        if ((result=tcpsenddata_nb(conn->sock, out_buff,
                        sizeof(FSProtoHeader) + body_front_len,
                        client_ctx->network_timeout)) != 0)
        {
            break;
        }

        if ((result=tcpsenddata_nb(conn->sock, (char *)data, bs_key->
                        slice.length, client_ctx->network_timeout)) != 0)
        {
            break;
        }

        if ((result=sf_recv_response(conn, &response, client_ctx->
                        network_timeout, FS_SERVICE_PROTO_SLICE_WRITE_RESP,
                        (char *)&resp, sizeof(FSProtoSliceUpdateResp))) != 0)
        {
            break;
        }

        *inc_alloc = buff2int(resp.inc_alloc);
    } while (0);

    if (result != 0) {
        *inc_alloc = 0;
        sf_log_network_error(&response, conn, result);
    }

    return result;
}

int fs_client_proto_slice_read(FSClientContext *client_ctx,
        ConnectionInfo *conn, const FSBlockSliceKeyInfo *bs_key,
        char *buff, int *read_bytes)
{
    const FSConnectionParameters *connection_params;
    char out_buff[sizeof(FSProtoHeader) + sizeof(FSProtoSliceReadReqHeader)];
    FSProtoHeader *proto_header;
    FSProtoSliceReadReqHeader *req_header;
    SFResponseInfo response;
    int hole_start;
    int hole_len;
    int buff_offet;
    int remain;
    int curr_len;
    int bytes;
    int result;

    proto_header = (FSProtoHeader *)out_buff;
    req_header = (FSProtoSliceReadReqHeader *)(proto_header + 1);
    SF_PROTO_SET_HEADER(proto_header, FS_SERVICE_PROTO_SLICE_READ_REQ,
            sizeof(FSProtoSliceReadReqHeader));
    proto_pack_block_key(&bs_key->block, &req_header->bs.bkey);

    connection_params = client_ctx->conn_manager.get_connection_params(
            client_ctx, conn);

    result = 0;
    hole_start = buff_offet = 0;
    remain = bs_key->slice.length;
    while (remain > 0) {
        if (remain <= connection_params->buffer_size) {
            curr_len = remain;
        } else {
            curr_len = connection_params->buffer_size;
        }

        int2buff(bs_key->slice.offset + buff_offet,
                req_header->bs.slice_size.offset);
        int2buff(curr_len, req_header->bs.slice_size.length);

        response.error.length = 0;
        if ((result=sf_send_and_recv_response_header(conn,
                        out_buff, sizeof(out_buff), &response,
                        client_ctx->network_timeout)) != 0)
        {
            break;
        }

        if ((result=sf_check_response(conn, &response,
                        client_ctx->network_timeout,
                        FS_SERVICE_PROTO_SLICE_READ_RESP)) != 0)
        {
            if (result == ENOENT) {  //ignore errno ENOENT
                bytes = 0;
                result = 0;
            } else {
                break;
            }
        } else {
            if (response.header.body_len > curr_len) {
                response.error.length = sprintf(response.error.message,
                        "response body length: %d > slice length: %d",
                        response.header.body_len, curr_len);
                result = EINVAL;
                break;
            }

            if ((result=tcprecvdata_nb_ex(conn->sock, buff + buff_offet,
                            response.header.body_len, client_ctx->
                            network_timeout, &bytes)) != 0)
            {
                response.error.length = snprintf(response.error.message,
                        sizeof(response.error.message),
                        "recv data fail, errno: %d, error info: %s",
                        result, STRERROR(result));
                break;
            }

            hole_len = buff_offet - hole_start;
            if (hole_len > 0) {
                memset(buff + hole_start, 0, hole_len);
            }
            hole_start = buff_offet + bytes;
        }

        /*
        logInfo("total recv: %d, current offset: %d, "
                "current length: %d, current read: %d, remain: %d, result: %d",
                hole_start, bs_key->slice.offset + buff_offet,
                curr_len, bytes, remain, result);
                */

        buff_offet += curr_len;
        remain -= curr_len;
    }

    if (result != 0) {
        sf_log_network_error(&response, conn, result);
    }

    *read_bytes = hole_start;
    if (result == 0) {
        return *read_bytes > 0 ? 0 : ENODATA;
    } else {
        return result;
    }
}

int fs_client_proto_bs_operate(FSClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id, const void *key,
        const int req_cmd, const int resp_cmd,
        const int enoent_log_level, int *inc_alloc)
{
    const FSBlockSliceKeyInfo *bs_key;
    char out_buff[sizeof(FSProtoHeader) +
        sizeof(SFProtoIdempotencyAdditionalHeader) +
        sizeof(FSProtoSliceAllocateReq)];
    FSProtoHeader *proto_header;
    FSProtoSliceAllocateReq *req;
    SFResponseInfo response;
    FSProtoSliceUpdateResp resp;
    int result;
    int body_len;
    int log_level;

    if (req_cmd == FS_SERVICE_PROTO_BLOCK_DELETE_REQ) {
        return fs_client_proto_block_delete(client_ctx, conn, req_id,
                (const FSBlockKey *)key, enoent_log_level, inc_alloc);
    }

    proto_header = (FSProtoHeader *)out_buff;
    body_len = sizeof(FSProtoSliceAllocateReq);
    if (req_id > 0) {
        long2buff(req_id, ((SFProtoIdempotencyAdditionalHeader *)
                    (proto_header + 1))->req_id);
        body_len += sizeof(SFProtoIdempotencyAdditionalHeader);
        req = (FSProtoSliceAllocateReq *)((char *)(proto_header
                    + 1) + sizeof(SFProtoIdempotencyAdditionalHeader));
    } else {
        req = (FSProtoSliceAllocateReq *)(proto_header + 1);
    }

    SF_PROTO_SET_HEADER(proto_header, req_cmd, body_len);

    bs_key = (const FSBlockSliceKeyInfo *)key;
    proto_pack_block_key(&bs_key->block, &req->bs.bkey);
    int2buff(bs_key->slice.offset, req->bs.slice_size.offset);
    int2buff(bs_key->slice.length, req->bs.slice_size.length);

    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff,
                    sizeof(FSProtoHeader) + body_len, &response,
                    client_ctx->network_timeout, resp_cmd, (char *)&resp,
                    sizeof(FSProtoSliceUpdateResp))) == 0)
    {
        *inc_alloc = buff2int(resp.inc_alloc);
    } else {
        *inc_alloc = 0;
        log_level = (result == ENOENT) ? enoent_log_level : LOG_ERR;
        sf_log_network_error_ex(&response, conn, result, log_level);
    }

    return result;
}

int fs_client_proto_block_delete(FSClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const FSBlockKey *bkey, const int enoent_log_level,
        int *dec_alloc)
{
    char out_buff[sizeof(FSProtoHeader) +
        sizeof(SFProtoIdempotencyAdditionalHeader) +
        sizeof(FSProtoBlockDeleteReq)];
    FSProtoHeader *proto_header;
    FSProtoBlockDeleteReq *req;
    SFResponseInfo response;
    FSProtoSliceUpdateResp resp;
    int result;
    int body_len;
    int log_level;

    proto_header = (FSProtoHeader *)out_buff;
    body_len = sizeof(FSProtoBlockDeleteReq);
    if (req_id > 0) {
        long2buff(req_id, ((SFProtoIdempotencyAdditionalHeader *)
                    (proto_header + 1))->req_id);
        body_len += sizeof(SFProtoIdempotencyAdditionalHeader);
        req = (FSProtoBlockDeleteReq *)((char *)(proto_header
                    + 1) + sizeof(SFProtoIdempotencyAdditionalHeader));
    } else {
        req = (FSProtoBlockDeleteReq *)(proto_header + 1);
    }

    proto_pack_block_key(bkey, &req->bkey);
    SF_PROTO_SET_HEADER(proto_header, FS_SERVICE_PROTO_BLOCK_DELETE_REQ,
            body_len);
    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff,
                    sizeof(FSProtoHeader) + body_len,
                    &response, client_ctx->network_timeout,
                    FS_SERVICE_PROTO_BLOCK_DELETE_RESP, (char *)&resp,
                    sizeof(FSProtoSliceUpdateResp))) == 0)
    {
        *dec_alloc = buff2int(resp.inc_alloc);
    } else {
        *dec_alloc = 0;
        log_level = (result == ENOENT) ? enoent_log_level : LOG_ERR;
        sf_log_network_error_ex(&response, conn, result, log_level);
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
    SFResponseInfo response;
    int result;
    int flags;

    proto_header = (FSProtoHeader *)out_buff;
    req = (FSProtoClientJoinReq *)(proto_header + 1);

    if (client_ctx->idempotency_enabled) {
        flags = FS_CLIENT_JOIN_FLAGS_IDEMPOTENCY_REQUEST;

        int2buff(__sync_add_and_fetch(&conn_params->channel->id, 0),
                req->idempotency.channel_id);
        int2buff(__sync_add_and_fetch(&conn_params->channel->key, 0),
                req->idempotency.key);
    } else {
        flags = 0;
    }
    int2buff(flags, req->flags);
    int2buff(FS_DATA_GROUP_COUNT(*client_ctx->cluster_cfg.ptr),
            req->data_group_count);
    int2buff(FS_FILE_BLOCK_SIZE, req->file_block_size);

    SF_PROTO_SET_HEADER(proto_header, FS_SERVICE_PROTO_CLIENT_JOIN_REQ,
            sizeof(FSProtoClientJoinReq));
    if ((result=sf_send_and_recv_response(conn, out_buff, sizeof(out_buff),
                    &response, client_ctx->network_timeout,
                    FS_SERVICE_PROTO_CLIENT_JOIN_RESP, (char *)&join_resp,
                    sizeof(FSProtoClientJoinResp))) != 0)
    {
        sf_log_network_error(&response, conn, result);
    } else {
        conn_params->buffer_size = buff2int(join_resp.buffer_size);
    }

    return result;
}

int fs_client_proto_get_master(FSClientContext *client_ctx,
        const int data_group_index, FSClientServerEntry *master)
{
    int result;
    ConnectionInfo *conn;
    FSProtoHeader *header;
    SFResponseInfo response;
    FSProtoGetServerResp server_resp;
    char out_buff[sizeof(FSProtoHeader) + 4];

    conn = client_ctx->conn_manager.get_connection(client_ctx,
            data_group_index, &result);
    if (conn == NULL) {
        return result;
    }

    header = (FSProtoHeader *)out_buff;
    int2buff(data_group_index + 1, (char *)(header + 1));
    SF_PROTO_SET_HEADER(header, FS_SERVICE_PROTO_GET_MASTER_REQ,
            sizeof(out_buff) - sizeof(FSProtoHeader));
    if ((result=sf_send_and_recv_response(conn, out_buff, sizeof(out_buff),
                    &response, client_ctx->network_timeout,
                    FS_SERVICE_PROTO_GET_MASTER_RESP,
                    (char *)&server_resp, sizeof(FSProtoGetServerResp))) != 0)
    {
        sf_log_network_error(&response, conn, result);
    } else {
        master->server_id = buff2int(server_resp.server_id);
        memcpy(master->conn.ip_addr, server_resp.ip_addr, IP_ADDRESS_SIZE);
        *(master->conn.ip_addr + IP_ADDRESS_SIZE - 1) = '\0';
        master->conn.port = buff2short(server_resp.port);
    }

    SF_CLIENT_RELEASE_CONNECTION(client_ctx, conn, result);
    return result;
}

int fs_client_proto_get_readable_server(FSClientContext *client_ctx,
        const int data_group_index, FSClientServerEntry *server)
{
    int result;
    ConnectionInfo *conn;
    FSProtoHeader *header;
    SFResponseInfo response;
    FSProtoGetReadableServerReq *req;
    FSProtoGetServerResp server_resp;
    char out_buff[sizeof(FSProtoHeader) +
        sizeof(FSProtoGetReadableServerReq)];

    conn = client_ctx->conn_manager.get_connection(client_ctx,
            data_group_index, &result);
    if (conn == NULL) {
        return result;
    }

    header = (FSProtoHeader *)out_buff;
    req = (FSProtoGetReadableServerReq *)(header + 1);
    int2buff(data_group_index + 1, req->data_group_id);
    req->read_rule = client_ctx->read_rule;
    SF_PROTO_SET_HEADER(header, FS_SERVICE_PROTO_GET_READABLE_SERVER_REQ,
            sizeof(out_buff) - sizeof(FSProtoHeader));
    if ((result=sf_send_and_recv_response(conn, out_buff, sizeof(out_buff),
                    &response, client_ctx->network_timeout,
                    FS_SERVICE_PROTO_GET_READABLE_SERVER_RESP,
                    (char *)&server_resp, sizeof(FSProtoGetServerResp))) != 0)
    {
        sf_log_network_error(&response, conn, result);
    } else {
        server->server_id = buff2int(server_resp.server_id);
        memcpy(server->conn.ip_addr, server_resp.ip_addr, IP_ADDRESS_SIZE);
        *(server->conn.ip_addr + IP_ADDRESS_SIZE - 1) = '\0';
        server->conn.port = buff2short(server_resp.port);
    }

    SF_CLIENT_RELEASE_CONNECTION(client_ctx, conn, result);
    return result;
}

int fs_client_proto_get_leader(FSClientContext *client_ctx,
        ConnectionInfo *conn, FSClientServerEntry *leader)
{
    int result;
    FSProtoHeader *header;
    SFResponseInfo response;
    FSProtoGetServerResp server_resp;
    char out_buff[sizeof(FSProtoHeader)];

    header = (FSProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, FS_SERVICE_PROTO_GET_LEADER_REQ,
            sizeof(out_buff) - sizeof(FSProtoHeader));
    if ((result=sf_send_and_recv_response(conn, out_buff, sizeof(out_buff),
                    &response, client_ctx->network_timeout,
                    FS_SERVICE_PROTO_GET_LEADER_RESP, (char *)&server_resp,
                    sizeof(FSProtoGetServerResp))) != 0)
    {
        sf_log_network_error(&response, conn, result);
    } else {
        leader->server_id = buff2int(server_resp.server_id);
        memcpy(leader->conn.ip_addr, server_resp.ip_addr, IP_ADDRESS_SIZE);
        *(leader->conn.ip_addr + IP_ADDRESS_SIZE - 1) = '\0';
        leader->conn.port = buff2short(server_resp.port);
    }

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
    SFResponseInfo response;
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
    SF_PROTO_SET_HEADER(header, FS_SERVICE_PROTO_CLUSTER_STAT_REQ,
            out_bytes - sizeof(FSProtoHeader));

    in_buff = fixed_buff;
    if ((result=sf_send_and_check_response_header(conn, out_buff,
                    out_bytes, &response, client_ctx->network_timeout,
                    FS_SERVICE_PROTO_CLUSTER_STAT_RESP)) == 0)
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
            result = tcprecvdata_nb(conn->sock, in_buff, response.header.
                    body_len, client_ctx->network_timeout);
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
        sf_log_network_error(&response, conn, result);
    } else {
        body_end = body_part + (*count);
        for (stat=stats; body_part<body_end; body_part++, stat++) {
            stat->data_group_id = buff2int(body_part->data_group_id);
            stat->server_id = buff2int(body_part->server_id);
            stat->is_preseted = body_part->is_preseted;
            stat->is_master = body_part->is_master;
            stat->status = body_part->status;
            memcpy(stat->ip_addr, body_part->ip_addr, IP_ADDRESS_SIZE);
            *(stat->ip_addr + IP_ADDRESS_SIZE - 1) = '\0';
            stat->port = buff2short(body_part->port);
            stat->data_version = buff2long(body_part->data_version);
        }
    }

    SF_CLIENT_RELEASE_CONNECTION(client_ctx, conn, result);
    if (in_buff != fixed_buff) {
        if (in_buff != NULL) {
            free(in_buff);
        }
    }

    return result;
}

int fs_client_proto_server_group_space_stat(FSClientContext *client_ctx,
        ConnectionInfo *conn, FSClientServerSpaceStat *stats,
        const int size, int *count)
{
    char out_buff[sizeof(FSProtoHeader)];
    char in_buff[4 * 1024];
    FSProtoHeader *proto_header;
    FSProtoDiskSpaceStatRespBodyHeader *body_header;
    FSProtoDiskSpaceStatRespBodyPart *body_part;
    FSClientServerSpaceStat *ps;
    FSClientServerSpaceStat *send;
    SFResponseInfo response;
    int body_len;
    int min_blen;
    int expect_len;
    int result;

    proto_header = (FSProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(proto_header, FS_SERVICE_PROTO_DISK_SPACE_STAT_REQ, 0);
    response.error.length = 0;
    do {
        if ((result=sf_send_and_recv_response_ex1(conn, out_buff,
                        sizeof(out_buff), &response, client_ctx->network_timeout,
                        FS_SERVICE_PROTO_DISK_SPACE_STAT_RESP, in_buff,
                        sizeof(in_buff), &body_len)) != 0)
        {
            break;
        }

        min_blen = sizeof(FSProtoDiskSpaceStatRespBodyHeader) +
            sizeof(FSProtoDiskSpaceStatRespBodyPart);
        if (body_len < min_blen) {
            response.error.length = snprintf(response.error.message,
                    sizeof(response.error.message), "invalid response "
                    "body length: %d < min length: %d", body_len, min_blen);
            result = EINVAL;
            break;
        }

        body_header = (FSProtoDiskSpaceStatRespBodyHeader *)in_buff;
        *count = buff2int(body_header->count);
        expect_len = sizeof(*body_header) + sizeof(*body_part) * (*count);
        if (body_len != expect_len) {
            response.error.length = snprintf(response.error.message,
                    sizeof(response.error.message), "invalid response "
                    "body length: %d != expect length: %d",
                    body_len, expect_len);
            result = EINVAL;
            break;
        }

        if (*count > size) {
            response.error.length = snprintf(response.error.message,
                    sizeof(response.error.message), "response server "
                    "count: %d exceeds entry size: %d", *count, size);
            result = ENOSPC;
            break;
        }

        body_part = (FSProtoDiskSpaceStatRespBodyPart *)(body_header + 1);
        send = stats + *count;
        for (ps=stats; ps<send; ps++, body_part++) {
            ps->server_id = buff2int(body_part->server_id);
            ps->stat.total = buff2long(body_part->total);
            ps->stat.avail = buff2long(body_part->avail);
            ps->stat.used = buff2long(body_part->used);
        }
    } while (0);

    if (result != 0) {
        *count = 0;
        sf_log_network_error(&response, conn, result);
    }

    return result;
}
