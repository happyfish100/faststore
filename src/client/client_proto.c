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
#include "sf/sf_iov.h"
#include "fs_proto.h"
#include "client_global.h"
#include "client_proto.h"

static inline void proto_pack_block_key(const FSBlockKey *
        bkey, FSProtoBlockKey *proto_bkey)
{
    long2buff(bkey->oid, proto_bkey->oid);
    long2buff(bkey->offset, proto_bkey->offset);
}

static int do_slice_write(FSClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const FSBlockSliceKeyInfo *bs_key, tcpsenddatafunc send_func,
        void *data, const int size, int *inc_alloc)
{
    char out_buff[sizeof(FSProtoHeader) +
        SF_PROTO_UPDATE_EXTRA_BODY_SIZE +
        sizeof(FSProtoSliceWriteReqHeader)];
    FSProtoHeader *proto_header;
    FSProtoSliceWriteReqHeader *req_header;
    SFResponseInfo response;
    FSProtoSliceUpdateResp resp;
    int result;
    int front_bytes;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff, proto_header,
            req_header, req_id, front_bytes);
    proto_pack_block_key(&bs_key->block, &req_header->bs.bkey);

    response.error.length = 0;
    do {
        SF_PROTO_SET_HEADER(proto_header, FS_SERVICE_PROTO_SLICE_WRITE_REQ,
                front_bytes + bs_key->slice.length - sizeof(FSProtoHeader));
        int2buff(bs_key->slice.offset, req_header->bs.slice_size.offset);
        int2buff(bs_key->slice.length, req_header->bs.slice_size.length);

        if ((result=tcpsenddata_nb(conn->sock, out_buff, front_bytes,
                        client_ctx->common_cfg.network_timeout)) != 0)
        {
            break;
        }

        if ((result=send_func(conn->sock, data, size, client_ctx->
                        common_cfg.network_timeout)) != 0)
        {
            break;
        }

        if ((result=sf_recv_response(conn, &response, client_ctx->common_cfg.
                        network_timeout, FS_SERVICE_PROTO_SLICE_WRITE_RESP,
                        (char *)&resp, sizeof(FSProtoSliceUpdateResp))) != 0)
        {
            break;
        }

        *inc_alloc = buff2int(resp.inc_alloc);
    } while (0);

    if (result != 0) {
        *inc_alloc = 0;
        fs_log_network_error_for_update(&response, conn, result);
    }

    return result;
}

int fs_client_proto_slice_write(FSClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const FSBlockSliceKeyInfo *bs_key, const char *data,
        int *inc_alloc)
{
    return do_slice_write(client_ctx, conn, req_id, bs_key, tcpsenddata_nb,
            (void *)data, bs_key->slice.length, inc_alloc);
}

int fs_client_proto_slice_writev(FSClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const FSBlockSliceKeyInfo *bs_key, const struct iovec *iov,
        const int iovcnt, int *inc_alloc)
{
    return do_slice_write(client_ctx, conn, req_id, bs_key,
            (tcpsenddatafunc)tcpwritev_nb,
            (void *)iov, iovcnt, inc_alloc);
}

static int do_slice_read(FSClientContext *client_ctx,
        ConnectionInfo *conn, const int slave_id, const int req_cmd,
        const int resp_cmd, const FSBlockSliceKeyInfo *bs_key,
        const bool is_readv, const void *data, const int iovcnt,
        int *read_bytes)
{
    const bool auth_enabled = false;
    const SFConnectionParameters *connection_params;
    char out_buff[sizeof(FSProtoHeader) +
        SF_PROTO_QUERY_EXTRA_BODY_SIZE +
        sizeof(FSProtoServiceSliceReadReq) +
        sizeof(FSProtoReplicaSliceReadReq)];
    FSProtoHeader *header;
    SFResponseInfo response;
    FSProtoServiceSliceReadReq *sreq;
    FSProtoReplicaSliceReadReq *rreq;
    FSProtoBlockSlice *proto_bs;
    SFDynamicIOVArray iova;
    int out_bytes;
    int hole_start;
    int hole_len;
    int buff_offet;
    int remain;
    int curr_len;
    int bytes;
    int result;

    if (req_cmd == FS_SERVICE_PROTO_SLICE_READ_REQ) {
        SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
                header, sreq, 0, out_bytes);
        proto_bs = &sreq->bs;
    } else {
        SF_PROTO_CLIENT_SET_REQ_EX(client_ctx, auth_enabled,
                out_buff, header, rreq, 0, out_bytes);
        int2buff(slave_id, rreq->slave_id);
        proto_bs = &rreq->bs;
    }
    SF_PROTO_SET_HEADER(header, req_cmd,
            out_bytes - sizeof(FSProtoHeader));
    proto_pack_block_key(&bs_key->block, &proto_bs->bkey);

    connection_params = client_ctx->cm.ops.
        get_connection_params(&client_ctx->cm, conn);

    result = 0;
    hole_start = buff_offet = 0;
    remain = bs_key->slice.length;
    if (is_readv) {
        sf_iova_init(iova, (struct iovec *)data, iovcnt);
    }
    while (remain > 0) {
        if (remain <= connection_params->buffer_size) {
            curr_len = remain;
        } else {
            curr_len = connection_params->buffer_size;
        }

        int2buff(bs_key->slice.offset + buff_offet,
                proto_bs->slice_size.offset);
        int2buff(curr_len, proto_bs->slice_size.length);

        response.error.length = 0;
        if ((result=sf_send_and_recv_response_header(conn, out_buff,
                        out_bytes, &response, client_ctx->common_cfg.
                        network_timeout)) != 0)
        {
            break;
        }

        if ((result=sf_check_response(conn, &response, client_ctx->
                        common_cfg.network_timeout, resp_cmd)) != 0)
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

            if (is_readv) {
                result = tcpreadv_nb_ex(conn->sock, response.header.
                        body_len, iova.iov, iova.cnt, client_ctx->common_cfg.
                        network_timeout, &bytes);
            } else {
                result = tcprecvdata_nb_ex(conn->sock, (char *)data +
                        buff_offet, response.header.body_len, client_ctx->
                        common_cfg.network_timeout, &bytes);
            }

            if (result != 0) {
                response.error.length = snprintf(response.error.message,
                        sizeof(response.error.message), "recv data fail, "
                        "errno: %d, error info: %s", result, STRERROR(result));
                break;
            }

            hole_len = buff_offet - hole_start;
            if (hole_len > 0) {
                if (is_readv) {
                    if ((result=sf_iova_memset_ex(iova.input.iov,
                                    iova.input.cnt, 0, hole_start,
                                    hole_len)) != 0)
                    {
                        break;
                    }
                } else {
                    memset((char *)data + hole_start, 0, hole_len);
                }
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
        if (remain <= 0) {
            break;
        }

        if (is_readv) {
            if ((result=sf_iova_consume(&iova, curr_len)) != 0) {
                break;
            }
        }
    }

    if (is_readv) {
        sf_iova_destroy(iova);
    }

    *read_bytes = hole_start;
    if (result != 0) {
        fs_log_network_error(&response, conn, result);
        return result;
    } else {
        return *read_bytes > 0 ? 0 : ENODATA;
    }
}

int fs_client_proto_slice_read_ex(FSClientContext *client_ctx,
        ConnectionInfo *conn, const int slave_id, const int req_cmd,
        const int resp_cmd, const FSBlockSliceKeyInfo *bs_key,
        char *buff, int *read_bytes)
{
    const bool is_readv = false;
    return do_slice_read(client_ctx, conn, slave_id, req_cmd,
            resp_cmd, bs_key, is_readv, buff, 0, read_bytes);
}

int fs_client_proto_slice_readv_ex(FSClientContext *client_ctx,
        ConnectionInfo *conn, const int slave_id, const int req_cmd,
        const int resp_cmd, const FSBlockSliceKeyInfo *bs_key,
        const struct iovec *iov, const int iovcnt, int *read_bytes)
{
    const bool is_readv = true;
    return do_slice_read(client_ctx, conn, slave_id, req_cmd,
            resp_cmd, bs_key, is_readv, iov, iovcnt, read_bytes);
}

int fs_client_proto_bs_operate(FSClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id, const void *key,
        const int req_cmd, const int resp_cmd,
        const int enoent_log_level, int *inc_alloc)
{
    const FSBlockSliceKeyInfo *bs_key;
    char out_buff[sizeof(FSProtoHeader) +
        SF_PROTO_UPDATE_EXTRA_BODY_SIZE +
        sizeof(FSProtoSliceAllocateReq)];
    FSProtoHeader *header;
    FSProtoSliceAllocateReq *req;
    SFResponseInfo response;
    FSProtoSliceUpdateResp resp;
    int result;
    int out_bytes;

    if (req_cmd == FS_SERVICE_PROTO_BLOCK_DELETE_REQ) {
        return fs_client_proto_block_delete(client_ctx, conn, req_id,
                (const FSBlockKey *)key, enoent_log_level, inc_alloc);
    }

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, req, req_id, out_bytes);
    SF_PROTO_SET_HEADER(header, req_cmd,
            out_bytes - sizeof(FSProtoHeader));

    bs_key = (const FSBlockSliceKeyInfo *)key;
    proto_pack_block_key(&bs_key->block, &req->bs.bkey);
    int2buff(bs_key->slice.offset, req->bs.slice_size.offset);
    int2buff(bs_key->slice.length, req->bs.slice_size.length);

    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff,
                    out_bytes, &response, client_ctx->common_cfg.
                    network_timeout, resp_cmd, (char *)&resp,
                    sizeof(FSProtoSliceUpdateResp))) == 0)
    {
        *inc_alloc = buff2int(resp.inc_alloc);
    } else {
        *inc_alloc = 0;
        fs_log_network_error_for_delete(&response, conn,
                result, enoent_log_level);
    }

    return result;
}

int fs_client_proto_block_delete(FSClientContext *client_ctx,
        ConnectionInfo *conn, const uint64_t req_id,
        const FSBlockKey *bkey, const int enoent_log_level,
        int *dec_alloc)
{
    char out_buff[sizeof(FSProtoHeader) +
        SF_PROTO_UPDATE_EXTRA_BODY_SIZE +
        sizeof(FSProtoBlockDeleteReq)];
    FSProtoHeader *header;
    FSProtoBlockDeleteReq *req;
    SFResponseInfo response;
    FSProtoSliceUpdateResp resp;
    int result;
    int out_bytes;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, req, req_id, out_bytes);
    SF_PROTO_SET_HEADER(header, FS_SERVICE_PROTO_BLOCK_DELETE_REQ,
            out_bytes - sizeof(FSProtoHeader));
    proto_pack_block_key(bkey, &req->bkey);

    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff, out_bytes,
                    &response, client_ctx->common_cfg.network_timeout,
                    FS_SERVICE_PROTO_BLOCK_DELETE_RESP, (char *)&resp,
                    sizeof(FSProtoSliceUpdateResp))) == 0)
    {
        *dec_alloc = buff2int(resp.inc_alloc);
    } else {
        *dec_alloc = 0;
        fs_log_network_error_for_delete(&response, conn,
                result, enoent_log_level);
    }

    return result;
}

int fs_client_proto_join_server(FSClientContext *client_ctx,
        ConnectionInfo *conn, SFConnectionParameters *conn_params)
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
    req->auth_enabled = (client_ctx->auth.enabled ? 1 : 0);
    memcpy(&req->cluster_cfg_signs, &client_ctx->cluster_cfg.ptr->
            md5_digests, sizeof(req->cluster_cfg_signs));

    SF_PROTO_SET_HEADER(proto_header, FS_COMMON_PROTO_CLIENT_JOIN_REQ,
            sizeof(FSProtoClientJoinReq));
    if ((result=sf_send_and_recv_response(conn, out_buff, sizeof(out_buff),
                    &response, client_ctx->common_cfg.network_timeout,
                    FS_COMMON_PROTO_CLIENT_JOIN_RESP, (char *)&join_resp,
                    sizeof(FSProtoClientJoinResp))) != 0)
    {
        fs_log_network_error(&response, conn, result);
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

    conn = client_ctx->cm.ops.get_connection(
            &client_ctx->cm, data_group_index, &result);
    if (conn == NULL) {
        return result;
    }

    header = (FSProtoHeader *)out_buff;
    int2buff(data_group_index + 1, (char *)(header + 1));
    SF_PROTO_SET_HEADER(header, FS_SERVICE_PROTO_GET_MASTER_REQ,
            sizeof(out_buff) - sizeof(FSProtoHeader));
    if ((result=sf_send_and_recv_response(conn, out_buff, sizeof(out_buff),
                    &response, client_ctx->common_cfg.network_timeout,
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

    SF_CLIENT_RELEASE_CONNECTION(&client_ctx->cm, conn, result);
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

    conn = client_ctx->cm.ops.get_connection(
            &client_ctx->cm, data_group_index, &result);
    if (conn == NULL) {
        return result;
    }

    header = (FSProtoHeader *)out_buff;
    req = (FSProtoGetReadableServerReq *)(header + 1);
    int2buff(data_group_index + 1, req->data_group_id);
    req->read_rule = client_ctx->common_cfg.read_rule;
    SF_PROTO_SET_HEADER(header, FS_COMMON_PROTO_GET_READABLE_SERVER_REQ,
            sizeof(out_buff) - sizeof(FSProtoHeader));
    if ((result=sf_send_and_recv_response(conn, out_buff, sizeof(out_buff),
                    &response, client_ctx->common_cfg.network_timeout,
                    FS_COMMON_PROTO_GET_READABLE_SERVER_RESP,
                    (char *)&server_resp, sizeof(FSProtoGetServerResp))) != 0)
    {
        fs_log_network_error(&response, conn, result);
    } else {
        server->server_id = buff2int(server_resp.server_id);
        memcpy(server->conn.ip_addr, server_resp.ip_addr, IP_ADDRESS_SIZE);
        *(server->conn.ip_addr + IP_ADDRESS_SIZE - 1) = '\0';
        server->conn.port = buff2short(server_resp.port);
    }

    SF_CLIENT_RELEASE_CONNECTION(&client_ctx->cm, conn, result);
    return result;
}

int fs_client_proto_cluster_stat(FSClientContext *client_ctx,
        const ConnectionInfo *spec_conn, const FSClusterStatFilter *filter,
        FSIdArray *gid_array, FSClientClusterStatEntryArray *cs_array)
{
    FSProtoHeader *header;
    FSProtoClusterStatReq *req;
    FSProtoClusterStatRespBodyHeader *body_header;
    FSProtoClusterStatRespBodyPart *body_part;
    FSProtoClusterStatRespBodyPart *body_end;
    FSClientClusterStatEntry *stat;
    ConnectionInfo *conn;
    char out_buff[sizeof(FSProtoHeader) +
        SF_PROTO_QUERY_EXTRA_BODY_SIZE +
        sizeof(FSProtoClusterStatReq)];
    char fixed_buff[16 * 1024];
    char *in_buff;
    char *p;
    int *gid;
    int *gid_end;
    SFResponseInfo response;
    int result;
    int out_bytes;
    int calc_size;

    if ((conn=client_ctx->cm.ops.get_spec_connection(&client_ctx->cm,
                    spec_conn, &result)) == NULL)
    {
        return result;
    }

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, req, 0, out_bytes);

    req->filter_by = filter->filter_by;
    req->op_type = filter->op_type;
    req->status = filter->status;
    req->is_master = filter->is_master;
    int2buff(filter->data_group_id, req->data_group_id);
    SF_PROTO_SET_HEADER(header, FS_SERVICE_PROTO_CLUSTER_STAT_REQ,
            out_bytes - sizeof(FSProtoHeader));

    in_buff = fixed_buff;
    if ((result=sf_send_and_check_response_header(conn, out_buff, out_bytes,
                    &response, client_ctx->common_cfg.network_timeout,
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
                    body_len, client_ctx->common_cfg.network_timeout);
        }
    }

    body_header = (FSProtoClusterStatRespBodyHeader *)in_buff;
    if (result == 0) {
        gid_array->count = buff2int(body_header->dg_count);
        cs_array->count = buff2int(body_header->ds_count);

        calc_size = sizeof(FSProtoClusterStatRespBodyHeader) +
            gid_array->count * 4 + cs_array->count *
            sizeof(FSProtoClusterStatRespBodyPart);
        if (calc_size != response.header.body_len) {
            response.error.length = sprintf(response.error.message,
                    "response body length: %d != calculate size: %d, "
                    "data group count: %d, server count: %d",
                    response.header.body_len, calc_size,
                    gid_array->count, cs_array->count);
            result = EINVAL;
        } else if (gid_array->alloc < gid_array->count) {
            response.error.length = sprintf(response.error.message,
                    "group id array size %d too small < %d",
                    gid_array->alloc, gid_array->count);
            result = ENOSPC;
        } else if (cs_array->size < cs_array->count) {
            response.error.length = sprintf(response.error.message,
                    "stat array size %d too small < %d",
                    cs_array->size, cs_array->count);
            result = ENOSPC;
        }
    }

    if (result != 0) {
        gid_array->count = 0;
        cs_array->count = 0;
        fs_log_network_error(&response, conn, result);
    } else {
        p = (char *)(body_header + 1);
        gid_end = gid_array->ids + gid_array->count;
        for (gid=gid_array->ids; gid<gid_end; gid++) {
            *gid = buff2int(p);
            p += 4;
        }

        body_part = (FSProtoClusterStatRespBodyPart *)p;
        body_end = body_part + cs_array->count;
        for (stat=cs_array->stats; body_part<body_end; body_part++, stat++) {
            stat->data_group_id = buff2int(body_part->data_group_id);
            stat->server_id = buff2int(body_part->server_id);
            stat->is_preseted = body_part->is_preseted;
            stat->is_master = body_part->is_master;
            stat->status = body_part->status;
            memcpy(stat->ip_addr, body_part->ip_addr, IP_ADDRESS_SIZE);
            *(stat->ip_addr + IP_ADDRESS_SIZE - 1) = '\0';
            stat->port = buff2short(body_part->port);
            stat->data_versions.current = buff2long(
                    body_part->data_versions.current);
            stat->data_versions.confirmed = buff2long(
                    body_part->data_versions.confirmed);
        }
    }

    SF_CLIENT_RELEASE_CONNECTION(&client_ctx->cm, conn, result);
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
    char out_buff[sizeof(FSProtoHeader) +
        SF_PROTO_QUERY_EXTRA_BODY_SIZE];
    char in_buff[4 * 1024];
    FSProtoHeader *proto_header;
    SFProtoEmptyBodyReq *req;
    FSProtoDiskSpaceStatRespBodyHeader *body_header;
    FSProtoDiskSpaceStatRespBodyPart *body_part;
    FSClientServerSpaceStat *ps;
    FSClientServerSpaceStat *send;
    SFResponseInfo response;
    int out_bytes;
    int body_len;
    int min_blen;
    int expect_len;
    int result;

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            proto_header, req, 0, out_bytes);
    SF_PROTO_SET_HEADER(proto_header, FS_SERVICE_PROTO_DISK_SPACE_STAT_REQ,
            out_bytes - sizeof(FSProtoHeader));
    response.error.length = 0;
    do {
        if ((result=sf_send_and_recv_response_ex1(conn, out_buff, out_bytes,
                        &response, client_ctx->common_cfg.network_timeout,
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
        fs_log_network_error(&response, conn, result);
    }

    return result;
}

static inline void parse_ob_slice_stat(
        const FSProtoServiceOBSliceStat *proto,
        FSServiceOBSliceStat *stat)
{
    stat->total_count = buff2long(proto->total_count);
    stat->cached_count = buff2long(proto->cached_count);
    stat->element_used = buff2long(proto->element_used);
}

int fs_client_proto_service_stat(FSClientContext *client_ctx,
        const ConnectionInfo *spec_conn, const int data_group_id,
        FSClientServiceStat *stat)
{
    FSProtoHeader *header;
    ConnectionInfo *conn;
    char out_buff[sizeof(FSProtoHeader) +
        SF_PROTO_QUERY_EXTRA_BODY_SIZE +
        sizeof(FSProtoServiceStatReq)];
    FSProtoServiceStatReq *req;
    SFResponseInfo response;
    FSProtoServiceStatResp stat_resp;
    int out_bytes;
    int result;

    if ((conn=client_ctx->cm.ops.get_spec_connection(&client_ctx->cm,
                    spec_conn, &result)) == NULL)
    {
        return result;
    }

    SF_PROTO_CLIENT_SET_REQ(client_ctx, out_buff,
            header, req, 0, out_bytes);
    SF_PROTO_SET_HEADER(header, FS_SERVICE_PROTO_SERVICE_STAT_REQ,
            out_bytes - sizeof(FSProtoHeader));

    int2buff(data_group_id, req->data_group_id);
    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff, out_bytes,
                    &response, client_ctx->common_cfg.network_timeout,
                    FS_SERVICE_PROTO_SERVICE_STAT_RESP,
                    (char *)&stat_resp, sizeof(FSProtoServiceStatResp))) != 0)
    {
        fs_log_network_error(&response, conn, result);
    }

    SF_CLIENT_RELEASE_CONNECTION(&client_ctx->cm, conn, result);
    if (result != 0) {
        return result;
    }

    stat->version.str = stat->version_holder;
    stat->version.len = stat_resp.version.len;
    if (stat->version.len <= 0 || stat->version.len >=
            sizeof(stat->version_holder))
    {
        logError("file: "__FILE__", line: %d, "
                "invalid version length: %d, which <= 0 or >= %d",
                __LINE__, stat->version.len, (int)
                sizeof(stat->version_holder));
        return EINVAL;
    }

    stat->is_leader = stat_resp.is_leader;
    stat->auth_enabled = stat_resp.auth_enabled;
    stat->storage_engine.enabled = stat_resp.storage_engine.enabled;
    stat->storage_engine.current_version = buff2long(
            stat_resp.storage_engine.current_version);
    stat->server_id = buff2int(stat_resp.server_id);
    memcpy(stat->version.str, stat_resp.version.str, stat->version.len);
    *(stat->version.str + stat->version.len) = '\0';

    stat->connection.current_count = buff2int(
            stat_resp.connection.current_count);
    stat->connection.max_count = buff2int(stat_resp.connection.max_count);

    stat->binlog.current_version = buff2long(
            stat_resp.binlog.current_version);
    stat->binlog.writer.total_count = buff2long(
            stat_resp.binlog.writer.total_count);
    stat->binlog.writer.next_version = buff2long(
            stat_resp.binlog.writer.next_version);
    stat->binlog.writer.waiting_count = buff2int(
            stat_resp.binlog.writer.waiting_count);
    stat->binlog.writer.max_waitings = buff2int(
            stat_resp.binlog.writer.max_waitings);

    parse_ob_slice_stat(&stat_resp.data.ob, &stat->data.ob);
    parse_ob_slice_stat(&stat_resp.data.slice, &stat->data.slice);
    return 0;
}
