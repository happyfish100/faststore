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

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <pthread.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/fc_atomic.h"
#include "sf/sf_func.h"
#include "../../common/fs_proto.h"
#include "../server_global.h"
#include "../cluster_relationship.h"
#include "../server_binlog.h"
#include "../server_replication.h"
#include "data_recovery.h"
#include "binlog_sync.h"

typedef struct {
    int fd;
    int wait_count;
    uint64_t until_version;
    SFSharedMBuffer *mbuffer;  //for ether network (socket)
} BinlogSyncContext;

static int query_binlog_info(ConnectionInfo *conn, DataRecoveryContext *ctx,
        int *start_index, int *last_index, int64_t *last_size)
{
    int result;
    FSProtoHeader *header;
    FSProtoReplicaQueryBinlogInfoReq *req;
    FSProtoReplicaQueryBinlogInfoResp resp;
    SFResponseInfo response;
    char out_buff[sizeof(FSProtoHeader) + sizeof(
            FSProtoReplicaQueryBinlogInfoReq)];

    header = (FSProtoHeader *)out_buff;
    req = (FSProtoReplicaQueryBinlogInfoReq *)
        (out_buff + sizeof(FSProtoHeader));
    int2buff(ctx->ds->dg->id, req->data_group_id);
    int2buff(CLUSTER_MYSELF_PTR->server->id, req->server_id);
    long2buff(ctx->fetch.last_data_version, req->until_version);
    SF_PROTO_SET_HEADER(header, FS_REPLICA_PROTO_QUERY_BINLOG_INFO_REQ,
            sizeof(out_buff) - sizeof(FSProtoHeader));
    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff,
            sizeof(out_buff), &response, REPLICA_NETWORK_TIMEOUT,
            FS_REPLICA_PROTO_QUERY_BINLOG_INFO_RESP,
            (char *)&resp, sizeof(resp))) != 0)
    {
        fs_log_network_error_ex(&response, conn, result, LOG_ERR);
        return result;
    }

    *start_index = buff2int(resp.start_index);
    *last_index = buff2int(resp.last_index);
    *last_size = buff2long(resp.last_size);
    return 0;
}

static int sync_binlog_to_local(ConnectionInfo *conn,
        DataRecoveryContext *ctx, const unsigned char req_cmd,
        char *out_buff, const int out_bytes, bool *is_last)
{
    int result;
    BinlogSyncContext *sync_ctx;
    FSProtoHeader *header;
    char *body;
    SFResponseInfo response;

    sync_ctx = (BinlogSyncContext *)ctx->arg;
    header = (FSProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, req_cmd, out_bytes - sizeof(FSProtoHeader));
    response.error.length = 0;
    if ((result=sf_send_and_check_response_header(conn, out_buff,
            out_bytes, &response, REPLICA_NETWORK_TIMEOUT,
            FS_REPLICA_PROTO_SYNC_BINLOG_RESP)) != 0)
    {
        fs_log_network_error_ex(&response, conn, result, LOG_ERR);
        return result;
    }

    *is_last = (response.header.flags & FS_COMMON_PROTO_FLAGS_LAST_PKG) != 0;
    if (response.header.body_len == 0) {
        return 0;
    }

    if (response.header.body_len > REPLICA_SF_CTX.
            net_buffer_cfg.max_buff_size)
    {
        logError("file: "__FILE__", line: %d, "
                "server %s:%u, response body length: %d is too large, "
                "the max body length is %d", __LINE__, conn->ip_addr,
                conn->port, response.header.body_len,
                REPLICA_SF_CTX.net_buffer_cfg.max_buff_size);
        return EOVERFLOW;
    }

    if (conn->comm_type == fc_comm_type_rdma) {
        body = G_RDMA_CONNECTION_CALLBACKS.get_recv_buffer(conn)->
            buff + sizeof(FSProtoHeader);
    } else {
        body = sync_ctx->mbuffer->buff;
        if ((result=tcprecvdata_nb(conn->sock, body, response.header.
                        body_len, REPLICA_NETWORK_TIMEOUT)) != 0)
        {
            response.error.length = snprintf(response.error.message,
                    sizeof(response.error.message),
                    "recv data fail, errno: %d, error info: %s",
                    result, STRERROR(result));
            fs_log_network_error(&response, conn, result);
            return result;
        }
    }

    if (fc_safe_write(((BinlogSyncContext *)ctx->arg)->fd, body, response.
                header.body_len) != response.header.body_len)
    {
        result = errno != 0 ? errno : EPERM;
        logError("file: "__FILE__", line: %d, "
                "write to file fail, errno: %d, error info: %s",
                __LINE__, result, STRERROR(result));
        return result;
    }

    return 0;
}

static int sync_binlog_first_to_local(ConnectionInfo *conn,
        DataRecoveryContext *ctx, const int binlog_index,
        const int64_t binlog_size, bool *is_last)
{
    FSProtoReplicaSyncBinlogFirstReq *req;
    char out_buff[sizeof(FSProtoHeader) + sizeof(
            FSProtoReplicaSyncBinlogFirstReq)];

    req = (FSProtoReplicaSyncBinlogFirstReq *)
        (out_buff + sizeof(FSProtoHeader));
    int2buff(ctx->ds->dg->id, req->data_group_id);
    int2buff(binlog_index, req->binlog_index);
    long2buff(binlog_size, req->binlog_size);
    return sync_binlog_to_local(conn, ctx,
            FS_REPLICA_PROTO_SYNC_BINLOG_FIRST_REQ,
            out_buff, sizeof(out_buff), is_last);
}

static int sync_binlog_next_to_local(ConnectionInfo *conn,
        DataRecoveryContext *ctx, bool *is_last)
{
    char out_buff[sizeof(FSProtoHeader)];
    return sync_binlog_to_local(conn, ctx,
            FS_REPLICA_PROTO_SYNC_BINLOG_NEXT_REQ,
            out_buff, sizeof(out_buff), is_last);
}

static int proto_sync_binlog(ConnectionInfo *conn, DataRecoveryContext *ctx,
        const int binlog_index, const int64_t binlog_size)
{
    int result;
    bool is_last;
    char tmp_filename[PATH_MAX];
    char full_filename[PATH_MAX];
    BinlogSyncContext *sync_ctx;

    sync_ctx = (BinlogSyncContext *)ctx->arg;
    replica_binlog_get_filename(ctx->ds->dg->id, binlog_index,
            full_filename, sizeof(full_filename));
    fc_combine_two_strings(full_filename, "tmp", '.', tmp_filename);
    if ((sync_ctx->fd=open(tmp_filename, O_WRONLY |
                    O_CREAT | O_TRUNC, 0644)) < 0)
    {
        logError("file: "__FILE__", line: %d, "
                "open binlog file %s fail, errno: %d, error info: %s",
                __LINE__, tmp_filename, errno, STRERROR(errno));
        return errno != 0 ? errno : EACCES;
    }

    is_last = false;
    if ((result=sync_binlog_first_to_local(conn, ctx, binlog_index,
                    binlog_size, &is_last)) != 0)
    {
        close(sync_ctx->fd);
        return result;
    }

    while (!is_last && SF_G_CONTINUE_FLAG) {
        if ((result=sync_binlog_next_to_local(conn, ctx, &is_last)) != 0) {
            break;
        }
    }

    if (!SF_G_CONTINUE_FLAG) {
        result = EINTR;
    }

    close(sync_ctx->fd);
    if (result != 0) {
        return result;
    }

    if (rename(tmp_filename, full_filename) != 0) {
        result = (errno != 0 ? errno : EPERM);
        logError("file: "__FILE__", line: %d, rename file "
                "%s to %s fail, errno: %d, error info: %s",
                __LINE__, tmp_filename, full_filename,
                result, STRERROR(result));
    }
    return result;
}

static int do_sync_binlogs(DataRecoveryContext *ctx)
{
    int result;
    int binlog_index;
    int start_index;
    int last_index;
    int64_t last_size;
    int64_t binlog_size;
    ConnectionInfo *conn;

    if ((conn=conn_pool_alloc_connection(REPLICA_SERVER_GROUP->comm_type,
                    &REPLICA_CONN_EXTRA_PARAMS, &result)) == NULL)
    {
        return result;
    }

    if ((result=fc_server_make_connection_ex(&REPLICA_GROUP_ADDRESS_ARRAY(
                        ctx->master->cs->server), conn, "fstore",
                    REPLICA_CONNECT_TIMEOUT, NULL, true)) != 0)
    {
        conn_pool_free_connection(conn);
        return result;
    }

    if ((result=query_binlog_info(conn, ctx, &start_index,
                    &last_index, &last_size)) != 0)
    {
        fc_server_destroy_connection(conn);
        return result;
    }

    for (binlog_index=start_index; binlog_index<=last_index; binlog_index++) {
        binlog_size = (binlog_index < last_index ? 0 : last_size);
        if ((result=proto_sync_binlog(conn, ctx, binlog_index,
                        binlog_size)) != 0)
        {
            break;
        }
    }

    if (result == 0) {
        if ((result=replica_binlog_set_binlog_start_index(
                        ctx->ds->dg->id, start_index)) == 0)
        {
            result = replica_binlog_writer_change_write_index(
                    ctx->ds->dg->id, last_index);
        }
    }

    fc_server_destroy_connection(conn);
    return result;
}

int data_recovery_sync_binlog(DataRecoveryContext *ctx)
{
    int result;
    BinlogSyncContext sync_ctx;
    char filename[PATH_MAX];

    ctx->arg = &sync_ctx;
    memset(&sync_ctx, 0, sizeof(sync_ctx));

    if (REPLICA_SERVER_GROUP->comm_type == fc_comm_type_sock) {
        sync_ctx.mbuffer = sf_shared_mbuffer_alloc(&SHARED_MBUFFER_CTX,
                REPLICA_SF_CTX.net_buffer_cfg.max_buff_size);
        if (sync_ctx.mbuffer == NULL) {
            return ENOMEM;
        }
    } else {
        sync_ctx.mbuffer = NULL;
    }

    replica_binlog_get_filename(ctx->ds->dg->id, 0,
            filename, sizeof(filename));
    if ((result=fc_delete_file_ex(filename, "replica binlog")) == 0) {
        result = do_sync_binlogs(ctx);
    }
    if (sync_ctx.mbuffer != NULL) {
        sf_shared_mbuffer_release(sync_ctx.mbuffer);
    }
    return result;
}
