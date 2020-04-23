#include <sys/stat.h>
#include <limits.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/connection_pool.h"
#include "fs_proto.h"
#include "client_global.h"
#include "client_proto.h"

static inline void log_network_error_ex(FSResponseInfo *response,
        const ConnectionInfo *conn, const int result, const int line)
{
    if (response->error.length > 0) {
        logError("file: "__FILE__", line: %d, "
                "server %s:%d, %s", line,
                conn->ip_addr, conn->port,
                response->error.message);
    } else {
        logError("file: "__FILE__", line: %d, "
                "communicate with dir server %s:%d fail, "
                "errno: %d, error info: %s", line,
                conn->ip_addr, conn->port,
                result, STRERROR(result));
    }
}

#define log_network_error(response, conn, result) \
        log_network_error_ex(response, conn, result, __LINE__)

static inline void fs_client_release_connection(
        FSClientContext *client_ctx,
        ConnectionInfo *conn, const int result)
{
    if (result != 0 && is_network_error(result)) {
        client_ctx->conn_manager.close_connection(client_ctx, conn);
    } else if (client_ctx->conn_manager.release_connection != NULL) {
        client_ctx->conn_manager.release_connection(client_ctx, conn);
    }
}

static inline void proto_pack_block_slice_key(const FSBlockSliceKeyInfo *
        bs_key, FSProtoBlockSlice *proto_bs)
{
    long2buff(bs_key->block.inode, proto_bs->bkey.inode);
    long2buff(bs_key->block.offset, proto_bs->bkey.offset);

    int2buff(bs_key->slice.offset, proto_bs->slice_size.offset);
    int2buff(bs_key->slice.length, proto_bs->slice_size.length);
}

int fs_client_proto_slice_write(FSClientContext *client_ctx,
        const FSBlockSliceKeyInfo *bs_key, char *data)
{
    ConnectionInfo *conn;
    char out_buff[sizeof(FSProtoHeader) + sizeof(FSProtoSliceWriteReqHeader)];
    FSProtoHeader *proto_header;
    FSProtoSliceWriteReqHeader *req_header;
    FSResponseInfo response;
    int result;
    int i;

    proto_header = (FSProtoHeader *)out_buff;
    req_header = (FSProtoSliceWriteReqHeader *)(proto_header + 1);
    FS_PROTO_SET_HEADER(proto_header, FS_SERVICE_PROTO_SLICE_WRITE_REQ,
            sizeof(FSProtoSliceWriteReqHeader) + bs_key->slice.length);
    proto_pack_block_slice_key(bs_key, &req_header->bs);

    for (i=0; i<3; i++) {
        if ((conn=client_ctx->conn_manager.get_connection(client_ctx,
                        bs_key->block.hash.codes, &result)) == NULL)
        {
            return result;
        }

        if ((result=tcpsenddata_nb(conn->sock, out_buff, sizeof(out_buff),
                        g_client_global_vars.network_timeout)) != 0)
        {
            client_ctx->conn_manager.close_connection(client_ctx, conn);
            continue;
        }

        if ((result=tcpsenddata_nb(conn->sock, data, bs_key->slice.length,
                        g_client_global_vars.network_timeout)) != 0)
        {
            client_ctx->conn_manager.close_connection(client_ctx, conn);
            continue;
        }

        result = fs_recv_response(conn, &response, g_client_global_vars.
                network_timeout, FS_SERVICE_PROTO_SLICE_WRITE_RESP, NULL, 0);
        if (result != 0 && is_network_error(result)) {
            client_ctx->conn_manager.close_connection(client_ctx, conn);
            continue;
        }

        fs_client_release_connection(client_ctx, conn, result);
        break;
    }

    if (result != 0) {
        log_network_error(&response, conn, result);
    }
    return result;
}
