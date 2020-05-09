
#ifndef _FS_CLIENT_PROTO_H
#define _FS_CLIENT_PROTO_H

#include "fastcommon/fast_mpool.h"
#include "fs_types.h"
#include "fs_proto.h"

#ifdef __cplusplus
extern "C" {
#endif

    int fs_client_proto_slice_write(FSClientContext *client_ctx,
            const FSBlockSliceKeyInfo *bs_key, const char *buff,
            int *write_bytes);

    int fs_client_proto_slice_read(FSClientContext *client_ctx,
            const FSBlockSliceKeyInfo *bs_key, char *buff, int *read_bytes);

    int fs_client_proto_slice_truncate(FSClientContext *client_ctx,
            const FSBlockSliceKeyInfo *bs_key);

    int fs_client_proto_slice_delete(FSClientContext *client_ctx,
            const FSBlockSliceKeyInfo *bs_key);

    int fs_client_proto_block_delete(FSClientContext *client_ctx,
            const FSBlockKey *bkey);

    int fs_client_proto_join_server(ConnectionInfo *conn,
            FSConnectionParameters *conn_params);

    static inline void fs_log_network_error_ex(FSResponseInfo *response,
            const ConnectionInfo *conn, const int result, const int line)
    {
        if (response->error.length > 0) {
            logError("file: "__FILE__", line: %d, "
                    "server %s:%d, %s", line,
                    conn->ip_addr, conn->port,
                    response->error.message);
        } else {
            logError("file: "__FILE__", line: %d, "
                    "communicate with server %s:%d fail, "
                    "errno: %d, error info: %s", line,
                    conn->ip_addr, conn->port,
                    result, STRERROR(result));
        }
    }

#define fs_log_network_error(response, conn, result)  \
    fs_log_network_error_ex(response, conn, result, __LINE__)

#ifdef __cplusplus
}
#endif

#endif
