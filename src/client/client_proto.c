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

int fs_client_proto_slice_write(FSClientContext *client_ctx,
        const FSBlockSliceKeyInfo *bs_key, char *buff, int *written)
{
    ConnectionInfo *conn;
    int result;

    if ((conn=client_ctx->conn_manager.get_connection(client_ctx,
        bs_key->block.hash.codes, &result)) == NULL)
    {
        return result;
    }

    fs_client_release_connection(client_ctx, conn, result);
    return 0;
}
