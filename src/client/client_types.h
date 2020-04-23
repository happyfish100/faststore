#ifndef _FS_CLIENT_TYPES_H
#define _FS_CLIENT_TYPES_H

#include "fastcommon/common_define.h"
#include "fastcommon/connection_pool.h"
#include "fs_types.h"
#include "fs_cluster_cfg.h"

struct fs_client_context;

typedef ConnectionInfo *(*fs_get_connection_func)(
        struct fs_client_context *client_ctx, const uint32_t *hash_codes,
        int *err_no);

typedef ConnectionInfo *(*fs_get_spec_connection_func)(
        struct fs_client_context *client_ctx,
        const ConnectionInfo *target, int *err_no);

typedef void (*fs_release_connection_func)(
        struct fs_client_context *client_ctx, ConnectionInfo *conn);
typedef void (*fs_close_connection_func)(
        struct fs_client_context *client_ctx, ConnectionInfo *conn);

typedef struct fs_connection_manager {
    /* get the specify connection by ip and port */
    fs_get_spec_connection_func get_spec_connection;

    /* get one connection of the configured servers */
    fs_get_connection_func get_connection;

    /* push back to connection pool when use connection pool */
    fs_release_connection_func release_connection;

     /* disconnect the connecton on network error */
    fs_close_connection_func close_connection;

    void *args;   //extra data
} FSConnectionManager;

typedef struct fs_client_context {
    FSClusterConfig cluster_cfg;
    FSConnectionManager conn_manager;
    bool inited;
    bool is_simple_conn_mananger;
} FSClientContext;

#endif
