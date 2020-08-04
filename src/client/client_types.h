#ifndef _FS_CLIENT_TYPES_H
#define _FS_CLIENT_TYPES_H

#include "fastcommon/common_define.h"
#include "fastcommon/connection_pool.h"
#include "fs_types.h"
#include "fs_cluster_cfg.h"

struct fs_connection_parameters;
struct fs_client_context;

typedef ConnectionInfo *(*fs_get_connection_func)(
        struct fs_client_context *client_ctx,
        const int data_group_index, int *err_no);

typedef ConnectionInfo *(*fs_get_spec_connection_func)(
        struct fs_client_context *client_ctx,
        const ConnectionInfo *target, int *err_no);

typedef void (*fs_release_connection_func)(
        struct fs_client_context *client_ctx, ConnectionInfo *conn);
typedef void (*fs_close_connection_func)(
        struct fs_client_context *client_ctx, ConnectionInfo *conn);

typedef const struct fs_connection_parameters * (*fs_get_connection_parameters)(
        struct fs_client_context *client_ctx, ConnectionInfo *conn);

typedef struct fs_connection_parameters {
    int buffer_size;
    int data_group_id;  //for master cache
} FSConnectionParameters;

typedef struct fs_client_server_entry {
    int server_id;
    ConnectionInfo conn;
    char status;
} FSClientServerEntry;

typedef struct fs_client_data_group_entry {
    /* master connection cache */
    struct {
        ConnectionInfo *conn;
        ConnectionInfo holder;
    } master_cache;
} FSClientDataGroupEntry;

typedef struct fs_client_data_group_array {
    FSClientDataGroupEntry *entries;
    int count;
} FSClientDataGroupArray;

typedef struct fs_connection_manager {
    /* get the specify connection by ip and port */
    fs_get_spec_connection_func get_spec_connection;

    /* get one connection of the configured servers */
    fs_get_connection_func get_connection;

    /* get the master connection from the server */
    fs_get_connection_func get_master_connection;

    /* get one readable connection from the server */
    fs_get_connection_func get_readable_connection;

    /* push back to connection pool when use connection pool */
    fs_release_connection_func release_connection;

     /* disconnect the connecton on network error */
    fs_close_connection_func close_connection;

    fs_get_connection_parameters get_connection_params;

    FSClientDataGroupArray data_group_array;

    void *args;   //extra data
} FSConnectionManager;

typedef struct fs_client_context {
    struct {
        FSClusterConfig *ptr;
        FSClusterConfig holder;
    } cluster_cfg;
    FSConnectionManager conn_manager;
    bool inited;
    bool is_simple_conn_mananger;
} FSClientContext;

#define FS_CFG_SERVICE_INDEX(client_ctx)  \
    client_ctx->cluster_cfg.ptr->service_group_index
#define FS_CFG_SERVICE_ADDRESS_ARRAY(client_ctx, server) \
    (server)->group_addrs[FS_CFG_SERVICE_INDEX(client_ctx)].address_array

#endif
