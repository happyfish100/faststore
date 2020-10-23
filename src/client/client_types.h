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

#ifndef _FS_CLIENT_TYPES_H
#define _FS_CLIENT_TYPES_H

#include "fastcommon/common_define.h"
#include "fastcommon/connection_pool.h"
#include "sf/sf_configs.h"
#include "sf/idempotency/client/client_types.h"
#include "fs_types.h"
#include "fs_cluster_cfg.h"

struct idempotency_client_channel;
struct fs_connection_parameters;
struct fs_client_context;

typedef ConnectionInfo *(*fs_get_connection_func)(
        struct fs_client_context *client_ctx,
        const int data_group_index, int *err_no);

typedef ConnectionInfo *(*fs_get_server_connection_func)(
        struct fs_client_context *client_ctx,
        FCServerInfo *server, int *err_no);

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
    struct idempotency_client_channel *channel;
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
        pthread_mutex_t lock;
    } master_cache;
} FSClientDataGroupEntry;

typedef struct fs_client_data_group_array {
    FSClientDataGroupEntry *entries;
    int count;
} FSClientDataGroupArray;

typedef struct fs_client_cluster_stat_entry {
    int data_group_id;
    int server_id;
    bool is_preseted;
    bool is_master;
    char status;
    uint16_t port;
    char ip_addr[IP_ADDRESS_SIZE];
    int64_t data_version;
} FSClientClusterStatEntry;

typedef struct fs_client_server_space_stat {
    int server_id;
    FSClusterSpaceStat stat;
} FSClientServerSpaceStat;

typedef struct fs_connection_manager {
    /* get the specify connection by ip and port */
    fs_get_spec_connection_func get_spec_connection;

    /* get one connection of the configured servers by data group */
    fs_get_connection_func get_connection;

    /* get one connection of the server */
    fs_get_server_connection_func get_server_connection;

    /* get the master connection from the server */
    fs_get_connection_func get_master_connection;

    /* get one readable connection from the server */
    fs_get_connection_func get_readable_connection;

    /* get the leader connection from the server */
    fs_get_server_connection_func get_leader_connection;

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
        int group_index;
    } cluster_cfg;
    FSConnectionManager conn_manager;
    bool inited;
    bool is_simple_conn_mananger;
    bool idempotency_enabled;
    SFDataReadRule read_rule;  //the rule for read
    int connect_timeout;
    int network_timeout;
    SFNetRetryConfig net_retry_cfg;
} FSClientContext;


#define FS_CFG_SERVICE_INDEX(client_ctx)  \
    client_ctx->cluster_cfg.group_index

#define FS_CFG_SERVICE_ADDRESS_ARRAY(client_ctx, server) \
    (server)->group_addrs[FS_CFG_SERVICE_INDEX(client_ctx)].address_array

#define FS_CLIENT_DATA_GROUP_INDEX(client_ctx, hash_code) \
    (hash_code % FS_DATA_GROUP_COUNT(*client_ctx->cluster_cfg.ptr))

#endif
