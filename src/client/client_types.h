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
#include "sf/sf_connection_manager.h"
#include "sf/idempotency/client/client_types.h"
#include "fastcfs/auth/client_types.h"
#include "fs_types.h"
#include "fs_cluster_cfg.h"

#define FS_CLIENT_DEFAULT_CONFIG_FILENAME "/etc/fastcfs/fstore/client.conf"

struct idempotency_client_channel;
struct fs_client_context;

typedef struct fs_client_server_entry {
    int server_id;
    char status;
    ConnectionInfo conn;
} FSClientServerEntry;

typedef struct fs_client_cluster_stat_entry {
    int data_group_id;
    int server_id;
    bool is_preseted;
    bool is_master;
    char status;
    uint16_t port;
    char ip_addr[IP_ADDRESS_SIZE];
    struct {
        int64_t current;
        int64_t confirmed;
    } data_versions;
} FSClientClusterStatEntry;

typedef struct fs_client_cluster_stat_entry_array {
    FSClientClusterStatEntry *stats;
    int size;
    int count;
} FSClientClusterStatEntryArray;

typedef struct fs_client_server_space_stat {
    int server_id;
    FSClusterSpaceStat stat;
} FSClientServerSpaceStat;

typedef struct fs_client_context {
    struct {
        FSClusterConfig *ptr;
        FSClusterConfig holder;
        int group_index;
    } cluster_cfg;
    bool inited;
    bool is_simple_conn_mananger;
    bool idempotency_enabled;
    bool auth_enabled;
    SFClientCommonConfig common_cfg;
    SFConnectionManager cm;
    FCFSAuthClientFullContext auth;
} FSClientContext;

#define FS_FILE_BLOCK_SIZE  g_fs_client_vars.client_ctx.  \
    cluster_cfg.ptr->file_block.size
#define FS_FILE_BLOCK_MASK  g_fs_client_vars.client_ctx.  \
    cluster_cfg.ptr->file_block.mask

#define FS_USE_HASH_FUNC  g_fs_client_vars.client_ctx.    \
    cluster_cfg.ptr->use_hash_func

#define FS_CALC_BLOCK_HASHCODE(bkey) fs_calc_block_hashcode( \
        bkey, FS_FILE_BLOCK_SIZE, FS_USE_HASH_FUNC)

#define FS_CLUSTER_SERVER_CFG(client_ctx)  \
    (client_ctx)->cluster_cfg.ptr->server_cfg

#define FS_CFG_SERVICE_INDEX(client_ctx)  \
    (client_ctx)->cluster_cfg.group_index

#define FS_CFG_SERVICE_ADDRESS_ARRAY(client_ctx, server) \
    (server)->group_addrs[FS_CFG_SERVICE_INDEX(client_ctx)].address_array

#define FS_CLIENT_DATA_GROUP_INDEX(client_ctx, hash_code) \
    (hash_code % FS_DATA_GROUP_COUNT(*(client_ctx)->cluster_cfg.ptr))

#endif
