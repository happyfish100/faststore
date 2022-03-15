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


#ifndef _FS_SERVER_GLOBAL_H
#define _FS_SERVER_GLOBAL_H

#include "fastcommon/common_define.h"
#include "fastcommon/server_id_func.h"
#include "fastcommon/thread_pool.h"
#include "common/fs_cluster_cfg.h"
#include "sf/sf_global.h"
#include "fastcfs/auth/client_types.h"
#include "common/fs_global.h"
#include "server_types.h"
#include "storage/storage_config.h"

typedef struct server_global_vars {
    struct {
        int cpu_count;
    } system;

    struct {
        FCFSAuthClientFullContext auth;
        FSClusterServerInfo *myself;
        volatile FSClusterServerInfo *leader;
        struct {
            FSClusterConfig ctx;
            bool migrate_clean;
            struct {
                bool force;
                int leader_lost_timeout;
                int max_wait_time;
            } leader_election;

            struct {
                bool failover;
                char policy;
                int timeouts;   //in seconds
            } master_election;
        } config;

        FSClusterServerArray server_array;
        FSClusterDataGroupArray data_group_array;

        volatile uint64_t current_version;

        SFContext sf_context;  //for cluster communication
    } cluster;

    struct {
        string_t path;   //data path
        int thread_count;
        int binlog_buffer_size;
        int local_binlog_check_last_seconds;
        int slave_binlog_check_last_rows;
        volatile uint64_t slice_binlog_sn;  //slice binlog sn
    } data;

    struct {
        FSStorageConfig cfg;
        struct {
            volatile int64_t slice_count; //slice count in slice binlog
            int index;
            const char *str;
        } rebuild_path;
    } storage;

    struct {
        int channels_between_two_servers;
        int recovery_threads_per_data_group;
        int recovery_max_queue_depth;
        int active_test_interval;   //round(nework_timeout / 2)
        SFContext sf_context;       //for replica communication
    } replica;

    SFSlowLogContext slow_log;

    FCThreadPool thread_pool;

} FSServerGlobalVars;

#define SYSTEM_CPU_COUNT      g_server_global_vars.system.cpu_count

#define CLUSTER_CONFIG_CTX    g_server_global_vars.cluster.config.ctx
#define SERVER_CONFIG_CTX     g_server_global_vars.cluster.config.ctx.server_cfg
#define AUTH_CTX              g_server_global_vars.cluster.auth
#define AUTH_CLIENT_CTX       AUTH_CTX.ctx
#define AUTH_ENABLED          AUTH_CTX.enabled

#define MIGRATE_CLEAN_ENABLED  g_server_global_vars.cluster. \
    config.migrate_clean

#define DATA_REBUILD_PATH_STR    g_server_global_vars.storage.rebuild_path.str
#define DATA_REBUILD_PATH_INDEX  g_server_global_vars.storage.rebuild_path.index
#define DATA_REBUILD_SLICE_COUNT g_server_global_vars. \
    storage.rebuild_path.slice_count

#define FORCE_LEADER_ELECTION  g_server_global_vars.cluster. \
    config.leader_election.force
#define LEADER_ELECTION_LOST_TIMEOUT  g_server_global_vars.cluster. \
    config.leader_election.leader_lost_timeout
#define LEADER_ELECTION_MAX_WAIT_TIME g_server_global_vars.cluster. \
    config.leader_election.max_wait_time

#define MASTER_ELECTION_FAILOVER  g_server_global_vars.cluster. \
    config.master_election.failover
#define MASTER_ELECTION_POLICY  g_server_global_vars.cluster.   \
    config.master_election.policy
#define MASTER_ELECTION_TIMEOUTS  g_server_global_vars.cluster. \
    config.master_election.timeouts

#define CLUSTER_MYSELF_PTR    g_server_global_vars.cluster.myself
#define MYSELF_IS_LEADER      CLUSTER_MYSELF_PTR->is_leader
#define CLUSTER_LEADER_PTR    g_server_global_vars.cluster.leader
#define CLUSTER_LEADER_ATOM_PTR  ((FSClusterServerInfo *)__sync_add_and_fetch(  \
        &CLUSTER_LEADER_PTR, 0))

#define CLUSTER_SERVER_ARRAY  g_server_global_vars.cluster.server_array
#define CLUSTER_DATA_RGOUP_ARRAY g_server_global_vars.cluster.data_group_array

#define CLUSTER_MY_SERVER_ID  CLUSTER_MYSELF_PTR->server->id

#define CLUSTER_CURRENT_VERSION   g_server_global_vars.cluster.current_version
#define SLICE_BINLOG_SN           g_server_global_vars.data.slice_binlog_sn
#define LOCAL_BINLOG_CHECK_LAST_SECONDS g_server_global_vars.data. \
    local_binlog_check_last_seconds

#define SLAVE_BINLOG_CHECK_LAST_ROWS    g_server_global_vars.data. \
    slave_binlog_check_last_rows

#define CLUSTER_SF_CTX        g_server_global_vars.cluster.sf_context
#define REPLICA_SF_CTX        g_server_global_vars.replica.sf_context

#define STORAGE_CFG           g_server_global_vars.storage.cfg
#define PATHS_BY_INDEX_PPTR   STORAGE_CFG.paths_by_index.paths

#define DATA_THREAD_COUNT     g_server_global_vars.data.thread_count
#define BINLOG_BUFFER_SIZE    g_server_global_vars.data.binlog_buffer_size
#define DATA_PATH             g_server_global_vars.data.path
#define DATA_PATH_STR         DATA_PATH.str
#define DATA_PATH_LEN         DATA_PATH.len

#define SLOW_LOG              g_server_global_vars.slow_log
#define SLOW_LOG_CFG          SLOW_LOG.cfg
#define SLOW_LOG_CTX          SLOW_LOG.ctx

#define THREAD_POOL           g_server_global_vars.thread_pool

#define REPLICA_CHANNELS_BETWEEN_TWO_SERVERS  \
    g_server_global_vars.replica.channels_between_two_servers

#define RECOVERY_THREADS_PER_DATA_GROUP \
    g_server_global_vars.replica.recovery_threads_per_data_group

#define RECOVERY_MAX_QUEUE_DEPTH \
    g_server_global_vars.replica.recovery_max_queue_depth

#define FS_DATA_GROUP_ID(bkey) (FS_BLOCK_HASH_CODE(bkey) % \
       FS_DATA_GROUP_COUNT(CLUSTER_CONFIG_CTX) + 1)

#define CLUSTER_GROUP_INDEX  g_server_global_vars.cluster.config.ctx.cluster_group_index
#define REPLICA_GROUP_INDEX  g_server_global_vars.cluster.config.ctx.replica_group_index
#define SERVICE_GROUP_INDEX  g_server_global_vars.cluster.config.ctx.service_group_index

#define CLUSTER_GROUP_ADDRESS_ARRAY(server) \
    (server)->group_addrs[CLUSTER_GROUP_INDEX].address_array
#define REPLICA_GROUP_ADDRESS_ARRAY(server) \
    (server)->group_addrs[REPLICA_GROUP_INDEX].address_array
#define SERVICE_GROUP_ADDRESS_ARRAY(server) \
    (server)->group_addrs[SERVICE_GROUP_INDEX].address_array

#define CLUSTER_GROUP_ADDRESS_FIRST_PTR(server) \
    (*(server)->group_addrs[CLUSTER_GROUP_INDEX].address_array.addrs)
#define REPLICA_GROUP_ADDRESS_FIRST_PTR(server) \
    (*(server)->group_addrs[REPLICA_GROUP_INDEX].address_array.addrs)
#define SERVICE_GROUP_ADDRESS_FIRST_PTR(server) \
    (*(server)->group_addrs[SERVICE_GROUP_INDEX].address_array.addrs)


#define CLUSTER_GROUP_ADDRESS_FIRST_IP(server) \
    CLUSTER_GROUP_ADDRESS_FIRST_PTR(server)->conn.ip_addr
#define CLUSTER_GROUP_ADDRESS_FIRST_PORT(server) \
    CLUSTER_GROUP_ADDRESS_FIRST_PTR(server)->conn.port

#define REPLICA_GROUP_ADDRESS_FIRST_IP(server) \
    REPLICA_GROUP_ADDRESS_FIRST_PTR(server)->conn.ip_addr
#define REPLICA_GROUP_ADDRESS_FIRST_PORT(server) \
    REPLICA_GROUP_ADDRESS_FIRST_PTR(server)->conn.port

#define SERVICE_GROUP_ADDRESS_FIRST_IP(server) \
    SERVICE_GROUP_ADDRESS_FIRST_PTR(server)->conn.ip_addr
#define SERVICE_GROUP_ADDRESS_FIRST_PORT(server) \
    SERVICE_GROUP_ADDRESS_FIRST_PTR(server)->conn.port

#define CLUSTER_CONFIG_MD5_SIGNS g_server_global_vars. \
    cluster.config.ctx.md5_digests
#define SERVERS_CONFIG_SIGN_BUF  CLUSTER_CONFIG_MD5_SIGNS.servers
#define CLUSTER_CONFIG_SIGN_BUF  CLUSTER_CONFIG_MD5_SIGNS.cluster

#ifdef __cplusplus
extern "C" {
#endif

    extern FSServerGlobalVars g_server_global_vars;

#ifdef __cplusplus
}
#endif

#endif
