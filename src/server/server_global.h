
#ifndef _FS_SERVER_GLOBAL_H
#define _FS_SERVER_GLOBAL_H

#include "fastcommon/common_define.h"
#include "fastcommon/server_id_func.h"
#include "common/fs_cluster_cfg.h"
#include "sf/sf_global.h"
#include "server_types.h"
#include "storage/storage_config.h"

typedef struct server_global_vars {
    struct {
        FSClusterServerInfo *myself;
        volatile FSClusterServerInfo *leader;
        struct {
            FSClusterConfig ctx;
            struct {
                unsigned char servers[16];
                unsigned char cluster[16];
            } md5_digests;
        } config;

        FSClusterServerArray server_array;
        FSClusterDataGroupArray data_group_array;

        volatile uint64_t current_version;

        SFContext sf_context;  //for cluster communication
    } cluster;

    struct {
        string_t path;   //data path
        int binlog_buffer_size;
        int binlog_check_last_seconds;
        volatile uint64_t slice_binlog_sn;  //slice binlog sn
    } data;

    FSStorageConfig storage_cfg;
    volatile uint64_t next_task_version;

    struct {
        int channels_between_two_servers;
        int recovery_threads_per_data_group;
        int recovery_max_queue_depth;
        int active_test_interval;   //round(nework_timeout / 2)
        SFContext sf_context;       //for replica communication
    } replica;

} FSServerGlobalVars;

#define CLUSTER_CONFIG_CTX    g_server_global_vars.cluster.config.ctx
#define SERVER_CONFIG_CTX     g_server_global_vars.cluster.config.ctx.server_cfg

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
#define BINLOG_CHECK_LAST_SECONDS g_server_global_vars.data.binlog_check_last_seconds

#define CLUSTER_SF_CTX        g_server_global_vars.cluster.sf_context
#define REPLICA_SF_CTX        g_server_global_vars.replica.sf_context

#define STORAGE_CFG           g_server_global_vars.storage_cfg
#define PATHS_BY_INDEX_PPTR   STORAGE_CFG.paths_by_index.paths

#define NEXT_TASK_VERSION     g_server_global_vars.next_task_version

#define BINLOG_BUFFER_SIZE    g_server_global_vars.data.binlog_buffer_size
#define DATA_PATH             g_server_global_vars.data.path
#define DATA_PATH_STR         DATA_PATH.str
#define DATA_PATH_LEN         DATA_PATH.len

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

#define SERVERS_CONFIG_SIGN_BUF g_server_global_vars.cluster.config.md5_digests.servers
#define SERVERS_CONFIG_SIGN_LEN 16

#define CLUSTER_CONFIG_SIGN_BUF g_server_global_vars.cluster.config.md5_digests.cluster
#define CLUSTER_CONFIG_SIGN_LEN 16

#ifdef __cplusplus
extern "C" {
#endif

    extern FSServerGlobalVars g_server_global_vars;

#ifdef __cplusplus
}
#endif

#endif
