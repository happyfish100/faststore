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
#include "sf/sf_shared_mbuffer.h"
#include "fastcfs/auth/client_types.h"
#include "common/fs_global.h"
#include "db/db_interface.h"
#include "server_types.h"
#include "storage/committed_version.h"

struct ibv_pd;

typedef enum {
    fs_sn_type_slice_loading = 0,  /* slice load */
    fs_sn_type_block_removing,     /* migrate clean */
    fs_sn_type_slice_binlog        /* normal running */
} FSStorageSNType;

typedef struct server_global_vars {
    struct {
        int cpu_count;
        int64_t total_memory;
    } system;

    struct {
        const char *program_filename;
        const char *config_filename;
    } cmdline;

    struct {
        int task_padding_size;
        sf_init_connection_callback init_connection;
        struct ibv_pd *cluster_pd;
        struct ibv_pd *replica_pd;
        struct ibv_pd *service_pd;
    } rdma;

    struct {
        struct {
            int64_t value;
            double ratio;
        } net_buffer_memory_limit;
    } network;

    struct {
        FCFSAuthClientFullContext auth;
        FSClusterServerInfo *myself;
        volatile FSClusterServerInfo *leader;
        struct {
            FSClusterConfig ctx;
            int min_server_group_id;  //for vote node
            bool migrate_clean;
            struct {
                SFElectionQuorum quorum;
                bool force;
                bool vote_node_enabled;
                int leader_lost_timeout;
                int max_wait_time;
                int max_shutdown_duration;
            } leader_election;

            struct {
                bool resume_master_role;
                bool failover;
                char policy;
                int timeouts;   //in seconds
            } master_election;
        } config;

        FSClusterServerArray server_array;
        FSClusterDataGroupArray data_group_array;
        int dg_server_count;
        int my_data_group_count;

        volatile uint64_t current_version;

        /* follower ping leader or leader check brain-split */
        volatile time_t last_heartbeat_time;
        time_t last_shutdown_time;

#ifdef FS_EVENT_DEBUG_FLAG
        struct {
            volatile int64_t produce;
            volatile int64_t consume;
        } event_stats;
#endif
        struct {
            int max_events_per_pkg;
            int active_test_interval;
        } topology;

        SFContext sf_context;  //for cluster communication
        FCServerGroupInfo *server_group;
        ConnectionExtraParams conn_extra_params;
    } cluster;

    struct {
        string_t path;   //data path
        int thread_count;
        int binlog_buffer_size;
        bool binlog_call_fsync;
        bool all_groups_have_data;
        int local_binlog_check_last_seconds;
        int slave_binlog_check_last_rows;
    } data;

    struct {
        DAContext da_ctx;
        short ob_element_size;
        struct {
            bool write_to_cache;
            int cache_flush_max_delay;  //in seconds
            struct {
                int shared_lock_count;
                int64_t hashtable_capacity;
            } object_block;

            DADataConfig data_cfg;
            char *storage_filename;
        } cfg;
        int rebuild_threads;
        struct {
            volatile int64_t slice_count;//slice count in slice binlog
            int index;
            const char *str;
        } rebuild_path;
        struct fast_allocator_context wbuffer_allocator;
        FSCommittedVersionRing committed_version_ring;
        FSSliceSpaceLogContext slice_space_log_ctx;
    } storage;

    struct {
        bool enabled;
        short event_dealer_thread_count;
        int batch_store_on_modifies;
        int batch_store_interval;
        int eliminate_interval;
        FSStorageSNType sn_type;
        FSStorageEngineConfig cfg;
        double memory_limit;  //ratio, server + plugin
        struct {
            int64_t ob_count;
            int64_t slice_count;
        } stats;
        char *library;
        FSStorageEngineInterface api;
        struct fast_mblock_man event_allocator; //element: FSChangeNotifyEvent
    } slice_storage;   //slice storage engine

    struct {
        SFReplicationQuorum quorum;
        int deactive_on_failures;

        /* cached result of SF_REPLICATION_QUORUM_NEED_MAJORITY */
        bool quorum_need_majority;

        /* cached result of SF_REPLICATION_QUORUM_NEED_DETECT */
        bool quorum_need_detect;

        bool quorum_rollback_done;  //for startup
        int channels_between_two_servers;
        int recovery_concurrent;
        int recovery_threads_per_data_group;
        int recovery_max_queue_depth;
        int active_test_interval;   //round(nework_timeout / 2)
        struct {
            int keep_days;
            TimeInfo delete_time;

            struct {
                SFBinlogWriterInfo **writers;
                SFBinlogWriterInfo *holders;
                int count;
                int base_id;
            } binlog_writer_array;
            SFBinlogWriterThread binlog_writer_thread; //only one write thread
        } binlog;

        int64_t dedup_memory_limit; //memory limit for recovery dedup
        SFContext sf_context;       //for replica communication
        FCServerGroupInfo *server_group;
        SFNetworkHandler *network_handler;
        ConnectionExtraParams conn_extra_params;
    } replica;

    struct {
        //volatile int64_t total_count;
        struct {
            struct {
                volatile bool done;
                volatile uint64_t last_sn;
            } data_load;

            struct {
                bool enabled;
                bool in_progress;
                double target_ratio;
                TimeInfo time;
            } dedup;

            struct {
                int keep_days;
                TimeInfo time;
                volatile int remove_files;
            } cleanup;

            volatile int64_t record_count;
            volatile uint64_t sn;  //slice binlog sn
            SFBinlogWriterContext binlog_writer;
        } binlog;

    } slice;

    OBHashtable ob_hashtable;

    SFSlowLogContext slow_log;

    FCThreadPool thread_pool;

    SFSharedMBufferContext shared_mbuffer_ctx;

} FSServerGlobalVars;

#define SYSTEM_CPU_COUNT      g_server_global_vars->system.cpu_count
#define SYSTEM_TOTAL_MEMORY   g_server_global_vars->system.total_memory

#define CMDLINE_PROGRAM_FILENAME g_server_global_vars->cmdline.program_filename
#define CMDLINE_CONFIG_FILENAME  g_server_global_vars->cmdline.config_filename

#define TASK_PADDING_SIZE      g_server_global_vars->rdma.task_padding_size
#define RDMA_INIT_CONNECTION   g_server_global_vars->rdma.init_connection
#define CLUSTER_RDMA_PD        g_server_global_vars->rdma.cluster_pd
#define REPLICA_RDMA_PD        g_server_global_vars->rdma.replica_pd
#define SERVICE_RDMA_PD        g_server_global_vars->rdma.service_pd

#define CLUSTER_CONFIG_CTX    g_server_global_vars->cluster.config.ctx
#define SERVER_CONFIG_CTX     g_server_global_vars->cluster.config.ctx.server_cfg
#define AUTH_CTX              g_server_global_vars->cluster.auth
#define AUTH_CLIENT_CTX       AUTH_CTX.ctx
#define AUTH_ENABLED          AUTH_CTX.enabled

#define CT_MAX_EVENTS_PER_PKG   g_server_global_vars->cluster. \
    topology.max_events_per_pkg
#define CT_ACTIVE_TEST_INTERVAL g_server_global_vars->cluster. \
    topology.active_test_interval

#define EVENT_STATS_PRODUCE   g_server_global_vars->cluster.event_stats.produce
#define EVENT_STATS_CONSUME   g_server_global_vars->cluster.event_stats.consume

#define MIGRATE_CLEAN_ENABLED  g_server_global_vars->cluster. \
    config.migrate_clean

#define DATA_REBUILD_PATH_STR    g_server_global_vars->storage.rebuild_path.str
#define DATA_REBUILD_PATH_INDEX  g_server_global_vars->storage.rebuild_path.index
#define DATA_REBUILD_SLICE_COUNT g_server_global_vars-> \
    storage.rebuild_path.slice_count
#define DATA_REBUILD_THREADS g_server_global_vars->storage.rebuild_threads
#define READ_DIRECT_IO_PATHS DA_CTX.storage.read_direct_io_paths

#define COMMITTED_VERSION_RING  g_server_global_vars-> \
    storage.committed_version_ring

#define SLICE_SPACE_LOG_CTX  g_server_global_vars->storage.slice_space_log_ctx

#define LEADER_ELECTION_QUORUM g_server_global_vars->cluster. \
    config.leader_election.quorum
#define FORCE_LEADER_ELECTION  g_server_global_vars->cluster. \
    config.leader_election.force
#define VOTE_NODE_ENABLED      g_server_global_vars->cluster. \
    config.leader_election.vote_node_enabled
#define LEADER_ELECTION_LOST_TIMEOUT  g_server_global_vars->cluster. \
    config.leader_election.leader_lost_timeout
#define LEADER_ELECTION_MAX_WAIT_TIME g_server_global_vars->cluster. \
    config.leader_election.max_wait_time
#define LEADER_ELECTION_MAX_SHUTDOWN_DURATION g_server_global_vars->cluster. \
    config.leader_election.max_shutdown_duration

#define RESUME_MASTER_ROLE        g_server_global_vars->cluster. \
    config.master_election.resume_master_role
#define MASTER_ELECTION_FAILOVER  g_server_global_vars->cluster. \
    config.master_election.failover
#define MASTER_ELECTION_POLICY  g_server_global_vars->cluster.   \
    config.master_election.policy
#define MASTER_ELECTION_TIMEOUTS  g_server_global_vars->cluster. \
    config.master_election.timeouts

#define CLUSTER_MYSELF_PTR    g_server_global_vars->cluster.myself
#define MYSELF_IS_LEADER      CLUSTER_MYSELF_PTR->is_leader
#define CLUSTER_LEADER_PTR    g_server_global_vars->cluster.leader
#define CLUSTER_LEADER_ATOM_PTR  ((FSClusterServerInfo *)__sync_add_and_fetch(  \
        &CLUSTER_LEADER_PTR, 0))

#define CLUSTER_SERVER_ARRAY  g_server_global_vars->cluster.server_array
#define CLUSTER_DATA_GROUP_ARRAY g_server_global_vars->cluster.data_group_array
#define CLUSTER_DG_SERVER_COUNT  g_server_global_vars->cluster.dg_server_count

#define CLUSTER_MY_SERVER_ID  CLUSTER_MYSELF_PTR->server->id
#define MY_DATA_GROUP_COUNT   g_server_global_vars->cluster.my_data_group_count

#define CLUSTER_SERVER_GROUP_ID  g_server_global_vars->cluster. \
    config.min_server_group_id

#define CLUSTER_LAST_HEARTBEAT_TIME g_server_global_vars-> \
    cluster.last_heartbeat_time
#define CLUSTER_LAST_SHUTDOWN_TIME  g_server_global_vars-> \
    cluster.last_shutdown_time

#define CLUSTER_CURRENT_VERSION g_server_global_vars->cluster.current_version

//#define SLICE_TOTAL_COUNT g_server_global_vars->slice.total_count
#define SLICE_BINLOG_COUNT  g_server_global_vars->slice.binlog.record_count
#define SLICE_BINLOG_SN     g_server_global_vars->slice.binlog.sn
#define SLICE_LOAD_DONE     g_server_global_vars->slice.binlog.data_load.done
#define SLICE_LOAD_LAST_SN  g_server_global_vars->slice.binlog.data_load.last_sn
#define SLICE_BINLOG_WRITER g_server_global_vars->slice.binlog.binlog_writer

#define SLICE_DEDUP_ENABLED     g_server_global_vars->slice.binlog.dedup.enabled
#define SLICE_DEDUP_IN_PROGRESS g_server_global_vars->slice.binlog.dedup.in_progress
#define SLICE_DEDUP_RATIO       g_server_global_vars->slice.binlog.dedup.target_ratio
#define SLICE_DEDUP_TIME        g_server_global_vars->slice.binlog.dedup.time

#define SLICE_KEEP_DAYS    g_server_global_vars->slice.binlog.cleanup.keep_days
#define SLICE_DELETE_TIME  g_server_global_vars->slice.binlog.cleanup.time
#define SLICE_REMOVE_FILES g_server_global_vars->slice.binlog.cleanup.remove_files

#define REPLICATION_QUORUM  g_server_global_vars->replica.quorum
#define REPLICA_QUORUM_NEED_MAJORITY g_server_global_vars-> \
    replica.quorum_need_majority
#define REPLICA_QUORUM_NEED_DETECT   g_server_global_vars-> \
    replica.quorum_need_detect
#define REPLICA_QUORUM_DEACTIVE_ON_FAILURES g_server_global_vars-> \
    replica.deactive_on_failures
#define REPLICA_QUORUM_ROLLBACK_DONE g_server_global_vars-> \
    replica.quorum_rollback_done
#define REPLICA_KEEP_DAYS   g_server_global_vars->replica.binlog.keep_days
#define REPLICA_DELETE_TIME g_server_global_vars->replica.binlog.delete_time

#define REPLICA_BINLOG_WRITER_ARRAY   g_server_global_vars-> \
    replica.binlog.binlog_writer_array
#define REPLICA_BINLOG_WRITER_THREAD  g_server_global_vars-> \
    replica.binlog.binlog_writer_thread

#define LOCAL_BINLOG_CHECK_LAST_SECONDS g_server_global_vars->data. \
    local_binlog_check_last_seconds

#define SLAVE_BINLOG_CHECK_LAST_ROWS    g_server_global_vars->data. \
    slave_binlog_check_last_rows

#define SERVICE_SF_CTX            g_sf_context
#define CLUSTER_SF_CTX            g_server_global_vars->cluster.sf_context
#define CLUSTER_SERVER_GROUP      g_server_global_vars->cluster.server_group
#define CLUSTER_CONN_EXTRA_PARAMS g_server_global_vars->cluster.conn_extra_params

#define REPLICA_SF_CTX            g_server_global_vars->replica.sf_context
#define REPLICA_SERVER_GROUP      g_server_global_vars->replica.server_group
#define REPLICA_NET_HANDLER       g_server_global_vars->replica.network_handler
#define REPLICA_CONN_EXTRA_PARAMS g_server_global_vars->replica.conn_extra_params

#define CLUSTER_CONNECT_TIMEOUT   CLUSTER_SF_CTX.net_buffer_cfg.connect_timeout
#define CLUSTER_NETWORK_TIMEOUT   CLUSTER_SF_CTX.net_buffer_cfg.network_timeout

#define REPLICA_CONNECT_TIMEOUT   REPLICA_SF_CTX.net_buffer_cfg.connect_timeout
#define REPLICA_NETWORK_TIMEOUT   REPLICA_SF_CTX.net_buffer_cfg.network_timeout

#define OB_ELEMENT_SIZE       g_server_global_vars->storage.ob_element_size
#define DA_CTX                g_server_global_vars->storage.da_ctx
#define STORAGE_CFG           DA_CTX.storage.cfg
#define PATHS_BY_INDEX_PPTR   STORAGE_CFG.paths_by_index.paths

#define WRITE_TO_CACHE            g_server_global_vars->  \
    storage.cfg.write_to_cache
#define CACHE_FLUSH_MAX_DELAY     g_server_global_vars->  \
    storage.cfg.cache_flush_max_delay

#define FILE_BLOCK_SIZE g_server_global_vars->cluster.config.ctx.file_block.size
#define USE_HASH_FUNC   g_server_global_vars->cluster.config.ctx.use_hash_func
#define CALC_BLOCK_HASHCODE(bkey) fs_calc_block_hashcode( \
        bkey, FILE_BLOCK_SIZE, USE_HASH_FUNC)

#define OB_HASHTABLE_CAPACITY     g_server_global_vars->  \
    storage.cfg.object_block.hashtable_capacity
#define OB_SHARED_LOCK_COUNT      g_server_global_vars->  \
    storage.cfg.object_block.shared_lock_count

#define DATA_CFG          g_server_global_vars->storage.cfg.data_cfg
#define STORAGE_FILENAME  g_server_global_vars->storage.cfg.storage_filename

#define NET_BUFFER_MEMORY_LIMIT g_server_global_vars->  \
    network.net_buffer_memory_limit

#define DATA_THREAD_COUNT     g_server_global_vars->data.thread_count
#define BINLOG_BUFFER_SIZE    g_server_global_vars->data.binlog_buffer_size
#define BINLOG_CALL_FSYNC     g_server_global_vars->data.binlog_call_fsync
#define ALL_GROUPS_HAVE_DATA  g_server_global_vars->data.all_groups_have_data
#define DATA_PATH             g_server_global_vars->data.path
#define DATA_PATH_STR         DATA_PATH.str
#define DATA_PATH_LEN         DATA_PATH.len


#define STORAGE_ENABLED         g_server_global_vars->slice_storage.enabled
#define STORAGE_PATH            g_server_global_vars->slice_storage.cfg.path
#define STORAGE_PATH_STR        STORAGE_PATH.str
#define STORAGE_PATH_LEN        STORAGE_PATH.len

#define STORAGE_SN_TYPE         g_server_global_vars->slice_storage.sn_type

#define STORAGE_ENGINE_LIBRARY  g_server_global_vars->slice_storage.library
#define BATCH_STORE_INTERVAL    g_server_global_vars->slice_storage.batch_store_interval
#define BATCH_STORE_ON_MODIFIES g_server_global_vars->slice_storage.batch_store_on_modifies
#define BLOCK_BINLOG_SUBDIRS    g_server_global_vars->slice_storage.cfg.block_segment.subdirs
#define TRUNK_INDEX_DUMP_INTERVAL   g_server_global_vars->slice_storage.cfg.trunk.index_dump_interval
#define TRUNK_INDEX_DUMP_BASE_TIME  g_server_global_vars->slice_storage.cfg.trunk.index_dump_base_time
#define BLOCK_ELIMINATE_INTERVAL  g_server_global_vars->slice_storage.eliminate_interval
#define EVENT_DEALER_THREAD_COUNT g_server_global_vars->slice_storage.event_dealer_thread_count
#define STORAGE_MEMORY_TOTAL_LIMIT  g_server_global_vars->slice_storage.memory_limit
#define STORAGE_ENGINE_OB_COUNT     g_server_global_vars->slice_storage.stats.ob_count
#define STORAGE_ENGINE_SLICE_COUNT  g_server_global_vars->slice_storage.stats.slice_count
#define STORAGE_EVENT_ALLOCATOR     g_server_global_vars->slice_storage.event_allocator

#define STORAGE_ENGINE_INIT_API      g_server_global_vars->slice_storage.api.init
#define STORAGE_ENGINE_START_API     g_server_global_vars->slice_storage.api.start
#define STORAGE_ENGINE_TERMINATE_API g_server_global_vars->slice_storage.api.terminate
#define STORAGE_ENGINE_STORE_API     g_server_global_vars->slice_storage.api.store
#define STORAGE_ENGINE_REDO_API      g_server_global_vars->slice_storage.api.redo
#define STORAGE_ENGINE_FETCH_API     g_server_global_vars->slice_storage.api.fetch
#define STORAGE_ENGINE_WALK_API      g_server_global_vars->slice_storage.api.walk
#define STORAGE_ENGINE_SPACES_STAT_API   \
    g_server_global_vars->slice_storage.api.spaces_stat

#define SLOW_LOG              g_server_global_vars->slow_log
#define SLOW_LOG_CFG          SLOW_LOG.cfg
#define SLOW_LOG_CTX          SLOW_LOG.ctx

#define THREAD_POOL           g_server_global_vars->thread_pool
#define SHARED_MBUFFER_CTX    g_server_global_vars->shared_mbuffer_ctx

#define REPLICA_CHANNELS_BETWEEN_TWO_SERVERS  \
    g_server_global_vars->replica.channels_between_two_servers

#define RECOVERY_CONCURRENT \
    g_server_global_vars->replica.recovery_concurrent

#define RECOVERY_THREADS_PER_DATA_GROUP \
    g_server_global_vars->replica.recovery_threads_per_data_group

#define RECOVERY_MAX_QUEUE_DEPTH \
    g_server_global_vars->replica.recovery_max_queue_depth

#define RECOVERY_DEDUP_MEMORY_LIMIT \
    g_server_global_vars->replica.dedup_memory_limit

#define FS_DATA_GROUP_ID(bkey) (FS_BLOCK_HASH_CODE(bkey) % \
       FS_DATA_GROUP_COUNT(CLUSTER_CONFIG_CTX) + 1)

#define CLUSTER_GROUP_INDEX  g_server_global_vars->cluster.config.ctx.cluster_group_index
#define REPLICA_GROUP_INDEX  g_server_global_vars->cluster.config.ctx.replica_group_index
#define SERVICE_GROUP_INDEX  g_server_global_vars->cluster.config.ctx.service_group_index

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

#define CLUSTER_CONFIG_MD5_SIGNS g_server_global_vars-> \
    cluster.config.ctx.md5_digests
#define SERVERS_CONFIG_SIGN_BUF  CLUSTER_CONFIG_MD5_SIGNS.servers
#define CLUSTER_CONFIG_SIGN_BUF  CLUSTER_CONFIG_MD5_SIGNS.cluster

#define G_OB_HASHTABLE  g_server_global_vars->ob_hashtable

#ifdef __cplusplus
extern "C" {
#endif

    extern FSServerGlobalVars *g_server_global_vars;

#ifdef __cplusplus
}
#endif

#endif
