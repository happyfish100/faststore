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

#ifndef _FS_SERVER_TYPES_H
#define _FS_SERVER_TYPES_H

#include <time.h>
#include <pthread.h>
#include "fastcommon/common_define.h"
#include "fastcommon/fc_queue.h"
#include "fastcommon/fast_task_queue.h"
#include "fastcommon/fast_mblock.h"
#include "fastcommon/fast_allocator.h"
#include "fastcommon/server_id_func.h"
#include "fastcommon/shared_buffer.h"
#include "fastcommon/fc_atomic.h"
#include "sf/idempotency/server/request_metadata.h"
#include "diskallocator/binlog/common/binlog_types.h"
#include "common/fs_types.h"
#include "common/fs_proto.h"
#include "common/fs_server_types.h"
#include "storage/storage_types.h"

//for event debug
//#define FS_EVENT_DEBUG_FLAG

#define FS_SPACE_ALIGN_SIZE  8
#define FS_TRUNK_BINLOG_MAX_RECORD_SIZE    128
#define FS_TRUNK_BINLOG_SUBDIR_NAME_STR    "trunk"
#define FS_TRUNK_BINLOG_SUBDIR_NAME_LEN    \
    (sizeof(FS_TRUNK_BINLOG_SUBDIR_NAME_STR) - 1)

#define FS_SLICE_BINLOG_MIN_RECORD_SIZE     32
#define FS_SLICE_BINLOG_MAX_RECORD_SIZE    256
#define FS_SLICE_BINLOG_SUBDIR_NAME_STR    "slice"
#define FS_SLICE_BINLOG_SUBDIR_NAME_LEN    \
    (sizeof(FS_SLICE_BINLOG_SUBDIR_NAME_STR) - 1)

#define FS_REPLICA_BINLOG_MIN_RECORD_SIZE   32
#define FS_REPLICA_BINLOG_MAX_RECORD_SIZE  128
#define FS_REPLICA_BINLOG_SUBDIR_NAME_STR  "replica"
#define FS_REPLICA_BINLOG_SUBDIR_NAME_LEN  \
    (sizeof(FS_REPLICA_BINLOG_SUBDIR_NAME_STR) - 1)

#define FS_RECOVERY_BINLOG_SUBDIR_NAME_STR   "recovery"
#define FS_RECOVERY_BINLOG_SUBDIR_NAME_LEN   \
    (sizeof(FS_RECOVERY_BINLOG_SUBDIR_NAME_STR) - 1)

#define FS_REBUILD_BINLOG_SUBDIR_NAME_STR  "rebuild"
#define FS_REBUILD_BINLOG_SUBDIR_NAME_LEN  \
    (sizeof(FS_REBUILD_BINLOG_SUBDIR_NAME_STR) - 1)

#define FS_BINLOG_SUBDIR_NAME_SIZE          64
#define FS_BINLOG_FILENAME_SUFFIX_SIZE      32
#define FS_BINLOG_MAX_RECORD_SIZE  FS_SLICE_BINLOG_MAX_RECORD_SIZE

#define FS_SERVER_TASK_TYPE_RELATIONSHIP        1   //follower -> leader
#define FS_SERVER_TASK_TYPE_CLUSTER_PUSH        2   //leader -> follower
#define FS_SERVER_TASK_TYPE_FETCH_BINLOG        3   //slave  -> master
#define FS_SERVER_TASK_TYPE_SYNC_BINLOG         4   //slave  -> master
#define FS_SERVER_TASK_TYPE_REPLICATION_CLIENT  5
#define FS_SERVER_TASK_TYPE_REPLICATION_SERVER  6

#define FS_REPLICATION_STAGE_NONE               0
#define FS_REPLICATION_STAGE_INITED             1
#define FS_REPLICATION_STAGE_CONNECTING         2
#define FS_REPLICATION_STAGE_WAITING_JOIN_RESP  3
#define FS_REPLICATION_STAGE_SYNCING            4

#define FS_CLUSTER_DELAY_DECISION_NO_OP         0
#define FS_CLUSTER_DELAY_DECISION_SELECT_MASTER 1

#define FS_DEFAULT_DATA_THREAD_COUNT                     8
#define FS_MIN_DATA_THREAD_COUNT                         2
#define FS_MAX_DATA_THREAD_COUNT                       256

#define FS_DEFAULT_RECOVERY_CONCURRENT                   4
#define FS_MIN_RECOVERY_CONCURRENT                       1
#define FS_MAX_RECOVERY_CONCURRENT                      64

#define FS_DEFAULT_REPLICA_CHANNELS_BETWEEN_TWO_SERVERS  2
#define FS_MIN_REPLICA_CHANNELS_BETWEEN_TWO_SERVERS      1
#define FS_MAX_REPLICA_CHANNELS_BETWEEN_TWO_SERVERS     64

#define FS_DEFAULT_RECOVERY_THREADS_PER_DATA_GROUP       4
#define FS_MIN_RECOVERY_THREADS_PER_DATA_GROUP           1
#define FS_MAX_RECOVERY_THREADS_PER_DATA_GROUP         256

#define FS_DEFAULT_RECOVERY_MAX_QUEUE_DEPTH              2
#define FS_MIN_RECOVERY_MAX_QUEUE_DEPTH                  1
#define FS_MAX_RECOVERY_MAX_QUEUE_DEPTH                 64

#define FS_DEFAULT_DATA_REBUILD_THREADS                  8
#define FS_MIN_DATA_REBUILD_THREADS                      1
#define FS_MAX_DATA_REBUILD_THREADS                   1024

#define FS_DEFAULT_LOCAL_BINLOG_CHECK_LAST_SECONDS       5
#define FS_DEFAULT_SLAVE_BINLOG_CHECK_LAST_ROWS          5
#define FS_MIN_SLAVE_BINLOG_CHECK_LAST_ROWS              0
#define FS_MAX_SLAVE_BINLOG_CHECK_LAST_ROWS            128

#define FS_DEFAULT_TRUNK_FILE_SIZE  (256 * 1024 * 1024)
#define FS_TRUNK_FILE_MIN_SIZE      ( 64 * 1024 * 1024)
#define FS_TRUNK_FILE_MAX_SIZE      (  2 * 1024 * 1024 * 1024)

#define FS_DEFAULT_DISCARD_REMAIN_SPACE_SIZE  4096
#define FS_DISCARD_REMAIN_SPACE_MIN_SIZE       256
#define FS_DISCARD_REMAIN_SPACE_MAX_SIZE      (256 * 1024)

#define FS_BLOCK_BINLOG_DEFAULT_SUBDIRS          128
#define FS_BLOCK_BINLOG_MIN_SUBDIRS               32
#define FS_BLOCK_BINLOG_MAX_SUBDIRS              256
#define FS_DEFAULT_BATCH_STORE_ON_MODIFIES    102400
#define FS_DEFAULT_BATCH_STORE_INTERVAL           60
#define FS_DEFAULT_TRUNK_INDEX_DUMP_INTERVAL   86400
#define FS_DEFAULT_ELIMINATE_INTERVAL              1

#define TASK_STATUS_CONTINUE   12345

#define FS_SERVER_STATUS_OFFLINE    0
#define FS_SERVER_STATUS_ONLINE     1
#define FS_SERVER_STATUS_ACTIVE     2

#define FS_DEFAULT_MASTER_ELECTION_TIMEOUTS    60
#define FS_MASTER_ELECTION_POLICY_STRICT_INT   'S'
#define FS_MASTER_ELECTION_POLICY_TIMEOUT_INT  'T'

#define FS_MASTER_ELECTION_POLICY_STRICT_STR   "strict"
#define FS_MASTER_ELECTION_POLICY_TIMEOUT_STR  "timeout"
#define FS_MASTER_ELECTION_POLICY_STRICT_LEN   \
    (sizeof(FS_MASTER_ELECTION_POLICY_STRICT_STR) - 1)
#define FS_MASTER_ELECTION_POLICY_TIMEOUT_LEN  \
    (sizeof(FS_MASTER_ELECTION_POLICY_TIMEOUT_STR) - 1)

#define FS_WHICH_SIDE_MASTER    'M'
#define FS_WHICH_SIDE_SLAVE     'S'

#define FS_EVENT_TYPE_STATUS_CHANGE       1
#define FS_EVENT_TYPE_CURRENT_DV_CHANGE   2
#define FS_EVENT_TYPE_CONFIRMED_DV_CHANGE 4
#define FS_EVENT_TYPE_MASTER_CHANGE       8

#define FS_EVENT_TYPE_DV_CHANGE (FS_EVENT_TYPE_CURRENT_DV_CHANGE | \
        FS_EVENT_TYPE_CONFIRMED_DV_CHANGE)

#define FS_EVENT_SOURCE_SELF_PING       'P'
#define FS_EVENT_SOURCE_SELF_REPORT     'R'
#define FS_EVENT_SOURCE_MASTER_REPORT   'M'
#define FS_EVENT_SOURCE_MASTER_OFFLINE  'm'
#define FS_EVENT_SOURCE_CS_LEADER       'L'

#define FS_FORCE_ELECTION_LONG_OPTION_STR  "force-leader-election"
#define FS_FORCE_ELECTION_LONG_OPTION_LEN  (sizeof( \
            FS_FORCE_ELECTION_LONG_OPTION_STR) - 1)

#define FS_MIGRATE_CLEAN_LONG_OPTION_STR  "migrate-clean"
#define FS_MIGRATE_CLEAN_LONG_OPTION_LEN  (sizeof( \
            FS_MIGRATE_CLEAN_LONG_OPTION_STR) - 1)

#define FS_DATA_REBUILD_LONG_OPTION_STR  "data-rebuild"
#define FS_DATA_REBUILD_LONG_OPTION_LEN  (sizeof( \
            FS_DATA_REBUILD_LONG_OPTION_STR) - 1)

#define FS_TASK_BUFFER_FRONT_PADDING_SIZE  (sizeof(FSProtoHeader) + \
        4 * sizeof(FSProtoSliceWriteReqHeader) +  \
        sizeof(FSProtoReplicaRPCReqBodyPart))

#define TASK_PENDING_SEND_COUNT task->pending_send_count
#define TASK_ARG          ((FSServerTaskArg *)task->arg)
#define TASK_CTX          TASK_ARG->context
#define REQUEST           TASK_CTX.common.request
#define RESPONSE          TASK_CTX.common.response
#define RESPONSE_STATUS   RESPONSE.header.status
#define REQUEST_STATUS    REQUEST.header.status
#define RECORD            TASK_CTX.service.record
#define CLUSTER_PEER      TASK_CTX.shared.cluster.peer
#define REPLICA_REPLICATION     TASK_CTX.shared.replica.replication

#define REPLICA_RPC_WAITING_COUNT   TASK_CTX.shared.replica.rpc_waiting_count
#define REPLICA_RPC_FAIL_COUNT      TASK_CTX.shared.replica.rpc_fail_count

#define REPLICA_READER       TASK_CTX.shared.replica.reader
#define REPLICA_UNTIL_OFFSET TASK_CTX.shared.replica.until_offset
#define IDEMPOTENCY_CHANNEL  TASK_CTX.shared.service.idempotency_channel
#define IDEMPOTENCY_REQUEST  TASK_CTX.service.idempotency_request
#define SERVER_TASK_TYPE  TASK_CTX.task_type
#define SLICE_OP_CTX      TASK_CTX.slice_op_ctx
#define OP_CTX_INFO       TASK_CTX.slice_op_ctx.info
#define OP_CTX_NOTIFY_FUNC TASK_CTX.slice_op_ctx.notify_func

#define SERVER_CTX           ((FSServerContext *)task->thread_data->arg)

typedef void (*server_free_func)(void *ptr);
typedef void (*server_free_func_ex)(void *ctx, void *ptr);

typedef struct {
    int inc_alloc;
} FSUpdateOutput;  //for idempotency

struct fs_replication;
typedef struct fs_replication_array {
    struct fs_replication *replications;
    int count;
} FSReplicationArray;

typedef struct fs_replication_ptr_array {
    int count;
    struct fs_replication **replications;
} FSReplicationPtrArray;

struct fs_cluster_data_server_info;
typedef struct fs_data_server_change_event {
    struct fs_cluster_data_server_info *ds;
    short source;  //for hint/debug only
    short type;    //for hint/debug only
#ifdef FS_EVENT_DEBUG_FLAG
    int64_t sn;
#endif
    volatile int in_queue;
    struct fs_data_server_change_event *next;  //for queue
} FSDataServerChangeEvent;

typedef struct fs_cluster_topology_notify_context {
    int server_id;
    volatile struct fast_task_info *task;
    struct fc_queue queue; //push data_server changes to the follower
    FSDataServerChangeEvent *events; //event array
} FSClusterTopologyNotifyContext;

typedef struct fs_cluster_notify_context_ptr_array {
    FSClusterTopologyNotifyContext **contexts;
    int count;
    int alloc;
} FSClusterNotifyContextPtrArray;

typedef struct fs_cluster_data_server_ptr_array {
    struct fs_cluster_data_server_info **servers;
    int count;
    int alloc;
} FSClusterDataServerPtrArray;

typedef FSClusterSpaceStat FSClusterServerSpaceStat;

typedef struct fs_cluster_server_info {
    FCServerInfo *server;
    FSReplicationPtrArray repl_ptr_array;
    FSClusterTopologyNotifyContext notify_ctx;
    FSClusterDataServerPtrArray ds_ptr_array;
    bool is_leader;       //for hint only
    volatile char status; //for push topology change notify
    int status_changed_time;
    int server_index;       //for offset
    int link_index;         //for next links
    time_t last_ping_time;  //for the leader
    time_t last_net_comm_time;   //last network communication time for cluster
    int64_t leader_version; //for generation check
    int64_t key;            //for leader call follower to unset master
    FSClusterServerSpaceStat space_stat;
} FSClusterServerInfo;

typedef struct fs_cluster_server_array {
    FSClusterServerInfo *servers;
    int count;
} FSClusterServerArray;

typedef struct fs_cluster_server_ptr_array {
    FSClusterServerInfo **servers;
    int count;
} FSClusterServerPtrArray;

typedef struct fs_cluster_data_version_pair {
    int64_t current;
    int64_t confirmed;
} FSClusterDataVersionPair;

struct fs_cluster_data_group_info;

typedef struct fs_cluster_data_server_info {
    struct fs_cluster_data_group_info *dg;
    FSClusterServerInfo *cs;
    volatile int master_dealing_count;
    char is_preseted;
    volatile char is_master;
    volatile char status;   //the data server status

    struct {
        struct {
            bool in_queue;
            int check_count;
            struct fc_list_head dlink;
        } detect;

        struct {
            bool in_queue;
            int check_count;
            struct fc_list_head dlink;
        } cleanup;
    } replica_quorum;

    struct {
        volatile char in_progress;  //if recovery in progress
        int continuous_fail_count;
        volatile uint64_t until_version;
    } recovery;

    struct {
        pthread_lock_cond_pair_t notify; //lock and waiting for slave status change
        volatile uint64_t rpc_last_version;  //check rpc finished when recovery
    } replica;

    struct {
        volatile uint64_t current_version;
        volatile uint64_t confirmed_version; //for replication quorum majority
    } data;

    FSClusterDataVersionPair last_report_versions; //record last data versions to the leader
} FSClusterDataServerInfo;

typedef struct fs_cluster_data_server_array {
    FSClusterDataServerInfo *servers;
    int count;
} FSClusterDataServerArray;

typedef struct fs_replication_quorum_entry {
    int64_t data_version;
    struct fast_task_info *task;
    struct fs_replication_quorum_entry *next;
} FSReplicationQuorumEntry;

typedef struct fs_replication_quorum_context {
    SFSynchronizeContext sctx;

    struct {
        struct fast_mblock_man allocator;
        volatile int count;
        FSReplicationQuorumEntry *head;
        FSReplicationQuorumEntry *tail;
    } list;

    volatile int dealing;
    struct {
        volatile int64_t counter;

        volatile char slave_version_changed;
        struct {
            volatile char flag;
            volatile int64_t value;
        } set_version;

    } confirmed;

    FSClusterDataServerInfo *myself;
    struct fs_replication_quorum_context *next;  //for queue
} FSReplicationQuorumContext;

typedef struct fs_cluster_data_group_info {
    int id;
    int index;
    volatile char active_count;
    volatile char is_my_term;
    volatile char master_swapping;

    struct {
        /* cached result of SF_REPLICATION_QUORUM_NEED_MAJORITY */
        volatile char need_majority;

        /* cached result of SF_REPLICATION_QUORUM_NEED_DETECT */
        bool need_detect;
    } replica_quorum;

    struct {
        uint32_t hash_code;  //for master assignment
        volatile char in_queue;
        volatile char in_delay_queue;
        volatile char reselect;
        int retry_count;
        int64_t start_time_ms;
    } election;  //for master select

    FSClusterDataServerArray data_server_array;
    FSClusterDataServerPtrArray ds_ptr_array;  //for leader select master
    FSClusterDataServerPtrArray slave_ds_array;
    FSClusterDataServerInfo *myself;
    volatile FSClusterDataServerInfo *master;
    volatile FSClusterDataServerInfo *old_master;
    FSReplicationQuorumContext repl_quorum_ctx;
    IdempotencyRequestMetadataContext req_meta_ctx;
} FSClusterDataGroupInfo;

typedef struct fs_cluster_data_group_array {
    FSClusterDataGroupInfo *groups;
    int count;
    int base_id;
} FSClusterDataGroupArray;

typedef struct fs_rpc_result_entry {
    int data_group_id;
    uint64_t data_version;
    struct fast_task_info *waiting_task;
} FSReplicaRPCResultEntry;

typedef struct fs_rpc_result_array {
    FSReplicaRPCResultEntry *results;
    int alloc;
    int count;
} FSReplicaRPCResultArray;

typedef struct fs_replication_context {
    struct {
        struct fc_queue rpc_queue;
        FSReplicaRPCResultArray rpc_result_array;
    } caller;  //master side
} FSReplicationContext;

typedef struct fs_replication {
    struct fast_task_info *task;
    FSClusterServerInfo *peer;
    volatile uint32_t version; //for ds ONLINE to ACTIVE check
    volatile char stage;
    int id;                    //for debug
    int thread_index;          //for nio thread
    int conn_index;            //for connect failover
    time_t last_net_comm_time;    //last network communication time
    struct {
        time_t start_time;
        time_t next_connect_time;
        int last_errno;
        int fail_count;
    } connection_info;  //for client to make connection

    struct {
        FSProtoReplicaRPCReqBodyPart *body_parts;
        struct iovec *io_vecs;
    } rpc;  //forward rpc request

    FSReplicationContext context;
} FSReplication;

typedef struct {
    SFCommonTaskContext common;
    int task_type;
    union {
        struct {
            struct idempotency_channel *idempotency_channel;
        } service;

        struct {
            FSClusterServerInfo *peer;   //the peer server in the cluster
        } cluster;

        struct {
            union {
                struct {
                    FSReplication *replication;
                    volatile int rpc_waiting_count;
                    volatile int rpc_fail_count;
                };

                struct {
                    struct server_binlog_reader *reader; //for fetch/sync binlog
                    int64_t until_offset;  //for sync binlog only
                };
            };
        } replica;
    } shared;

    struct {
        struct idempotency_request *idempotency_request;
        struct {
            volatile int waiting_count;
            volatile int success_count;
        } rpc;
    } service;

    int which_side;   //master or slave
    FSSliceOpContext slice_op_ctx;
} FSServerTaskContext;

typedef struct server_task_arg {
    FSServerTaskContext context;
} FSServerTaskArg;

typedef struct fs_server_context {
    union {
        struct {
            struct fast_mblock_man request_allocator; //for idempotency_request
        } service;

        struct {
            FSClusterNotifyContextPtrArray notify_ctx_ptr_array;
        } cluster;

        struct {
            FSReplicationPtrArray connectings;
            FSReplicationPtrArray connected;
            struct fast_mblock_man op_ctx_allocator; //for slice op buffer context
        } replica;
    };

} FSServerContext;

#endif
