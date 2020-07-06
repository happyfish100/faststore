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
#include "common/fs_types.h"
#include "storage/storage_types.h"

#define FS_TRUNK_BINLOG_MAX_RECORD_SIZE    128
#define FS_TRUNK_BINLOG_SUBDIR_NAME      "trunk"

#define FS_SLICE_BINLOG_MAX_RECORD_SIZE    256
#define FS_SLICE_BINLOG_SUBDIR_NAME      "slice"

#define FS_REPLICA_BINLOG_MAX_RECORD_SIZE  128
#define FS_REPLICA_BINLOG_SUBDIR_NAME    "replica"

#define FS_BINLOG_SUBDIR_NAME_SIZE          32

#define FS_CLUSTER_TASK_TYPE_NONE               0
#define FS_CLUSTER_TASK_TYPE_RELATIONSHIP       1   //slave  -> master
#define FS_CLUSTER_TASK_TYPE_REPLICATION        2

#define FS_REPLICATION_STAGE_NONE               0
#define FS_REPLICATION_STAGE_INITED             1
#define FS_REPLICATION_STAGE_CONNECTING         2
#define FS_REPLICATION_STAGE_WAITING_JOIN_RESP  3
#define FS_REPLICATION_STAGE_SYNCING            4

#define FS_CLUSTER_DELAY_DECISION_NO_OP         0
#define FS_CLUSTER_DELAY_DECISION_CHECK_MASTER  1
#define FS_CLUSTER_DELAY_DECISION_SELECT_MASTER 2

#define FS_DEFAULT_REPLICA_CHANNELS_BETWEEN_TWO_SERVERS  2
#define FS_DEFAULT_TRUNK_FILE_SIZE  (  1 * 1024 * 1024 * 1024LL)
#define FS_TRUNK_FILE_MIN_SIZE      (256 * 1024 * 1024LL)
#define FS_TRUNK_FILE_MAX_SIZE      ( 16 * 1024 * 1024 * 1024LL)

#define FS_DEFAULT_DISCARD_REMAIN_SPACE_SIZE  4096
#define FS_DISCARD_REMAIN_SPACE_MIN_SIZE       256
#define FS_DISCARD_REMAIN_SPACE_MAX_SIZE      (256 * 1024)

#define TASK_STATUS_CONTINUE   12345

#define FS_WHICH_SIDE_MASTER    'M'
#define FS_WHICH_SIDE_SLAVE     'S'

#define TASK_ARG          ((FSServerTaskArg *)task->arg)
#define TASK_CTX          TASK_ARG->context
#define REQUEST           TASK_CTX.request
#define RESPONSE          TASK_CTX.response
#define RESPONSE_STATUS   RESPONSE.header.status
#define REQUEST_STATUS    REQUEST.header.status
#define RECORD            TASK_CTX.service.record
#define WAITING_RPC_COUNT TASK_CTX.service.waiting_rpc_count
#define CLUSTER_PEER      TASK_CTX.cluster.peer
#define CLUSTER_REPLICA   TASK_CTX.cluster.replica
#define CLUSTER_CONSUMER_CTX  TASK_CTX.cluster.consumer_ctx
#define CLUSTER_TASK_TYPE TASK_CTX.cluster.task_type
#define SLICE_OP_CTX      TASK_CTX.slice_op_ctx
#define OP_CTX_INFO       TASK_CTX.slice_op_ctx.info
#define OP_CTX_NOTIFY       TASK_CTX.slice_op_ctx.notify

typedef void (*server_free_func)(void *ptr);
typedef void (*server_free_func_ex)(void *ctx, void *ptr);

typedef struct {
    int index;   //the inner index is important!
    string_t path;
} FSStorePath;

typedef struct {
    int64_t id;
    int64_t subdir;     //in which subdir
} FSTrunkIdInfo;

typedef struct {
    FSStorePath *store;
    FSTrunkIdInfo id_info;
    int64_t offset; //offset of the trunk file
    int64_t size;   //alloced space size
} FSTrunkSpaceInfo;

typedef struct fs_binlog_file_position {
    int index;      //current binlog file
    int64_t offset; //current file offset
} FSBinlogFilePosition;

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
    struct fs_cluster_data_server_info *data_server;
    volatile int in_queue;
    struct fs_data_server_change_event *next;  //for queue
} FSDataServerChangeEvent;

typedef struct fs_cluster_topology_notify_context {
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

typedef struct fs_cluster_server_info {
    FCServerInfo *server;
    FSReplicationPtrArray repl_ptr_array;
    FSClusterTopologyNotifyContext notify_ctx;
    FSClusterDataServerPtrArray ds_ptr_array;
    bool is_leader;
    bool is_partner;     //if my partner
    volatile int active; //for push topology change notify
    int server_index;    //for offset
    int link_index;      //for next links
} FSClusterServerInfo;

typedef struct fs_cluster_server_array {
    FSClusterServerInfo *servers;
    int count;
} FSClusterServerArray;

typedef struct fs_cluster_server_ptr_array {
    FSClusterServerInfo **servers;
    int count;
} FSClusterServerPtrArray;

struct fs_cluster_data_group_info;
typedef struct fs_cluster_data_server_info {
    struct fs_cluster_data_group_info *dg;
    FSClusterServerInfo *cs;
    bool is_preseted;
    volatile char is_master;
    volatile char status;   //the data server status
    uint64_t data_version;  //for replication
    int64_t last_report_version; //for report last data version to the leader
} FSClusterDataServerInfo;

typedef struct fs_cluster_data_server_array {
    FSClusterDataServerInfo *servers;
    int count;
} FSClusterDataServerArray;

typedef struct fs_cluster_data_group_info {
    int id;
    int index;
    uint32_t hash_code;  //for master election
    struct {
        volatile int action;
        int expire_time;
    } delay_decision;
    FSClusterDataServerArray data_server_array;
    FSClusterDataServerPtrArray slave_ds_array;
    FSClusterDataServerInfo *myself;
    volatile FSClusterDataServerInfo *master;
} FSClusterDataGroupInfo;

typedef struct fs_cluster_data_group_array {
    FSClusterDataGroupInfo *groups;
    int count;
    int base_id;
    volatile int delay_decision_count;
} FSClusterDataGroupArray;

typedef struct fs_rpc_result_entry {
    uint64_t data_version;
    int64_t task_version;
    time_t expires;
    struct fast_task_info *waiting_task;
    struct fs_rpc_result_entry *next;
} FSReplicaRPCResultEntry;

typedef struct fs_rpc_result_context {
    struct {
        FSReplicaRPCResultEntry *entries;
        FSReplicaRPCResultEntry *start; //for consumer
        FSReplicaRPCResultEntry *end;   //for producer
        int size;
    } ring;

    struct {
        FSReplicaRPCResultEntry *head;
        FSReplicaRPCResultEntry *tail;
        struct fast_mblock_man rentry_allocator;
    } queue;   //for overflow exceptions

    time_t last_check_timeout_time;
} FSReplicaRPCResultContext;

typedef struct fs_replication_context {
    struct {
        struct fc_queue rpc_queue;
        FSReplicaRPCResultContext rpc_result_ctx;   //push result recv from peer
    } caller;  //master side

    struct {
        struct fc_queue done_queue;
        struct fast_mblock_man result_allocator;
    } callee;  //slave side
} FSReplicationContext;

typedef struct fs_replication {
    struct fast_task_info *task;
    FSClusterServerInfo *peer;
    short stage;
    bool is_client;
    int thread_index; //for nio thread
    int conn_index;
    struct {
        int start_time;
        int next_connect_time;
        int last_errno;
        int fail_count;
        ConnectionInfo conn;
    } connection_info;

    FSReplicationContext context;
} FSReplication;

typedef struct {
    FSRequestInfo request;
    FSResponseInfo response;
    int (*deal_func)(struct fast_task_info *task);
    bool response_done;
    bool log_error;
    bool need_response;

    union {
        struct {
            volatile int waiting_rpc_count;
        } service;

        struct {
            int task_type;

            FSClusterServerInfo *peer;   //the peer server in the cluster
            FSReplication *replica;
        } cluster;
    };

    int which_side;   //master or slave
    FSSliceOpContext slice_op_ctx;
} FSServerTaskContext;

typedef struct server_task_arg {
    volatile int64_t task_version;
    int64_t req_start_time;

    FSServerTaskContext context;
} FSServerTaskArg;


struct ob_slice_ptr_array;
typedef struct fs_server_context {
    union {
        struct {
            struct fast_mblock_man record_allocator;
            struct ob_slice_ptr_array *slice_ptr_array;
        } service;

        struct {
            FSReplicationPtrArray connectings;
            FSReplicationPtrArray connected;
            FSClusterNotifyContextPtrArray notify_ctx_ptr_array;
        } cluster;
    };

} FSServerContext;

#endif
