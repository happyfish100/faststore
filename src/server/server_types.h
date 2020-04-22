#ifndef _FS_SERVER_TYPES_H
#define _FS_SERVER_TYPES_H

#include <time.h>
#include <pthread.h>
#include "fastcommon/common_define.h"
#include "fastcommon/fast_task_queue.h"
#include "fastcommon/fast_mblock.h"
#include "fastcommon/fast_allocator.h"
#include "fastcommon/server_id_func.h"
#include "common/fs_types.h"
#include "storage/storage_types.h"

#define FS_CLUSTER_TASK_TYPE_NONE               0
#define FS_CLUSTER_TASK_TYPE_RELATIONSHIP       1   //slave  -> master
#define FS_CLUSTER_TASK_TYPE_REPLICA_MASTER     2   //[Master] -> slave
#define FS_CLUSTER_TASK_TYPE_REPLICA_SLAVE      3   //master -> [Slave]

#define FS_DEFAULT_REPLICA_CHANNELS_BETWEEN_TWO_SERVERS  1
#define FS_DEFAULT_TRUNK_FILE_SIZE  (  1 * 1024 * 1024 * 1024LL)
#define FS_TRUNK_FILE_MIN_SIZE      (256 * 1024 * 1024LL)
#define FS_TRUNK_FILE_MAX_SIZE      ( 16 * 1024 * 1024 * 1024LL)

#define FS_DEFAULT_DISCARD_REMAIN_SPACE_SIZE  4096
#define FS_DISCARD_REMAIN_SPACE_MIN_SIZE       256
#define FS_DISCARD_REMAIN_SPACE_MAX_SIZE      (256 * 1024)

#define TASK_STATUS_CONTINUE   12345

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

struct fs_server_dentry;
typedef struct fs_server_dentry_array {
    int alloc;
    int count;
    struct fs_server_dentry **entries;
} FSServerDentryArray;  //for list entry

typedef struct fs_binlog_file_position {
    int index;      //current binlog file
    int64_t offset; //current file offset
} FSBinlogFilePosition;

typedef struct fs_cluster_server_info {
    FCServerInfo *server;
    char key[FS_REPLICA_KEY_SIZE];  //for slave server
    char status;                      //the slave status
    int64_t last_data_version;  //for replication
    int last_change_version;    //for push server status to the slave
} FSClusterServerInfo;

typedef struct fs_cluster_server_array {
    FSClusterServerInfo *servers;
    int count;
    volatile int change_version;
} FSClusterServerArray;

typedef struct fs_replication_context {
    struct {
        int64_t by_queue;
        struct {
            int64_t previous;
            int64_t current;
        } by_disk;
        int64_t by_resp;  //for flow control
    } last_data_versions;

    struct {
        int64_t start_time_ms;
        int64_t binlog_size;
        int64_t record_count;
    } sync_by_disk_stat;
} FSReplicationContext;

typedef struct fs_slave_replication {
    struct fast_task_info *task;
    FSClusterServerInfo *slave;
    int stage;
    int index;  //for next links
    struct {
        int start_time;
        int next_connect_time;
        int last_errno;
        int fail_count;
        ConnectionInfo conn;
    } connection_info;

    FSReplicationContext context;
} FSSlaveReplication;

typedef struct fs_slave_replication_array {
    FSSlaveReplication *replications;
    int count;
} FSSlaveReplicationArray;

typedef struct fs_slave_replication_ptr_array {
    int count;
    FSSlaveReplication **replications;
} FSSlaveReplicationPtrArray;

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

            /*
               FSSlaveReplication *replica; //master side
             */
        } cluster;
    };

    FSBlockKey bkey;
    struct {
        int offset;
        int length;
    } slice;
    FSSliceOpNotify slice_notify;
} FSServerTaskContext;

typedef struct server_task_arg {
    volatile int64_t task_version;
    int64_t req_start_time;

    FSServerTaskContext context;
} FSServerTaskArg;


typedef struct fs_server_context {
    union {
        struct {
            struct fast_mblock_man record_allocator;
        } service;

        struct {
            FSSlaveReplicationPtrArray connectings;  //master side
            FSSlaveReplicationPtrArray connected;    //master side
        } cluster;
    };

} FSServerContext;

#endif
