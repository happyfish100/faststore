//replication_types.h

#ifndef _REPLICATION_TYPES_H_
#define _REPLICATION_TYPES_H_

#include <time.h>
#include <limits.h>
#include <pthread.h>
#include "../server_types.h"

typedef struct replication_rpc_entry {
    uint64_t task_version;
    struct fast_task_info *task;
    volatile short reffer_count;
    short body_offset;
    int body_length;
    struct replication_rpc_entry *nexts[0];  //for slave replications
} ReplicationRPCEntry;

typedef struct replication_rpc_result {
    FSReplication *replication;
    short err_no;
    uint64_t data_version;
    struct replication_rpc_result *next;
} ReplicationRPCResult;

#endif
