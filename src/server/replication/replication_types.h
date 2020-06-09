//replication_types.h

#ifndef _REPLICATION_TYPES_H_
#define _REPLICATION_TYPES_H_

#include <time.h>
#include <limits.h>
#include <pthread.h>
#include "../server_types.h"

struct server_binlog_record_buffer;
typedef void (*release_binlog_rbuffer_func)(
        struct server_binlog_record_buffer *rbuffer);

typedef struct server_binlog_record_buffer {
    uint64_t data_version; //for idempotency (slave only)
    int64_t task_version;
    struct fast_task_info *task;
    volatile int reffer_count;
    release_binlog_rbuffer_func release_func;
    struct server_binlog_record_buffer *next;      //for producer
    struct server_binlog_record_buffer *nexts[0];  //for slave replications
} ServerBinlogRecordBuffer;

#endif
