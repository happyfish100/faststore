//replication_types.h

#ifndef _REPLICATION_TYPES_H_
#define _REPLICATION_TYPES_H_

#include <time.h>
#include <limits.h>
#include <pthread.h>
#include "../server_types.h"

typedef struct server_binlog_record_buffer {
    int64_t task_version;
    struct fast_task_info *task;
    volatile int reffer_count;
    struct server_binlog_record_buffer *nexts[0];  //for slave replications
} ServerBinlogRecordBuffer;

#endif
