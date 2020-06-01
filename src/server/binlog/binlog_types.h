//binlog_types.h

#ifndef _BINLOG_TYPES_H_
#define _BINLOG_TYPES_H_

#include <time.h>
#include <limits.h>
#include <pthread.h>
#include "fastcommon/fast_buffer.h"
#include "fastcommon/common_blocked_queue.h"
#include "../server_types.h"

#define BINLOG_FILE_PREFIX     "binlog"
#define BINLOG_FILE_EXT_FMT    ".%06d"

#define BINLOG_BUFFER_INIT_SIZE      4096
#define BINLOG_BUFFER_LENGTH(buffer) ((buffer).end - (buffer).buff)
#define BINLOG_BUFFER_REMAIN(buffer) ((buffer).end - (buffer).current)

struct server_binlog_record_buffer;
struct fs_binlog_record;

typedef void (*data_thread_notify_func)(struct fs_binlog_record *record,
        const int result, const bool is_error);

typedef void (*release_binlog_rbuffer_func)(
        struct server_binlog_record_buffer *rbuffer);

typedef struct server_binlog_buffer {
    char *buff;    //the buffer pointer
    char *current; //for the consumer
    char *end;     //data end ptr
    int size;      //the buffer size (capacity)
} ServerBinlogBuffer;

typedef struct server_binlog_record_buffer {
    uint64_t data_version; //for idempotency (slave only)
    int64_t task_version;
    volatile int reffer_count;
    void *args;  //for notify & release 
    release_binlog_rbuffer_func release_func;
    FastBuffer buffer;
    struct server_binlog_record_buffer *next;      //for producer
    struct server_binlog_record_buffer *nexts[0];  //for slave replications
} ServerBinlogRecordBuffer;

#ifdef __cplusplus
extern "C" {
#endif

#ifdef __cplusplus
}
#endif

#endif
