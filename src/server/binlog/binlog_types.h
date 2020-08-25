//binlog_types.h

#ifndef _BINLOG_TYPES_H_
#define _BINLOG_TYPES_H_

#include <time.h>
#include <limits.h>
#include <pthread.h>
#include "fastcommon/fast_buffer.h"
#include "fastcommon/common_blocked_queue.h"
#include "../server_types.h"

#define BINLOG_COMMON_FIELD_INDEX_TIMESTAMP      0
#define BINLOG_COMMON_FIELD_INDEX_DATA_VERSION   1
#define BINLOG_COMMON_FIELD_INDEX_OP_TYPE        2

#define BINLOG_OP_TYPE_NO_OP   'N'

#define BINLOG_FILE_MAX_SIZE   (1024 * 1024 * 1024)  //for binlog rotating by size
#define BINLOG_FILE_PREFIX     "binlog"
#define BINLOG_FILE_EXT_FMT    ".%06d"

#define BINLOG_BUFFER_INIT_SIZE      4096
#define BINLOG_BUFFER_LENGTH(buffer) ((buffer).end - (buffer).buff)
#define BINLOG_BUFFER_REMAIN(buffer) ((buffer).end - (buffer).current)

#define BINLOG_IS_INTERNAL_RECORD(op_type, data_version)  \
    (op_type == BINLOG_OP_TYPE_NO_OP || data_version == 0)

#define BINLOG_REPAIR_KEEP_RECORD(op_type, data_version)  \
    (data_version == 0)

#define BINLOG_REPAIR_DISCARD_RECORD(op_type, data_version)  \
    (op_type == BINLOG_OP_TYPE_NO_OP)

struct fs_binlog_record;

typedef void (*data_thread_notify_func)(struct fs_binlog_record *record,
        const int result, const bool is_error);

typedef struct server_binlog_buffer {
    char *buff;    //the buffer pointer
    char *current; //for the consumer
    char *end;     //data end ptr
    int size;      //the buffer size (capacity)
} ServerBinlogBuffer;

typedef struct binlog_common_fields {
    time_t timestamp;
    int op_type;
    FSBlockKey bkey;
    int64_t data_version;
} BinlogCommonFields;

#ifdef __cplusplus
extern "C" {
#endif

#ifdef __cplusplus
}
#endif

#endif
