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

//binlog_types.h

#ifndef _BINLOG_TYPES_H_
#define _BINLOG_TYPES_H_

#include <time.h>
#include <limits.h>
#include <pthread.h>
#include "fastcommon/fast_buffer.h"
#include "fastcommon/common_blocked_queue.h"
#include "../server_types.h"

#define BINLOG_COMMON_FIELD_INDEX_TIMESTAMP     0

#define REPLICA_BINLOG_FIELD_INDEX_TIMESTAMP    \
    BINLOG_COMMON_FIELD_INDEX_TIMESTAMP
#define REPLICA_BINLOG_FIELD_INDEX_DATA_VERSION 1
#define REPLICA_BINLOG_FIELD_INDEX_SOURCE       2
#define REPLICA_BINLOG_FIELD_INDEX_OP_TYPE      3
#define REPLICA_BINLOG_FIELD_INDEX_BLOCK_OID    4
#define REPLICA_BINLOG_FIELD_INDEX_BLOCK_OFFSET 5
#define REPLICA_BINLOG_FIELD_INDEX_SLICE_OFFSET 6
#define REPLICA_BINLOG_FIELD_INDEX_SLICE_LENGTH 7

#define SLICE_BINLOG_FIELD_INDEX_TIMESTAMP     \
    BINLOG_COMMON_FIELD_INDEX_TIMESTAMP
#define SLICE_BINLOG_FIELD_INDEX_SN            1
#define SLICE_BINLOG_FIELD_INDEX_DATA_VERSION  2
#define SLICE_BINLOG_FIELD_INDEX_SOURCE        3
#define SLICE_BINLOG_FIELD_INDEX_OP_TYPE       4
#define SLICE_BINLOG_FIELD_INDEX_BLOCK_OID     5
#define SLICE_BINLOG_FIELD_INDEX_BLOCK_OFFSET  6
#define SLICE_BINLOG_FIELD_INDEX_SLICE_OFFSET  7
#define SLICE_BINLOG_FIELD_INDEX_SLICE_LENGTH  8

#define BINLOG_MIN_FIELD_COUNT   6
#define BINLOG_MAX_FIELD_COUNT  16

#define REPLICA_MIN_FIELD_COUNT  BINLOG_MIN_FIELD_COUNT
#define SLICE_MIN_FIELD_COUNT    7

#define BINLOG_OP_TYPE_WRITE_SLICE  'w'
#define BINLOG_OP_TYPE_ALLOC_SLICE  'a'
#define BINLOG_OP_TYPE_DEL_SLICE    'd'
#define BINLOG_OP_TYPE_DEL_BLOCK    'D'
#define BINLOG_OP_TYPE_NO_OP        'N'

#define BINLOG_SOURCE_RECLAIM       'M'  //by trunk reclaim
#define BINLOG_SOURCE_REBUILD       'B'  //by data rebuild
#define BINLOG_SOURCE_DUMP          'F'  //by binlog dump
#define BINLOG_SOURCE_RPC_MASTER    'C'  //by user call (master side)
#define BINLOG_SOURCE_RPC_SLAVE     'c'  //by user call (slave side)
#define BINLOG_SOURCE_REPLAY        'r'  //by binlog replay  (slave side)
#define BINLOG_SOURCE_ROLLBACK      'R'  //revert by binlog rollback
#define BINLOG_SOURCE_RESTORE       'S'  //restore by binlog rollback

#define FS_IS_BINLOG_SOURCE_RPC(source) \
    (source == BINLOG_SOURCE_RPC_MASTER || source == BINLOG_SOURCE_RPC_SLAVE)

#define FS_BINLOG_CHECKED_BY_SOURCE(source) \
    FS_IS_BINLOG_SOURCE_RPC(source)

#define BINLOG_REPAIR_KEEP_RECORD(source) !FS_BINLOG_CHECKED_BY_SOURCE(source)

struct fs_binlog_record;

typedef void (*data_thread_notify_func)(struct fs_binlog_record *record,
        const int result, const bool is_error);

typedef struct binlog_common_field_indexes {
    uint8_t timestamp;
    uint8_t data_version;
    uint8_t source;
    uint8_t op_type;
    uint8_t block_oid;
    uint8_t block_offset;
    uint8_t slice_offset;
    uint8_t slice_length;
} BinlogCommonFieldIndexs;

typedef struct binlog_common_fields {
    time_t timestamp;
    short source;
    short op_type;
    FSBlockKey bkey;
    int64_t data_version;
} BinlogCommonFields;

typedef struct {
    int alloc;
    int count;
    BinlogCommonFields *records;
} BinlogCommonFieldsArray;

#ifdef __cplusplus
extern "C" {
#endif

#ifdef __cplusplus
}
#endif

#endif
