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

#define BINLOG_COMMON_FIELD_INDEX_TIMESTAMP      0
#define BINLOG_COMMON_FIELD_INDEX_DATA_VERSION   1
#define BINLOG_COMMON_FIELD_INDEX_SOURCE         2
#define BINLOG_COMMON_FIELD_INDEX_OP_TYPE        3
#define BINLOG_COMMON_FIELD_INDEX_BLOCK_OID      4
#define BINLOG_COMMON_FIELD_INDEX_BLOCK_OFFSET   5
#define BINLOG_COMMON_FIELD_INDEX_SLICE_OFFSET   6
#define BINLOG_COMMON_FIELD_INDEX_SLICE_LENGTH   7

#define BINLOG_MAX_FIELD_COUNT  16
#define BINLOG_MIN_FIELD_COUNT   6

#define BINLOG_OP_TYPE_WRITE_SLICE  'w'
#define BINLOG_OP_TYPE_ALLOC_SLICE  'a'
#define BINLOG_OP_TYPE_DEL_SLICE    'd'
#define BINLOG_OP_TYPE_DEL_BLOCK    'D'
#define BINLOG_OP_TYPE_NO_OP        'N'

#define BINLOG_SOURCE_RPC           'C'  //by user call
#define BINLOG_SOURCE_REPLAY        'R'  //by binlog replay

#define BINLOG_IS_INTERNAL_RECORD(op_type, data_version)  \
    (op_type == BINLOG_OP_TYPE_NO_OP || data_version == 0)

#define BINLOG_REPAIR_KEEP_RECORD(op_type, data_version)  \
    BINLOG_IS_INTERNAL_RECORD(op_type, data_version)

struct fs_binlog_record;

typedef void (*data_thread_notify_func)(struct fs_binlog_record *record,
        const int result, const bool is_error);

typedef struct binlog_common_fields {
    time_t timestamp;
    short source;
    short op_type;
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
