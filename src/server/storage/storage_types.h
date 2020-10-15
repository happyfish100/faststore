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


#ifndef _STORAGE_TYPES_H
#define _STORAGE_TYPES_H

#include "fastcommon/shared_buffer.h"
#include "../../common/fs_types.h"

#define FS_MAX_SPLIT_COUNT_PER_SPACE_ALLOC   2
#define FS_SLICE_SN_PARRAY_INIT_ALLOC_COUNT  4

#define DATA_OPERATION_SLICE_READ     'r'
#define DATA_OPERATION_SLICE_WRITE    'w'
#define DATA_OPERATION_SLICE_ALLOCATE 'a'
#define DATA_OPERATION_SLICE_DELETE   'd'
#define DATA_OPERATION_BLOCK_DELETE   'D'

struct fs_slice_op_context;

typedef void (*fs_slice_op_notify_func)(struct fs_slice_op_context *ctx);

struct ob_slice_entry;

typedef struct {
    struct ob_slice_entry *slice;
    uint64_t sn;     //for slice binlog
} FSSliceSNPair;

typedef struct {
    int count;
    int alloc;
    FSSliceSNPair *slice_sn_pairs;
} FSSliceSNPairArray;

typedef struct {
    fs_slice_op_notify_func func;
    void *arg;
} FSNotifyFuncArgPair;

struct fs_cluster_data_server_info;
struct fs_data_thread_context;
typedef struct fs_slice_op_context {
    FSNotifyFuncArgPair notify;
    struct fs_data_thread_context *data_thread_ctx;  //for signal data thread
    volatile short counter;
    short result;
    int done_bytes;

    struct {
        struct {
            bool log_replica;  //false for trunk reclaim
            bool immediately;  //false for master update
        } write_binlog;
        short source;           //for binlog write
        int data_group_id;
        uint64_t data_version;  //for replica binlog
        uint64_t sn;            //for slice binlog
        FSBlockSliceKeyInfo bs_key;
        struct fs_cluster_data_server_info *myself;
        int body_len;
        char *body;
        char *buff;  //read or write buffer
    } info;

    struct {
        int space_changed;  //increase /decrease space in bytes for slice operate
        FSSliceSNPairArray sarray;
    } update;  //for slice update

} FSSliceOpContext;

typedef struct fs_slice_op_buffer_context {
    FSSliceOpContext op_ctx;
    SharedBuffer *buffer;
} FSSliceOpBufferContext;

#endif
