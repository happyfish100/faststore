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

//data_thread.h

#ifndef _DATA_THREAD_H_
#define _DATA_THREAD_H_

#include "fastcommon/fc_queue.h"
#include "fastcommon/fc_atomic.h"
#include "storage/slice_op.h"

#define DATA_OPERATION_NONE           '\0'
#define DATA_OPERATION_SLICE_READ     'r'
#define DATA_OPERATION_SLICE_WRITE    'w'
#define DATA_OPERATION_SLICE_ALLOCATE 'a'
#define DATA_OPERATION_SLICE_DELETE   'd'
#define DATA_OPERATION_BLOCK_DELETE   'D'

#define DATA_SOURCE_MASTER_SERVICE     1
#define DATA_SOURCE_SLAVE_REPLICA      2
#define DATA_SOURCE_SLAVE_RECOVERY     3

typedef struct fs_data_operation {
    short operation;
    char source;
    bool binlog_write_done;
    FSSliceOpContext *ctx;
    void *arg;
    struct fs_data_operation *next;  //for queue
} FSDataOperation;

typedef struct fs_data_thread_context {
    short index;
    short role;
    bool notify_done;
    volatile char blocked;  //for data replication quorum
    pthread_lock_cond_pair_t lcp;
    struct fc_queue queue;
    struct fast_mblock_man allocator;  //element: FSDataOperation
} FSDataThreadContext;

typedef struct fs_data_thread_array {
    FSDataThreadContext *contexts;
    int count;
} FSDataThreadArray;

typedef struct fdir_data_thread_variables {
    struct {
        FSDataThreadArray master;  //for master data groups
        FSDataThreadArray slave;   //for slave data groups
    } thread_arrays;
    volatile int running_count;
} FSDataThreadVariables;

#ifdef __cplusplus
extern "C" {
#endif

    extern FSDataThreadVariables g_data_thread_vars;

    int data_thread_init();
    void data_thread_destroy();
    void data_thread_terminate();

    static inline int push_to_data_thread_queue(const int operation,
            const int source, void *arg, FSSliceOpContext *op_ctx)
    {
        FSDataThreadContext *thread_ctx;
        FSDataOperation *op;
        uint32_t hash_code;

        hash_code = op_ctx->info.data_group_id;
        if (FC_ATOMIC_GET(op_ctx->info.myself->is_master)) {
            thread_ctx = g_data_thread_vars.thread_arrays.master.contexts +
                 hash_code % g_data_thread_vars.thread_arrays.master.count;
        } else {
            thread_ctx = g_data_thread_vars.thread_arrays.slave.contexts +
                hash_code % g_data_thread_vars.thread_arrays.slave.count;
        }

        op = (FSDataOperation *)fast_mblock_alloc_object(
                &thread_ctx->allocator);
        if (op == NULL) {
            return ENOMEM;
        }

        op->operation = operation;
        op->source = source;
        op->arg = arg;
        op->ctx = op_ctx;
        fc_queue_push(&thread_ctx->queue, op);
        return 0;
    }

    static inline int data_thread_is_blocked(const uint32_t hash_code)
    {
        FSDataThreadContext *context;

        context = g_data_thread_vars.thread_arrays.master.contexts +
            hash_code % g_data_thread_vars.thread_arrays.master.count;
        return FC_ATOMIC_GET(context->blocked);
    }

    static inline void data_thread_notify(FSDataThreadContext *thread_ctx)
    {
        PTHREAD_MUTEX_LOCK(&thread_ctx->lcp.lock);
        thread_ctx->notify_done = true;
        pthread_cond_signal(&thread_ctx->lcp.cond);
        PTHREAD_MUTEX_UNLOCK(&thread_ctx->lcp.lock);
    }

    static inline const char *fs_get_data_operation_caption(const int operation)
    {
        switch (operation) {
            case DATA_OPERATION_SLICE_READ:
                return "slice read";
            case DATA_OPERATION_SLICE_WRITE:
                return "slice write";
            case DATA_OPERATION_SLICE_ALLOCATE:
                return "slice allocate";
            case DATA_OPERATION_SLICE_DELETE:
                return "slice delete";
            case DATA_OPERATION_BLOCK_DELETE:
                return "block delete";
            default:
                return "unkown";
        }
    }

    static inline int data_thread_log_data_update(FSDataOperation *op)
    {
        if (op->binlog_write_done) {
            return 0;
        }
        op->binlog_write_done = true;

        switch (op->operation) {
            case DATA_OPERATION_SLICE_WRITE:
                return fs_log_slice_write(op->ctx);
            case DATA_OPERATION_SLICE_ALLOCATE:
                return fs_log_slice_allocate(op->ctx);
            case DATA_OPERATION_SLICE_DELETE:
                return fs_log_delete_slice(op->ctx);
            case DATA_OPERATION_BLOCK_DELETE:
                return fs_log_delete_block(op->ctx);
            default:
                logError("file: "__FILE__", line: %d, "
                        "invalid operation: %d",
                        __LINE__, op->operation);
                return EINVAL;
        }
    }

#ifdef __cplusplus
}
#endif

#endif
