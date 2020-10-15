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
#include "server_types.h"

typedef struct fs_data_operation {
    int operation;
    struct fast_task_info *task;
    FSSliceOpContext *ctx;
    struct fs_data_operation *next;  //for queue
} FSDataOperation;

typedef struct fs_data_thread_context {
    bool notify_done;
    struct fc_queue queue;
    struct fast_mblock_man allocator;
    pthread_lock_cond_pair_t lc_pair;
} FSDataThreadContext;

typedef struct fs_data_thread_array {
    FSDataThreadContext *contexts;
    int count;
} FSDataThreadArray;

typedef struct fdir_data_thread_variables {
    FSDataThreadArray thread_array;
    volatile int running_count;
} FSDataThreadVariables;

#ifdef __cplusplus
extern "C" {
#endif

    extern FSDataThreadVariables g_data_thread_vars;

    int data_thread_init();
    void data_thread_destroy();
    void data_thread_terminate();

    static inline int push_to_data_thread_queue(struct fast_task_info *task,
            const int operation, FSSliceOpContext *op_ctx)
    {
        FSDataThreadContext *context;
        FSDataOperation *op;

        context = g_data_thread_vars.thread_array.contexts +
            FS_BLOCK_HASH_CODE(op_ctx->info.bs_key.block) %
            g_data_thread_vars.thread_array.count;
        op = (FSDataOperation *)fast_mblock_alloc_object(&context->allocator);
        if (op == NULL) {
            return ENOMEM;
        }

        op->operation = operation;
        op->task = task;
        op->ctx = op_ctx;
        fc_queue_push(&context->queue, op);
        return 0;
    }

    static inline void data_thread_notify(FSDataThreadContext *thread_ctx)
    {
        PTHREAD_MUTEX_LOCK(&thread_ctx->lc_pair.lock);
        thread_ctx->notify_done = true;
        pthread_cond_signal(&thread_ctx->lc_pair.cond);
        PTHREAD_MUTEX_UNLOCK(&thread_ctx->lc_pair.lock);
    }

#ifdef __cplusplus
}
#endif

#endif
