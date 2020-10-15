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
    struct fast_task_info *task;
    FSSliceOpContext *ctx;
    struct fs_data_operation *next;  //for queue
} FSDataOperation;

typedef struct fs_data_thread_context {
    struct fc_queue queue;
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

    static inline void push_to_data_thread_queue(FSDataOperation *op)
    {
        FSDataThreadContext *context;
        context = g_data_thread_vars.thread_array.contexts +
            FS_BLOCK_HASH_CODE(op->ctx->info.bs_key.block) %
            g_data_thread_vars.thread_array.count;
        fc_queue_push(&context->queue, op);
    }

#ifdef __cplusplus
}
#endif

#endif
