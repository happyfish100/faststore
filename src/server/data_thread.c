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

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <pthread.h>
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/pthread_func.h"
#include "sf/sf_global.h"
#include "server_global.h"
#include "server_storage.h"
#include "server_replication.h"
#include "data_thread.h"

#define DATA_THREAD_RUNNING_COUNT g_data_thread_vars.running_count

FSDataThreadVariables g_data_thread_vars;
static void *data_thread_func(void *arg);

static inline int init_thread_ctx(FSDataThreadContext *context)
{
    int result;

    if ((result=init_pthread_lock_cond_pair(&context->lc_pair)) != 0) {
        return result;
    }

    if ((result=fast_mblock_init_ex1(&context->allocator,
                    "data_operation", sizeof(FSDataOperation),
                    4 * 1024, 0, NULL, NULL, true)) != 0)
    {
        return result;
    }

    if ((result=fc_queue_init(&context->queue, (long)
                    (&((FSDataOperation *)NULL)->next))) != 0)
    {
        return result;
    }

    return 0;
}

static int init_data_thread_array()
{
    int result;
    int bytes;
    FSDataThreadContext *context;
    FSDataThreadContext *end;

    bytes = sizeof(FSDataThreadContext) * DATA_THREAD_COUNT;
    g_data_thread_vars.thread_array.contexts =
        (FSDataThreadContext *)fc_malloc(bytes);
    if (g_data_thread_vars.thread_array.contexts == NULL) {
        return ENOMEM;
    }
    memset(g_data_thread_vars.thread_array.contexts, 0, bytes);

    end = g_data_thread_vars.thread_array.contexts + DATA_THREAD_COUNT;
    for (context=g_data_thread_vars.thread_array.contexts;
            context<end; context++)
    {
        if ((result=init_thread_ctx(context)) != 0) {
            return result;
        }
    }
    g_data_thread_vars.thread_array.count = DATA_THREAD_COUNT;
    return 0;
}

int data_thread_init()
{
    int result;
    int count;

    if ((result=init_data_thread_array()) != 0) {
        return result;
    }

    count = g_data_thread_vars.thread_array.count;
    if ((result=create_work_threads_ex(&count, data_thread_func,
            g_data_thread_vars.thread_array.contexts,
            sizeof(FSDataThreadContext), NULL,
            SF_G_THREAD_STACK_SIZE)) == 0)
    {
        count = 0;
        while (__sync_add_and_fetch(&DATA_THREAD_RUNNING_COUNT, 0) <
                g_data_thread_vars.thread_array.count && count++ < 100)
        {
            fc_sleep_ms(1);
        }
    }
    return result;
}

void data_thread_destroy()
{
    if (g_data_thread_vars.thread_array.contexts != NULL) {
        FSDataThreadContext *context;
        FSDataThreadContext *end;

        end = g_data_thread_vars.thread_array.contexts +
            g_data_thread_vars.thread_array.count;
        for (context=g_data_thread_vars.thread_array.contexts;
                context<end; context++)
        {
            destroy_pthread_lock_cond_pair(&context->lc_pair);
            fc_queue_destroy(&context->queue);
            fast_mblock_destroy(&context->allocator);
        }
        free(g_data_thread_vars.thread_array.contexts);
        g_data_thread_vars.thread_array.contexts = NULL;
    }
}

void data_thread_terminate()
{
    FSDataThreadContext *context;
    FSDataThreadContext *end;
    int count;

    end = g_data_thread_vars.thread_array.contexts +
        g_data_thread_vars.thread_array.count;
    for (context=g_data_thread_vars.thread_array.contexts;
            context<end; context++)
    {
        fc_queue_terminate(&context->queue);
    }

    count = 0;
    while (__sync_add_and_fetch(&DATA_THREAD_RUNNING_COUNT, 0) != 0 &&
            count++ < 100)
    {
        fc_sleep_ms(1);
    }
}

#define DATA_THREAD_COND_WAIT(thread_ctx) \
    do { \
        do {  \
            pthread_cond_wait(&thread_ctx->lc_pair.cond,  \
                    &thread_ctx->lc_pair.lock);  \
        } while (!thread_ctx->notify_done && SF_G_CONTINUE_FLAG); \
        \
        if (!SF_G_CONTINUE_FLAG) {  \
            PTHREAD_MUTEX_UNLOCK(&thread_ctx->lc_pair.lock); \
            return;  \
        }  \
    } while (0)

static inline int log_data_update(const int operation,
        FSSliceOpContext *op_ctx)
{
    switch (operation) {
        case DATA_OPERATION_SLICE_WRITE:
            return fs_log_slice_write(op_ctx);
        case DATA_OPERATION_SLICE_ALLOCATE:
            return fs_log_slice_allocate(op_ctx);
        case DATA_OPERATION_SLICE_DELETE:
            return fs_log_delete_slices(op_ctx);
        case DATA_OPERATION_BLOCK_DELETE:
            return fs_log_delete_block(op_ctx);
        default:
            logError("file: "__FILE__", line: %d, "
                    "invalid operation: %d",
                    __LINE__, operation);
            return EINVAL;
    }
}

static void deal_one_operation(FSDataThreadContext *thread_ctx,
        FSDataOperation *op)
{
    bool is_update;
    int result;

    op->ctx->data_thread_ctx = thread_ctx;
    switch (op->operation) {
        case DATA_OPERATION_SLICE_READ:
            is_update = false;
            PTHREAD_MUTEX_LOCK(&thread_ctx->lc_pair.lock);
            thread_ctx->notify_done = false;
            if ((op->ctx->result=fs_slice_read_ex(op->ctx,
                            op->slice_ptr_array)) == 0)
            {
                DATA_THREAD_COND_WAIT(thread_ctx);
            }
            PTHREAD_MUTEX_UNLOCK(&thread_ctx->lc_pair.lock);
            break;
        case DATA_OPERATION_SLICE_WRITE:
            is_update = true;
            PTHREAD_MUTEX_LOCK(&thread_ctx->lc_pair.lock);
            thread_ctx->notify_done = false;
            if ((op->ctx->result=fs_slice_write(op->ctx)) == 0) {
                DATA_THREAD_COND_WAIT(thread_ctx);
            }
            PTHREAD_MUTEX_UNLOCK(&thread_ctx->lc_pair.lock);
            break;
        case DATA_OPERATION_SLICE_ALLOCATE:
            is_update = true;
            op->ctx->result = fs_slice_allocate_ex(op->ctx,
                    op->slice_ptr_array);
            break;
        case DATA_OPERATION_SLICE_DELETE:
            is_update = true;
            op->ctx->result = fs_delete_slices(op->ctx);
            break;
        case DATA_OPERATION_BLOCK_DELETE:
            is_update = true;
            op->ctx->result = fs_delete_block(op->ctx);
            break;
        default:
            is_update = false;
            op->ctx->result = EINVAL;
            logInfo("file: "__FILE__", line: %d, "
                    "unkown operation: %d", __LINE__, op->operation);
            break;
    }

    if (op->ctx->result == 0 && is_update) {
        if (op->source == DATA_SOURCE_MASTER_SERVICE) {
            PTHREAD_MUTEX_LOCK(&thread_ctx->lc_pair.lock);
            thread_ctx->notify_done = false;
            if ((result=replication_caller_push_to_slave_queues(
                            (struct fast_task_info *)op->arg)) ==
                    TASK_STATUS_CONTINUE)
            {
                DATA_THREAD_COND_WAIT(thread_ctx);
            }
            PTHREAD_MUTEX_UNLOCK(&thread_ctx->lc_pair.lock);
        }

        log_data_update(op->operation, op->ctx);
    }

    op->ctx->notify_func(op);

    /*
    logInfo("file: "__FILE__", line: %d, record: %p, "
            "operation: %d, hash code: %u, inode: %"PRId64
             ", data_version: %"PRId64", result: %d", __LINE__,
             record, record->operation, record->hash_code,
             record->inode, record->data_version, result);
             */
}

static void *data_thread_func(void *arg)
{
    FSDataOperation *op;
    FSDataOperation *current;
    FSDataThreadContext *thread_ctx;

    __sync_add_and_fetch(&DATA_THREAD_RUNNING_COUNT, 1);
    thread_ctx = (FSDataThreadContext *)arg;
    while (SF_G_CONTINUE_FLAG) {
        op = (FSDataOperation *)fc_queue_pop_all(&thread_ctx->queue);
        if (op == NULL) {
            continue;
        }

        do {
            current = op;
            op = op->next;
            deal_one_operation(thread_ctx, current);
            fast_mblock_free_object(&thread_ctx->allocator, current);
        } while (op != NULL);
    }

    __sync_sub_and_fetch(&DATA_THREAD_RUNNING_COUNT, 1);
    return NULL;
}
