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

#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/pthread_func.h"
#include "sf/sf_global.h"
#include "sf/sf_func.h"
#include "server_global.h"
#include "server_replication.h"
#include "data_thread.h"

#define DATA_THREAD_ROLE_MASTER  'm'
#define DATA_THREAD_ROLE_SLAVE   's'

#define DATA_THREAD_RUNNING_COUNT g_data_thread_vars.running_count

FSDataThreadVariables g_data_thread_vars;
static void *data_thread_func(void *arg);

static inline int init_thread_ctx(FSDataThreadContext *context)
{
    int result;

    if ((result=init_pthread_lock_cond_pair(&context->lcp)) != 0) {
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

static int init_data_thread_array(FSDataThreadArray *thread_array,
        const int count, const int role)
{
    int result;
    int thread_count;
    int bytes;
    FSDataThreadContext *context;
    FSDataThreadContext *end;

    bytes = sizeof(FSDataThreadContext) * count;
    thread_array->contexts = (FSDataThreadContext *)fc_malloc(bytes);
    if (thread_array->contexts == NULL) {
        return ENOMEM;
    }
    memset(thread_array->contexts, 0, bytes);

    end = thread_array->contexts + count;
    for (context=thread_array->contexts;
            context<end; context++)
    {
        if (count == 1) {
            context->index = -1;
        } else {
            context->index = context - thread_array->contexts;
        }
        context->role = role;
        if ((result=init_thread_ctx(context)) != 0) {
            return result;
        }
    }
    thread_array->count = count;

    thread_count = thread_array->count;
    return create_work_threads_ex(&thread_count, data_thread_func,
            thread_array->contexts, sizeof(FSDataThreadContext), NULL,
            SF_G_THREAD_STACK_SIZE);
}

int data_thread_init()
{
    int result;
    int count;
    int thread_count;
    int n;

    count = (DATA_THREAD_COUNT + 1) / 2;
    if ((result=init_data_thread_array(&g_data_thread_vars.thread_arrays.
                    master, count, DATA_THREAD_ROLE_MASTER)) != 0)
    {
        return result;
    }
    if ((result=init_data_thread_array(&g_data_thread_vars.thread_arrays.
                    slave, count, DATA_THREAD_ROLE_SLAVE)) != 0)
    {
        return result;
    }

    thread_count = 2 * count;
    n = 0;
    while (__sync_add_and_fetch(&DATA_THREAD_RUNNING_COUNT, 0) <
            thread_count && n++ < 100)
    {
        fc_sleep_ms(10);
    }

    return 0;
}

static void destroy_data_thread_array(FSDataThreadArray *thread_array)
{
    if (thread_array->contexts != NULL) {
        FSDataThreadContext *context;
        FSDataThreadContext *end;

        end = thread_array->contexts + thread_array->count;
        for (context=thread_array->contexts; context<end; context++) {
            destroy_pthread_lock_cond_pair(&context->lcp);
            fc_queue_destroy(&context->queue);
            fast_mblock_destroy(&context->allocator);
        }
        free(thread_array->contexts);
        thread_array->contexts = NULL;
        thread_array->count = 0;
    }
}

void data_thread_destroy()
{
    destroy_data_thread_array(&g_data_thread_vars.thread_arrays.master);
    destroy_data_thread_array(&g_data_thread_vars.thread_arrays.slave);
}

static void terminate_data_thread_array(FSDataThreadArray *thread_array)
{
    FSDataThreadContext *context;
    FSDataThreadContext *end;
    int count;

    end = thread_array->contexts + thread_array->count;
    for (context=thread_array->contexts; context<end; context++) {
        fc_queue_terminate(&context->queue);
    }

    count = 0;
    while (__sync_add_and_fetch(&DATA_THREAD_RUNNING_COUNT, 0) != 0 &&
            count++ < 100)
    {
        fc_sleep_ms(10);
    }
}

void data_thread_terminate()
{
    terminate_data_thread_array(&g_data_thread_vars.thread_arrays.master);
    terminate_data_thread_array(&g_data_thread_vars.thread_arrays.slave);
}

#define DATA_THREAD_COND_WAIT(thread_ctx) \
    do { \
        PTHREAD_MUTEX_LOCK(&thread_ctx->lcp.lock);   \
        while (!thread_ctx->notify_done && SF_G_CONTINUE_FLAG) { \
            pthread_cond_wait(&thread_ctx->lcp.cond,  \
                    &thread_ctx->lcp.lock);  \
        } \
        thread_ctx->notify_done = false; /* reset for next */ \
        PTHREAD_MUTEX_UNLOCK(&thread_ctx->lcp.lock); \
        \
        if (!SF_G_CONTINUE_FLAG) {  \
            return;  \
        }  \
    } while (0)

static void data_thread_rw_done_callback(
        FSSliceOpContext *op_ctx, void *arg)
{
    data_thread_notify((FSDataThreadContext *)arg);
}

static void deal_operation_finish(FSDataThreadContext *thread_ctx,
        FSDataOperation *op)
{
    struct fast_task_info *task;

    if (op->ctx->result != 0) {
        if (op->ctx->info.is_update && op->source ==
                DATA_SOURCE_SLAVE_REPLICA)
        {
            logCrit("file: "__FILE__", line: %d, "
                    "rpc update data fail, errno: %d, "
                    "program terminate!", __LINE__,
                    op->ctx->result);
            sf_terminate_myself();
        }
    } else if (op->ctx->info.is_update) {
        op->binlog_write_done = false;
        if (op->source == DATA_SOURCE_MASTER_SERVICE) {
            if (!MASTER_ELECTION_FAILOVER) {
                data_thread_log_data_update(op);  //log first
            }

            if (replication_caller_push_to_slave_queues(op) ==
                    TASK_STATUS_CONTINUE)
            {
                DATA_THREAD_COND_WAIT(thread_ctx);
            }
            data_thread_log_data_update(op);

            if (REPLICA_QUORUM_NEED_MAJORITY) {
                int success_count;

                task = op->arg;
                success_count = FC_ATOMIC_GET(TASK_CTX.
                        service.rpc.success_count) + 1;
                if (!SF_REPLICATION_QUORUM_MAJORITY(op->ctx->info.myself->
                            dg->ds_ptr_array.count, success_count))
                {
                    bool finished;
                    if ((op->ctx->result=replication_quorum_add(&op->ctx->
                                    info.myself->dg->repl_quorum_ctx,
                                    task, op->ctx->info.data_version,
                                    &finished)) == 0)
                    {
                        if (!finished) {
                            __sync_bool_compare_and_swap(&thread_ctx->
                                    blocked, 0, 1);
                            op->ctx->result = sf_synchronize_finished_wait(
                                    &op->ctx->info.myself->dg->
                                    repl_quorum_ctx.sctx);
                            __sync_bool_compare_and_swap(&thread_ctx->
                                    blocked, 1, 0);

                            if (!SF_G_CONTINUE_FLAG) {
                                op->ctx->result = EAGAIN;
                                return;
                            }
                        }
                    }
                }
            } else {
                FC_ATOMIC_SET(op->ctx->info.myself->data.confirmed_version,
                        op->ctx->info.data_version);
            }
        } else {
            data_thread_log_data_update(op);
            FC_ATOMIC_SET(op->ctx->info.myself->data.confirmed_version,
                    op->ctx->info.data_version);
        }

        /*
           logInfo("file: "__FILE__", line: %d, op ptr: %p, "
           "operation: %d, log_replica: %d, source: %c, "
           "data_group_id: %d, data_version: %"PRId64", "
           "block {oid: %"PRId64", offset: %"PRId64"}, "
           "slice {offset: %d, length: %d}, "
           "body_len: %d", __LINE__, op, op->operation,
           op->ctx->info.write_binlog.log_replica,
           op->ctx->info.source, op->ctx->info.data_group_id,
           op->ctx->info.data_version, op->ctx->info.bs_key.block.oid,
           op->ctx->info.bs_key.block.offset,
           op->ctx->info.bs_key.slice.offset,
           op->ctx->info.bs_key.slice.length,
           op->ctx->info.body_len);
         */
    }
}

static void deal_one_operation(FSDataThreadContext *thread_ctx,
        FSDataOperation *op)
{
    int result;

    op->ctx->arg = thread_ctx;
    switch (op->operation) {
        case DATA_OPERATION_SLICE_READ:
            op->ctx->rw_done_callback = data_thread_rw_done_callback;
            if ((op->ctx->result=fs_slice_read(op->ctx)) == 0) {
                DATA_THREAD_COND_WAIT(thread_ctx);
            }
            break;
        case DATA_OPERATION_SLICE_WRITE:
#ifdef OS_LINUX
            op->ctx->info.buffer_type = fs_buffer_type_direct;
#endif
            op->ctx->rw_done_callback = data_thread_rw_done_callback;
            if ((result=fs_slice_write(op->ctx)) == 0) {
                DATA_THREAD_COND_WAIT(thread_ctx);
            } else {
                op->ctx->result = result;
            }
            if (result == 0) {
                fs_write_finish(op->ctx);  //for add slice index and cleanup
            }
            break;
        case DATA_OPERATION_SLICE_ALLOCATE:
            op->ctx->result = fs_slice_allocate(op->ctx);
            break;
        case DATA_OPERATION_SLICE_DELETE:
            op->ctx->result = fs_delete_slices(op->ctx);
            break;
        case DATA_OPERATION_BLOCK_DELETE:
            op->ctx->result = fs_delete_block(op->ctx);
            break;
        default:
            op->ctx->result = EINVAL;
            logInfo("file: "__FILE__", line: %d, "
                    "unkown operation: %d", __LINE__, op->operation);
            break;
    }

    deal_operation_finish(thread_ctx, op);
    op->ctx->notify_func(op);
}

static void *data_thread_func(void *arg)
{
    FSDataOperation *op;
    FSDataOperation *current;
    FSDataThreadContext *thread_ctx;

    __sync_add_and_fetch(&DATA_THREAD_RUNNING_COUNT, 1);
    thread_ctx = (FSDataThreadContext *)arg;

#ifdef OS_LINUX
    {
        char thread_name[16];
        int len;

        len = snprintf(thread_name, sizeof(thread_name), "data-%s",
                (thread_ctx->role == DATA_THREAD_ROLE_MASTER ?
                 "master" : "slave"));
        if (thread_ctx->index >= 0) {
            snprintf(thread_name + len, sizeof(thread_name) - len,
                    "[%d]", thread_ctx->index);
        }
        prctl(PR_SET_NAME, thread_name);
    }
#endif

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
