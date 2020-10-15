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
#include "data_thread.h"

#define DATA_THREAD_RUNNING_COUNT g_data_thread_vars.running_count

FSDataThreadVariables g_data_thread_vars;
static void *data_thread_func(void *arg);

static inline int init_thread_ctx(FSDataThreadContext *context)
{
    int result;

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
            fc_queue_destroy(&context->queue);
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

static int deal_binlog_one_record(FSDataThreadContext *thread_ctx,
        FSDataOperation *op)
{
    int result;

    result = 0;
    /*
    logInfo("file: "__FILE__", line: %d, record: %p, "
            "operation: %d, hash code: %u, inode: %"PRId64
             ", data_version: %"PRId64", result: %d", __LINE__,
             record, record->operation, record->hash_code,
             record->inode, record->data_version, result);
             */

    return result;
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
            deal_binlog_one_record(thread_ctx, current);
        } while (op != NULL);
    }

    __sync_sub_and_fetch(&DATA_THREAD_RUNNING_COUNT, 1);
    return NULL;
}
