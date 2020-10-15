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
#include "fastcommon/shared_func.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_nio.h"
#include "sf/sf_global.h"
#include "../../common/fs_cluster_cfg.h"
#include "../server_global.h"
#include "../data_thread.h"
#include "rpc_result_ring.h"

static int init_rpc_result_instance(FSReplicaRPCResultInstance *instance,
        const int alloc_size)
{
    int bytes;

    bytes = sizeof(FSReplicaRPCResultEntry) * alloc_size;
    instance->ring.entries = (FSReplicaRPCResultEntry *)fc_malloc(bytes);
    if (instance->ring.entries == NULL) {
        return ENOMEM;
    }
    memset(instance->ring.entries, 0, bytes);

    instance->ring.start = instance->ring.end = instance->ring.entries;
    instance->ring.size = alloc_size;
    instance->queue.head = instance->queue.tail = NULL;
    return 0;
}

int rpc_result_ring_check_init(FSReplicaRPCResultContext *ctx,
        const int alloc_size)
{
    int bytes;
    int result;
    FSIdArray *id_array;
    FSReplicaRPCResultInstance *instance;
    FSReplicaRPCResultInstance *end;

    if (ctx->instances != NULL) {
        return 0;
    }

    id_array = fs_cluster_cfg_get_my_data_group_ids(&CLUSTER_CONFIG_CTX,
            CLUSTER_MYSELF_PTR->server->id);
    ctx->dg_base_id = fs_cluster_cfg_get_min_data_group_id(id_array);

    bytes = sizeof(FSReplicaRPCResultInstance) * id_array->count;
    ctx->instances = (FSReplicaRPCResultInstance *)fc_malloc(bytes);
    if (ctx->instances == NULL) {
        return ENOMEM;
    }
    memset(ctx->instances, 0, bytes);

    ctx->dg_count = id_array->count;
    end = ctx->instances + ctx->dg_count;
    for (instance=ctx->instances; instance<end; instance++) {
        instance->data_group_id = ctx->dg_base_id +
            (instance - ctx->instances);
        if ((result=init_rpc_result_instance(instance, alloc_size)) != 0) {
            return result;
        }
    }

    return fast_mblock_init_ex1(&ctx->rentry_allocator,
        "push_result", sizeof(FSReplicaRPCResultEntry), 4096,
        0, NULL, NULL, false);
}

static inline void desc_task_waiting_rpc_count(
        FSReplicaRPCResultEntry *entry)
{
    FSServerTaskArg *task_arg;

    if (entry->waiting_task == NULL) {
        return;
    }

    task_arg = (FSServerTaskArg *)entry->waiting_task->arg;
    if (entry->task_version != task_arg->task_version) {
        logWarning("file: "__FILE__", line: %d, "
                "task %p already cleanup",
                __LINE__, entry->waiting_task);
        return;
    }

    if (__sync_sub_and_fetch(&((FSServerTaskArg *)
                    entry->waiting_task->arg)->context.
                service.waiting_rpc_count, 1) == 0)
    {
        data_thread_notify(task_arg->context.slice_op_ctx.data_thread_ctx);
        //sf_nio_notify(entry->waiting_task, SF_NIO_STAGE_CONTINUE);
    }
}

static void rpc_result_instance_clear_queue_all(FSReplicaRPCResultContext *ctx,
        FSReplicaRPCResultInstance *instance)
{
    FSReplicaRPCResultEntry *current;
    FSReplicaRPCResultEntry *deleted;

    if (instance->queue.head == NULL) {
        return;
    }

    current = instance->queue.head;
    while (current != NULL) {
        deleted = current;
        current = current->next;

        desc_task_waiting_rpc_count(deleted);
        fast_mblock_free_object(&ctx->rentry_allocator, deleted);
    }

    instance->queue.head = instance->queue.tail = NULL;
}

static void rpc_result_instance_clear_all(FSReplicaRPCResultContext *ctx,
        FSReplicaRPCResultInstance *instance)
{
    int index;

    if (instance->ring.start == instance->ring.end) {
        rpc_result_instance_clear_queue_all(ctx, instance);
        return;
    }

    index = instance->ring.start - instance->ring.entries;
    while (instance->ring.start != instance->ring.end) {
        desc_task_waiting_rpc_count(instance->ring.start);
        instance->ring.start->data_version = 0;
        instance->ring.start->waiting_task = NULL;

        instance->ring.start = instance->ring.entries +
            (++index % instance->ring.size);
    }

    rpc_result_instance_clear_queue_all(ctx, instance);
}

void rpc_result_ring_clear_all(FSReplicaRPCResultContext *ctx)
{
    FSReplicaRPCResultInstance *instance;
    FSReplicaRPCResultInstance *end;

    end = ctx->instances + ctx->dg_count;
    for (instance=ctx->instances; instance<end; instance++) {
        rpc_result_instance_clear_all(ctx, instance);
    }
}

static int rpc_result_instance_clear_queue_timeouts(
        FSReplicaRPCResultContext *ctx,
        FSReplicaRPCResultInstance *instance)
{
    FSReplicaRPCResultEntry *current;
    FSReplicaRPCResultEntry *deleted;
    int count;

    if (instance->queue.head == NULL) {
        return 0;
    }

    if (instance->queue.head->expires >= g_current_time) {
        return 0;
    }

    count = 0;
    current = instance->queue.head;
    while (current != NULL && current->expires < g_current_time) {
        deleted = current;
        current = current->next;

        logWarning("file: "__FILE__", line: %d, "
                "waiting push response timeout, "
                "data_version: %"PRId64", task: %p",
                __LINE__, deleted->data_version,
                deleted->waiting_task);
        desc_task_waiting_rpc_count(deleted);
        fast_mblock_free_object(&ctx->rentry_allocator, deleted);
        ++count;
    }

    instance->queue.head = current;
    if (current == NULL) {
        instance->queue.tail = NULL;
    }

    return count;
}

static void rpc_result_instance_clear_timeouts(
        FSReplicaRPCResultContext *ctx,
        FSReplicaRPCResultInstance *instance)
{
    int index;
    int clear_count;

    clear_count = 0;
    if (instance->ring.start != instance->ring.end) {
        index = instance->ring.start - instance->ring.entries;
        while (instance->ring.start != instance->ring.end &&
                instance->ring.start->expires < g_current_time)
        {
            logWarning("file: "__FILE__", line: %d, "
                    "waiting push response timeout, "
                    "data_version: %"PRId64", task: %p",
                    __LINE__, instance->ring.start->data_version,
                    instance->ring.start->waiting_task);

            desc_task_waiting_rpc_count(instance->ring.start);
            instance->ring.start->data_version = 0;
            instance->ring.start->waiting_task = NULL;

            instance->ring.start = instance->ring.entries +
                (++index % instance->ring.size);
            ++clear_count;
        }
    }

    clear_count += rpc_result_instance_clear_queue_timeouts(ctx, instance);
    if (clear_count > 0) {
        logWarning("file: "__FILE__", line: %d, "
                "data group id: %d, clear timeout push response "
                "waiting entries count: %d", __LINE__,
                instance->data_group_id, clear_count);
    }
}

void rpc_result_ring_clear_timeouts(FSReplicaRPCResultContext *ctx)
{
    FSReplicaRPCResultInstance *instance;
    FSReplicaRPCResultInstance *end;

    if (ctx->last_check_timeout_time == g_current_time) {
        return;
    }
    ctx->last_check_timeout_time = g_current_time;

    end = ctx->instances + ctx->dg_count;
    for (instance=ctx->instances; instance<end; instance++) {
        rpc_result_instance_clear_timeouts(ctx, instance);
    }
}

void rpc_result_ring_destroy(FSReplicaRPCResultContext *ctx)
{
    FSReplicaRPCResultInstance *instance;
    FSReplicaRPCResultInstance *end;

    end = ctx->instances + ctx->dg_count;
    for (instance=ctx->instances; instance<end; instance++) {
        if (instance->ring.entries != NULL) {
            free(instance->ring.entries);
            instance->ring.start = instance->ring.end =
                instance->ring.entries = NULL;
            instance->ring.size = 0;
        }
    }

    free(ctx->instances);
    ctx->instances = NULL;
    fast_mblock_destroy(&ctx->rentry_allocator);
}

static int add_to_queue(FSReplicaRPCResultContext *ctx,
        FSReplicaRPCResultInstance *instance, const uint64_t data_version,
        struct fast_task_info *waiting_task, const int64_t task_version)
{
    FSReplicaRPCResultEntry *entry;
    FSReplicaRPCResultEntry *previous;
    FSReplicaRPCResultEntry *current;

    entry = (FSReplicaRPCResultEntry *)fast_mblock_alloc_object(
            &ctx->rentry_allocator);
    if (entry == NULL) {
        return ENOMEM;
    }

    entry->data_version = data_version;
    entry->waiting_task = waiting_task;
    entry->task_version = task_version;
    entry->expires = g_current_time + SF_G_NETWORK_TIMEOUT;

    if (instance->queue.tail == NULL) {  //empty queue
        entry->next = NULL;
        instance->queue.head = instance->queue.tail = entry;
        return 0;
    }

    if (data_version > instance->queue.tail->data_version) {
        entry->next = NULL;
        instance->queue.tail->next = entry;
        instance->queue.tail = entry;
        return 0;
    }

    if (data_version < instance->queue.head->data_version) {
        entry->next = instance->queue.head;
        instance->queue.head = entry;
        return 0;
    }

    previous = instance->queue.head;
    current = instance->queue.head->next;
    while (current != NULL && data_version > current->data_version) {
        previous = current;
        current = current->next;
    }

    entry->next = previous->next;
    previous->next = entry;
    return 0;
}

int rpc_result_ring_add(FSReplicaRPCResultContext *ctx,
        const int data_group_id, const uint64_t data_version,
        struct fast_task_info *waiting_task, const int64_t task_version)
{
    FSReplicaRPCResultInstance *instance;
    FSReplicaRPCResultEntry *entry;
    FSReplicaRPCResultEntry *previous;
    FSReplicaRPCResultEntry *next;
    int index;
    bool matched;

    matched = false;
    instance = ctx->instances + (data_group_id - ctx->dg_base_id);
    index = data_version % instance->ring.size;
    entry = instance->ring.entries + index;
    if (instance->ring.end == instance->ring.start) {  //empty
        instance->ring.start = entry;
        instance->ring.end = instance->ring.entries +
            (index + 1) % instance->ring.size;
        matched = true;
    } else if (entry == instance->ring.end) {
        previous = instance->ring.entries + (index - 1 +
                instance->ring.size) % instance->ring.size;
        next = instance->ring.entries + (index + 1) % instance->ring.size;
        if ((next != instance->ring.start) &&
                data_version == previous->data_version + 1)
        {
            instance->ring.end = next;
            matched = true;
        }
    }

    if (matched) {
        entry->data_version = data_version;
        entry->waiting_task = waiting_task;
        entry->task_version = task_version;
        entry->expires = g_current_time + SF_G_NETWORK_TIMEOUT;
        return 0;
    }

    logWarning("file: "__FILE__", line: %d, "
            "data group id: %d, can't found data version %"PRId64", "
            "in the ring", __LINE__, instance->data_group_id, data_version);
    return add_to_queue(ctx, instance, data_version,
            waiting_task, task_version);
}

static int remove_from_queue(FSReplicaRPCResultContext *ctx,
        FSReplicaRPCResultInstance *instance, const uint64_t data_version)
{
    FSReplicaRPCResultEntry *entry;
    FSReplicaRPCResultEntry *previous;
    FSReplicaRPCResultEntry *current;

    if (instance->queue.head == NULL) {  //empty queue
        return ENOENT;
    }

    if (data_version == instance->queue.head->data_version) {
        entry = instance->queue.head;
        instance->queue.head = entry->next;
        if (instance->queue.head == NULL) {
            instance->queue.tail = NULL;
        }
    } else {
        previous = instance->queue.head;
        current = instance->queue.head->next;
        while (current != NULL && data_version > current->data_version) {
            previous = current;
            current = current->next;
        }

        if (current == NULL || data_version != current->data_version) {
            return ENOENT;
        }

        entry = current;
        previous->next = current->next;
        if (instance->queue.tail == current) {
            instance->queue.tail = previous;
        }
    }

    desc_task_waiting_rpc_count(entry);
    fast_mblock_free_object(&ctx->rentry_allocator, entry);
    return 0;
}

int rpc_result_ring_remove(FSReplicaRPCResultContext *ctx,
        const int data_group_id, const uint64_t data_version)
{
    FSReplicaRPCResultInstance *instance;
    FSReplicaRPCResultEntry *entry;
    int index;

    instance = ctx->instances + (data_group_id - ctx->dg_base_id);
    if (instance->ring.end != instance->ring.start) {
        index = data_version % instance->ring.size;
        entry = instance->ring.entries + index;

        if (entry->data_version == data_version) {
            if (instance->ring.start == entry) {
                instance->ring.start = instance->ring.entries +
                    (++index % instance->ring.size);
                while (instance->ring.start != instance->ring.end &&
                        instance->ring.start->data_version == 0)
                {
                    instance->ring.start = instance->ring.entries +
                        (++index % instance->ring.size);
                }
            }

            desc_task_waiting_rpc_count(entry);
            entry->data_version = 0;
            entry->waiting_task = NULL;
            return 0;
        }
    }

    return remove_from_queue(ctx, instance, data_version);
}
