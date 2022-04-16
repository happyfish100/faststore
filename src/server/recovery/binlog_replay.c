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
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/fc_atomic.h"
#include "sf/sf_global.h"
#include "../../common/fs_proto.h"
#include "../../common/fs_func.h"
#include "../../client/fs_client.h"
#include "../server_global.h"
#include "../data_thread.h"
#include "../cluster_relationship.h"
#include "../server_binlog.h"
#include "../server_replication.h"
#include "../server_storage.h"
#include "../shared_thread_pool.h"
#include "data_recovery.h"
#include "binlog_replay.h"

#define FIXED_THREAD_CONTEXT_COUNT  16

#define FS_THREAD_STAGE_NONE       0
#define FS_THREAD_STAGE_RUNNING    1
#define FS_THREAD_STAGE_CLEANUP    2
#define FS_THREAD_STAGE_FINISHED   3

#define BINLOG_REPLAY_POSITION_FILENAME  "position.mark"

struct binlog_replay_context;
struct replay_thread_context;

typedef struct replay_task_info {
    int op_type;
    FSSliceOpContext op_ctx;
    struct replay_task_info *next;
} ReplayTaskInfo;

typedef struct {
    FSCounterTripple write;
    FSCounterTripple allocate;
    FSCounterTripple remove;
} ReplayStatInfo;

typedef struct common_thread_context {
    struct fc_queue queue;         //element: ReplayTaskInfo
    pthread_lock_cond_pair_t lcp;  //for notify
    volatile int64_t total_count;
    volatile int stage;
} CommonThreadContext;

typedef struct dispatch_thread_context {
    CommonThreadContext common;
    struct {
        volatile int fetch_data_count;
    } notify;
    volatile int64_t replay_total_count;
} DispatchThreadContext;

typedef struct fetch_data_thread_context {
    struct fc_queue queue;  //element: ReplayTaskInfo
    volatile int is_running;

    struct binlog_replay_context *replay_ctx;
} FetchDataThreadContext;

typedef struct replay_thread_context {
    CommonThreadContext common;

    struct {
        bool done;
    } notify;
    ReplayStatInfo stat;
} ReplayThreadContext;

typedef struct binlog_replay_context {
    volatile int continue_flag;

    struct {
        int fd;
        char filename[PATH_MAX];
    } position;

    int64_t start_time;
    int64_t total_count;
    int64_t start_data_version;
    volatile int64_t fail_count;
    BinlogReadThreadContext rdthread_ctx;
    BinlogReadThreadResult *r;

    DispatchThreadContext dispatch_thread;
    ReplayThreadContext replay_thread;

    struct {
        FetchDataThreadContext *contexts;
        FetchDataThreadContext fixed[FIXED_THREAD_CONTEXT_COUNT];
    } thread_env;
    ReplicaBinlogRecord record;
    DataRecoveryContext *recovery_ctx;
} BinlogReplayContext;

typedef struct binlog_replay_global_vars {
    DataReplayTaskAllocatorArray allocator_array;
} BinlogReplayGlobalVars;

static BinlogReplayGlobalVars replay_global_vars;

static void slice_write_done_notify(FSDataOperation *op)
{
    ReplayThreadContext *thread_ctx;

    thread_ctx = (ReplayThreadContext *)op->arg;
    PTHREAD_MUTEX_LOCK(&thread_ctx->common.lcp.lock);
    thread_ctx->notify.done = true;
    pthread_cond_signal(&thread_ctx->common.lcp.cond);
    PTHREAD_MUTEX_UNLOCK(&thread_ctx->common.lcp.lock);
}

static int replay_task_alloc_init(void *element, void *args)
{
    int result;
    ReplayTaskInfo *task;

    task = (ReplayTaskInfo *)element;
    task->op_ctx.notify_func = slice_write_done_notify;
    task->op_ctx.info.source = BINLOG_SOURCE_REPLAY;
    task->op_ctx.info.write_binlog.log_replica = false;
    task->op_ctx.info.buff = (char *)(task + 1);

    if ((result=fs_slice_array_init(&task->op_ctx.update.sarray)) != 0) {
        return result;
    }

    return 0;
}

static int init_task_allocator_array(DataReplayTaskAllocatorArray
        *allocator_array, const int count, const int elements_limit)
{
    const bool need_wait = true;
    int result;
    DataReplayTaskAllocatorInfo *ai;
    DataReplayTaskAllocatorInfo *end;
    int element_size;

    allocator_array->allocators = (DataReplayTaskAllocatorInfo *)
        fc_malloc(sizeof(DataReplayTaskAllocatorInfo) * count);
    if (allocator_array->allocators == NULL) {
        return ENOMEM;
    }

    element_size = sizeof(ReplayTaskInfo) + FS_FILE_BLOCK_SIZE;
    end = allocator_array->allocators + count;
    for (ai=allocator_array->allocators; ai<end; ai++) {
        if ((result=fast_mblock_init_ex1(&ai->allocator, "replay_task",
                        element_size, 16, elements_limit,
                        replay_task_alloc_init, NULL, true)) != 0)
        {
            return result;
        }

        ai->used = 0;
        fast_mblock_set_need_wait(&ai->allocator, need_wait,
                (bool *)&SF_G_CONTINUE_FLAG);
    }

    allocator_array->count = count;
    return 0;
}

DataReplayTaskAllocatorInfo *binlog_replay_get_task_allocator()
{
    DataReplayTaskAllocatorInfo *ai;
    DataReplayTaskAllocatorInfo *end;

    end = replay_global_vars.allocator_array.allocators +
        replay_global_vars.allocator_array.count;
    for (ai=replay_global_vars.allocator_array.allocators; ai<end; ai++) {
        if (__sync_bool_compare_and_swap(&ai->used, 0, 1)) {
            return ai;
        }
    }

    return NULL;
}

void binlog_replay_release_task_allocator(DataReplayTaskAllocatorInfo *ai)
{
    __sync_bool_compare_and_swap(&ai->used, 1, 0);
}

int binlog_replay_init(const char *config_filename)
{
    return init_task_allocator_array(&replay_global_vars.
            allocator_array, FS_DATA_RECOVERY_THREADS_LIMIT,
            RECOVERY_THREADS_PER_DATA_GROUP *
            RECOVERY_MAX_QUEUE_DEPTH * 2);
}

void binlog_replay_destroy()
{
}

static inline void binlog_replay_fail(BinlogReplayContext *replay_ctx)
{
    __sync_add_and_fetch(&replay_ctx->fail_count, 1);
    FC_ATOMIC_SET(replay_ctx->continue_flag, 0);
}

static int write_replay_position(BinlogReplayContext *replay_ctx,
        const int64_t data_version)
{
    char buff[32];
    int result;
    int len;

    if (lseek(replay_ctx->position.fd, 0, SEEK_SET) < 0) {
        result = (errno != 0 ? errno : EIO);
        logError("file: "__FILE__", line: %d, "
                "lseek file %s fail, errno: %d, error info: %s",
                __LINE__, replay_ctx->position.filename,
                result, STRERROR(result));
        return result;
    }

    len = sprintf(buff, "%-20"PRId64, data_version);
    if (fc_safe_write(replay_ctx->position.fd, buff, len) != len) {
        result = (errno != 0 ? errno : EIO);
        logError("file: "__FILE__", line: %d, "
                "write file %s fail, errno: %d, error info: %s",
                __LINE__, replay_ctx->position.filename,
                result, STRERROR(result));
        return result;
    }

    if (fsync(replay_ctx->position.fd) != 0) {
        result = (errno != 0 ? errno : EIO);
        logError("file: "__FILE__", line: %d, "
                "fsync file %s fail, errno: %d, error info: %s",
                __LINE__, replay_ctx->position.filename,
                result, STRERROR(result));
        return result;
    }

    logInfo("file: "__FILE__", line: %d, "
            "write data version: %"PRId64" to position file %s",
            __LINE__, data_version, replay_ctx->position.filename);

    return 0;
}

static int deal_task(ReplayThreadContext *thread_ctx, ReplayTaskInfo *task)
{
    int result;
    int operation;
    bool log_padding;
    BinlogReplayContext *replay_ctx;
    int64_t *success_ptr;

    replay_ctx = fc_list_entry(thread_ctx, BinlogReplayContext, replay_thread);
    log_padding = false;
    result = 0;
    operation = DATA_OPERATION_NONE;
    success_ptr = NULL;
    switch (task->op_type) {
        case BINLOG_OP_TYPE_WRITE_SLICE:
            thread_ctx->stat.write.total++;
            if (task->op_ctx.result == 0) {
                operation = DATA_OPERATION_SLICE_WRITE;
                success_ptr = &thread_ctx->stat.write.success;
            } else if (task->op_ctx.result == ENODATA) {
                log_padding = true;
                thread_ctx->stat.write.ignore++;
            }
            break;
        case BINLOG_OP_TYPE_ALLOC_SLICE:
            thread_ctx->stat.allocate.total++;
            operation = DATA_OPERATION_SLICE_ALLOCATE;
            success_ptr = &thread_ctx->stat.allocate.success;
            break;
        case BINLOG_OP_TYPE_DEL_SLICE:
            thread_ctx->stat.remove.total++;
            operation = DATA_OPERATION_SLICE_DELETE;
            success_ptr = &thread_ctx->stat.remove.success;
            break;
        case BINLOG_OP_TYPE_DEL_BLOCK:
            thread_ctx->stat.remove.total++;
            operation = DATA_OPERATION_BLOCK_DELETE;
            success_ptr = &thread_ctx->stat.remove.success;
            break;
        case BINLOG_OP_TYPE_NO_OP:
            log_padding = true;
            break;
        default:
            logError("file: "__FILE__", line: %d, "
                    "unkown op type: %c (0x%02x)",
                    __LINE__, task->op_type, task->op_type);
            result = EINVAL;
            break;
    }

    if (operation != DATA_OPERATION_NONE) {
        if ((result=push_to_data_thread_queue(operation,
                        DATA_SOURCE_SLAVE_RECOVERY, thread_ctx,
                        &task->op_ctx)) == 0)
        {
            PTHREAD_MUTEX_LOCK(&thread_ctx->common.lcp.lock);
            while (!thread_ctx->notify.done) {
                pthread_cond_wait(&thread_ctx->common.lcp.cond,
                        &thread_ctx->common.lcp.lock);
            }
            thread_ctx->notify.done = false;  /* reset for next */
            PTHREAD_MUTEX_UNLOCK(&thread_ctx->common.lcp.lock);
            result = task->op_ctx.result;
        }

        if (result == 0) {
            (*success_ptr)++;
        } else if (result == ENOENT) {
            if (operation == DATA_OPERATION_SLICE_DELETE ||
                    operation == DATA_OPERATION_BLOCK_DELETE)
            {
                result = 0;
                log_padding = true;
                thread_ctx->stat.remove.ignore++;
            }
        }
    }

    if (result == 0) {
        if (thread_ctx->common.total_count % 100 == 0) {
            result = write_replay_position(replay_ctx,
                    task->op_ctx.info.data_version);
        }

        if (log_padding) {
            DataRecoveryContext *ctx;
            ctx = replay_ctx->recovery_ctx;
            FC_ATOMIC_SET(ctx->ds->data.version,
                    task->op_ctx.info.data_version);
        }
    } else {
        logError("file: "__FILE__", line: %d, "
                "data group id: %d, %s fail, "
                "oid: %"PRId64", block offset: %"PRId64", "
                "slice offset: %d, length: %d, "
                "errno: %d, error info: %s",
                __LINE__, replay_ctx->recovery_ctx->ds->dg->id,
                replica_binlog_get_op_type_caption(task->op_type),
                task->op_ctx.info.bs_key.block.oid,
                task->op_ctx.info.bs_key.block.offset,
                task->op_ctx.info.bs_key.slice.offset,
                task->op_ctx.info.bs_key.slice.length,
                result, STRERROR(result));
    }

    return result;
}

static int clear_task_queue(BinlogReplayContext *replay_ctx,
        struct fc_queue *queue)
{
    ReplayTaskInfo *task;
    ReplayTaskInfo *current;
    int count;

    while (FC_ATOMIC_GET(replay_ctx->continue_flag)) {
        fc_sleep_ms(10);
    }

    if ((task=fc_queue_try_pop_all(queue)) == NULL) {
        return 0;
    }

    count = 0;
    do {
        ++count;
        current = task;
        task = task->next;

        fast_mblock_free_object(&replay_ctx->recovery_ctx->
                tallocator_info->allocator, current);
    } while (task != NULL);

    return count;
}

static int task_dispatch(BinlogReplayContext *replay_ctx,
        ReplayTaskInfo **tasks, const int count)
{
    FetchDataThreadContext *fetch_thread;
    ReplayTaskInfo **ppt;
    ReplayTaskInfo **end;
    int write_count;

    end = tasks + count;
    if (!FC_ATOMIC_GET(replay_ctx->continue_flag)) {
        for (ppt=tasks; ppt<end; ppt++) {
            fast_mblock_free_object(&replay_ctx->recovery_ctx->
                    tallocator_info->allocator, *ppt);
        }
        return EINTR;
    }

    write_count = 0;
    for (ppt=tasks; ppt<end; ppt++) {
        if ((*ppt)->op_type == BINLOG_OP_TYPE_WRITE_SLICE) {
            ++write_count;
        }
    }

    if (write_count > 0) {
        FC_ATOMIC_INC_EX(replay_ctx->dispatch_thread.notify.
                fetch_data_count, write_count);
        for (ppt=tasks; ppt<end; ppt++) {
            if ((*ppt)->op_type == BINLOG_OP_TYPE_WRITE_SLICE) {
                fetch_thread = replay_ctx->thread_env.contexts + (ppt - tasks);
                fc_queue_push(&fetch_thread->queue, *ppt);
            }
        }

        PTHREAD_MUTEX_LOCK(&replay_ctx->dispatch_thread.common.lcp.lock);
        while (FC_ATOMIC_GET(replay_ctx->dispatch_thread.
                    notify.fetch_data_count) > 0)
        {
            pthread_cond_wait(&replay_ctx->dispatch_thread.common.lcp.cond,
                    &replay_ctx->dispatch_thread.common.lcp.lock);
        }
        PTHREAD_MUTEX_UNLOCK(&replay_ctx->dispatch_thread.common.lcp.lock);
    }

    if (!FC_ATOMIC_GET(replay_ctx->continue_flag)) {
        for (ppt=tasks; ppt<end; ppt++) {
            fast_mblock_free_object(&replay_ctx->recovery_ctx->
                    tallocator_info->allocator, *ppt);
        }
        return EINTR;
    }

    for (ppt=tasks; ppt<end; ppt++) {
        fc_queue_push(&replay_ctx->replay_thread.common.queue, *ppt);
    }

    return 0;
}

static void task_dispatch_run(void *arg, void *thread_data)
{
    BinlogReplayContext *replay_ctx;
    DispatchThreadContext *dispatch_thread;
    ReplayTaskInfo *tasks[FS_MAX_RECOVERY_THREADS_PER_DATA_GROUP];
    int running_count;
    int waiting_count;
    int remain_count;
    int count;
    int i;

    replay_ctx = (BinlogReplayContext *)arg;
    dispatch_thread = &replay_ctx->dispatch_thread;

    while (FC_ATOMIC_GET(replay_ctx->continue_flag)) {
        for (count=0; count<RECOVERY_THREADS_PER_DATA_GROUP; count++) {
            if ((tasks[count]=(ReplayTaskInfo *)fc_queue_pop(
                            &dispatch_thread->common.queue)) == NULL)
            {
                break;
            }
        }

        if (count == 0) {
            continue;
        }

        if (task_dispatch(replay_ctx, tasks, count) == 0) {
            FC_ATOMIC_INC_EX(dispatch_thread->replay_total_count, count);
        }
        dispatch_thread->common.total_count += count;
    }

    FC_ATOMIC_SET(dispatch_thread->common.stage, FS_THREAD_STAGE_CLEANUP);
    remain_count = clear_task_queue(replay_ctx,
            &dispatch_thread->common.queue);
    dispatch_thread->common.total_count += remain_count;

    waiting_count = 0;
    while (1) {
        running_count = 0;
        for (i=0; i<RECOVERY_THREADS_PER_DATA_GROUP; i++) {
            if (FC_ATOMIC_GET(replay_ctx->thread_env.contexts[i].is_running)) {
                fc_queue_terminate(&replay_ctx->thread_env.contexts[i].queue);
                running_count++;
            }
        }

        if (running_count == 0) {
            break;
        }

        waiting_count++;
        fc_sleep_ms(5);
    }

    if (waiting_count > 1) {
        logInfo("file: "__FILE__", line: %d, "
                "data group id: %d, wait fetch data thread exit count: %d",
                __LINE__, replay_ctx->recovery_ctx->ds->dg->id, waiting_count);
    }

    FC_ATOMIC_SET(dispatch_thread->common.stage, FS_THREAD_STAGE_FINISHED);
}

static void fetch_data_run(void *arg, void *thread_data)
{
    DispatchThreadContext *dispatch_thread;
    FetchDataThreadContext *thread_ctx;
    ReplayTaskInfo *task;
    int read_bytes;

    thread_ctx = (FetchDataThreadContext *)arg;
    dispatch_thread = &thread_ctx->replay_ctx->dispatch_thread;
    while (FC_ATOMIC_GET(dispatch_thread->common.stage) ==
            FS_THREAD_STAGE_RUNNING)
    {
        if ((task=(ReplayTaskInfo *)fc_queue_pop(
                        &thread_ctx->queue)) == NULL)
        {
            continue;
        }

        if ((task->op_ctx.result=fs_client_slice_read_by_slave(
                        &g_fs_client_vars.client_ctx, thread_ctx->
                        replay_ctx->recovery_ctx->is_online ?
                        CLUSTER_MY_SERVER_ID : 0, &task->op_ctx.info.bs_key,
                        task->op_ctx.info.buff, &read_bytes)) == 0)
        {
            if (read_bytes != task->op_ctx.info.bs_key.slice.length) {
                logWarning("file: "__FILE__", line: %d, "
                        "data group id: %d, is_online: %d, block "
                        "{oid: %"PRId64", offset: %"PRId64"}, "
                        "slice {offset: %d, length: %d}, "
                        "read bytes: %d != slice length, "
                        "maybe delete later?", __LINE__,
                        thread_ctx->replay_ctx->recovery_ctx->ds->dg->id,
                        thread_ctx->replay_ctx->recovery_ctx->is_online,
                        task->op_ctx.info.bs_key.block.oid,
                        task->op_ctx.info.bs_key.block.offset,
                        task->op_ctx.info.bs_key.slice.offset,
                        task->op_ctx.info.bs_key.slice.length,
                        read_bytes);
                task->op_ctx.info.bs_key.slice.length = read_bytes;
            }
        } else if (task->op_ctx.result == ENODATA) {
            logWarning("file: "__FILE__", line: %d, "
                    "data group id: %d, block {oid: %"PRId64", "
                    "offset: %"PRId64"}, slice {offset: %d, "
                    "length: %d}, slice not exist, "
                    "maybe delete later?", __LINE__,
                    thread_ctx->replay_ctx->recovery_ctx->ds->dg->id,
                    task->op_ctx.info.bs_key.block.oid,
                    task->op_ctx.info.bs_key.block.offset,
                    task->op_ctx.info.bs_key.slice.offset,
                    task->op_ctx.info.bs_key.slice.length);
        } else {
            logError("file: "__FILE__", line: %d, "
                    "data group id: %d, block {oid: %"PRId64", "
                    "offset: %"PRId64"}, slice {offset: %d, length: %d}, "
                    "fetch data fail, errno: %d, error info: %s", __LINE__,
                    thread_ctx->replay_ctx->recovery_ctx->ds->dg->id,
                    task->op_ctx.info.bs_key.block.oid,
                    task->op_ctx.info.bs_key.block.offset,
                    task->op_ctx.info.bs_key.slice.offset,
                    task->op_ctx.info.bs_key.slice.length,
                    task->op_ctx.result, STRERROR(task->op_ctx.result));
            binlog_replay_fail(thread_ctx->replay_ctx);
        }

        PTHREAD_MUTEX_LOCK(&dispatch_thread->common.lcp.lock);
        if (FC_ATOMIC_DEC(dispatch_thread->notify.fetch_data_count) == 0) {
            pthread_cond_signal(&dispatch_thread->common.lcp.cond);
        }
        PTHREAD_MUTEX_UNLOCK(&dispatch_thread->common.lcp.lock);
    }

    __sync_bool_compare_and_swap(&thread_ctx->is_running, 1, 0);
}

static void replay_output(BinlogReplayContext *replay_ctx)
{
    DataRecoveryContext *ctx;
    ReplayStatInfo *stat;
    char prompt[32];
    char until_version_prompt[64];
    char total_tm_buff[32];
    char fetch_tm_buff[32];
    char dedup_tm_buff[32];
    char replay_tm_buff[32];
    char skip_count_buff[64];
    int64_t success_count;
    int64_t fail_count;
    int64_t ignore_count;
    int64_t skip_count;

    ctx = replay_ctx->recovery_ctx;
    stat = &replay_ctx->replay_thread.stat;
    success_count = stat->write.success + stat->allocate.success +
        stat->remove.success;
    fail_count = __sync_add_and_fetch(&replay_ctx->fail_count, 0);
    ignore_count = stat->write.ignore + stat->remove.ignore;
    skip_count = replay_ctx->total_count - (success_count +
            fail_count + ignore_count);

    if (fail_count == 0) {
        strcpy(prompt, "success");
    } else {
        strcpy(prompt, "fail");
    }
    if (skip_count == 0) {
        *skip_count_buff = '\0';
    } else {
        sprintf(skip_count_buff, ", skip_count: %"PRId64, skip_count);
    }

    ctx->time_used.replay  = get_current_time_ms() - replay_ctx->start_time;
    long_to_comma_str(get_current_time_ms() - ctx->start_time, total_tm_buff);
    long_to_comma_str(ctx->time_used.fetch, fetch_tm_buff);
    long_to_comma_str(ctx->time_used.dedup, dedup_tm_buff);
    long_to_comma_str(ctx->time_used.replay, replay_tm_buff);
    if (ctx->is_online) {
        sprintf(until_version_prompt, ", until_version: %"PRId64,
                FC_ATOMIC_GET(ctx->ds->recovery.until_version));
    } else {
        *until_version_prompt = '\0';
    }
    logInfo("file: "__FILE__", line: %d, "
            "data group id: %d, loop_count: %d, start_data_version: %"
            PRId64", last_data_version: %"PRId64"%s, data recovery %s, "
            "is_online: %d. all : {total : %"PRId64", success : %"PRId64", "
            "fail : %"PRId64", ignore : %"PRId64"%s}, "
            "write : {total : %"PRId64", success : %"PRId64", "
            "ignore : %"PRId64"}, "
            "allocate: {total : %"PRId64", success : %"PRId64", "
            "ignore : %"PRId64"}, "
            "remove: {total : %"PRId64", success : %"PRId64", "
            "ignore : %"PRId64"}, total time used: %s ms {fetch: %s ms, "
            "dedup: %s ms, replay: %s ms}", __LINE__,
            ctx->ds->dg->id, ctx->loop_count, replay_ctx->start_data_version,
            ctx->fetch.last_data_version, until_version_prompt, prompt,
            ctx->is_online, replay_ctx->total_count, success_count,
            fail_count, ignore_count, skip_count_buff,
            stat->write.total, stat->write.success, stat->write.ignore,
            stat->allocate.total, stat->allocate.success, stat->allocate.ignore,
            stat->remove.total, stat->remove.success, stat->remove.ignore,
            total_tm_buff, fetch_tm_buff, dedup_tm_buff, replay_tm_buff);
}

static void binlog_replay_run(void *arg, void *thread_data)
{
    BinlogReplayContext *replay_ctx;
    ReplayThreadContext *thread_ctx;
    ReplayTaskInfo *task;
    int remain_count;
    int result;

    replay_ctx = (BinlogReplayContext *)arg;
    thread_ctx = &replay_ctx->replay_thread;
    while (FC_ATOMIC_GET(replay_ctx->continue_flag)) {
        if ((task=(ReplayTaskInfo *)fc_queue_pop(
                        &thread_ctx->common.queue)) == NULL)
        {
            continue;
        }

        thread_ctx->common.total_count++;
        if ((result=deal_task(thread_ctx, task)) != 0) {
            binlog_replay_fail(replay_ctx);
        }
        fast_mblock_free_object(&replay_ctx->recovery_ctx->
                tallocator_info->allocator, task);
    }

    FC_ATOMIC_SET(thread_ctx->common.stage, FS_THREAD_STAGE_CLEANUP);
    while (FC_ATOMIC_GET(replay_ctx->dispatch_thread.common.stage) ==
            FS_THREAD_STAGE_RUNNING)
    {
        fc_sleep_ms(10);
    }

    remain_count = clear_task_queue(replay_ctx, &thread_ctx->common.queue);
    if (remain_count > 0) {
        thread_ctx->common.total_count += remain_count;
    }

    replay_output(replay_ctx);
    FC_ATOMIC_SET(thread_ctx->common.stage, FS_THREAD_STAGE_FINISHED);
}

static int deal_binlog_buffer(DataRecoveryContext *ctx)
{
    BinlogReplayContext *replay_ctx;
    ReplayTaskInfo *task;
    char *p;
    char *line_end;
    char *end;
    BufferInfo *buffer;
    string_t line;
    char error_info[256];
    int result;

    replay_ctx = (BinlogReplayContext *)ctx->arg;
    result = 0;
    *error_info = '\0';
    buffer = &replay_ctx->r->buffer;
    end = buffer->buff + buffer->length;
    p = buffer->buff;
    while (p < end && SF_G_CONTINUE_FLAG &&
            FC_ATOMIC_GET(replay_ctx->continue_flag)) {
        line_end = (char *)memchr(p, '\n', end - p);
        if (line_end == NULL) {
            strcpy(error_info, "expect end line (\\n)");
            result = EINVAL;
            break;
        }

        line_end++;
        line.str = p;
        line.len = line_end - p;
        if ((result=replica_binlog_record_unpack(&line,
                        &replay_ctx->record, error_info)) != 0)
        {
            break;
        }

        if (replay_ctx->start_data_version == 0) {
            replay_ctx->start_data_version = replay_ctx->record.data_version;
        }

        if (replay_ctx->record.bs_key.slice.length >
                FS_FILE_BLOCK_SIZE)
        {
            sprintf(error_info, "slice length: %d > block size: %d!",
                    replay_ctx->record.bs_key.slice.length,
                    FS_FILE_BLOCK_SIZE);
            result = EINVAL;
            break;
        }

        if ((task=fast_mblock_alloc_object(&replay_ctx->recovery_ctx->
                        tallocator_info->allocator)) == NULL)
        {
            result = EINTR;
            break;
        }

        if (!(SF_G_CONTINUE_FLAG && FC_ATOMIC_GET(
                        replay_ctx->continue_flag)))
        {
            fast_mblock_free_object(&replay_ctx->recovery_ctx->
                    tallocator_info->allocator, task);
            return EINTR;
        }

        fs_calc_block_hashcode(&replay_ctx->record.bs_key.block);
        task->op_type = replay_ctx->record.op_type;
        task->op_ctx.info.data_group_id = ctx->ds->dg->id;
        task->op_ctx.info.myself = ctx->master->dg->myself;
        task->op_ctx.info.data_version = replay_ctx->record.data_version;
        task->op_ctx.info.bs_key = replay_ctx->record.bs_key;
        replay_ctx->total_count++;
        fc_queue_push(&replay_ctx->dispatch_thread.common.queue, task);

        p = line_end;
    }

    if (result != 0 && *error_info != '\0') {
        ServerBinlogReader *reader;
        int64_t offset;
        int64_t line_count;

        reader = &replay_ctx->rdthread_ctx.reader;
        offset = reader->position.offset + (p - buffer->buff);
        fc_get_file_line_count_ex(reader->filename, offset, &line_count);

        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", %s",
                __LINE__, reader->filename,
                line_count + 1, error_info);
    }

    return result;
}

static void waiting_thread_exit(BinlogReplayContext *replay_ctx,
        CommonThreadContext *common_ctx, const char *caption)
{
    int count;
    int stage;

    if (FC_ATOMIC_GET(common_ctx->stage) == FS_THREAD_STAGE_RUNNING) {
        fc_queue_terminate(&common_ctx->queue);
    }

    count = 0;
    while ((stage=FC_ATOMIC_GET(common_ctx->stage)) !=
            FS_THREAD_STAGE_FINISHED)
    {
        if (FC_LOG_BY_LEVEL(LOG_DEBUG) && count % 10 == 1) {
            logDebug("data group id: %d, %dth waiting %s thread exit, "
                    "stage: %d, fetch_data_count: %d ...", replay_ctx->
                    recovery_ctx->ds->dg->id, ++count, caption, stage,
                    FC_ATOMIC_GET(replay_ctx->dispatch_thread.
                        notify.fetch_data_count));
        }

        fc_sleep_ms(10);
    }
}

static void waiting_work_threads_exit(DataRecoveryContext *ctx)
{
    BinlogReplayContext *replay_ctx;

    replay_ctx = (BinlogReplayContext *)ctx->arg;
    if (FC_ATOMIC_GET(replay_ctx->continue_flag)) {
        FC_ATOMIC_SET(replay_ctx->continue_flag, 0);
    }

    waiting_thread_exit(replay_ctx, &replay_ctx->
            dispatch_thread.common, "dispatch");
    waiting_thread_exit(replay_ctx, &replay_ctx->
            replay_thread.common, "replay");
}

static void replay_finish(DataRecoveryContext *ctx)
{
#define REPLAY_WAIT_TIMES  300
    BinlogReplayContext *replay_ctx;
    int64_t replay_total_count;
    int sub;
    int i;

    replay_ctx = (BinlogReplayContext *)ctx->arg;
    i = 0;
    while ((sub=replay_ctx->total_count - replay_ctx->
                    dispatch_thread.common.total_count) > 0)
    {
        if (sub <= RECOVERY_THREADS_PER_DATA_GROUP) {
            fc_queue_terminate(&replay_ctx->dispatch_thread.common.queue);
        }
        fc_sleep_ms(20);
        ++i;
    }

    if (i > 3000) {
        logWarning("file: "__FILE__", line: %d, "
                "data group id: %d, waiting dispatch thread time count: %d, "
                "input record count: %"PRId64", current deal "
                "count: %"PRId64, __LINE__, ctx->ds->dg->id, i,
                replay_ctx->total_count, replay_ctx->
                dispatch_thread.common.total_count);
    }

    replay_total_count = FC_ATOMIC_GET(replay_ctx->
            dispatch_thread.replay_total_count);
    i = 0;
    while (replay_ctx->replay_thread.common.total_count <
            replay_total_count)
    {
        fc_sleep_ms(10);
        ++i;
    }

    if (i > 100) {
        logInfo("file: "__FILE__", line: %d, "
                "data group id: %d, waiting replay thread "
                "time count: %d", __LINE__, replay_ctx->
                recovery_ctx->ds->dg->id, i);
    }

    waiting_work_threads_exit(ctx);
}

static inline const char *get_replay_position_filename(
        DataRecoveryContext *ctx,
        char *filename, const int size)
{
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];
    char filepath[PATH_MAX];

    data_recovery_get_subdir_name(ctx,
            RECOVERY_BINLOG_SUBDIR_NAME_REPLAY,
            subdir_name);
    sf_binlog_writer_get_filepath(DATA_PATH_STR, subdir_name,
            filepath, sizeof(filepath));
    snprintf(filename, size, "%s/%s", filepath,
            BINLOG_REPLAY_POSITION_FILENAME);
    return filename;
}

static inline int get_replay_last_data_version(BinlogReplayContext
        *replay_ctx, uint64_t *last_data_version)
{
    const int64_t offset = 0;
    int64_t file_size;
    int result;
    char buff[32];

    if (access(replay_ctx->position.filename, F_OK) != 0) {
        result = (errno != 0 ? errno : EPERM);
        if (result == ENOENT) {
            *last_data_version = 0;
            return 0;
        } else {
            logError("file: "__FILE__", line: %d, "
                    "access file %s fail, errno: %d, error info: %s",
                    __LINE__, replay_ctx->position.filename,
                    result, STRERROR(result));
            return result;
        }
    }

    if ((result=getFileContentEx(replay_ctx->position.filename,
                    buff, offset, &file_size)) != 0)
    {
        return result;
    }

    *last_data_version = strtoll(buff, NULL, 10);
    return 0;
}

static int replay_prepare(DataRecoveryContext *ctx)
{
    BinlogReplayContext *replay_ctx;
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];
    int result;
    uint64_t last_data_version;
    struct {
        SFBinlogFilePosition holder;
        SFBinlogFilePosition *ptr;
    } position;

    replay_ctx = (BinlogReplayContext *)ctx->arg;
    get_replay_position_filename(ctx, replay_ctx->position.filename,
            sizeof(replay_ctx->position.filename));
    if ((result=get_replay_last_data_version(replay_ctx,
                    &last_data_version)) != 0)
    {
        return result;
    }

    data_recovery_get_subdir_name(ctx,
            RECOVERY_BINLOG_SUBDIR_NAME_REPLAY,
            subdir_name);
    if (last_data_version == 0) {
        position.ptr = NULL;
        result = 0;
    } else {
        position.ptr = &position.holder;
        if ((result=replica_binlog_get_position_by_dv(subdir_name, NULL,
                        last_data_version, &position.holder, true)) != 0)
        {
            return result;
        }
    }

    if ((result=binlog_read_thread_init(&replay_ctx->
                    rdthread_ctx, subdir_name, NULL,
                    position.ptr, BINLOG_BUFFER_SIZE)) == 0)
    {
        if ((replay_ctx->position.fd=open(replay_ctx->position.filename,
                        O_WRONLY | O_CREAT | O_TRUNC, 0644)) < 0)
        {
            result = errno != 0 ? errno : EPERM;
            logError("file: "__FILE__", line: %d, "
                    "open file %s fail, errno: %d, error info: %s",
                    __LINE__, replay_ctx->position.filename,
                    result, STRERROR(result));
        }
    } else {
        if (result == ENOENT) {
            logWarning("file: "__FILE__", line: %d, "
                    "%s, the replay / deduped binlog not exist, "
                    "cleanup!", __LINE__, subdir_name);
            data_recovery_unlink_sys_data(ctx);  //cleanup for bad case
        }
    }

    logInfo("file: "__FILE__", line: %d, "
            "%s, replay start offset: %"PRId64" ...",
            __LINE__, subdir_name, (position.ptr != NULL ?
                position.ptr->offset : 0));

    return result;
}

static int do_replay_binlog(DataRecoveryContext *ctx)
{
    BinlogReplayContext *replay_ctx;
    int result;

    replay_ctx = (BinlogReplayContext *)ctx->arg;
    if ((result=replay_prepare(ctx)) != 0) {
        waiting_work_threads_exit(ctx);
        return result;
    }

    while (SF_G_CONTINUE_FLAG && FC_ATOMIC_GET(
                replay_ctx->continue_flag))
    {
        if ((replay_ctx->r=binlog_read_thread_fetch_result(
                        &replay_ctx->rdthread_ctx)) == NULL)
        {
            result = EINTR;
            break;
        }

        if (FC_LOG_BY_LEVEL(LOG_DEBUG)) {
            logDebug("data group id: %d, replay thread running stage: %d, "
                    "errno: %d, buffer length: %d", ctx->ds->dg->id,
                    FC_ATOMIC_GET(replay_ctx->replay_thread.common.stage),
                    replay_ctx->r->err_no, replay_ctx->r->buffer.length);
        }

        if (replay_ctx->r->err_no == ENOENT) {
            break;
        } else if (replay_ctx->r->err_no != 0) {
            result = replay_ctx->r->err_no;
            break;
        }

        if ((result=deal_binlog_buffer(ctx)) != 0) {
            break;
        }

        binlog_read_thread_return_result_buffer(
                &replay_ctx->rdthread_ctx,
                replay_ctx->r);
    }
    binlog_read_thread_terminate(&replay_ctx->rdthread_ctx);

    if (result != 0) {
        binlog_replay_fail(replay_ctx);
    }
    replay_finish(ctx);

    if (result == 0) {
        if (FC_ATOMIC_GET(replay_ctx->fail_count) > 0) {
            result = EBUSY;
        } else {
            result = write_replay_position(replay_ctx,
                    ctx->fetch.last_data_version);
        }
    }
    close(replay_ctx->position.fd);

    return result;
}

static int init_common_thread_ctx(CommonThreadContext *common_ctx)
{
    int result;

    if ((result=fc_queue_init(&common_ctx->queue, (long)
                    (&((ReplayTaskInfo *)NULL)->next))) != 0)
    {
        return result;
    }

    if ((result=init_pthread_lock_cond_pair(&common_ctx->lcp)) != 0) {
        return result;
    }

    return 0;
}

static int init_fetch_thread_ctx(FetchDataThreadContext *thread_ctx)
{
    int result;

    if ((result=fc_queue_init(&thread_ctx->queue, (long)
                    (&((ReplayTaskInfo *)NULL)->next))) != 0)
    {
        return result;
    }

    return 0;
}

static int int_replay_context(DataRecoveryContext *ctx)
{
    BinlogReplayContext *replay_ctx;
    FetchDataThreadContext *context;
    FetchDataThreadContext *end;
    int bytes;
    int result;

    replay_ctx = (BinlogReplayContext *)ctx->arg;
    bytes = sizeof(FetchDataThreadContext) * RECOVERY_THREADS_PER_DATA_GROUP;
    if (RECOVERY_THREADS_PER_DATA_GROUP <= FIXED_THREAD_CONTEXT_COUNT) {
        replay_ctx->thread_env.contexts = replay_ctx->thread_env.fixed;
    } else {
        replay_ctx->thread_env.contexts = (FetchDataThreadContext *)
            fc_malloc(bytes);
        if (replay_ctx->thread_env.contexts == NULL) {
            return ENOMEM;
        }
    }
    memset(replay_ctx->thread_env.contexts, 0, bytes);

    if ((result=init_common_thread_ctx(&replay_ctx->
                    dispatch_thread.common)) != 0)
    {
        return result;
    }
    if ((result=init_common_thread_ctx(&replay_ctx->
                    replay_thread.common)) != 0)
    {
        return result;
    }

    FC_ATOMIC_SET(replay_ctx->continue_flag, 1);
    do {
        FC_ATOMIC_SET(replay_ctx->dispatch_thread.common.stage,
                FS_THREAD_STAGE_RUNNING);
        if ((result=shared_thread_pool_run(task_dispatch_run,
                        replay_ctx)) != 0)
        {
            /* rollback the stage */
            FC_ATOMIC_SET(replay_ctx->dispatch_thread.common.stage,
                    FS_THREAD_STAGE_FINISHED);
            break;
        }

        end = replay_ctx->thread_env.contexts +
            RECOVERY_THREADS_PER_DATA_GROUP;
        for (context=replay_ctx->thread_env.contexts;
                context<end; context++)
        {
            if ((result=init_fetch_thread_ctx(context)) != 0) {
                break;
            }
            context->replay_ctx = replay_ctx;
            __sync_bool_compare_and_swap(&context->is_running, 0, 1);
            if ((result=shared_thread_pool_run(fetch_data_run,
                            context)) != 0)
            {
                /* rollback the running status */
                __sync_bool_compare_and_swap(&context->is_running, 1, 0);
                break;
            }
        }
        if (result != 0) {
            break;
        }

        FC_ATOMIC_SET(replay_ctx->replay_thread.common.stage,
                FS_THREAD_STAGE_RUNNING);
        if ((result=shared_thread_pool_run(binlog_replay_run,
                        replay_ctx)) != 0)
        {
            break;
        }
    } while (0);

    if (result != 0) {
        FC_ATOMIC_SET(replay_ctx->continue_flag, 0);
        FC_ATOMIC_SET(replay_ctx->replay_thread.common.stage,
                FS_THREAD_STAGE_FINISHED);
        waiting_work_threads_exit(ctx);
    }
    return result;
}

static inline void destroy_common_thread_ctx(CommonThreadContext *common_ctx)
{
    fc_queue_destroy(&common_ctx->queue);
    destroy_pthread_lock_cond_pair(&common_ctx->lcp);
}

static void destroy_replay_context(BinlogReplayContext *replay_ctx)
{
    FetchDataThreadContext *context;
    FetchDataThreadContext *cend;

    cend = replay_ctx->thread_env.contexts + RECOVERY_THREADS_PER_DATA_GROUP;
    for (context=replay_ctx->thread_env.contexts; context<cend; context++) {
        fc_queue_destroy(&context->queue);
    }

    destroy_common_thread_ctx(&replay_ctx->dispatch_thread.common);
    destroy_common_thread_ctx(&replay_ctx->replay_thread.common);

    if (replay_ctx->thread_env.contexts != replay_ctx->thread_env.fixed) {
        free(replay_ctx->thread_env.contexts);
    }
}

int data_recovery_replay_binlog(DataRecoveryContext *ctx)
{
    int result;
    BinlogReplayContext replay_ctx;

    ctx->arg = &replay_ctx;
    memset(&replay_ctx, 0, sizeof(replay_ctx));
    replay_ctx.recovery_ctx = ctx;
    replay_ctx.start_time = get_current_time_ms();

    if ((result=int_replay_context(ctx)) != 0) {
        return result;
    }
    result = do_replay_binlog(ctx);
    destroy_replay_context(&replay_ctx);
    return result;
}

int data_recovery_unlink_replay_binlog(DataRecoveryContext *ctx)
{
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];
    char full_filename[PATH_MAX];
    int result;

    data_recovery_get_subdir_name(ctx,
            RECOVERY_BINLOG_SUBDIR_NAME_REPLAY,
            subdir_name);
    binlog_reader_get_filename(subdir_name, 0,
            full_filename, sizeof(full_filename));
    if ((result=fc_delete_file(full_filename)) != 0) {
        return result;
    }

    get_replay_position_filename(ctx, full_filename, sizeof(full_filename));
    return fc_delete_file(full_filename);
}
