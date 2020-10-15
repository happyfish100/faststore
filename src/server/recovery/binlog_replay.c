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
#include "fastcommon/thread_pool.h"
#include "sf/sf_global.h"
#include "../../common/fs_proto.h"
#include "../../common/fs_func.h"
#include "../../client/fs_client.h"
#include "../server_global.h"
#include "../cluster_relationship.h"
#include "../server_binlog.h"
#include "../server_replication.h"
#include "../server_storage.h"
#include "data_recovery.h"
#include "binlog_replay.h"

#define FIXED_THREAD_CONTEXT_COUNT  16

struct binlog_replay_context;
struct replay_thread_context;

typedef struct replay_task_info {
    int op_type;
    FSSliceOpContext op_ctx;
    struct replay_thread_context *thread_ctx;
    struct replay_task_info *next;
} ReplayTaskInfo;

typedef struct {
    FSCounterTripple write;
    FSCounterTripple allocate;
    FSCounterTripple remove;
} ReplayStatInfo;

typedef struct replay_thread_context {
    struct {
        struct fc_queue freelist; //element: ReplayTaskInfo
        struct fc_queue waiting;  //element: ReplayTaskInfo
    } queues;

    struct {
        pthread_lock_cond_pair_t lcp;
        bool done;
    } notify;

    ReplayStatInfo stat;
    struct ob_slice_ptr_array slice_ptr_array;
    struct binlog_replay_context *replay_ctx;
} ReplayThreadContext;

typedef struct binlog_replay_context {
    volatile int running_count;
    volatile bool continue_flag;
    int64_t total_count;
    volatile int64_t fail_count;
    BinlogReadThreadContext rdthread_ctx;
    BinlogReadThreadResult *r;
    struct {
        ReplayThreadContext *contexts;
        ReplayThreadContext fixed[FIXED_THREAD_CONTEXT_COUNT];
        ReplayTaskInfo *tasks;   //holder
        int task_count;
    } thread_env;
    ReplicaBinlogRecord record;
    DataRecoveryContext *recovery_ctx;
} BinlogReplayContext;

static FCThreadPool replay_thread_pool;

static void *alloc_thread_extra_data_func()
{
    return fc_malloc(FS_FILE_BLOCK_SIZE);
}

static void free_thread_extra_data_func(void *ptr)
{
    free(ptr);
}

int binlog_replay_init()
{
    int result;
    int limit;
    const int max_idle_time = 60;
    const int min_idle_count = 0;
    FCThreadExtraDataCallbacks extra_data_callbacks;

    limit = DATA_RECOVERY_THREADS_LIMIT * RECOVERY_THREADS_PER_DATA_GROUP;
    extra_data_callbacks.alloc = alloc_thread_extra_data_func;
    extra_data_callbacks.free = free_thread_extra_data_func;
    if ((result=fc_thread_pool_init_ex(&replay_thread_pool, "binlog replay",
                    limit, SF_G_THREAD_STACK_SIZE, max_idle_time,
                    min_idle_count, (bool *)&SF_G_CONTINUE_FLAG,
                    &extra_data_callbacks)) != 0)
    {
        return result;
    }

    g_fs_client_vars.client_ctx.connect_timeout = SF_G_CONNECT_TIMEOUT;
    g_fs_client_vars.client_ctx.network_timeout = SF_G_NETWORK_TIMEOUT;
    snprintf(g_fs_client_vars.base_path, sizeof(g_fs_client_vars.base_path),
            "%s", SF_G_BASE_PATH);
    g_fs_client_vars.client_ctx.cluster_cfg.ptr = &CLUSTER_CONFIG_CTX;
    if ((result=fs_simple_connection_manager_init(&g_fs_client_vars.client_ctx,
                    &g_fs_client_vars.client_ctx.conn_manager)) != 0)
    {
        return result;
    }
    g_fs_client_vars.client_ctx.is_simple_conn_mananger = true;

    return 0;
}

void binlog_replay_destroy()
{
}

static void slice_write_done_notify(FSSliceOpContext *op_ctx)
{
    ReplayThreadContext *thread_ctx;

    thread_ctx = (ReplayThreadContext *)op_ctx->notify.arg;
    PTHREAD_MUTEX_LOCK(&thread_ctx->notify.lcp.lock);
    if (!thread_ctx->notify.done) {
        thread_ctx->notify.done = true;
        pthread_cond_signal(&thread_ctx->notify.lcp.cond);
    }
    PTHREAD_MUTEX_UNLOCK(&thread_ctx->notify.lcp.lock);
}

static int deal_task(ReplayTaskInfo *task, char *buff)
{
    int result;
    int read_bytes;
    bool log_padding;

    log_padding = false;
    switch (task->op_type) {
        case REPLICA_BINLOG_OP_TYPE_WRITE_SLICE:
            task->thread_ctx->notify.done = false;
            task->thread_ctx->stat.write.total++;

            if (task->op_ctx.info.bs_key.slice.length > FS_FILE_BLOCK_SIZE) {
                logError("file: "__FILE__", line: %d, "
                        "slice length: %d > block size: %d!",
                        __LINE__, task->op_ctx.info.bs_key.slice.length,
                        FS_FILE_BLOCK_SIZE);
                result = EINVAL;
                break;
            }

            if ((result=fs_client_slice_read(&g_fs_client_vars.
                            client_ctx, &task->op_ctx.info.bs_key,
                            buff, &read_bytes)) == 0)
            {
                if (read_bytes != task->op_ctx.info.bs_key.slice.length) {
                    logWarning("file: "__FILE__", line: %d, "
                            "oid: %"PRId64", block offset: %"PRId64", "
                            "slice offset: %d, length: %d, "
                            "read bytes: %d != slice length, "
                            "maybe delete later?", __LINE__,
                            task->op_ctx.info.bs_key.block.oid,
                            task->op_ctx.info.bs_key.block.offset,
                            task->op_ctx.info.bs_key.slice.offset,
                            task->op_ctx.info.bs_key.slice.length,
                            read_bytes);
                    task->op_ctx.info.bs_key.slice.length = read_bytes;
                }
                task->op_ctx.info.buff = buff;
                if ((result=fs_slice_write(&task->op_ctx)) == 0) {
                    PTHREAD_MUTEX_LOCK(&task->thread_ctx->notify.lcp.lock);
                    while (!task->thread_ctx->notify.done) {
                        pthread_cond_wait(&task->thread_ctx->notify.lcp.cond,
                                &task->thread_ctx->notify.lcp.lock);
                    }
                    PTHREAD_MUTEX_UNLOCK(&task->thread_ctx->notify.lcp.lock);

                    if (task->op_ctx.result == 0) {
                        task->thread_ctx->stat.write.success++;
                    } else {
                        result = task->op_ctx.result;
                    }
                }
            } else if (result == ENODATA) {
                logWarning("file: "__FILE__", line: %d, "
                        "oid: %"PRId64", block offset: %"PRId64", "
                        "slice offset: %d, length: %d, slice not exist, "
                        "maybe delete later?", __LINE__,
                        task->op_ctx.info.bs_key.block.oid,
                        task->op_ctx.info.bs_key.block.offset,
                        task->op_ctx.info.bs_key.slice.offset,
                        task->op_ctx.info.bs_key.slice.length);
                result = 0;
                log_padding = true;
                task->thread_ctx->stat.write.ignore++;
            }
            break;
        case REPLICA_BINLOG_OP_TYPE_ALLOC_SLICE:
            task->thread_ctx->stat.allocate.total++;
            if ((result=fs_slice_allocate_ex(&task->op_ctx, &task->
                            thread_ctx->slice_ptr_array)) == 0)
            {
                task->thread_ctx->stat.allocate.success++;
            }
            break;
        case REPLICA_BINLOG_OP_TYPE_DEL_SLICE:
            task->thread_ctx->stat.remove.total++;
            if ((result=fs_delete_slices(&task->op_ctx)) == 0) {
                task->thread_ctx->stat.remove.success++;
            } else if (result == ENOENT) {
                result = 0;
                log_padding = true;
                task->thread_ctx->stat.remove.ignore++;
            }
            break;
        default:
            logError("file: "__FILE__", line: %d, "
                    "unkown op type: %c (0x%02x)",
                    __LINE__, task->op_type, task->op_type);
            result = EINVAL;
            break;
    }

    if (result == 0) {
        if (log_padding) {
            result = replica_binlog_log_no_op(task->thread_ctx->
                    replay_ctx->recovery_ctx->ds->dg->id,
                    task->op_ctx.info.data_version,
                    &task->op_ctx.info.bs_key.block);
        }
    } else {
        __sync_add_and_fetch(&task->thread_ctx->replay_ctx->fail_count, 1);
        task->thread_ctx->replay_ctx->continue_flag = false;
        logError("file: "__FILE__", line: %d, "
                "data group id: %d, %s fail, "
                "oid: %"PRId64", block offset: %"PRId64", "
                "slice offset: %d, length: %d, "
                "errno: %d, error info: %s",
                __LINE__, task->thread_ctx->replay_ctx->
                recovery_ctx->ds->dg->id,
                replica_binlog_get_op_type_caption(task->op_type),
                task->op_ctx.info.bs_key.block.oid,
                task->op_ctx.info.bs_key.block.offset,
                task->op_ctx.info.bs_key.slice.offset,
                task->op_ctx.info.bs_key.slice.length,
                result, STRERROR(result));
    }

    return result;
}

static void binlog_replay_run(void *arg, void *thread_data)
{
    ReplayThreadContext *thread_ctx;
    ReplayTaskInfo *task;
    char *buff;

    buff = (char *)thread_data;
    thread_ctx = (ReplayThreadContext *)arg;
    __sync_add_and_fetch(&thread_ctx->replay_ctx->running_count, 1);
    while (thread_ctx->replay_ctx->continue_flag) {
        if ((task=(ReplayTaskInfo *)fc_queue_try_pop(
                        &thread_ctx->queues.waiting)) == NULL)
        {
            fc_sleep_ms(100);
            continue;
        }

        do {
            if (deal_task(task, buff) != 0) {
                break;
            }
            fc_queue_push(&thread_ctx->queues.freelist, task);

            task = (ReplayTaskInfo *)fc_queue_try_pop(
                    &thread_ctx->queues.waiting);
        } while (task != NULL && SF_G_CONTINUE_FLAG);
    }

    __sync_sub_and_fetch(&thread_ctx->replay_ctx->running_count, 1);
}

static int deal_binlog_buffer(DataRecoveryContext *ctx)
{
    BinlogReplayContext *replay_ctx;
    ReplayThreadContext *thread_ctx;
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
    while (p < end) {
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

        fs_calc_block_hashcode(&replay_ctx->record.bs_key.block);

        thread_ctx = replay_ctx->thread_env.contexts +
            FS_BLOCK_HASH_CODE(replay_ctx->record.bs_key.block) %
            RECOVERY_THREADS_PER_DATA_GROUP;
        while (1) {
            if ((task=(ReplayTaskInfo *)fc_queue_pop(
                            &thread_ctx->queues.freelist)) != NULL)
            {
                break;
            }

            if (!SF_G_CONTINUE_FLAG) {
                return EINTR;
            }
        }

        task->op_type = replay_ctx->record.op_type;
        task->op_ctx.info.source = BINLOG_SOURCE_REPLAY;
        task->op_ctx.info.data_version = replay_ctx->record.data_version;
        task->op_ctx.info.bs_key = replay_ctx->record.bs_key;
        fc_queue_push(&thread_ctx->queues.waiting, task);
        replay_ctx->total_count++;

        p = line_end;
    }

    if (result != 0) {
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

static void calc_replay_stat(BinlogReplayContext *replay_ctx,
        ReplayStatInfo *stat)
{
    ReplayThreadContext *context;
    ReplayThreadContext *end;

    memset(stat, 0, sizeof(*stat));
    end = replay_ctx->thread_env.contexts + RECOVERY_THREADS_PER_DATA_GROUP;
    for (context=replay_ctx->thread_env.contexts; context<end; context++) {
        stat->write.total += context->stat.write.total;
        stat->write.success += context->stat.write.success;
        stat->write.ignore += context->stat.write.ignore;
        stat->allocate.total += context->stat.allocate.total;
        stat->allocate.success += context->stat.allocate.success;
        stat->remove.total += context->stat.remove.total;
        stat->remove.success += context->stat.remove.success;
        stat->remove.ignore += context->stat.remove.ignore;
    }
}

static void waiting_replay_threads_exit(DataRecoveryContext *ctx)
{
    BinlogReplayContext *replay_ctx;

    replay_ctx = (BinlogReplayContext *)ctx->arg;
    replay_ctx->continue_flag = false;
    while (__sync_add_and_fetch(&replay_ctx->running_count, 0) > 0) {
        logDebug("data group id: %d, replay running threads: %d",
                ctx->ds->dg->id, __sync_add_and_fetch(
                    &replay_ctx->running_count, 0));
        fc_sleep_ms(10);
    }
}

static void replay_finish(DataRecoveryContext *ctx, const int err_no)
{
#define REPLAY_WAIT_TIMES  30
    BinlogReplayContext *replay_ctx;
    ReplayStatInfo stat;
    int64_t total_count;
    int i;

    replay_ctx = (BinlogReplayContext *)ctx->arg;
    if (err_no == 0) {
        for (i=0; i<REPLAY_WAIT_TIMES; i++) {
            fc_sleep_ms(100);
            calc_replay_stat(replay_ctx, &stat);
            total_count = stat.write.total + stat.allocate.total +
                stat.remove.total;
            if (total_count == replay_ctx->total_count) {
                break;
            }
        }

        if (i == REPLAY_WAIT_TIMES) {
            logWarning("file: "__FILE__", line: %d, "
                    "data group id: %d, replay running threads: %d, "
                    "waiting thread ready timeout, input record "
                    "count: %"PRId64", current deal count: %"PRId64,
                    __LINE__, ctx->ds->dg->id, __sync_add_and_fetch(
                        &replay_ctx->running_count, 0),
                    replay_ctx->total_count, total_count);
        }
    }

    waiting_replay_threads_exit(ctx);
}

static int replay_output(DataRecoveryContext *ctx, const int err_no)
{
    BinlogReplayContext *replay_ctx;
    ReplayStatInfo stat;
    char prompt[32];
    char time_buff[32];
    int64_t end_time;
    int64_t total_count;
    int64_t success_count;
    int64_t fail_count;
    int64_t ignore_count;
    int result;

    replay_ctx = (BinlogReplayContext *)ctx->arg;
    calc_replay_stat(replay_ctx, &stat);
    total_count = stat.write.total + stat.allocate.total + stat.remove.total;
    success_count = stat.write.success + stat.allocate.success + stat.remove.success;
    fail_count = __sync_add_and_fetch(&replay_ctx->fail_count, 0);
    ignore_count = stat.write.ignore + stat.remove.ignore;

    result = fail_count == 0 ? err_no : EBUSY;
    if (result == 0) {
        strcpy(prompt, "success");
    } else {
        strcpy(prompt, "fail");
    }

    end_time = get_current_time_ms();
    long_to_comma_str(end_time - ctx->start_time, time_buff);
    logInfo("file: "__FILE__", line: %d, "
            "data group id: %d, data recovery %s, time used: %s ms. "
            "all : {total : %"PRId64", success : %"PRId64", "
            "fail : %"PRId64", ignore : %"PRId64"}, "
            "write : {total : %"PRId64", success : %"PRId64", "
            "ignore : %"PRId64"}, "
            "allocate: {total : %"PRId64", success : %"PRId64", "
            "ignore : %"PRId64"}, "
            "remove: {total : %"PRId64", success : %"PRId64", "
            "ignore : %"PRId64"}", __LINE__, ctx->ds->dg->id, prompt,
            time_buff, total_count, success_count, fail_count, ignore_count,
            stat.write.total, stat.write.success, stat.write.ignore,
            stat.allocate.total, stat.allocate.success, stat.allocate.ignore,
            stat.remove.total, stat.remove.success, stat.remove.ignore);

    return result;
}

static int do_replay_binlog(DataRecoveryContext *ctx)
{
    BinlogReplayContext *replay_ctx;
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];
    int result;
    uint64_t last_data_version;
    SFBinlogFilePosition position;

    replay_ctx = (BinlogReplayContext *)ctx->arg;
    last_data_version = __sync_add_and_fetch(
            &ctx->ds->data.version, 0);
    data_recovery_get_subdir_name(ctx,
            RECOVERY_BINLOG_SUBDIR_NAME_REPLAY,
            subdir_name);
    if ((result=replica_binlog_get_position_by_dv(subdir_name,
                    NULL, last_data_version, &position, true)) == 0)
    {
        if ((result=binlog_read_thread_init(&replay_ctx->
                        rdthread_ctx, subdir_name, NULL,
                        &position, BINLOG_BUFFER_SIZE)) != 0)
        {
            if (result == ENOENT) {
                logWarning("file: "__FILE__", line: %d, "
                        "%s, the replay / deduped binlog not exist, "
                        "cleanup!", __LINE__, subdir_name);
                data_recovery_unlink_sys_data(ctx);  //cleanup for bad case
            }
        }
    }

    if (result != 0) {
        waiting_replay_threads_exit(ctx);
        return result;
    }

    logDebug("file: "__FILE__", line: %d, "
            "%s, replay start offset: %"PRId64" ...",
            __LINE__, subdir_name, position.offset);

    result = 0;
    while (SF_G_CONTINUE_FLAG) {
        if ((replay_ctx->r=binlog_read_thread_fetch_result(
                        &replay_ctx->rdthread_ctx)) == NULL)
        {
            result = EINTR;
            break;
        }

        logDebug("data group id: %d, replay running threads: %d, "
                "errno: %d, buffer length: %d",
                ctx->ds->dg->id, __sync_add_and_fetch(
                &replay_ctx->running_count, 0),
                replay_ctx->r->err_no,
                replay_ctx->r->buffer.length);
        if (replay_ctx->r->err_no == ENOENT) {
            break;
        } else if (replay_ctx->r->err_no != 0) {
            result = replay_ctx->r->err_no;
            break;
        }

        if ((result=deal_binlog_buffer(ctx)) != 0) {
            break;
        }

        binlog_read_thread_return_result_buffer(&replay_ctx->rdthread_ctx,
                replay_ctx->r);
    }
    binlog_read_thread_terminate(&replay_ctx->rdthread_ctx);

    replay_finish(ctx, result);
    return replay_output(ctx, result);
}

static int init_rthread_context(ReplayThreadContext *thread_ctx,
        ReplayTaskInfo *tasks)
{
    int result;
    bool notify;
    ReplayTaskInfo *task;
    ReplayTaskInfo *end;

    if ((result=fc_queue_init(&thread_ctx->queues.freelist, (long)
                    (&((ReplayTaskInfo *)NULL)->next))) != 0)
    {
        return result;
    }

    if ((result=fc_queue_init(&thread_ctx->queues.waiting, (long)
                    (&((ReplayTaskInfo *)NULL)->next))) != 0)
    {
        return result;
    }

    if ((result=init_pthread_lock_cond_pair(&thread_ctx->notify.lcp)) != 0) {
        return result;
    }

    end = tasks + RECOVERY_MAX_QUEUE_DEPTH;
    for (task=tasks; task<end; task++) {
        task->op_ctx.notify.func = slice_write_done_notify;
        task->op_ctx.notify.arg = thread_ctx;
        task->thread_ctx = thread_ctx;
        fc_queue_push_ex(&thread_ctx->queues.freelist, task, &notify);
    }

    ob_index_init_slice_ptr_array(&thread_ctx->slice_ptr_array);
    return 0;
}

static int init_replay_tasks(DataRecoveryContext *ctx)
{
    BinlogReplayContext *replay_ctx;
    ReplayTaskInfo *task;
    ReplayTaskInfo *end;
    int result;
    int count;
    int bytes;

    replay_ctx = (BinlogReplayContext *)ctx->arg;
    count = RECOVERY_THREADS_PER_DATA_GROUP * RECOVERY_MAX_QUEUE_DEPTH;
    bytes = sizeof(ReplayTaskInfo) * count;
    replay_ctx->thread_env.tasks = (ReplayTaskInfo *)fc_malloc(bytes);
    if (replay_ctx->thread_env.tasks == NULL) {
        return ENOMEM;
    }

    end = replay_ctx->thread_env.tasks + count;
    for (task=replay_ctx->thread_env.tasks; task<end; task++) {
        task->op_ctx.info.write_binlog.log_replica = true;
        task->op_ctx.info.write_binlog.immediately = true;
        task->op_ctx.info.data_group_id = ctx->ds->dg->id;
        task->op_ctx.info.myself = ctx->master->dg->myself;

        if ((result=fs_init_slice_op_ctx(&task->op_ctx.update.sarray)) != 0) {
            return result;
        }
    }
    replay_ctx->thread_env.task_count = count;

    return 0;
}

static int int_replay_context(DataRecoveryContext *ctx)
{
    BinlogReplayContext *replay_ctx;
    ReplayThreadContext *context;
    ReplayThreadContext *end;
    ReplayTaskInfo *tasks;
    int bytes;
    int result;

    replay_ctx = (BinlogReplayContext *)ctx->arg;
    bytes = sizeof(ReplayThreadContext) * RECOVERY_THREADS_PER_DATA_GROUP;
    if (RECOVERY_THREADS_PER_DATA_GROUP <= FIXED_THREAD_CONTEXT_COUNT) {
        replay_ctx->thread_env.contexts = replay_ctx->thread_env.fixed;
    } else {
        replay_ctx->thread_env.contexts = (ReplayThreadContext *)
            fc_malloc(bytes);
        if (replay_ctx->thread_env.contexts == NULL) {
            return ENOMEM;
        }
    }
    memset(replay_ctx->thread_env.contexts, 0, bytes);

    if ((result=init_replay_tasks(ctx)) != 0) {
        return result;
    }

    replay_ctx->continue_flag = true;
    end = replay_ctx->thread_env.contexts + RECOVERY_THREADS_PER_DATA_GROUP;
    for (context=replay_ctx->thread_env.contexts,
            tasks=replay_ctx->thread_env.tasks; context<end;
            context++, tasks += RECOVERY_MAX_QUEUE_DEPTH)
    {
        context->replay_ctx = replay_ctx;
        if ((result=init_rthread_context(context, tasks)) != 0) {
            break;
        }

        if ((result=fc_thread_pool_run(&replay_thread_pool,
                        binlog_replay_run, context)) != 0)
        {
            break;
        }
    }

    if (result != 0) {
        replay_ctx->continue_flag = false;
    }

    return result;
}

static void destroy_replay_context(BinlogReplayContext *replay_ctx)
{
    ReplayThreadContext *context;
    ReplayThreadContext *cend;
    ReplayTaskInfo *task;
    ReplayTaskInfo *tend;

    cend = replay_ctx->thread_env.contexts + RECOVERY_THREADS_PER_DATA_GROUP;
    for (context=replay_ctx->thread_env.contexts; context<cend; context++) {
        fc_queue_destroy(&context->queues.freelist);
        fc_queue_destroy(&context->queues.waiting);

        destroy_pthread_lock_cond_pair(&context->notify.lcp);
        ob_index_free_slice_ptr_array(&context->slice_ptr_array);
    }

    tend = replay_ctx->thread_env.tasks + replay_ctx->thread_env.task_count;
    for (task=replay_ctx->thread_env.tasks; task<tend; task++) {
        fs_free_slice_op_ctx(&task->op_ctx.update.sarray);
    }
    free(replay_ctx->thread_env.tasks);

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

    if ((result=int_replay_context(ctx)) != 0) {
        return result;
    }
    result = do_replay_binlog(ctx);
    destroy_replay_context(&replay_ctx);
    return result;
}

int data_recovery_unlink_replay_binlog(DataRecoveryContext *ctx)
{
    char full_filename[PATH_MAX];
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];

    data_recovery_get_subdir_name(ctx, RECOVERY_BINLOG_SUBDIR_NAME_REPLAY,
            subdir_name);
    binlog_reader_get_filename(subdir_name, 0, full_filename, sizeof(full_filename));
    return fc_delete_file(full_filename);
}
