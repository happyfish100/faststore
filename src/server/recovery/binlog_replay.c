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
#include "../server_global.h"
#include "../cluster_relationship.h"
#include "../server_binlog.h"
#include "../server_replication.h"
#include "data_recovery.h"
#include "binlog_replay.h"

#define FIXED_THREAD_CONTEXT_COUNT  16

struct binlog_replay_context;

typedef struct {
    struct {
        struct fc_queue freelist; //element: ReplicaBinlogRecord
        struct fc_queue waiting;  //element: ReplicaBinlogRecord
    } queues;

    struct binlog_replay_context *replay_ctx;
} ReplayThreadContext;

typedef struct binlog_replay_context {
    volatile int running_count;
    bool continue_flag;
    BinlogReadThreadContext rdthread_ctx;
    BinlogReadThreadResult *r;
    struct {
        ReplayThreadContext *contexts;
        ReplayThreadContext fixed[FIXED_THREAD_CONTEXT_COUNT];
        ReplicaBinlogRecord *records;   //holder
    } thread_env;
    ReplicaBinlogRecord record;
} BinlogReplayContext;

static FCThreadPool replay_thread_pool;

int binlog_replay_init()
{
    int result;
    int limit;
    const int max_idle_time = 60;
    const int min_idle_count = 0;

    limit = DATA_RECOVERY_THREADS_LIMIT * RECOVERY_THREADS_PER_DATA_GROUP;
    if ((result=fc_thread_pool_init(&replay_thread_pool,
                    limit, SF_G_THREAD_STACK_SIZE, max_idle_time,
                    min_idle_count, (bool *)&SF_G_CONTINUE_FLAG)) != 0)
    {
        return result;
    }

    return 0;
}

void binlog_replay_destroy()
{
}

static void binlog_replay_run(void *arg)
{
    ReplayThreadContext *thread_ctx;

    thread_ctx = (ReplayThreadContext *)arg;

    while (thread_ctx->replay_ctx->continue_flag) {
        //TODO
    }
}

static int deal_binlog_buffer(DataRecoveryContext *ctx)
{
    BinlogReplayContext *replay_ctx;
    ReplayThreadContext *thread_ctx;
    ReplicaBinlogRecord *record;
    char *p;
    char *line_end;
    char *end;
    BufferInfo *buffer;
    string_t line;
    char error_info[256];
    int result;
    int op_type;

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

        op_type = replay_ctx->record.op_type;
        fs_calc_block_hashcode(&replay_ctx->record.bs_key.block);

        thread_ctx = replay_ctx->thread_env.contexts +
            FS_BLOCK_HASH_CODE(replay_ctx->record.bs_key.block) %
            RECOVERY_THREADS_PER_DATA_GROUP;
        while (1) {
            if ((record=fc_queue_pop(&thread_ctx->queues.freelist)) != NULL) {
                break;
            }

            if (!SF_G_CONTINUE_FLAG) {
                return EINTR;
            }
        }

        *record = replay_ctx->record;
        fc_queue_push(&thread_ctx->queues.waiting, record);

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

static int do_replay_binlog(DataRecoveryContext *ctx)
{
    BinlogReplayContext *replay_ctx;
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];
    int result;

    replay_ctx = (BinlogReplayContext *)ctx->arg;
    data_recovery_get_subdir_name(ctx, RECOVERY_BINLOG_SUBDIR_NAME_REPLAY,
            subdir_name);
    if ((result=binlog_read_thread_init(&replay_ctx->rdthread_ctx, subdir_name,
                    NULL, NULL, BINLOG_BUFFER_SIZE)) != 0)
    {
        return result;
    }

    logInfo("file: "__FILE__", line: %d, "
            "dedup %s data ...", __LINE__, subdir_name);

    result = 0;
    while (SF_G_CONTINUE_FLAG) {
        if ((replay_ctx->r=binlog_read_thread_fetch_result(
                        &replay_ctx->rdthread_ctx)) == NULL)
        {
            result = EINTR;
            break;
        }

        logInfo("errno: %d, buffer length: %d", replay_ctx->r->err_no,
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
    return result;
}

static int int_rthread_context(ReplayThreadContext *thread_ctx,
        ReplicaBinlogRecord *records)
{
    int result;
    bool notify;
    ReplicaBinlogRecord *record;
    ReplicaBinlogRecord *end;

    if ((result=fc_queue_init(&thread_ctx->queues.freelist, (long)
                    (&((ReplicaBinlogRecord *)NULL)->next))) != 0)
    {
        return result;
    }

    if ((result=fc_queue_init(&thread_ctx->queues.waiting, (long)
                    (&((ReplicaBinlogRecord *)NULL)->next))) != 0)
    {
        return result;
    }

    end = records + RECOVERY_MAX_QUEUE_DEPTH;
    for (record=records; record<end; record++) {
        fc_queue_push_ex(&thread_ctx->queues.freelist, record, &notify);
    }

    return 0;
}

static int int_replay_context(BinlogReplayContext *replay_ctx)
{
    int bytes;
    int result;
    ReplayThreadContext *context;
    ReplayThreadContext *end;
    ReplicaBinlogRecord *records;

    if (RECOVERY_THREADS_PER_DATA_GROUP <= FIXED_THREAD_CONTEXT_COUNT) {
        replay_ctx->thread_env.contexts = replay_ctx->thread_env.fixed;
    } else {
        bytes = sizeof(ReplayThreadContext) * RECOVERY_THREADS_PER_DATA_GROUP;
        replay_ctx->thread_env.contexts = (ReplayThreadContext *)
            fc_malloc(bytes);
        if (replay_ctx->thread_env.contexts == NULL) {
            return ENOMEM;
        }
    }

    bytes = sizeof(ReplicaBinlogRecord) * (RECOVERY_THREADS_PER_DATA_GROUP *
            RECOVERY_MAX_QUEUE_DEPTH);
    replay_ctx->thread_env.records = (ReplicaBinlogRecord *)
        fc_malloc(bytes);
    if (replay_ctx->thread_env.records == NULL) {
        return ENOMEM;
    }

    result = 0;
    replay_ctx->continue_flag = true;
    end = replay_ctx->thread_env.contexts + RECOVERY_THREADS_PER_DATA_GROUP;
    for (context=replay_ctx->thread_env.contexts,
            records=replay_ctx->thread_env.records; context<end;
            context++, records += RECOVERY_MAX_QUEUE_DEPTH)
    {
        context->replay_ctx = replay_ctx;
        if ((result=int_rthread_context(context, records)) != 0) {
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
    ReplayThreadContext *end;

    end = replay_ctx->thread_env.contexts + RECOVERY_THREADS_PER_DATA_GROUP;
    for (context=replay_ctx->thread_env.contexts; context<end; context++) {
        fc_queue_destroy(&context->queues.freelist);
        fc_queue_destroy(&context->queues.waiting);
    }
    free(replay_ctx->thread_env.records);

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

    if ((result=int_replay_context(&replay_ctx)) != 0) {
        return result;
    }
    result = do_replay_binlog(ctx);
    destroy_replay_context(&replay_ctx);
    return result;
}
