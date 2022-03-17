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

#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "sf/sf_func.h"
#include "../../common/fs_func.h"
#include "../server_global.h"
#include "../shared_thread_pool.h"
#include "binlog_spliter.h"

typedef struct data_read_thread_info {
    ServerBinlogReaderArray rda;
    struct data_read_context *ctx;
} DataReadThreadInfo;

typedef struct data_read_thread_array {
    DataReadThreadInfo *threads;
    int count;
} DataReadThreadArray;

typedef struct data_read_context {
    volatile int running_threads;
    DataReadThreadArray thread_array;
} DataReadContext;

static inline int read_and_dispatch(ServerBinlogReader *reader)
{
    return 0;
}

static void *data_read_thread_run(DataReadThreadInfo *thread)
{
    ServerBinlogReader *reader;
    ServerBinlogReader *end;

    end = thread->rda.readers + thread->rda.count;
    for (reader=thread->rda.readers; reader<end; reader++) {
        if (read_and_dispatch(reader) != 0) {
            sf_terminate_myself();
        }
    }
    __sync_sub_and_fetch(&thread->ctx->running_threads, 1);
    return NULL;
}

int binlog_spliter_do(ServerBinlogReaderArray *rda,
        const int read_threads, const int split_count)
{
    int result;
    int thread_count;
    int bytes;
    int avg_count;
    int remain_count;
    pthread_t tid;
    DataReadContext ctx;
    DataReadThreadInfo *thread;
    DataReadThreadInfo *end;
    ServerBinlogReader *reader;

    if (rda->count == 1) {
        return read_and_dispatch(rda->readers);
    }

    if (rda->count < read_threads) {
        thread_count = rda->count;
    } else {
        thread_count = read_threads;
    }
    avg_count = rda->count / thread_count;
    remain_count = rda->count - (avg_count * thread_count);

    bytes = sizeof(DataReadThreadInfo) * thread_count;
    ctx.thread_array.threads = fc_malloc(bytes);
    if (ctx.thread_array.threads == NULL) {
        return ENOMEM;
    }

    reader = rda->readers;
    ctx.running_threads = thread_count;
    end = ctx.thread_array.threads + thread_count;
    for (thread=ctx.thread_array.threads; thread<end; thread++) {
        thread->rda.readers = reader;
        if (remain_count > 0) {
            thread->rda.count = avg_count + 1;
            --remain_count;
        } else {
            thread->rda.count = avg_count;
        }
        reader += thread->rda.count;
        thread->ctx = &ctx;
        if ((result=fc_create_thread(&tid, (void *(*)(void *))
                        data_read_thread_run, thread,
                        SF_G_THREAD_STACK_SIZE)) != 0)
        {
            return result;
        }
    }

    do {
        fc_sleep_ms(10);
        if (__sync_add_and_fetch(&ctx.running_threads, 0) == 0) {
            break;
        }
    } while (SF_G_CONTINUE_FLAG);

    free(ctx.thread_array.threads);
    return (SF_G_CONTINUE_FLAG ? 0 : EINTR);
}
