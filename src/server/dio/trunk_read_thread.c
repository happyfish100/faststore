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

#include <limits.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fast_mblock.h"
#include "fastcommon/uniq_skiplist.h"
#include "sf/sf_global.h"
#include "sf/sf_func.h"
#include "../server_global.h"
#include "../binlog/trunk_binlog.h"
#include "trunk_fd_cache.h"
#include "trunk_read_thread.h"

typedef struct trunk_read_thread_context {
    struct {
        short path;
        short thread;
    } indexes;
    struct fc_queue queue;
    struct fast_mblock_man mblock;
    TrunkFDCacheContext fd_cache;
} TrunkReadThreadContext;

typedef struct trunk_read_thread_context_array {
    int count;
    TrunkReadThreadContext *contexts;
} TrunkReadThreadContextArray;

typedef struct trunk_io_path_context {
    TrunkReadThreadContextArray reads;
} TrunkReadPathContext;

typedef struct trunk_io_path_contexts_array {
    int count;
    TrunkReadPathContext *paths;
} TrunkReadPathContextArray;

typedef struct trunk_io_context {
    TrunkReadPathContextArray path_ctx_array;
    UniqSkiplistFactory factory;
} TrunkReadContext;

static TrunkReadContext trunk_io_ctx = {{0, NULL}};

static void *trunk_read_thread_func(void *arg);

static int alloc_path_contexts()
{
    int bytes;

    trunk_io_ctx.path_ctx_array.count = STORAGE_CFG.max_store_path_index + 1;
    bytes = sizeof(TrunkReadPathContext) * trunk_io_ctx.path_ctx_array.count;
    trunk_io_ctx.path_ctx_array.paths = (TrunkReadPathContext *)fc_malloc(bytes);
    if (trunk_io_ctx.path_ctx_array.paths == NULL) {
        return ENOMEM;
    }
    memset(trunk_io_ctx.path_ctx_array.paths, 0, bytes);
    return 0;
}

static TrunkReadThreadContext *alloc_thread_contexts(const int count)
{
    TrunkReadThreadContext *contexts;
    int bytes;

    bytes = sizeof(TrunkReadThreadContext) * count;
    contexts = (TrunkReadThreadContext *)fc_malloc(bytes);
    if (contexts == NULL) {
        return NULL;
    }
    memset(contexts, 0, bytes);
    return contexts;
}

static int init_thread_context(TrunkReadThreadContext *ctx)
{
    int result;
    pthread_t tid;

    if ((result=fast_mblock_init_ex1(&ctx->mblock, "trunk_read_buffer",
                    sizeof(TrunkReadIOBuffer), 1024, 0, NULL, NULL, true)) != 0)
    {
        return result;
    }

    if ((result=fc_queue_init(&ctx->queue, (long)
                    (&((TrunkReadIOBuffer *)NULL)->next))) != 0)
    {
        return result;
    }


    if ((result=trunk_fd_cache_init(&ctx->fd_cache,
                    STORAGE_CFG.fd_cache_capacity_per_read_thread)) != 0)
    {
        return result;
    }

    return fc_create_thread(&tid, trunk_read_thread_func,
            ctx, SF_G_THREAD_STACK_SIZE);
}

static int init_thread_contexts(TrunkReadThreadContextArray *ctx_array,
        const int path_index)
{
    int result;
    TrunkReadThreadContext *ctx;
    TrunkReadThreadContext *end;
    
    end = ctx_array->contexts + ctx_array->count;
    for (ctx=ctx_array->contexts; ctx<end; ctx++) {
        ctx->indexes.path = path_index;
        if (ctx_array->count == 1) {
            ctx->indexes.thread = -1;
        } else {
            ctx->indexes.thread = ctx - ctx_array->contexts;
        }
        if ((result=init_thread_context(ctx)) != 0) {
            return result;
        }
    }

    return 0;
}

static int init_path_contexts(FSStoragePathArray *parray)
{
    FSStoragePathInfo *p;
    FSStoragePathInfo *end;
    TrunkReadThreadContext *thread_ctxs;
    TrunkReadPathContext *path_ctx;
    int result;

    end = parray->paths + parray->count;
    for (p=parray->paths; p<end; p++) {
        path_ctx = trunk_io_ctx.path_ctx_array.paths + p->store.index;
        if ((thread_ctxs=alloc_thread_contexts(
                        p->read_thread_count)) == NULL)
        {
            return ENOMEM;
        }

        path_ctx->reads.contexts = thread_ctxs;
        path_ctx->reads.count = p->read_thread_count;
        if ((result=init_thread_contexts(&path_ctx->reads,
                        p->store.index)) != 0)
        {
            return result;
        }
    }

    return 0;
}

int trunk_read_thread_init()
{
    int result;

    if ((result=alloc_path_contexts()) != 0) {
        return result;
    }

    if ((result=init_path_contexts(&STORAGE_CFG.write_cache)) != 0) {
        return result;
    }
    if ((result=init_path_contexts(&STORAGE_CFG.store_path)) != 0) {
        return result;
    }

    /*
       logInfo("trunk_io_ctx.path_ctx_array.count: %d",
               trunk_io_ctx.path_ctx_array.count);
     */
    return 0;
}

void trunk_read_thread_terminate()
{
}

int trunk_read_thread_push(OBSliceEntry *slice, char *buff,
            trunk_read_io_notify_func notify_func, void *notify_arg)
{
    TrunkReadPathContext *path_ctx;
    TrunkReadThreadContext *thread_ctx;
    TrunkReadIOBuffer *iob;

    path_ctx = trunk_io_ctx.path_ctx_array.paths +
        slice->space.store->index;
    thread_ctx = path_ctx->reads.contexts + slice->
        space.id_info.id % path_ctx->reads.count;
    iob = (TrunkReadIOBuffer *)fast_mblock_alloc_object(&thread_ctx->mblock);
    if (iob == NULL) {
        return ENOMEM;
    }

    iob->slice = slice;
    iob->buff = buff;
    iob->notify.func = notify_func;
    iob->notify.arg = notify_arg;
    fc_queue_push(&thread_ctx->queue, iob);
    return 0;
}

static inline void get_trunk_filename(FSTrunkSpaceInfo *space,
        char *trunk_filename, const int size)
{
    snprintf(trunk_filename, size, "%s/%04"PRId64"/%06"PRId64,
            space->store->path.str, space->id_info.subdir,
            space->id_info.id);
}

static int get_read_fd(TrunkReadThreadContext *ctx,
        FSTrunkSpaceInfo *space, int *fd)
{
    char trunk_filename[PATH_MAX];
    int result;

    if ((*fd=trunk_fd_cache_get(&ctx->fd_cache,
                    space->id_info.id)) >= 0)
    {
        return 0;
    }

    get_trunk_filename(space, trunk_filename, sizeof(trunk_filename));
    *fd = open(trunk_filename, O_RDONLY);
    if (*fd < 0) {
        result = errno != 0 ? errno : EACCES;
        logError("file: "__FILE__", line: %d, "
                "open file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, trunk_filename, result, STRERROR(result));
        return result;
    }

    trunk_fd_cache_add(&ctx->fd_cache, space->id_info.id, *fd);
    return 0;
}

static int do_read_slice(TrunkReadThreadContext *ctx, TrunkReadIOBuffer *iob)
{
    int fd;
    int remain;
    int bytes;
    int data_len;
    int result;

    if ((result=get_read_fd(ctx, &iob->slice->space, &fd)) != 0) {
        return result;
    }

    data_len = 0;
    remain = iob->slice->ssize.length;
    while (remain > 0) {
        if ((bytes=pread(fd, iob->buff + data_len, remain,
                        iob->slice->space.offset + data_len)) <= 0)
        {
            char trunk_filename[PATH_MAX];

            result = errno != 0 ? errno : EIO;
            if (result == EINTR) {
                continue;
            }

            trunk_fd_cache_delete(&ctx->fd_cache,
                    iob->slice->space.id_info.id);

            get_trunk_filename(&iob->slice->space, trunk_filename,
                    sizeof(trunk_filename));
            logError("file: "__FILE__", line: %d, "
                    "read trunk file: %s fail, offset: %"PRId64", "
                    "errno: %d, error info: %s", __LINE__, trunk_filename,
                    iob->slice->space.offset + data_len,
                    result, STRERROR(result));
            return result;
        }

        data_len += bytes;
        remain -= bytes;
    }

    return 0;
}

static void *trunk_read_thread_func(void *arg)
{
    TrunkReadThreadContext *ctx;
    TrunkReadIOBuffer *iob;
    int result;

    ctx = (TrunkReadThreadContext *)arg;
#ifdef OS_LINUX
    {
        int len;
        char thread_name[16];

        len = snprintf(thread_name, sizeof(thread_name),
                "dio-p%02d-r", ctx->indexes.path);
        if (ctx->indexes.thread >= 0) {
            snprintf(thread_name + len, sizeof(thread_name) - len,
                    "[%d]", ctx->indexes.thread);
        }
        prctl(PR_SET_NAME, thread_name);
    }
#endif

    while (SF_G_CONTINUE_FLAG) {
        if ((iob=(TrunkReadIOBuffer *)fc_queue_pop(&ctx->queue)) == NULL) {
            continue;
        }

        if ((result=do_read_slice(ctx, iob)) != 0) {
            logError("file: "__FILE__", line: %d, "
                    "slice read fail, result: %d",
                    __LINE__, result);
        }

        if (iob->notify.func != NULL) {
            iob->notify.func(iob, result);
        }
        fast_mblock_free_object(&ctx->mblock, iob);
    }

    return NULL;
}
