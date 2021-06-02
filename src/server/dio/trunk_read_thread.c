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
#include <sys/param.h>
#include <sys/mount.h>
#include <sys/vfs.h>
#include "fastcommon/common_define.h"

#ifdef OS_LINUX
#include <sys/eventfd.h>
#include <sys/epoll.h>

#define DIO_MAX_EVENT_COUNT  2
#endif

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
    int block_size;
    struct fc_queue queue;
    struct fast_mblock_man mblock;
    TrunkFDCacheContext fd_cache;

#ifdef OS_LINUX
    struct {
        int count;
        int alloc;
        struct iocb **pp;
    } iocbs;

    struct {
        int doing_count;  //in progress count
        int max_event;
        struct io_event *events;
        io_context_t ctx;
    } aio;

#endif

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

static int init_thread_context(TrunkReadThreadContext *ctx,
        const FSStoragePathInfo *path_info)
{
    int result;
    struct statfs stbuf;
    pthread_t tid;

    if ((result=fast_mblock_init_ex1(&ctx->mblock, "trunk_read_buffer",
                    sizeof(TrunkReadIOBuffer), 1024, 0, NULL,
                    NULL, true)) != 0)
    {
        return result;
    }

    if ((result=fc_queue_init(&ctx->queue, (long)
                    (&((TrunkReadIOBuffer *)NULL)->next))) != 0)
    {
        return result;
    }


    if ((result=trunk_fd_cache_init(&ctx->fd_cache, STORAGE_CFG.
                    fd_cache_capacity_per_read_thread)) != 0)
    {
        return result;
    }

    if (statfs(path_info->store.path.str, &stbuf) != 0) {
        result = errno != 0 ? errno : ENOMEM;
        logError("file: "__FILE__", line: %d, "
                "statfs %s fail, errno: %d, error info: %s",
                __LINE__, path_info->store.path.str,
                result, STRERROR(result));
        return result;
    }

    ctx->block_size = stbuf.f_bsize;
    logInfo("block size======= %d", (int)stbuf.f_bsize);

#ifdef OS_LINUX

    ctx->iocbs.alloc = path_info->read_io_depth;
    ctx->iocbs.pp = (struct iocb **)fc_malloc(sizeof(
                struct iocb *) * ctx->iocbs.alloc);
    if (ctx->iocbs.pp == NULL) {
        return ENOMEM;
    }

    ctx->aio.max_event = path_info->read_io_depth;
    ctx->aio.events = (struct io_event *)fc_malloc(sizeof(
                struct io_event) * ctx->aio.max_event);
    if (ctx->aio.events == NULL) {
        return ENOMEM;
    }

    ctx->aio.ctx = 0;
    if (io_setup(ctx->aio.max_event, &ctx->aio.ctx) != 0) {
        result = errno != 0 ? errno : ENOMEM;
        logError("file: "__FILE__", line: %d, "
                "io_setup fail, errno: %d, error info: %s",
                __LINE__, result, STRERROR(result));
        return result;
    }
    ctx->aio.doing_count = 0;

#endif

    return fc_create_thread(&tid, trunk_read_thread_func,
            ctx, SF_G_THREAD_STACK_SIZE);
}

static int init_thread_contexts(TrunkReadThreadContextArray *ctx_array,
        const FSStoragePathInfo *path_info)
{
    int result;
    TrunkReadThreadContext *ctx;
    TrunkReadThreadContext *end;
    
    end = ctx_array->contexts + ctx_array->count;
    for (ctx=ctx_array->contexts; ctx<end; ctx++) {
        ctx->indexes.path = path_info->store.index;
        if (ctx_array->count == 1) {
            ctx->indexes.thread = -1;
        } else {
            ctx->indexes.thread = ctx - ctx_array->contexts;
        }
        if ((result=init_thread_context(ctx, path_info)) != 0) {
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
        if ((result=init_thread_contexts(&path_ctx->reads, p)) != 0) {
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
    iob->data = buff;
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
#ifdef OS_LINUX
    *fd = open(trunk_filename, O_RDONLY | O_DIRECT);
#else
    *fd = open(trunk_filename, O_RDONLY);
#endif
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

#ifdef OS_LINUX

static inline int prepare_read_slice(TrunkReadThreadContext *ctx,
        TrunkReadIOBuffer *iob)
{
#define MEM_ALIGN_FLOOR(x, align_size) ((x) & (~(align_size - 1)))
#define MEM_ALIGN_CEIL(x, align_size) \
    (((x) + (align_size - 1)) & (~(align_size - 1)))

    int64_t new_offset;
    int result;
    int fd;
    int new_alloc;
    char *new_buff;

    new_offset = MEM_ALIGN_FLOOR(iob->slice->
            space.offset, ctx->block_size);
    iob->aligned.length = MEM_ALIGN_CEIL(iob->slice->
            ssize.length, ctx->block_size);

    iob->aligned.offset = iob->slice->space.offset - new_offset;
    if (iob->aligned.offset > 0) {
        if (new_offset + iob->aligned.length < iob->slice->
                space.offset + iob->slice->ssize.length)
        {
            iob->aligned.length += ctx->block_size;
        }
    }

    if (iob->aligned.length > iob->aligned.alloc) {
        if (iob->aligned.alloc == 0) {
            new_alloc = 4096;
        } else {
            new_alloc = 2 * iob->aligned.alloc;
        }

        while (new_alloc < iob->aligned.length) {
            new_alloc *= 2;
        }

        if ((result=posix_memalign((void **)&new_buff,
                        ctx->block_size, new_alloc)) != 0)
        {
            logError("file: "__FILE__", line: %d, "
                    "posix_memalign %d bytes fail, "
                    "errno: %d, error info: %s", __LINE__,
                    new_alloc, result, STRERROR(result));
            return result;
        }

        if (iob->aligned.buff != NULL) {
            free(iob->aligned.buff);
        }

        iob->aligned.buff = new_buff;
        iob->aligned.alloc = new_alloc;
    }

    if ((result=get_read_fd(ctx, &iob->slice->space, &fd)) != 0) {
        return result;
    }

    io_prep_pread(&iob->iocb, fd, iob->aligned.buff,
            iob->aligned.length, new_offset);
    iob->iocb.data = iob;
    ctx->iocbs.pp[ctx->iocbs.count++] = &iob->iocb;
    return 0;
}

static int consume_queue(TrunkReadThreadContext *ctx)
{
    struct fc_queue_info qinfo;
    TrunkReadIOBuffer *iob;
    int target_count;
    int count;
    int remain;
    int n;
    int result;

    fc_queue_pop_to_queue_ex(&ctx->queue, &qinfo, ctx->aio.doing_count == 0);
    if (qinfo.head == NULL) {
        return 0;
    }

    target_count = ctx->aio.max_event - ctx->aio.doing_count;
    ctx->iocbs.count = 0;
    iob = (TrunkReadIOBuffer *)qinfo.head;
    do {
        if ((result=prepare_read_slice(ctx, iob)) != 0) {
            return result;
        }

        iob = iob->next;
    } while (iob != NULL && ctx->iocbs.count < target_count);

    count = 0;
    while ((remain=ctx->iocbs.count - count) > 0) {
        if ((n=io_submit(ctx->aio.ctx, remain,
                        ctx->iocbs.pp + count)) <= 0)
        {
            result = errno != 0 ? errno : ENOMEM;
            if (result == EINTR) {
                continue;
            }

            logError("file: "__FILE__", line: %d, "
                    "io_submiti return %d != %d, "
                    "errno: %d, error info: %s",
                    __LINE__, count, ctx->iocbs.count,
                    result, STRERROR(result));
            return result;
        }

        count += n;
    }

    ctx->aio.doing_count += ctx->iocbs.count;
    if (iob != NULL) {
        qinfo.head = iob;
        fc_queue_push_queue_to_head_silence(&ctx->queue, &qinfo);
    }
    return 0;
}

static int process_aio(TrunkReadThreadContext *ctx)
{
    struct timespec tms;
    TrunkReadIOBuffer *iob;
    struct io_event *event;
    struct io_event *end;
    char trunk_filename[PATH_MAX];
    bool full;
    int count;
    int result;

    full = ctx->aio.doing_count >= ctx->aio.max_event;
    while (1) {
        if (full) {
            tms.tv_sec = 1;
            tms.tv_nsec = 0;
        } else {
            tms.tv_sec = 0;
            if (ctx->aio.doing_count < 10) {
                tms.tv_nsec = ctx->aio.doing_count * 1000 * 1000;
            } else {
                tms.tv_nsec = 10 * 1000 * 1000;
            }
        }
        count = io_getevents(ctx->aio.ctx, 1, ctx->aio.
                max_event, ctx->aio.events, &tms);
        if (count > 0) {
            break;
        } else if (count == 0) {  //timeout
            if (full) {
                continue;
            } else {
                return 0;
            }
        } else {
            result = errno != 0 ? errno : ENOMEM;
            if (result == EINTR) {
                if (full) {
                    continue;
                } else {
                    return 0;
                }
            }

            logCrit("file: "__FILE__", line: %d, "
                    "io_getevents fail, errno: %d, error info: %s",
                    __LINE__, result, STRERROR(result));
            return result;
        }
    }

    end = ctx->aio.events + count;
    for (event=ctx->aio.events; event<end; event++) {
        iob = (TrunkReadIOBuffer *)event->data;
        if (event->res == iob->aligned.length) {
            memcpy(iob->data, iob->aligned.buff +
                    iob->aligned.offset,
                    iob->slice->ssize.length);
            result = 0;
        } else {
            trunk_fd_cache_delete(&ctx->fd_cache,
                    iob->slice->space.id_info.id);

            if ((int)event->res < 0) {
                result = -1 * event->res;
            } else {
                result = EBUSY;
            }
            get_trunk_filename(&iob->slice->space, trunk_filename,
                    sizeof(trunk_filename));
            logError("file: "__FILE__", line: %d, "
                    "read trunk file: %s fail, offset: %"PRId64", "
                    "expect length: %d, read return: %d, errno: %d, "
                    "error info: %s", __LINE__, trunk_filename,
                    iob->slice->space.offset - iob->aligned.offset,
                    iob->aligned.length, (int)event->res, result,
                    STRERROR(result));
        }

        if (iob->notify.func != NULL) {
            iob->notify.func(iob, result);
        }
        fast_mblock_free_object(&ctx->mblock, iob);
    }
    ctx->aio.doing_count -= count;

    return 0;
}

static inline int process(TrunkReadThreadContext *ctx)
{
    int result;

    if ((result=consume_queue(ctx)) != 0) {
        return result;
    }

    if (ctx->aio.doing_count <= 0) {
        return 0;
    }

    return process_aio(ctx);
}

#else

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
        if ((bytes=pread(fd, iob->data + data_len, remain,
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

#endif

static void *trunk_read_thread_func(void *arg)
{
    TrunkReadThreadContext *ctx;
#ifdef OS_LINUX
    int len;
    char thread_name[16];
#else
    int result;
    TrunkReadIOBuffer *iob;
#endif

    ctx = (TrunkReadThreadContext *)arg;

#ifdef OS_LINUX
    len = snprintf(thread_name, sizeof(thread_name),
            "dio-p%02d-r", ctx->indexes.path);
    if (ctx->indexes.thread >= 0) {
        snprintf(thread_name + len, sizeof(thread_name) - len,
                "[%d]", ctx->indexes.thread);
    }
    prctl(PR_SET_NAME, thread_name);

    while (SF_G_CONTINUE_FLAG) {
        if (process(ctx) != 0) {
            sf_terminate_myself();
            break;
        }
    }

#else

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
#endif

    return NULL;
}
