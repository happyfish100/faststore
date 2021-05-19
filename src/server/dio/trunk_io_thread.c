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
#include "trunk_io_thread.h"

#define IO_THREAD_ROLE_WRITER   'w'
#define IO_THREAD_ROLE_READER   'r'

#define IO_THREAD_IOV_MAX     256
#define IO_THREAD_BYTES_MAX   (64 * 1024 * 1024)

typedef struct write_file_handle {
    int64_t trunk_id;
    int64_t offset;
    int fd;
} WriteFileHandle;

typedef struct trunk_io_thread_context {
    struct {
        short path;
        short thread;
    } indexes;
    int role;
    struct fc_queue queue;
    struct fast_mblock_man mblock;
    union {
        TrunkFDCacheContext read_context; //for read
        WriteFileHandle write_handle;     //for write
    } fd_cache;

    struct {
        UniqSkiplistPair *sl_pair;
        struct {
            int alloc;
            int count;
            int bytes;   //total bytes of iov
            int success; //write success count
            struct iovec *iovs;
            TrunkIOBuffer **iobs;
        } iovb_array;
    } write;
} TrunkIOThreadContext;

typedef struct trunk_io_thread_context_array {
    int count;
    TrunkIOThreadContext *contexts;
} TrunkIOThreadContextArray;

typedef struct trunk_io_path_context {
    TrunkIOThreadContextArray writes;
    TrunkIOThreadContextArray reads;
} TrunkIOPathContext;

typedef struct trunk_io_path_contexts_array {
    int count;
    TrunkIOPathContext *paths;
} TrunkIOPathContextArray;

typedef struct trunk_io_context {
    TrunkIOPathContextArray path_ctx_array;
    UniqSkiplistFactory factory;
} TrunkIOContext;

static TrunkIOContext trunk_io_ctx = {{0, NULL}};

static void *trunk_io_thread_func(void *arg);

static int alloc_path_contexts()
{
    int bytes;

    trunk_io_ctx.path_ctx_array.count = STORAGE_CFG.max_store_path_index + 1;
    bytes = sizeof(TrunkIOPathContext) * trunk_io_ctx.path_ctx_array.count;
    trunk_io_ctx.path_ctx_array.paths = (TrunkIOPathContext *)fc_malloc(bytes);
    if (trunk_io_ctx.path_ctx_array.paths == NULL) {
        return ENOMEM;
    }
    memset(trunk_io_ctx.path_ctx_array.paths, 0, bytes);
    return 0;
}

static TrunkIOThreadContext *alloc_thread_contexts(const int count)
{
    TrunkIOThreadContext *contexts;
    int bytes;

    bytes = sizeof(TrunkIOThreadContext) * count;
    contexts = (TrunkIOThreadContext *)fc_malloc(bytes);
    if (contexts == NULL) {
        return NULL;
    }
    memset(contexts, 0, bytes);
    return contexts;
}

static int compare_by_version(const void *p1, const void *p2)
{
    return ((TrunkIOBuffer *)p1)->version - ((TrunkIOBuffer *)p2)->version;
}

static int init_write_context(TrunkIOThreadContext *ctx)
{
    const int init_level_count = 2;
    const int max_level_count = 8;
    const int min_alloc_elements_once = 8;
    const int delay_free_seconds = 0;
    char *buff;
    int result;

    ctx->write.iovb_array.alloc = FC_MIN(IOV_MAX, IO_THREAD_IOV_MAX);
    buff = (char *)fc_malloc(sizeof(UniqSkiplistPair) +
            (sizeof(struct iovec) + sizeof(TrunkIOBuffer *)) *
            ctx->write.iovb_array.alloc);
    if (buff == NULL) {
        return ENOMEM;
    }

    ctx->write.sl_pair = (UniqSkiplistPair *)buff;
    ctx->write.iovb_array.iovs = (struct iovec *)(ctx->write.sl_pair + 1);
    ctx->write.iovb_array.iobs = (TrunkIOBuffer **)
        (ctx->write.iovb_array.iovs + ctx->write.iovb_array.alloc);
    if ((result=uniq_skiplist_init_pair(ctx->write.sl_pair, init_level_count,
                    max_level_count, compare_by_version, NULL,
                    min_alloc_elements_once, delay_free_seconds)) != 0)
    {
        return result;
    }

    return 0;
}

static int init_thread_context(TrunkIOThreadContext *ctx)
{
    int result;
    pthread_t tid;

    if ((result=fast_mblock_init_ex1(&ctx->mblock, "trunk_io_buffer",
                    sizeof(TrunkIOBuffer), 1024, 0, NULL, NULL, true)) != 0)
    {
        return result;
    }

    if ((result=fc_queue_init(&ctx->queue, (long)
                    (&((TrunkIOBuffer *)NULL)->next))) != 0)
    {
        return result;
    }

    if (ctx->role == IO_THREAD_ROLE_WRITER) {
        ctx->fd_cache.write_handle.trunk_id = 0;
        ctx->fd_cache.write_handle.fd = -1;
        if ((result=init_write_context(ctx)) != 0) {
            return result;
        }
    } else {
        if ((result=trunk_fd_cache_init(&ctx->fd_cache.read_context,
                        STORAGE_CFG.fd_cache_capacity_per_read_thread)) != 0)
        {
            return result;
        }
    }

    return fc_create_thread(&tid, trunk_io_thread_func,
            ctx, SF_G_THREAD_STACK_SIZE);
}

static int init_thread_contexts(TrunkIOThreadContextArray *ctx_array,
        const int path_index, const int role)
{
    int result;
    TrunkIOThreadContext *ctx;
    TrunkIOThreadContext *end;
    
    end = ctx_array->contexts + ctx_array->count;
    for (ctx=ctx_array->contexts; ctx<end; ctx++) {
        ctx->indexes.path = path_index;
        if (ctx_array->count == 1) {
            ctx->indexes.thread = -1;
        } else {
            ctx->indexes.thread = ctx - ctx_array->contexts;
        }
        ctx->role = role;
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
    TrunkIOThreadContext *thread_ctxs;
    TrunkIOPathContext *path_ctx;
    int result;
    int thread_count;

    end = parray->paths + parray->count;
    for (p=parray->paths; p<end; p++) {
        path_ctx = trunk_io_ctx.path_ctx_array.paths + p->store.index;
        thread_count = p->write_thread_count + p->read_thread_count;
        if ((thread_ctxs=alloc_thread_contexts(thread_count)) == NULL)
        {
            return ENOMEM;
        }

        path_ctx->writes.contexts = thread_ctxs;
        path_ctx->writes.count = p->write_thread_count;
        if ((result=init_thread_contexts(&path_ctx->writes, p->store.index,
                        IO_THREAD_ROLE_WRITER)) != 0)
        {
            return result;
        }

        path_ctx->reads.contexts = thread_ctxs + p->write_thread_count;
        path_ctx->reads.count = p->read_thread_count;
        if ((result=init_thread_contexts(&path_ctx->reads, p->store.index,
                        IO_THREAD_ROLE_READER)) != 0)
        {
            return result;
        }
    }

    return 0;
}

int trunk_io_thread_init()
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

void trunk_io_thread_terminate()
{
}

int trunk_io_thread_push(const int type, const int64_t version,
        const int path_index, const uint64_t hash_code, void *entry,
        char *buff, trunk_io_notify_func notify_func, void *notify_arg)
{
    TrunkIOPathContext *path_ctx;
    TrunkIOThreadContext *thread_ctx;
    TrunkIOThreadContextArray *ctx_array;
    TrunkIOBuffer *iob;

    path_ctx = trunk_io_ctx.path_ctx_array.paths + path_index;
    if (type == FS_IO_TYPE_READ_SLICE) {
        ctx_array = &path_ctx->reads;
    } else {
        ctx_array = &path_ctx->writes;
    }

    thread_ctx = ctx_array->contexts + hash_code % ctx_array->count;
    iob = (TrunkIOBuffer *)fast_mblock_alloc_object(&thread_ctx->mblock);
    if (iob == NULL) {
        return ENOMEM;
    }

    iob->type = type;
    iob->version = version;
    if (type == FS_IO_TYPE_CREATE_TRUNK || type == FS_IO_TYPE_DELETE_TRUNK) {
        iob->space = *((FSTrunkSpaceInfo *)entry);
    } else {
        iob->slice = (OBSliceEntry *)entry;
    }

    if (buff != NULL) {
        iob->data.str = buff;
    } else {
        iob->data.str = NULL;
    }
    iob->data.len = 0;
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

static inline void clear_write_fd(TrunkIOThreadContext *ctx)
{
    if (ctx->fd_cache.write_handle.fd >= 0) {
        close(ctx->fd_cache.write_handle.fd);
        ctx->fd_cache.write_handle.fd = -1;
        ctx->fd_cache.write_handle.trunk_id = 0;
    }
}

static int get_write_fd(TrunkIOThreadContext *ctx,
        FSTrunkSpaceInfo *space, int *fd)
{
    char trunk_filename[PATH_MAX];
    int result;

    if (space->id_info.id == ctx->fd_cache.write_handle.trunk_id) {
        *fd = ctx->fd_cache.write_handle.fd;
        return 0;
    }

    get_trunk_filename(space, trunk_filename, sizeof(trunk_filename));
    *fd = open(trunk_filename, O_WRONLY, 0644);
    if (*fd < 0) {
        result = errno != 0 ? errno : EACCES;
        logError("file: "__FILE__", line: %d, "
                "open file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, trunk_filename, result, STRERROR(result));
        return result;
    }

    if (ctx->fd_cache.write_handle.fd >= 0) {
        close(ctx->fd_cache.write_handle.fd);
    }

    ctx->fd_cache.write_handle.trunk_id = space->id_info.id;
    ctx->fd_cache.write_handle.fd = *fd;
    ctx->fd_cache.write_handle.offset = 0;
    return 0;
}

static int get_read_fd(TrunkIOThreadContext *ctx,
        FSTrunkSpaceInfo *space, int *fd)
{
    char trunk_filename[PATH_MAX];
    int result;

    if ((*fd=trunk_fd_cache_get(&ctx->fd_cache.read_context,
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

    trunk_fd_cache_add(&ctx->fd_cache.read_context, space->id_info.id, *fd);
    return 0;
}

static int do_create_trunk(TrunkIOThreadContext *ctx, TrunkIOBuffer *iob)
{
    char trunk_filename[PATH_MAX];
    int fd;
    int result;

    get_trunk_filename(&iob->space, trunk_filename, sizeof(trunk_filename));
    fd = open(trunk_filename, O_WRONLY | O_CREAT, 0644);
    if (fd < 0) {
        if (errno == ENOENT) {
            char filepath[PATH_MAX];
            char *pend;
            int len;

            pend = strrchr(trunk_filename, '/');
            len = pend - trunk_filename;
            memcpy(filepath, trunk_filename, len);
            *(filepath + len) = '\0';
            if (mkdir(filepath, 0755) < 0) {
                result = errno != 0 ? errno : EACCES;
                logError("file: "__FILE__", line: %d, "
                        "mkdir \"%s\" fail, errno: %d, error info: %s",
                        __LINE__, filepath, result, STRERROR(result));
                return result;
            }
            fd = open(trunk_filename, O_WRONLY | O_CREAT, 0644);
        }
    }

    if (fd < 0) {
        result = errno != 0 ? errno : EACCES;
        logError("file: "__FILE__", line: %d, "
                "open file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, trunk_filename, result, STRERROR(result));
        return result;
    }

    if (fc_fallocate(fd, iob->space.size) == 0) {
        result = trunk_binlog_write(FS_IO_TYPE_CREATE_TRUNK,
                iob->space.store->index, &iob->space.id_info,
                iob->space.size);
    } else {
        result = errno != 0 ? errno : EACCES;
        logError("file: "__FILE__", line: %d, "
                "ftruncate file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, trunk_filename, result, STRERROR(result));
    }

    close(fd);
    return result;
}

static int do_delete_trunk(TrunkIOThreadContext *ctx, TrunkIOBuffer *iob)
{
    char trunk_filename[PATH_MAX];
    int result;

    get_trunk_filename(&iob->space, trunk_filename, sizeof(trunk_filename));
    if (unlink(trunk_filename) == 0) {
        result = trunk_binlog_write(FS_IO_TYPE_DELETE_TRUNK,
                iob->space.store->index, &iob->space.id_info,
                iob->space.size);
    } else {
        result = errno != 0 ? errno : EACCES;
        logError("file: "__FILE__", line: %d, "
                "trunk_filename file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, trunk_filename, result, STRERROR(result));
    }

    return result;
}

static int do_write_slices(TrunkIOThreadContext *ctx)
{
    char trunk_filename[PATH_MAX];
    TrunkIOBuffer *first;
    struct iovec *iov;
    struct iovec *end;
    int fd;
    int remain;
    int write_bytes;
    int iovcnt;
    int iov_sum;
    int iov_remain;
    int result;

    first = ctx->write.iovb_array.iobs[0];
    if ((result=get_write_fd(ctx, &first->slice->space, &fd)) != 0) {
        ctx->write.iovb_array.success = 0;
        return result;
    }

    if (ctx->fd_cache.write_handle.offset != first->slice->space.offset) {
        if (lseek(fd, first->slice->space.offset, SEEK_SET) < 0) {
            get_trunk_filename(&first->slice->space, trunk_filename,
                    sizeof(trunk_filename));
            result = errno != 0 ? errno : EIO;
            logError("file: "__FILE__", line: %d, "
                    "lseek file: %s fail, offset: %"PRId64", "
                    "errno: %d, error info: %s", __LINE__, trunk_filename,
                    first->slice->space.offset, result, STRERROR(result));
            clear_write_fd(ctx);
            ctx->write.iovb_array.success = 0;
            return result;
        }

        /*
        get_trunk_filename(&first->slice->space, trunk_filename,
                sizeof(trunk_filename));
        logInfo("trunk file: %s, lseek to offset: %"PRId64,
                trunk_filename, first->slice->space.offset);
                */
    }

    remain = ctx->write.iovb_array.bytes;
    iov = ctx->write.iovb_array.iovs;
    iovcnt = ctx->write.iovb_array.count;
    end = iov + iovcnt;
    while (iovcnt > 0) {
        if ((write_bytes=writev(fd, iov, iovcnt)) < 0) {
            result = errno != 0 ? errno : EIO;
            if (result == EINTR) {
                continue;
            }

            clear_write_fd(ctx);

            get_trunk_filename(&first->slice->space, trunk_filename,
                    sizeof(trunk_filename));
            logError("file: "__FILE__", line: %d, "
                    "write to trunk file: %s fail, offset: %"PRId64", "
                    "errno: %d, error info: %s", __LINE__, trunk_filename,
                    first->slice->space.offset + (ctx->write.iovb_array.
                        bytes - remain), result, STRERROR(result));
            ctx->write.iovb_array.success = iov - ctx->write.iovb_array.iovs;
            return result;
        }

        remain -= write_bytes;
        if (remain == 0) {
            break;
        }

        iov_sum = 0;
        while (1) {
            iov_sum += iov->iov_len;
            iov_remain = iov_sum - write_bytes;
            if (iov_remain == 0) {
                iov++;
                break;
            } else if (iov_remain > 0) {
                iov->iov_base += (iov->iov_len - iov_remain);
                iov->iov_len = iov_remain;
                break;
            }

            iov++;
        }

        iovcnt = end - iov;
    }

    ctx->write.iovb_array.success = ctx->write.iovb_array.count;
    ctx->fd_cache.write_handle.offset = first->slice->space.offset +
        ctx->write.iovb_array.bytes;
    return 0;
}

static int batch_write(TrunkIOThreadContext *ctx)
{
    int result;
    TrunkIOBuffer **iob;
    TrunkIOBuffer **end;

    result = do_write_slices(ctx);
    iob = ctx->write.iovb_array.iobs;
    if (ctx->write.iovb_array.success > 0) {
        end = ctx->write.iovb_array.iobs + ctx->write.iovb_array.success;
        for (; iob < end; iob++) {
            if ((*iob)->notify.func != NULL) {
                (*iob)->notify.func(*iob, 0);
            }

            fast_mblock_free_object(&ctx->mblock, *iob);
        }
    }

    if (result != 0) {
        end = ctx->write.iovb_array.iobs + ctx->write.iovb_array.count;
        for (; iob < end; iob++) {
            if ((*iob)->notify.func != NULL) {
                (*iob)->notify.func(*iob, result);
            }
        }
        fast_mblock_free_object(&ctx->mblock, *iob);
    }

    /*
    if (ctx->write.iovb_array.count > 1) {
        logInfo("batch_write count: %d, success: %d, bytes: %d",
                ctx->write.iovb_array.count, ctx->write.iovb_array.success,
                ctx->write.iovb_array.bytes);
    }
    */

    ctx->write.iovb_array.count = 0;
    ctx->write.iovb_array.bytes = 0;
    return result;
}

static int do_read_slice(TrunkIOThreadContext *ctx, TrunkIOBuffer *iob)
{
    int fd;
    int remain;
    int bytes;
    int result;

    if ((result=get_read_fd(ctx, &iob->slice->space, &fd)) != 0) {
        return result;
    }

    remain = iob->slice->ssize.length;
    while (remain > 0) {
        if ((bytes=pread(fd, iob->data.str + iob->data.len, remain,
                        iob->slice->space.offset + iob->data.len)) < 0)
        {
            char trunk_filename[PATH_MAX];

            result = errno != 0 ? errno : EIO;
            if (result == EINTR) {
                continue;
            }

            trunk_fd_cache_delete(&ctx->fd_cache.read_context,
                    iob->slice->space.id_info.id);

            get_trunk_filename(&iob->slice->space, trunk_filename,
                    sizeof(trunk_filename));
            logError("file: "__FILE__", line: %d, "
                    "read trunk file: %s fail, offset: %"PRId64", "
                    "errno: %d, error info: %s", __LINE__, trunk_filename,
                    iob->slice->space.offset + iob->data.len,
                    result, STRERROR(result));
            return result;
        }

        iob->data.len += bytes;
        remain -= bytes;
    }

    return 0;
}

static inline int pop_to_request_skiplist(TrunkIOThreadContext *ctx,
        const bool blocked)
{
    TrunkIOBuffer *head;
    int count;
    int result;

    if ((head=(TrunkIOBuffer *)fc_queue_pop_all_ex(
                    &ctx->queue, blocked)) == NULL)
    {
        return 0;
    }

    count = 0;
    do {
        ++count;
        if ((result=uniq_skiplist_insert(ctx->write.
                        sl_pair->skiplist, head)) != 0)
        {
            logCrit("file: "__FILE__", line: %d, "
                    "uniq_skiplist_insert fail, result: %d",
                    __LINE__, result);
            sf_terminate_myself();
            return -1;
        }

        head = head->next;
    } while (head != NULL);

    return count;
}

#define IOB_IS_SUCCESSIVE(last, current)  \
    ((current->slice->space.id_info.id == last->slice->space.id_info.id) && \
     (last->slice->space.offset + last->slice->space.size ==  \
      current->slice->space.offset))

static void deal_request_skiplist(TrunkIOThreadContext *ctx)
{
    TrunkIOBuffer *iob;
    TrunkIOBuffer *last;
    struct iovec *current;
    int io_count;
    int result;

    io_count = 0;
    while (1) {
        iob = (TrunkIOBuffer *)uniq_skiplist_get_first(
                ctx->write.sl_pair->skiplist);
        if (iob == NULL) {
            break;
        }

        switch (iob->type) {
            case FS_IO_TYPE_CREATE_TRUNK:
            case FS_IO_TYPE_DELETE_TRUNK:
                if (ctx->write.iovb_array.count > 0) {
                    batch_write(ctx);
                    ++io_count;
                }

                if (iob->type == FS_IO_TYPE_CREATE_TRUNK) {
                    result = do_create_trunk(ctx, iob);
                } else {
                    result = do_delete_trunk(ctx, iob);
                }

                if (iob->notify.func != NULL) {
                    iob->notify.func(iob, result);
                }
                ++io_count;
                break;
            case FS_IO_TYPE_WRITE_SLICE:
                if (ctx->write.iovb_array.count > 0) {
                    last = ctx->write.iovb_array.iobs[
                        ctx->write.iovb_array.count - 1];
                    if (!(IOB_IS_SUCCESSIVE(last, iob) &&
                                (ctx->write.iovb_array.count <
                                 ctx->write.iovb_array.alloc) &&
                                (ctx->write.iovb_array.bytes <
                                 IO_THREAD_BYTES_MAX)))
                    {
                        batch_write(ctx);
                        ++io_count;
                    }
                }

                current = ctx->write.iovb_array.iovs +
                    ctx->write.iovb_array.count;
                current->iov_base = iob->data.str;
                current->iov_len = iob->slice->space.size;
                ctx->write.iovb_array.iobs[ctx->write.iovb_array.count] = iob;
                ctx->write.iovb_array.bytes += iob->slice->space.size;
                ctx->write.iovb_array.count++;
                break;
            default:
                logError("file: "__FILE__", line: %d, "
                        "invalid IO type: %d", __LINE__, iob->type);
                sf_terminate_myself();
                return;
        }

        if ((result=uniq_skiplist_delete(ctx->write.
                        sl_pair->skiplist, iob)) != 0)
        {
            logCrit("file: "__FILE__", line: %d, "
                    "uniq_skiplist_delete fail, result: %d",
                    __LINE__, result);
            sf_terminate_myself();
            return;
        }

        if (iob->type == FS_IO_TYPE_CREATE_TRUNK ||
                iob->type == FS_IO_TYPE_DELETE_TRUNK)
        {
            fast_mblock_free_object(&ctx->mblock, iob);
        }
    }

    if (ctx->write.iovb_array.count > 0) {
        if (io_count == 0) {
            batch_write(ctx);
        }
    }
}

static void trunk_io_writer_run(TrunkIOThreadContext *ctx)
{
    int count;
    while (SF_G_CONTINUE_FLAG) {
        count = pop_to_request_skiplist(ctx,
                ctx->write.iovb_array.count == 0);
        if (count < 0) {  //error
            continue;
        }

        if (count == 0) {
            if (ctx->write.iovb_array.count > 0) {
                batch_write(ctx);
            }
            continue;
        }

        deal_request_skiplist(ctx);
    }
}

static void trunk_io_reader_run(TrunkIOThreadContext *ctx)
{
    TrunkIOBuffer *iob;
    int result;

    while (SF_G_CONTINUE_FLAG) {
        if ((iob=(TrunkIOBuffer *)fc_queue_pop(&ctx->queue)) == NULL) {
            continue;
        }

        if (iob->type != FS_IO_TYPE_READ_SLICE) {
            logError("file: "__FILE__", line: %d, "
                    "invalid IO type: %d", __LINE__, iob->type);
            sf_terminate_myself();
            break;
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
}

static void *trunk_io_thread_func(void *arg)
{
    TrunkIOThreadContext *ctx;

    ctx = (TrunkIOThreadContext *)arg;
#ifdef OS_LINUX
    {
        int len;
        char thread_name[16];

        len = snprintf(thread_name, sizeof(thread_name),
                "dio-p%02d-%c", ctx->indexes.path, ctx->role);
        if (ctx->indexes.thread >= 0) {
            snprintf(thread_name + len, sizeof(thread_name) - len,
                    "[%d]", ctx->indexes.thread);
        }
        prctl(PR_SET_NAME, thread_name);
    }
#endif

    if (ctx->role == IO_THREAD_ROLE_WRITER) {
        trunk_io_writer_run(ctx);
    } else {
        trunk_io_reader_run(ctx);
    }

    return NULL;
}
