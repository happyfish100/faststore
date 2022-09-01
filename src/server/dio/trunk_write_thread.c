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
#include "../binlog/binlog_types.h"
#include "../binlog/trunk_binlog.h"
#include "trunk_write_thread.h"

#define IO_THREAD_IOB_MAX     256
#define IO_THREAD_BYTES_MAX   (64 * 1024 * 1024)

typedef struct write_file_handle {
    int64_t trunk_id;
    int64_t offset;
    int fd;
} WriteFileHandle;

typedef struct trunk_write_thread_context {
    const FSStoragePathInfo *path_info;
    struct {
        short path;
        short thread;
    } indexes;
    struct fc_queue queue;
    struct fast_mblock_man mblock;
    WriteFileHandle file_handle;

    UniqSkiplistPair *sl_pair;
    struct {
        int count;
        struct iovec *iovs;
    };

    int iovec_bytes;
    iovec_array_t iovec_array;

    struct {
        int alloc;
        int count;
        int success; //write success count
        TrunkWriteIOBuffer **iobs;
    } iob_array;

    int64_t written_count;  //for fsync

} TrunkWriteThreadContext;

typedef struct trunk_write_thread_context_array {
    int count;
    TrunkWriteThreadContext *contexts;
} TrunkWriteThreadContextArray;

typedef struct trunk_write_path_context {
    TrunkWriteThreadContextArray writes;
} TrunkWritePathContext;

typedef struct trunk_write_path_contexts_array {
    int count;
    TrunkWritePathContext *paths;
} TrunkWritePathContextArray;

typedef struct trunk_write_context {
    TrunkWritePathContextArray path_ctx_array;
} TrunkWriteContext;

static TrunkWriteContext trunk_io_ctx = {{0, NULL}};

static void *trunk_write_thread_func(void *arg);

static int alloc_path_contexts()
{
    int bytes;

    trunk_io_ctx.path_ctx_array.count = STORAGE_CFG.max_store_path_index + 1;
    bytes = sizeof(TrunkWritePathContext) * trunk_io_ctx.path_ctx_array.count;
    trunk_io_ctx.path_ctx_array.paths = (TrunkWritePathContext *)
        fc_malloc(bytes);
    if (trunk_io_ctx.path_ctx_array.paths == NULL) {
        return ENOMEM;
    }
    memset(trunk_io_ctx.path_ctx_array.paths, 0, bytes);
    return 0;
}

static TrunkWriteThreadContext *alloc_thread_contexts(const int count)
{
    TrunkWriteThreadContext *contexts;
    int bytes;

    bytes = sizeof(TrunkWriteThreadContext) * count;
    contexts = (TrunkWriteThreadContext *)fc_malloc(bytes);
    if (contexts == NULL) {
        return NULL;
    }
    memset(contexts, 0, bytes);
    return contexts;
}

static int compare_by_version(const void *p1, const void *p2)
{
    return (int64_t)((TrunkWriteIOBuffer *)p1)->version -
        (int64_t)((TrunkWriteIOBuffer *)p2)->version;
}

static int init_write_context(TrunkWriteThreadContext *ctx)
{
    const int init_level_count = 2;
    const int max_level_count = 8;
    const int min_alloc_elements_once = 8;
    const int delay_free_seconds = 0;
    char *buff;
    int result;

    if ((result=fc_check_realloc_iovec_array(&ctx->
                    iovec_array, IOV_MAX)) != 0)
    {
        return result;
    }

    ctx->iob_array.alloc = FC_MIN(IOV_MAX, IO_THREAD_IOB_MAX);
    buff = (char *)fc_malloc(sizeof(UniqSkiplistPair) +
            sizeof(TrunkWriteIOBuffer *) * ctx->iob_array.alloc);
    ctx->sl_pair = (UniqSkiplistPair *)buff;
    ctx->iob_array.iobs = (TrunkWriteIOBuffer **)(ctx->sl_pair + 1);

    if ((result=uniq_skiplist_init_pair(ctx->sl_pair, init_level_count,
                    max_level_count, compare_by_version, NULL,
                    min_alloc_elements_once, delay_free_seconds)) != 0)
    {
        return result;
    }

    return 0;
}

static int init_thread_context(TrunkWriteThreadContext *ctx)
{
    int result;
    pthread_t tid;

    if ((result=fast_mblock_init_ex1(&ctx->mblock, "trunk_write_buffer",
                    sizeof(TrunkWriteIOBuffer), 4 * 1024, 0, NULL,
                    NULL, true)) != 0)
    {
        return result;
    }

    if ((result=fc_queue_init(&ctx->queue, (long)
                    (&((TrunkWriteIOBuffer *)NULL)->next))) != 0)
    {
        return result;
    }

    ctx->file_handle.trunk_id = 0;
    ctx->file_handle.fd = -1;
    if ((result=init_write_context(ctx)) != 0) {
        return result;
    }
    return fc_create_thread(&tid, trunk_write_thread_func,
            ctx, SF_G_THREAD_STACK_SIZE);
}

static int init_thread_contexts(TrunkWriteThreadContextArray *ctx_array,
        const int path_index)
{
    int result;
    TrunkWriteThreadContext *ctx;
    TrunkWriteThreadContext *end;
    
    end = ctx_array->contexts + ctx_array->count;
    for (ctx=ctx_array->contexts; ctx<end; ctx++) {
        ctx->indexes.path = path_index;
        if (ctx_array->count == 1) {
            ctx->indexes.thread = -1;
        } else {
            ctx->indexes.thread = ctx - ctx_array->contexts;
        }
        ctx->path_info = STORAGE_CFG.paths_by_index.paths[path_index];
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
    TrunkWriteThreadContext *thread_ctxs;
    TrunkWritePathContext *path_ctx;
    int result;

    end = parray->paths + parray->count;
    for (p=parray->paths; p<end; p++) {
        path_ctx = trunk_io_ctx.path_ctx_array.paths + p->store.index;
        if ((thread_ctxs=alloc_thread_contexts(p->
                        write_thread_count)) == NULL)
        {
            return ENOMEM;
        }

        path_ctx->writes.contexts = thread_ctxs;
        path_ctx->writes.count = p->write_thread_count;
        if ((result=init_thread_contexts(&path_ctx->writes,
                        p->store.index)) != 0)
        {
            return result;
        }
    }

    return 0;
}

int trunk_write_thread_init()
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

void trunk_write_thread_terminate()
{
}

static inline TrunkWriteIOBuffer *alloc_init_buffer(
        TrunkWriteThreadContext **thread_ctx, const int type,
        const int64_t version, const int path_index,
        const uint64_t hash_code, void *entry, void *data)
{
    TrunkWritePathContext *path_ctx;
    TrunkWriteIOBuffer *iob;

    path_ctx = trunk_io_ctx.path_ctx_array.paths + path_index;
    *thread_ctx = path_ctx->writes.contexts +
        hash_code % path_ctx->writes.count;
    iob = fast_mblock_alloc_object(&(*thread_ctx)->mblock);
    if (iob == NULL) {
        return NULL;
    }

    iob->type = type;
    iob->version = version;
    if (type == FS_IO_TYPE_CREATE_TRUNK || type == FS_IO_TYPE_DELETE_TRUNK) {
        iob->space = *((FSTrunkSpaceInfo *)entry);
    } else {
        iob->slice = (OBSliceEntry *)entry;
    }

    if (type == FS_IO_TYPE_WRITE_SLICE_BY_IOVEC) {
        iob->iovec_array = *((iovec_array_t *)data);
    } else {
        iob->buff = (char *)data;
    }

    return iob;
}

int trunk_write_thread_push(const int type, const int64_t version,
        const int path_index, const uint64_t hash_code, void *entry,
        void *data, trunk_write_io_notify_func notify_func, void *notify_arg)
{
    TrunkWriteThreadContext *thread_ctx;
    TrunkWriteIOBuffer *iob;

    if ((iob=alloc_init_buffer(&thread_ctx, type, version, path_index,
                    hash_code, entry, data)) == NULL)
    {
        return ENOMEM;
    }

    iob->notify.func = notify_func;
    iob->notify.arg = notify_arg;
    fc_queue_push(&thread_ctx->queue, iob);
    return 0;
}

int trunk_write_thread_push_cached_slice(FSSliceOpContext *op_ctx,
        const int type, const int64_t version,
        OBSliceEntry *slice, void *data)
{
    int result;
    int inc_alloc;
    TrunkWriteThreadContext *thread_ctx;
    TrunkWriteIOBuffer *iob;

    if ((result=ob_index_add_slice(slice, &op_ctx->info.sn, &inc_alloc,
                    op_ctx->info.source == BINLOG_SOURCE_RECLAIM)) != 0)
    {
        return result;
    }
    op_ctx->update.space_changed += inc_alloc;

    if ((iob=alloc_init_buffer(&thread_ctx, type, version,
                    slice->space.store->index,
                    slice->space.id_info.id,
                    slice, data)) == NULL)
    {
        return ENOMEM;
    }

    iob->binlog.source = op_ctx->info.source;
    iob->binlog.timestamp = op_ctx->update.timestamp;
    iob->binlog.data_version = op_ctx->info.data_version;
    iob->binlog.sn = op_ctx->info.sn;
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

static inline void close_write_fd(TrunkWriteThreadContext *ctx)
{
#ifdef OS_LINUX
    if (ctx->path_info->read_direct_io) {
        posix_fadvise(ctx->file_handle.fd, 0, 0, POSIX_FADV_DONTNEED);
    }
#endif

    close(ctx->file_handle.fd);
}

static inline void clear_write_fd(TrunkWriteThreadContext *ctx)
{
    if (ctx->file_handle.fd >= 0) {
        close_write_fd(ctx);
        ctx->file_handle.fd = -1;
        ctx->file_handle.trunk_id = 0;
    }
}

static int get_write_fd(TrunkWriteThreadContext *ctx,
        FSTrunkSpaceInfo *space, int *fd)
{
    char trunk_filename[PATH_MAX];
    int result;

    if (space->id_info.id == ctx->file_handle.trunk_id) {
        *fd = ctx->file_handle.fd;
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

    if (ctx->file_handle.fd >= 0) {
        close_write_fd(ctx);
    }

    ctx->file_handle.trunk_id = space->id_info.id;
    ctx->file_handle.fd = *fd;
    ctx->file_handle.offset = 0;
    return 0;
}

static int do_create_trunk(TrunkWriteThreadContext *ctx,
        TrunkWriteIOBuffer *iob)
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

static int do_delete_trunk(TrunkWriteThreadContext *ctx,
        TrunkWriteIOBuffer *iob)
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

static int write_iovec(TrunkWriteThreadContext *ctx, int fd,
        struct iovec *iovec, int iovcnt, int *remain_bytes)
{
    struct iovec *iov;
    struct iovec *end;
    int write_bytes;
    int iov_sum;
    int iov_remain;
    int result;

    iov = iovec;
    end = iovec + iovcnt;
    while (iovcnt > 0) {
        if ((write_bytes=writev(fd, iov, iovcnt)) < 0) {
            result = errno != 0 ? errno : EIO;
            if (result == EINTR) {
                continue;
            }

            return result;
        }

        *remain_bytes -= write_bytes;
        if (*remain_bytes == 0) {
            break;
        }

        iov_sum = 0;
        do {
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
        } while (iov < end);

        iovcnt = end - iov;
    }

    return 0;
}

static int do_write_slices(TrunkWriteThreadContext *ctx)
{
    char trunk_filename[PATH_MAX];
    TrunkWriteIOBuffer *first;
    struct iovec *iovec;
    int iovcnt;
    int fd;
    int remain_count;
    int remain_bytes;
    int result;

    first = ctx->iob_array.iobs[0];
    if ((result=get_write_fd(ctx, &first->slice->space, &fd)) != 0) {
        ctx->iob_array.success = 0;
        return result;
    }

    if (ctx->file_handle.offset != first->slice->space.offset) {
        if (lseek(fd, first->slice->space.offset, SEEK_SET) < 0) {
            get_trunk_filename(&first->slice->space, trunk_filename,
                    sizeof(trunk_filename));
            result = errno != 0 ? errno : EIO;
            logError("file: "__FILE__", line: %d, "
                    "lseek file: %s fail, offset: %"PRId64", "
                    "errno: %d, error info: %s", __LINE__, trunk_filename,
                    first->slice->space.offset, result, STRERROR(result));
            clear_write_fd(ctx);
            ctx->iob_array.success = 0;
            return result;
        }

        /*
        get_trunk_filename(&first->slice->space, trunk_filename,
                sizeof(trunk_filename));
        logInfo("trunk file: %s, lseek to offset: %"PRId64,
                trunk_filename, first->slice->space.offset);
                */
    }

    remain_bytes = ctx->iovec_bytes;
    if (ctx->iovec_array.count <= IOV_MAX) {
        result = write_iovec(ctx, fd, ctx->iovec_array.iovs,
                ctx->iovec_array.count, &remain_bytes);
    } else {
        iovec = ctx->iovec_array.iovs;
        remain_count = ctx->iovec_array.count;
        while (remain_count > 0) {
            iovcnt = (remain_count < IOV_MAX ? remain_count : IOV_MAX);
            if ((result=write_iovec(ctx, fd, iovec, iovcnt,
                            &remain_bytes)) != 0)
            {
                break;
            }

            remain_count -= iovcnt;
            iovec += iovcnt;
        }
    }

    ctx->written_count++;
    if (result == 0) {
        if (ctx->path_info->fsync_every_n_writes > 0 && ctx->written_count %
                ctx->path_info->fsync_every_n_writes == 0)
        {
            if (fsync(fd) != 0) {
                result = errno != 0 ? errno : EIO;
                logError("file: "__FILE__", line: %d, "
                        "sync to trunk file: %s fail, "
                        "errno: %d, error info: %s", __LINE__,
                        trunk_filename, result, STRERROR(result));
            }
        }
    }

    if (result != 0) {
        clear_write_fd(ctx);

        get_trunk_filename(&first->slice->space, trunk_filename,
                sizeof(trunk_filename));
        logError("file: "__FILE__", line: %d, "
                "write to trunk file: %s fail, offset: %"PRId64", "
                "errno: %d, error info: %s", __LINE__, trunk_filename,
                first->slice->space.offset + (ctx->iovec_bytes -
                    remain_bytes), result, STRERROR(result));
        ctx->file_handle.offset = -1;
        ctx->iob_array.success = 0;
        return result;
    }

    ctx->iob_array.success = ctx->iob_array.count;
    ctx->file_handle.offset = first->slice->space.offset +
        ctx->iovec_bytes;
    return 0;
}

static int batch_write(TrunkWriteThreadContext *ctx)
{
    int result;
    TrunkWriteIOBuffer **iob;
    TrunkWriteIOBuffer **end;

    result = do_write_slices(ctx);
    iob = ctx->iob_array.iobs;
    if (ctx->iob_array.success > 0) {
        end = ctx->iob_array.iobs + ctx->iob_array.success;
        for (; iob < end; iob++) {
            if ((*iob)->slice->type != OB_SLICE_TYPE_CACHE) {
                (*iob)->notify.func(*iob, 0);
            }

            fast_mblock_free_object(&ctx->mblock, *iob);
        }
    }

    if (result != 0) {
        end = ctx->iob_array.iobs + ctx->iob_array.count;
        for (; iob < end; iob++) {
            if ((*iob)->slice->type != OB_SLICE_TYPE_CACHE) {
                (*iob)->notify.func(*iob, result);
            }

            fast_mblock_free_object(&ctx->mblock, *iob);
        }

        logCrit("file: "__FILE__", line: %d, "
                "write slice fail, result: %d",
                __LINE__, result);
        sf_terminate_myself();
    }

    /*
    if (ctx->iob_array.count > 1) {
        logInfo("batch_write count: %d, success: %d, bytes: %d",
                ctx->iob_array.count, ctx->iob_array.success,
                ctx->iovec_bytes);
    }
    */

    ctx->iovec_bytes = 0;
    ctx->iovec_array.count = 0;
    ctx->iob_array.count = 0;
    return result;
}

static inline int pop_to_request_skiplist(TrunkWriteThreadContext *ctx,
        const bool blocked)
{
    TrunkWriteIOBuffer *head;
    int count;
    int result;

    if ((head=(TrunkWriteIOBuffer *)fc_queue_pop_all_ex(
                    &ctx->queue, blocked)) == NULL)
    {
        return 0;
    }

    count = 0;
    do {
        ++count;
        if ((result=uniq_skiplist_insert(ctx->sl_pair->
                        skiplist, head)) != 0)
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

static void deal_request_skiplist(TrunkWriteThreadContext *ctx)
{
    TrunkWriteIOBuffer *iob;
    TrunkWriteIOBuffer *last;
    struct iovec *current;
    int inc_count;
    int io_count;
    int result;

    io_count = 0;
    while (1) {
        iob = (TrunkWriteIOBuffer *)uniq_skiplist_get_first(
                ctx->sl_pair->skiplist);
        if (iob == NULL) {
            break;
        }

        switch (iob->type) {
            case FS_IO_TYPE_CREATE_TRUNK:
            case FS_IO_TYPE_DELETE_TRUNK:
                if (ctx->iovec_array.count > 0) {
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
            case FS_IO_TYPE_WRITE_SLICE_BY_BUFF:
            case FS_IO_TYPE_WRITE_SLICE_BY_IOVEC:
                if (ctx->iob_array.count > 0) {
                    last = ctx->iob_array.iobs[ctx->iob_array.count - 1];
                    if (!(IOB_IS_SUCCESSIVE(last, iob) &&
                                (ctx->iob_array.count < ctx->iob_array.alloc) &&
                                (ctx->iovec_array.count < IOV_MAX) &&
                                (ctx->iovec_bytes < IO_THREAD_BYTES_MAX)))
                    {
                        batch_write(ctx);
                        ++io_count;
                    }
                }

                inc_count = (iob->type == FS_IO_TYPE_WRITE_SLICE_BY_BUFF ?
                        1 : iob->iovec_array.count);
                if ((result=fc_check_realloc_iovec_array(&ctx->iovec_array,
                                ctx->iovec_array.count + inc_count)) != 0)
                {
                    return;
                }

                if (iob->type == FS_IO_TYPE_WRITE_SLICE_BY_BUFF) {
                    current = ctx->iovec_array.iovs +
                        ctx->iovec_array.count++;
                    current->iov_base = iob->buff;
                    current->iov_len = iob->slice->space.size;
                } else if (iob->iovec_array.count == 1) {  //fast path
                    current = ctx->iovec_array.iovs +
                        ctx->iovec_array.count++;
                    current->iov_base = iob->iovec_array.iovs[0].iov_base;
                    current->iov_len = iob->slice->space.size;
                } else {
                    struct iovec *dest;
                    struct iovec *src;
                    struct iovec *end;
                    int total;
                    int padding;

                    total = 0;
                    dest = ctx->iovec_array.iovs + ctx->iovec_array.count;
                    end = iob->iovec_array.iovs + iob->iovec_array.count;
                    for (src=iob->iovec_array.iovs; src<end; src++) {
                        *dest++ = *src;
                        total += src->iov_len;
                    }

                    padding = iob->slice->space.size - total;
                    if (padding > 0) {
                        (dest - 1)->iov_len += padding;
                    }

                    ctx->iovec_array.count += iob->iovec_array.count;
                }

                ctx->iob_array.iobs[ctx->iob_array.count++] = iob;
                ctx->iovec_bytes += iob->slice->space.size;
                break;
            default:
                logError("file: "__FILE__", line: %d, "
                        "invalid IO type: %d", __LINE__, iob->type);
                sf_terminate_myself();
                return;
        }

        if ((result=uniq_skiplist_delete(ctx->sl_pair->
                        skiplist, iob)) != 0)
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

    if (ctx->iovec_array.count > 0) {
        if (io_count == 0) {
            batch_write(ctx);
        }
    }
}

static void *trunk_write_thread_func(void *arg)
{
    TrunkWriteThreadContext *ctx;
    int count;

    ctx = (TrunkWriteThreadContext *)arg;
#ifdef OS_LINUX
    {
        int len;
        char thread_name[16];

        len = snprintf(thread_name, sizeof(thread_name),
                "dio-p%02d-w", ctx->indexes.path);
        if (ctx->indexes.thread >= 0) {
            snprintf(thread_name + len, sizeof(thread_name) - len,
                    "[%d]", ctx->indexes.thread);
        }
        prctl(PR_SET_NAME, thread_name);
    }
#endif

    while (SF_G_CONTINUE_FLAG) {
        count = pop_to_request_skiplist(ctx,
                ctx->iovec_array.count == 0);
        if (count < 0) {  //error
            continue;
        }

        if (count == 0) {
            if (ctx->iovec_array.count > 0) {
                batch_write(ctx);
            }
            continue;
        }

        deal_request_skiplist(ctx);
    }

    return NULL;
}
