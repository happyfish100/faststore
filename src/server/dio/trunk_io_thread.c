#include <limits.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fast_mblock.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "../binlog/trunk_binlog.h"
#include "trunk_fd_cache.h"
#include "trunk_io_thread.h"

#define IO_THREAD_ROLE_WRITER   'W'
#define IO_THREAD_ROLE_READER   'R'

typedef struct trunk_io_thread_context {
    TrunkIOBuffer *head;
    TrunkIOBuffer *tail;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    struct fast_mblock_man mblock;
    union {
        TrunkFDCacheContext context;
        TrunkIdFDPair pair;
    } fd_cache;
    int role;
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

static TrunkIOPathContextArray io_path_context_array = {0, NULL};

static void *trunk_io_thread_func(void *arg);

static int alloc_path_contexts()
{
    int bytes;

    io_path_context_array.count = STORAGE_CFG.max_store_path_index + 1;
    bytes = sizeof(TrunkIOPathContext) * io_path_context_array.count;
    io_path_context_array.paths = (TrunkIOPathContext *)malloc(bytes);
    if (io_path_context_array.paths == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(io_path_context_array.paths, 0, bytes);
    return 0;
}

static TrunkIOThreadContext *alloc_thread_contexts(const int count)
{
    TrunkIOThreadContext *contexts;
    int bytes;

    bytes = sizeof(TrunkIOThreadContext) * count;
    contexts = (TrunkIOThreadContext *)malloc(bytes);
    if (contexts == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return NULL;
    }
    memset(contexts, 0, bytes);
    return contexts;
}

static int init_thread_context(TrunkIOThreadContext *ctx)
{
    int result;
    pthread_t tid;

    if ((result=init_pthread_lock(&ctx->lock)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "init_pthread_lock fail, errno: %d, error info: %s",
                __LINE__, result, STRERROR(result));
        return result;
    }

    if ((result=pthread_cond_init(&ctx->cond, NULL)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "pthread_cond_init fail, "
                "errno: %d, error info: %s",
                __LINE__, result, STRERROR(result));
        return result;
    }

    if ((result=fast_mblock_init_ex2(&ctx->mblock, "trunk_io_buffer",
                    sizeof(TrunkIOBuffer), 1024, NULL, NULL, false,
                    NULL, NULL, NULL)) != 0)
    {
        return result;
    }

    if (ctx->role == IO_THREAD_ROLE_WRITER) {
        ctx->fd_cache.pair.trunk_id = 0;
        ctx->fd_cache.pair.fd = -1;
    } else {
        if ((result=trunk_fd_cache_init(&ctx->fd_cache.context,
                        STORAGE_CFG.fd_cache_capacity_per_read_thread)) != 0)
        {
            return result;
        }
    }

    return fc_create_thread(&tid, trunk_io_thread_func,
            ctx, SF_G_THREAD_STACK_SIZE);
}

static int init_thread_contexts(TrunkIOThreadContextArray *ctx_array,
        const int role)
{
    int result;
    TrunkIOThreadContext *ctx;
    TrunkIOThreadContext *end;
    
    end = ctx_array->contexts + ctx_array->count;
    for (ctx=ctx_array->contexts; ctx<end; ctx++) {
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
        path_ctx = io_path_context_array.paths + p->store.index;
        thread_count = p->write_thread_count + p->read_thread_count;
        if ((thread_ctxs=alloc_thread_contexts(thread_count)) == NULL)
        {
            return ENOMEM;
        }

        path_ctx->writes.contexts = thread_ctxs;
        path_ctx->writes.count = p->write_thread_count;
        if ((result=init_thread_contexts(&path_ctx->writes,
                        IO_THREAD_ROLE_WRITER)) != 0)
        {
            return result;
        }

        path_ctx->reads.contexts = thread_ctxs + p->write_thread_count;
        path_ctx->reads.count = p->read_thread_count;
        if ((result=init_thread_contexts(&path_ctx->reads,
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

    logInfo("io_path_context_array.count: %d", io_path_context_array.count);
    return 0;
}

void trunk_io_thread_terminate()
{
}

int trunk_io_thread_push(const int type, const int path_index,
        const uint32_t hash_code, void *entry, char *buff,
        trunk_io_notify_func notify_func, void *notify_args)
{
    TrunkIOPathContext *path_ctx;
    TrunkIOThreadContext *thread_ctx;
    TrunkIOThreadContextArray *ctx_array;
    TrunkIOBuffer *iob;
    bool notify;

    path_ctx = io_path_context_array.paths + path_index;
    if (type == FS_IO_TYPE_READ_SLICE) {
        ctx_array = &path_ctx->reads;
    } else {
        ctx_array = &path_ctx->writes;
    }

    thread_ctx = ctx_array->contexts + hash_code % ctx_array->count;
    pthread_mutex_lock(&thread_ctx->lock);
    iob = (TrunkIOBuffer *)fast_mblock_alloc_object(&thread_ctx->mblock);
    if (iob == NULL) {
        pthread_mutex_unlock(&thread_ctx->lock);
        return ENOMEM;
    }

    iob->type = type;
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
    iob->notify.args = notify_args;
    iob->next = NULL;

    if (thread_ctx->tail == NULL) {
        thread_ctx->head = iob;
        notify = true;
    } else {
        thread_ctx->tail->next = iob;
        notify = false;
    }
    thread_ctx->tail = iob;
    pthread_mutex_unlock(&thread_ctx->lock);

    if (notify) {
        pthread_cond_signal(&thread_ctx->cond);
    }
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
    if (ctx->fd_cache.pair.fd >= 0) {
        close(ctx->fd_cache.pair.fd);
        ctx->fd_cache.pair.fd = -1;
        ctx->fd_cache.pair.trunk_id = 0;
    }
}

static int get_write_fd(TrunkIOThreadContext *ctx,
        FSTrunkSpaceInfo *space, int *fd)
{
    char trunk_filename[PATH_MAX];
    int result;

    if (space->id_info.id == ctx->fd_cache.pair.trunk_id) {
        *fd = ctx->fd_cache.pair.fd;
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

    if (ctx->fd_cache.pair.fd >= 0) {
        close(ctx->fd_cache.pair.fd);
    }

    ctx->fd_cache.pair.trunk_id = space->id_info.id;
    ctx->fd_cache.pair.fd = *fd;
    return 0;
}

static int get_read_fd(TrunkIOThreadContext *ctx,
        FSTrunkSpaceInfo *space, int *fd)
{
    char trunk_filename[PATH_MAX];
    int result;

    if ((*fd=trunk_fd_cache_get(&ctx->fd_cache.context,
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

    trunk_fd_cache_add(&ctx->fd_cache.context, space->id_info.id, *fd);
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

    if (ftruncate(fd, iob->space.size) == 0) {
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

static int do_write_slice(TrunkIOThreadContext *ctx, TrunkIOBuffer *iob)
{
    int fd;
    int remain;
    int bytes;
    int result;

    if ((result=get_write_fd(ctx, &iob->slice->space, &fd)) != 0) {
        return result;
    }

    remain = iob->slice->ssize.length;
    while (remain > 0) {
        if ((bytes=pwrite(fd, iob->data.str + iob->data.len, remain,
                        iob->slice->space.offset + iob->data.len)) < 0)
        {
            char trunk_filename[PATH_MAX];

            result = errno != 0 ? errno : EIO;
            if (result == EINTR) {
                continue;
            }

            clear_write_fd(ctx);

            get_trunk_filename(&iob->slice->space, trunk_filename,
                    sizeof(trunk_filename));
            logError("file: "__FILE__", line: %d, "
                    "write to trunk file: %s fail, offset: %"PRId64", "
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

static int do_read_slice(TrunkIOThreadContext *ctx, TrunkIOBuffer *iob)
{
    int fd;
    int remain;
    int bytes;
    int result;

    if ((result=get_read_fd(ctx, &iob->slice->space, &fd)) != 0) {
        return result;
    }

    if (iob->slice->read_offset > 0) {
        logInfo("==== file: "__FILE__", line: %d, "
                "slice {offset: %d, length: %d}, "
                "space {offset: %"PRId64", size: %"PRId64"}, read offset: %d ===",
                __LINE__, iob->slice->ssize.offset, iob->slice->ssize.length,
                iob->slice->space.offset, iob->slice->space.size,
                iob->slice->read_offset);
    }

    remain = iob->slice->ssize.length;
    while (remain > 0) {
        if ((bytes=pread(fd, iob->data.str + iob->data.len, remain,
                        iob->slice->space.offset + iob->slice->read_offset +
                        iob->data.len)) < 0)
        {
            char trunk_filename[PATH_MAX];

            result = errno != 0 ? errno : EIO;
            if (result == EINTR) {
                continue;
            }

            trunk_fd_cache_delete(&ctx->fd_cache.context,
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

static int trunk_io_deal_buffer(TrunkIOThreadContext *ctx, TrunkIOBuffer *iob)
{
    int result;

    switch (iob->type) {
        case FS_IO_TYPE_CREATE_TRUNK:
            result = do_create_trunk(ctx, iob);
            break;
        case FS_IO_TYPE_DELETE_TRUNK:
            result = do_delete_trunk(ctx, iob);
            break;
        case FS_IO_TYPE_WRITE_SLICE:
            result = do_write_slice(ctx, iob);
            break;
        case FS_IO_TYPE_READ_SLICE:
            result = do_read_slice(ctx, iob);
            break;
        default:
            logError("file: "__FILE__", line: %d, "
                    "invalid IO type: %d", __LINE__, iob->type);
            result = EINVAL;
            break;
    }

    if (iob->notify.func != NULL) {
        iob->notify.func(iob, result);
    }
    return result;
}

static void *trunk_io_thread_func(void *arg)
{
    TrunkIOThreadContext *ctx;
    TrunkIOBuffer *iob;
    TrunkIOBuffer *iob_ptr;
    TrunkIOBuffer iob_obj;
    int result;

    ctx = (TrunkIOThreadContext *)arg;
    while (SF_G_CONTINUE_FLAG) {
        pthread_mutex_lock(&ctx->lock);
        if (ctx->head == NULL) {
            pthread_cond_wait(&ctx->cond, &ctx->lock);
        }

        if (ctx->head == NULL) {
            iob_ptr = NULL;
        } else {
            iob = ctx->head;
            ctx->head = ctx->head->next;
            if (ctx->head == NULL) {
                ctx->tail = NULL;
            }

            iob_ptr = &iob_obj;
            iob_obj = *iob;
            fast_mblock_free_object(&ctx->mblock, iob);
        }
        pthread_mutex_unlock(&ctx->lock);

        if (iob_ptr == NULL) {
            continue;
        }

        if ((result=trunk_io_deal_buffer(ctx, iob_ptr)) != 0) {
            logError("file: "__FILE__", line: %d, "
                    "trunk_io_deal_buffer fail, result: %d",
                    __LINE__, result);
        }
    }

    return NULL;
}
