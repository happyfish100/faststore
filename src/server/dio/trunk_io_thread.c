#include <limits.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fast_mblock.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "trunk_io_thread.h"

typedef struct trunk_io_thread_context {
    TrunkIOBuffer *head;
    TrunkIOBuffer *tail;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    struct fast_mblock_man mblock;
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

    return fc_create_thread(&tid, trunk_io_thread_func,
            ctx, SF_G_THREAD_STACK_SIZE);
}

static int init_thread_contexts(TrunkIOThreadContext *contexts, const int count)
{
    int result;
    TrunkIOThreadContext *ctx;
    TrunkIOThreadContext *end;
    
    end = contexts + count;
    for (ctx=contexts; ctx<end; ctx++) {
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

        path_ctx->reads.contexts = thread_ctxs + p->write_thread_count;
        path_ctx->reads.count = p->read_thread_count;
        if ((result=init_thread_contexts(thread_ctxs, thread_count)) != 0) {
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

int trunk_io_thread_push(const int type, const FSTrunkSpaceInfo *space,
        string_t *data, trunk_io_notify_func notify_func, void *notify_args)
{
    TrunkIOPathContext *path_ctx;
    TrunkIOThreadContext *thread_ctx;
    TrunkIOThreadContextArray *ctx_array;
    TrunkIOBuffer *iob;
    bool notify;

    path_ctx = io_path_context_array.paths + space->store->index;
    if (type == FS_IO_TYPE_READ_SLICE) {
        ctx_array = &path_ctx->reads;
    } else {
        ctx_array = &path_ctx->writes;
    }

    thread_ctx = ctx_array->contexts + space->id_info.id % ctx_array->count;
    pthread_mutex_lock(&thread_ctx->lock);
    iob = (TrunkIOBuffer *)fast_mblock_alloc_object(&thread_ctx->mblock);
    if (iob == NULL) {
        pthread_mutex_unlock(&thread_ctx->lock);
        return ENOMEM;
    }

    iob->type = type;
    iob->space = *space;
    iob->data = *data;
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

static int do_create_trunk(TrunkIOBuffer *iob)
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

    if (ftruncate(fd, iob->space.size) < 0) {
        result = errno != 0 ? errno : EACCES;
        logError("file: "__FILE__", line: %d, "
                "ftruncate file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, trunk_filename, result, STRERROR(result));
        return result;
    }

    //TODO
    return 0;
}

static int trunk_io_deal_buffer(TrunkIOBuffer *iob)
{
    int result;

    switch (iob->type) {
        case FS_IO_TYPE_CREATE_TRUNK:
            result = do_create_trunk(iob);
            break;
        case FS_IO_TYPE_DELETE_TRUNK:
            result = 0;
            break;
        case FS_IO_TYPE_READ_SLICE:
            result = 0;
            break;
        case FS_IO_TYPE_WRITE_SLICE:
            result = 0;
            break;
        default:
            logError("file: "__FILE__", line: %d, "
                    "invalid IO type: %d", __LINE__, iob->type);
            result = EINVAL;
            break;
    }

    return result;
}

static void *trunk_io_thread_func(void *arg)
{
    TrunkIOThreadContext *ctx;
    TrunkIOBuffer *iob;
    TrunkIOBuffer *iob_ptr;
    TrunkIOBuffer iob_obj;

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

        if (trunk_io_deal_buffer(iob_ptr) != 0) {
            logError("file: "__FILE__", line: %d, "
                    "trunk_io_deal_buffer fail", __LINE__);
        }
    }

    return NULL;
}
