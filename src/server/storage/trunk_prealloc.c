#include <limits.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fast_mblock.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "../dio/trunk_io_thread.h"
#include "storage_allocator.h"
#include "trunk_prealloc.h"

struct trunk_prealloc_thread_context;
typedef struct trunk_prealloc_task {
    int target_count;
    FSTrunkAllocator *allocator;
    FSTrunkFreelist *freelist;
    struct trunk_prealloc_thread_context *ctx;
    struct trunk_prealloc_task *next;
} TrunkPreallocTask;

typedef struct trunk_prealloc_thread_context {
    struct trunk_prealloc_task *head;
    struct trunk_prealloc_task *tail;
    struct fast_mblock_man mblock;
    pthread_mutex_t lock;
    pthread_cond_t cond;
} TrunkPreallocThreadContext;

static TrunkPreallocThreadContext *thread_contexts = NULL;
static int prealloc_thread_count = 0;

int trunk_prealloc_push(FSTrunkAllocator *allocator,
        FSTrunkFreelist *freelist, const int target_count)
{
    TrunkPreallocThreadContext *ctx;
    TrunkPreallocTask *task;
    bool notify;
    int result;

    ctx = thread_contexts + allocator->path_info->
        store.index % prealloc_thread_count;
    PTHREAD_MUTEX_LOCK(&ctx->lock);
    task = (TrunkPreallocTask *)fast_mblock_alloc_object(&ctx->mblock);
    if (task != NULL) {
        result = 0;
        task->allocator = allocator;
        task->freelist = freelist;
        task->ctx = ctx;
        task->target_count = target_count;
        task->next = NULL;

        if (ctx->tail == NULL) {
            ctx->head = task;
            notify = true;
        } else {
            ctx->tail->next = task;
            notify = false;
        }
        ctx->tail = task;
    } else {
        result = ENOMEM;
        notify = false;
    }
    PTHREAD_MUTEX_UNLOCK(&ctx->lock);

    if (notify) {
        pthread_cond_signal(&ctx->cond);
    }
    return result;
}

static void create_trunk_done(struct trunk_io_buffer *record,
        const int result)
{
    TrunkPreallocTask *task;

    task = (TrunkPreallocTask *)record->notify.args;
    if (result == 0) {
        FSTrunkFileInfo *trunk_info;
        if (storage_allocator_add_trunk_ex(record->space.
                    store->index, &record->space.id_info,
                    record->space.size, &trunk_info) == 0)
        {
            trunk_allocator_add_to_freelist(task->allocator,
                    task->freelist, trunk_info);
        }
    }
    fast_mblock_free_object(&task->ctx->mblock, task);
}

static int trunk_prealloc_deal_task(TrunkPreallocTask *task)
{
    int result;
    FSTrunkSpaceInfo space;
    string_t data;

    if (task->freelist->count >= task->target_count) {
        return 0;
    }

    if ((result=storage_config_calc_path_spaces(task->allocator->
                    path_info)) != 0)
    {
        return result;
    }

    if (task->allocator->path_info->avail_space - STORAGE_CFG.trunk_file_size <
            task->allocator->path_info->reserved_space.value)
    {
        //TODO: trunk space reclaim
        //FS_TRUNK_STATUS_ALLOCING
    }

    space.store = &task->allocator->path_info->store;
    if ((result=trunk_id_info_generate(space.store->index,
                    &space.id_info)) != 0)
    {
        return result;
    }
    space.offset = 0;
    space.size = STORAGE_CFG.trunk_file_size;

    data.str = NULL;
    data.len = 0;
    return io_thread_push_trunk_op(FS_IO_TYPE_CREATE_TRUNK, &space,
            create_trunk_done, task);
}

static void *trunk_prealloc_thread_func(void *arg)
{
    TrunkPreallocThreadContext *ctx;
    TrunkPreallocTask *task;

    ctx = (TrunkPreallocThreadContext *)arg;
    while (SF_G_CONTINUE_FLAG) {
        PTHREAD_MUTEX_LOCK(&ctx->lock);
        if (ctx->head == NULL) {
            pthread_cond_wait(&ctx->cond, &ctx->lock);
        }

        if (ctx->head == NULL) {
            task = NULL;
        } else {
            task = ctx->head;
            ctx->head = ctx->head->next;
            if (ctx->head == NULL) {
                ctx->tail = NULL;
            }

        }
        PTHREAD_MUTEX_UNLOCK(&ctx->lock);

        if (task == NULL) {
            continue;
        }

        if (trunk_prealloc_deal_task(task) != 0) {
            logError("file: "__FILE__", line: %d, "
                    "trunk_prealloc_deal_task fail", __LINE__);
        }
    }

    return NULL;
}

int trunk_prealloc_init()
{
    int result;
    int bytes;
    TrunkPreallocThreadContext *ctx;
    TrunkPreallocThreadContext *end;

    prealloc_thread_count = STORAGE_CFG.prealloc_trunk_threads;
    bytes = sizeof(TrunkPreallocThreadContext) * prealloc_thread_count;
    thread_contexts = (TrunkPreallocThreadContext *)fc_malloc(bytes);
    if (thread_contexts == NULL) {
        return ENOMEM;
    }
    memset(thread_contexts, 0, bytes);

    end = thread_contexts + prealloc_thread_count;
    for (ctx=thread_contexts; ctx<end; ctx++) {
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

        if ((result=fast_mblock_init_ex2(&ctx->mblock,
                        "trunk_prealloc_task", sizeof(TrunkPreallocTask),
                        1024, NULL, NULL, false, NULL, NULL, NULL)) != 0)
        {
            return result;
        }
    }

    return create_work_threads_ex(&prealloc_thread_count,
            trunk_prealloc_thread_func, thread_contexts, sizeof(
                TrunkPreallocThreadContext), NULL, SF_G_THREAD_STACK_SIZE);
}
