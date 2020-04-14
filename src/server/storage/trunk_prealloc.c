#include <limits.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fast_mblock.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "trunk_prealloc.h"

typedef struct trunk_prealloc_task {
    FSTrunkAllocator *allocator;
    FSTrunkFreelist *freelist;
    int target_count;
    struct trunk_prealloc_task *next;
} TrunkPreallocTask;

typedef struct {
    struct trunk_prealloc_task *head;
    struct trunk_prealloc_task *tail;
    struct fast_mblock_man mblock;
    pthread_mutex_t lock;
    pthread_cond_t cond;
} TrunkPreallocThreadContext;

static TrunkPreallocThreadContext *thread_contexts = NULL;
static int thread_count = 0;

int trunk_prealloc_push(FSTrunkAllocator *allocator,
        FSTrunkFreelist *freelist, const int target_count)
{
    TrunkPreallocThreadContext *ctx;
    TrunkPreallocTask *task;
    bool notify;
    int result;

    ctx = thread_contexts + allocator->index % thread_count;
    pthread_mutex_lock(&ctx->lock);
    task = (TrunkPreallocTask *)fast_mblock_alloc_object(&ctx->mblock);
    if (task != NULL) {
        result = 0;
        task->allocator = allocator;
        task->freelist = freelist;
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
    pthread_mutex_unlock(&ctx->lock);

    if (notify) {
        pthread_cond_signal(&ctx->cond);
    }
    return result;
}

static int trunk_prealloc_deal_task(TrunkPreallocTask *task)
{
    if (task->freelist->count >= task->target_count) {
        return 0;
    }

    //FS_TRUNK_STATUS_ALLOCING
    //TODO
    return 0;
}

static void *trunk_prealloc_thread_func(void *arg)
{
    TrunkPreallocThreadContext *ctx;
    TrunkPreallocTask *task;
    TrunkPreallocTask *task_ptr;
    TrunkPreallocTask task_obj;

    ctx = (TrunkPreallocThreadContext *)arg;
    while (SF_G_CONTINUE_FLAG) {
        pthread_mutex_lock(&ctx->lock);
        if (ctx->head == NULL) {
            pthread_cond_wait(&ctx->cond, &ctx->lock);
        }

        if (ctx->head == NULL) {
            task_ptr = NULL;
        } else {
            task = ctx->head;
            ctx->head = ctx->head->next;
            if (ctx->head == NULL) {
                ctx->tail = NULL;
            }

            task_ptr = &task_obj;
            task_obj = *task;
            fast_mblock_free_object(&ctx->mblock, task);
        }
        pthread_mutex_unlock(&ctx->lock);

        if (task_ptr == NULL) {
            continue;
        }

        if (trunk_prealloc_deal_task(task_ptr) != 0) {
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

    thread_count = STORAGE_CFG.prealloc_trunk_threads;
    bytes = sizeof(TrunkPreallocThreadContext) * thread_count;
    thread_contexts = (TrunkPreallocThreadContext *)malloc(bytes);
    if (thread_contexts == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(thread_contexts, 0, bytes);

    end = thread_contexts + thread_count;
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

    return create_work_threads_ex(&thread_count, trunk_prealloc_thread_func,
            thread_contexts, sizeof(TrunkPreallocThreadContext), NULL,
            SF_G_THREAD_STACK_SIZE);
}
