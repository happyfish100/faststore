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
#include <sys/stat.h>
#include <sys/statvfs.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/fc_atomic.h"
#include "../server_global.h"
#include "read_buffer_pool.h"

typedef struct {
    int size;
    pthread_mutex_t lock;
    struct fc_list_head freelist;  //element: AlignedReadBuffer
} ReadBufferAllocator;

typedef struct {
    int block_size;
    short path_index;

    struct {
        volatile int64_t alloc;
        volatile int64_t used;
    } memory;

    struct fast_mblock_man mblock;  //element: AlignedReadBuffer
    struct {
        ReadBufferAllocator *allocators;
        ReadBufferAllocator *middle;
        ReadBufferAllocator *middle_plus_1;
        ReadBufferAllocator *end;
        int count;
    } mpool;

} ReadBufferPool;


static struct {
    struct {
        ReadBufferPool *pools;
        int count;
    } array;

    struct {
        ReadBufferPool **pools;
        ReadBufferPool **end;
        int count;
    } ptr_array;

    int max_idle_time;
    int sleep_ms;
    SFMemoryWatermark watermark;
} rbpool_ctx = {
    {NULL, 0}, {NULL, NULL, 0}, 0, 0
};

int read_buffer_pool_init(const int path_count,
        const SFMemoryWatermark *watermark)
{
    int bytes;

    bytes = sizeof(ReadBufferPool) * path_count;
    rbpool_ctx.array.pools = (ReadBufferPool *)fc_malloc(bytes);
    if (rbpool_ctx.array.pools == NULL) {
        return ENOMEM;
    }
    memset(rbpool_ctx.array.pools, 0, bytes);

    bytes = sizeof(ReadBufferPool *) * path_count;
    rbpool_ctx.ptr_array.pools = (ReadBufferPool **)fc_malloc(bytes);
    if (rbpool_ctx.ptr_array.pools == NULL) {
        return ENOMEM;
    }
    memset(rbpool_ctx.ptr_array.pools, 0, bytes);
    rbpool_ctx.watermark = *watermark;
    return 0;
}

static int aligned_buffer_alloc_init(AlignedReadBuffer *buffer,
        ReadBufferPool *pool)
{
    buffer->indexes.path = pool->path_index;
    FC_INIT_LIST_HEAD(&buffer->dlink);
    return 0;
}

static int init_allocators(ReadBufferPool *pool)
{
    int result;
    int size;
    ReadBufferAllocator *allocator;
    ReadBufferAllocator *last;

    size = pool->block_size;
    pool->mpool.count = 1;
    while (size <= FS_FILE_BLOCK_SIZE) {
        pool->mpool.count++;
        size *= 2;
    }

    pool->mpool.allocators = (ReadBufferAllocator *)fc_malloc(
            sizeof(ReadBufferAllocator) * pool->mpool.count);
    if (pool->mpool.allocators == NULL) {
        return ENOMEM;
    }

    size = 0;
    pool->mpool.middle = allocator=pool->mpool.
        allocators + pool->mpool.count / 2;
    pool->mpool.middle_plus_1 = pool->mpool.middle + 1;
    pool->mpool.end = pool->mpool.allocators + pool->mpool.count;
    last = pool->mpool.end - 1;
    for (allocator=pool->mpool.allocators;
            allocator<pool->mpool.end; allocator++)
    {
        if (size == 0) {
            allocator->size = pool->block_size;
            size = pool->block_size;
        } else {
            if (allocator != last) {
                allocator->size = size + pool->block_size;
            } else {
                allocator->size = size + 2 * pool->block_size;
            }
            size *= 2;
        }
        if ((result=init_pthread_lock(&allocator->lock)) != 0) {
            return result;
        }
        FC_INIT_LIST_HEAD(&allocator->freelist);
    }

    if ((result=fast_mblock_init_ex1(&pool->mblock, "aligned-rdbuffer",
                    sizeof(AlignedReadBuffer), 8192, 0,
                    (fast_mblock_alloc_init_func)aligned_buffer_alloc_init,
                    pool, true)) != 0)
    {
        return result;
    }

    return 0;
}

int read_buffer_pool_create(const short path_index, const int block_size)
{
    ReadBufferPool *pool;
    int result;

    pool = rbpool_ctx.array.pools + path_index;
    pool->path_index = path_index;
    pool->block_size = block_size;
    pool->memory.alloc = 0;
    pool->memory.used = 0;

    if ((result=init_allocators(pool)) != 0) {
        return result;
    }

    rbpool_ctx.ptr_array.pools[rbpool_ctx.ptr_array.count++] = pool;
    rbpool_ctx.ptr_array.end = rbpool_ctx.ptr_array.pools +
        rbpool_ctx.ptr_array.count;
    return 0;
}

static inline ReadBufferAllocator *get_allocator(
        ReadBufferPool *pool, const int size)
{
    ReadBufferAllocator *allocator;
    ReadBufferAllocator *start;
    ReadBufferAllocator *end;

    if (size < pool->mpool.middle->size) {
        start = pool->mpool.allocators;
        end = pool->mpool.middle_plus_1;
    } else if (size > pool->mpool.middle->size) {
        start = pool->mpool.middle_plus_1;
        end = pool->mpool.end;
    } else {
        return pool->mpool.middle;
    }

    for (allocator=start; allocator<end; allocator++) {
        if (size <= allocator->size) {
            return allocator;
        }
    }

    logError("file: "__FILE__", line: %d, "
            "alloc size: %d is too large, exceed: %d",
            __LINE__, size, (end-1)->size);
    return NULL;
}

static inline void free_aligned_buffer(ReadBufferPool *pool,
        AlignedReadBuffer *buffer)
{
    free(buffer->buff);
    buffer->buff = NULL;
    fc_list_del_init(&buffer->dlink);
    fast_mblock_free_object(&pool->mblock, buffer);
    FC_ATOMIC_DEC_EX(pool->memory.alloc, buffer->size);
}

static int reclaim_allocator_by_size(ReadBufferPool *pool,
        ReadBufferAllocator *allocator, const int target_size,
        int *reclaim_bytes)
{
    int result;
    struct fc_list_head *pos;
    AlignedReadBuffer *buffer;

    result = EAGAIN;
    PTHREAD_MUTEX_LOCK(&allocator->lock);
    fc_list_for_each_prev(pos, &allocator->freelist) {
        buffer = fc_list_entry(pos, AlignedReadBuffer, dlink);
        *reclaim_bytes += buffer->size;
        free_aligned_buffer(pool, buffer);
        if (*reclaim_bytes >= target_size) {
            result = 0;
            break;
        }
    }
    PTHREAD_MUTEX_UNLOCK(&allocator->lock);

    return result;
}

static int reclaim(ReadBufferPool *pool, const int target_size,
        int *reclaim_bytes)
{
    ReadBufferAllocator *allocator;

    *reclaim_bytes = 0;
    for (allocator = pool->mpool.middle_plus_1; allocator <
            pool->mpool.end; allocator++)
    {
        if (reclaim_allocator_by_size(pool, allocator,
                    target_size, reclaim_bytes) == 0)
        {
            return 0;
        }
    }

    for (allocator = pool->mpool.middle; allocator >=
            pool->mpool.allocators; allocator--)
    {
        if (reclaim_allocator_by_size(pool, allocator,
                    target_size, reclaim_bytes) == 0)
        {
            return 0;
        }
    }

    return EAGAIN;
}

static inline AlignedReadBuffer *do_aligned_alloc(ReadBufferPool *pool,
        ReadBufferAllocator *allocator)
{
    int result;
    AlignedReadBuffer *buffer;

    if ((buffer=(AlignedReadBuffer *)fast_mblock_alloc_object(
                    &pool->mblock)) == NULL)
    {
        return NULL;
    }

    if ((result=posix_memalign((void **)&buffer->buff,
                    pool->block_size, allocator->size)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "posix_memalign %d bytes fail, "
                "errno: %d, error info: %s", __LINE__,
                allocator->size, result, STRERROR(result));
        fast_mblock_free_object(&pool->mblock, buffer);
        return NULL;
    }

    buffer->size = allocator->size;
    buffer->indexes.allocator = allocator - pool->mpool.allocators;
    return buffer;
}

AlignedReadBuffer *read_buffer_pool_alloc(
        const short path_index, const int size)
{
    ReadBufferPool *pool;
    ReadBufferAllocator *allocator;
    AlignedReadBuffer *buffer;
    int64_t total_alloc;
    int reclaim_bytes;

    pool = rbpool_ctx.array.pools + path_index;
    if ((allocator=get_allocator(pool, size +
                    FS_SPACE_ALIGN_SIZE)) == NULL)
    {
        return NULL;
    }

    PTHREAD_MUTEX_LOCK(&allocator->lock);
    if ((buffer=fc_list_first_entry(&allocator->freelist,
                    AlignedReadBuffer, dlink)) != NULL)
    {
        fc_list_del_init(&buffer->dlink);
    }
    PTHREAD_MUTEX_UNLOCK(&allocator->lock);

    if (buffer == NULL) {
        total_alloc = FC_ATOMIC_GET(pool->memory.alloc);
        if (total_alloc + allocator->size > rbpool_ctx.watermark.high) {
            if (total_alloc - FC_ATOMIC_GET(pool->memory.used) >
                    FS_FILE_BLOCK_SIZE)
            {
                reclaim(pool, FS_FILE_BLOCK_SIZE, &reclaim_bytes);
                logInfo("file: "__FILE__", line: %d, "
                        "reach max memory limit, reclaim %d bytes",
                        __LINE__, reclaim_bytes);
            } else {
                logWarning("file: "__FILE__", line: %d, "
                        "reach max memory limit of pool: %"PRId64 " MB",
                        __LINE__, rbpool_ctx.watermark.high /
                        (1024 * 1024));
            }
        }

        if ((buffer=do_aligned_alloc(pool, allocator)) == NULL) {
            return NULL;
        }
    }

    FC_ATOMIC_INC_EX(pool->memory.used, buffer->size);
    return buffer;
}

void read_buffer_pool_free(AlignedReadBuffer *buffer)
{
    ReadBufferPool *pool;
    ReadBufferAllocator *allocator;

    pool = rbpool_ctx.array.pools + buffer->indexes.path;
    allocator = pool->mpool.allocators + buffer->indexes.allocator;
    PTHREAD_MUTEX_LOCK(&allocator->lock);
    buffer->last_access_time = g_current_time;
    fc_list_add(&buffer->dlink, &allocator->freelist);
    PTHREAD_MUTEX_UNLOCK(&allocator->lock);

    FC_ATOMIC_DEC_EX(pool->memory.used, allocator->size);
}

static void reclaim_allocator_by_ttl(ReadBufferPool *pool,
        ReadBufferAllocator *allocator)
{
    struct fc_list_head *pos;
    AlignedReadBuffer *buffer;

    PTHREAD_MUTEX_LOCK(&allocator->lock);
    fc_list_for_each_prev(pos, &allocator->freelist) {
        buffer = fc_list_entry(pos, AlignedReadBuffer, dlink);
        if (g_current_time - buffer->last_access_time <=
                rbpool_ctx.max_idle_time)
        {
            break;
        }
        free_aligned_buffer(pool, buffer);
    }
    PTHREAD_MUTEX_UNLOCK(&allocator->lock);
}

static void pool_reclaim(ReadBufferPool *pool)
{
    ReadBufferAllocator *allocator;

    /*
    logInfo("memory alloc: %"PRId64", watermark {low: %"PRId64", high: %"PRId64"}",
            FC_ATOMIC_GET(pool->memory.alloc), rbpool_ctx.watermark.low,
            rbpool_ctx.watermark.high);
            */

    if (FC_ATOMIC_GET(pool->memory.alloc) <=
            rbpool_ctx.watermark.low)
    {
        fc_sleep_ms(rbpool_ctx.sleep_ms * pool->mpool.count);
        return;
    }

    for (allocator=pool->mpool.allocators;
            allocator<pool->mpool.end; allocator++)
    {
        fc_sleep_ms(rbpool_ctx.sleep_ms);
        reclaim_allocator_by_ttl(pool, allocator);
    }
}

static void *reclaim_thread_entrance(void *arg)
{
    ReadBufferPool **pool;

    while (SF_G_CONTINUE_FLAG) {
        for (pool = rbpool_ctx.ptr_array.pools; pool <
                rbpool_ctx.ptr_array.end; pool++)
        {
            pool_reclaim(*pool);
        }
    }

    return NULL;
}

int read_buffer_pool_start(const int max_idle_time,
        const int reclaim_interval)
{
    ReadBufferPool **pool;
    int allocator_count;
    pthread_t tid;

    allocator_count = 0;
    for (pool = rbpool_ctx.ptr_array.pools; pool <
            rbpool_ctx.ptr_array.end; pool++)
    {
        allocator_count += (*pool)->mpool.count;
    }

    if (allocator_count == 0) {
        logError("file: "__FILE__", line: %d, "
                "pool array is empty!", __LINE__);
        return EINVAL;
    }

    rbpool_ctx.max_idle_time = max_idle_time;
    rbpool_ctx.sleep_ms = reclaim_interval * 1000 / allocator_count;
    return fc_create_thread(&tid, reclaim_thread_entrance,
            NULL, SF_G_THREAD_STACK_SIZE);
}
