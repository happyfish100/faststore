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
#include "fastcommon/pthread_func.h"
#include "../server_global.h"
#include "read_buffer_pool.h"

static int init_allocators(ReadBufferPool *pool)
{
    int result;
    int size;
    ReadBufferAllocator *allocator;
    ReadBufferAllocator *end;

    size = pool->block_size;
    pool->mpool.count = 0;
    while (size <= FS_FILE_BLOCK_SIZE) {
        pool->mpool.count++;
        size *= 2;
    }

    pool->mpool.allocators = (ReadBufferAllocator *)fc_malloc(
            sizeof(ReadBufferAllocator) * pool->mpool.count);
    if (pool->mpool.allocators == NULL) {
        return ENOMEM;
    }

    size = pool->block_size;
    end = pool->mpool.allocators + pool->mpool.count;
    for (allocator=pool->mpool.allocators; allocator<end; allocator++) {
        allocator->size = size;
        if ((result=init_pthread_lock(&allocator->lock)) != 0) {
            return result;
        }
        FC_INIT_LIST_HEAD(&allocator->freelist);

        size *= 2;
    }

    return 0;
}

int read_buffer_pool_init(ReadBufferPool *pool,
        const short path_index, const int block_size,
        const MemoryWatermark *watermark)
{
    pool->path_index = path_index;
    pool->block_size = block_size;
    pool->memory.watermark = *watermark;
    pool->memory.alloc = 0;
    pool->memory.used = 0;

    return init_allocators(pool);
}

AlignedReadBuffer *read_buffer_pool_alloc(ReadBufferPool *pool,
        const int size)
{
    //AlignedReadBuffer *entry;

    return NULL;
}

void read_buffer_pool_free(ReadBufferPool *pool,
        AlignedReadBuffer *buffer)
{
    //fc_list_add(&entry->dlink, &pool->lru.head);
}
