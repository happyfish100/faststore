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

#include <stdlib.h>
#include "fastcommon/shared_func.h"
#include "fs_api_buffer_pool.h"

static int buffer_alloc_init(FSAPIBuffer *buffer,
        FSAPIBufferAllocator *allocator)
{
    buffer->allocator = allocator;
    return 0;
}

static int init_buffer_allocator(FSAPIBufferAllocator *allocator)
{
    int result;
    int alloc_elements_once;
    int element_size;

    alloc_elements_once = (4 * 1024 * 1024) / allocator->buffer_size;
    if (alloc_elements_once <= 1) {
        alloc_elements_once = 4;
    }
    element_size = sizeof(FSAPIBuffer) + allocator->buffer_size;
    if ((result=fast_mblock_init_ex1(&allocator->mblock, "io-buffer",
                    element_size, alloc_elements_once, 0,
                    (fast_mblock_alloc_init_func)
                    buffer_alloc_init, allocator, true)) != 0)
    {
        return result;
    }

    return 0;
}

int fs_api_buffer_pool_init(FSAPIBufferPool *pool,
            const int min_buffer_size, const int max_buffer_size)
{
    int result;
    int size;
    int bytes;
    FSAPIBufferAllocator *allocator;
    FSAPIBufferAllocator *end;

    pool->array.count = 1;
    size = min_buffer_size;
    while (size < max_buffer_size) {
        pool->array.count++;
        size *= 2;
    }

    bytes = sizeof(FSAPIBufferAllocator) * pool->array.count;
    pool->array.allocators = (FSAPIBufferAllocator *)fc_malloc(bytes);
    if (pool->array.allocators == NULL) {
        return ENOMEM;
    }

    size = min_buffer_size;
    end = pool->array.allocators + pool->array.count;
    for (allocator=pool->array.allocators; allocator<end; allocator++) {
        allocator->buffer_size = size;
        if ((result=init_buffer_allocator(allocator)) != 0) {
            return result;
        }
        size *= 2;
    }

    return 0;
}

FSAPIBuffer *fs_api_buffer_alloc(FSAPIBufferPool *pool,
        const int size, const int inc_refers)
{
    FSAPIBufferAllocator *allocator;
    FSAPIBufferAllocator *end;
    FSAPIBuffer *buffer;

    end = pool->array.allocators + pool->array.count;
    for (allocator=pool->array.allocators; allocator<end; allocator++) {
        if (size <= allocator->buffer_size) {
            if ((buffer=(FSAPIBuffer *)fast_mblock_alloc_object(
                            &allocator->mblock)) == NULL)
            {
                return NULL;
            }

            FC_ATOMIC_INC_EX(buffer->refer_count, inc_refers);
            return buffer;
        }
    }

    logError("file: "__FILE__", line: %d, "
            "alloc size: %d is too large exceeds: %d",
            __LINE__, size, (allocator-1)->buffer_size);
    return NULL;
}
