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

#ifndef _FS_API_BUFFER_POOL_H
#define _FS_API_BUFFER_POOL_H

#include "fastcommon/fast_mblock.h"
#include "fastcommon/fc_atomic.h"

struct fs_api_buffer_allocator;

typedef struct fs_api_buffer {
    int64_t create_time_ms;
    int offset;
    int length; //data length
    bool dirty; //if data dirty
    volatile int refer_count;
    struct fs_api_buffer_allocator *allocator;
    char buff[0];
} FSAPIBuffer;

typedef struct fs_api_buffer_allocator {
    int buffer_size;
    struct fast_mblock_man mblock; //element: FSAPIBuffer
} FSAPIBufferAllocator;

typedef struct fs_api_buffer_allocator_array {
    int count;
    FSAPIBufferAllocator *allocators;
} FSAPIBufferAllocatorArray;

typedef struct fs_api_buffer_pool {
    FSAPIBufferAllocatorArray array;
} FSAPIBufferPool;

#ifdef __cplusplus
extern "C" {
#endif

    int fs_api_buffer_pool_init(FSAPIBufferPool *pool,
            const int min_buffer_size, const int max_buffer_size);

    FSAPIBuffer *fs_api_buffer_alloc(FSAPIBufferPool *pool,
            const int size, const int inc_refers);

    static inline void fs_api_buffer_release(FSAPIBuffer *buffer)
    {
        if (FC_ATOMIC_DEC(buffer->refer_count) == 0) {
            logInfo("file: "__FILE__", line: %d, free buffer: %p", __LINE__, buffer);
            fast_mblock_free_object(&buffer->allocator->mblock, buffer);
        }
    }

    static inline void fs_api_buffer_hold(FSAPIBuffer *buffer)
    {
        FC_ATOMIC_INC(buffer->refer_count);
    }

#ifdef __cplusplus
}
#endif

#endif
