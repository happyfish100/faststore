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

#ifndef _FS_API_ALLOCATOR_H
#define _FS_API_ALLOCATOR_H

#include "fastcommon/pthread_func.h"
#include "fastcommon/fast_mblock.h"
#include "fs_api_types.h"
#include "fs_api_buffer_pool.h"

typedef struct fs_api_allocator_context {
    struct {
        struct fast_mblock_man task_slice_pair; //element: FSAPIWaitingTaskSlicePair
        struct fast_mblock_man waiting_task;    //element: FSAPIWaitingTask
        struct {
            struct fast_mblock_man allocator;   //element: FSAPISliceEntry
            int64_t version_mask;
            volatile int64_t current_version;
        } slice;
        struct fast_mblock_man callback_arg;    //element: FSAPIWriteDoneCallbackArg
    } write_combine;

    struct {
        struct fast_mblock_man block; //element: FSPrereadBlockHEntry
        struct fast_mblock_man slice; //element: FSPrereadSliceEntry
        FSAPIBufferPool buffer_pool;
    } read_ahead;

} FSAPIAllocatorContext;

typedef struct fs_api_allocator_ctx_array {
    int count;
    FSAPIAllocatorContext *allocators;
    FSAPIContext *api_ctx;
} FSAPIAllocatorCtxArray;

#ifdef __cplusplus
extern "C" {
#endif

    extern FSAPIAllocatorCtxArray g_fs_api_allocator_array;

    int fs_api_allocator_init(FSAPIContext *api_ctx);

    static inline FSAPIAllocatorContext *fs_api_allocator_get(
            const uint64_t tid)
    {
        return g_fs_api_allocator_array.allocators +
            tid % g_fs_api_allocator_array.count;
    }

    static inline int64_t fs_api_next_slice_version(FSAPIAllocatorContext *ctx)
    {
        return ctx->write_combine.slice.version_mask | __sync_add_and_fetch(
                &ctx->write_combine.slice.current_version, 1);
    }

#ifdef __cplusplus
}
#endif

#endif
