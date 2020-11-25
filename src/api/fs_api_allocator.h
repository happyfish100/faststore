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

typedef struct fs_api_allocator_context {
    struct fast_mblock_man task_slice_pair; //element: FSAPIWaitingTaskSlicePair
    struct fast_mblock_man waiting_task;     //element: FSAPIWaitingTask
    struct fast_mblock_man slice_entry;      //element: FSAPISliceEntry
} FSAPIAllocatorContext;

typedef struct fs_api_allocator_ctx_array {
    int count;
    FSAPIAllocatorContext *allocators;
} FSAPIAllocatorCtxArray;

#ifdef __cplusplus
extern "C" {
#endif

    extern FSAPIAllocatorCtxArray g_allocator_array;

    int fs_api_allocator_init();

    static inline FSAPIAllocatorContext *fs_api_allocator_get(
            const uint64_t tid)
    {
        return g_allocator_array.allocators +
            tid % g_allocator_array.count;
    }

#ifdef __cplusplus
}
#endif

#endif
