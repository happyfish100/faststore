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
#include "fs_api_allocator.h"

FSAPIAllocatorCtxArray g_allocator_array;

static int task_writer_pair_alloc_init(void *element, void *arg)
{
    ((FSAPIWaitingTaskWriterPair *)element)->allocator =
        (struct fast_mblock_man *)arg;
    return 0;
}

static int waiting_task_alloc_init(void *element, void *arg)
{
    ((FSAPIWaitingTask *)element)->allocator =
        (struct fast_mblock_man *)arg;
    return 0;
}

static int combined_writer_alloc_init(void *element, void *arg)
{
    ((FSAPICombinedWriter *)element)->allocator =
        (struct fast_mblock_man *)arg;
    return 0;
}

static int slice_entry_alloc_init(void *element, void *arg)
{
    ((FSAPISliceEntry *)element)->allocator =
        (struct fast_mblock_man *)arg;
    return 0;
}

static int init_allocator_context(FSAPIAllocatorContext *ctx)
{
    int result;

    if ((result=fast_mblock_init_ex1(&ctx->task_writer_pair,
                    "task_writer_pair", sizeof(FSAPIWaitingTaskWriterPair),
                    4096, 0, task_writer_pair_alloc_init,
                    &ctx->task_writer_pair, true)) != 0)
    {
        return result;
    }

    if ((result=fast_mblock_init_ex1(&ctx->waiting_task,
                    "waiting_task", sizeof(FSAPIWaitingTask),
                    2048, 0, waiting_task_alloc_init,
                    &ctx->waiting_task, true)) != 0)
    {
        return result;
    }

    if ((result=fast_mblock_init_ex1(&ctx->combined_writer,
                    "combined_writer", sizeof(FSAPICombinedWriter),
                    1024, 0, combined_writer_alloc_init,
                    &ctx->combined_writer, true)) != 0)
    {
        return result;
    }

    if ((result=fast_mblock_init_ex1(&ctx->slice_entry,
                    "slice_entry", sizeof(FSAPISliceEntry),
                    8192, 0, slice_entry_alloc_init,
                    &ctx->slice_entry, true)) != 0)
    {
        return result;
    }

    return 0;
}

int fs_api_allocator_init()
{
    int result;
    int bytes;
    FSAPIAllocatorContext *ctx;
    FSAPIAllocatorContext *end;

    g_allocator_array.count = 17;
    bytes = sizeof(FSAPIAllocatorContext) * g_allocator_array.count;
    g_allocator_array.allocators = (FSAPIAllocatorContext *)fc_malloc(bytes);
    if (g_allocator_array.allocators == NULL) {
        return ENOMEM;
    }

    end = g_allocator_array.allocators + g_allocator_array.count;
    for (ctx=g_allocator_array.allocators; ctx<end; ctx++) {
        if ((result=init_allocator_context(ctx)) != 0) {
            return result;
        }
    }

    return 0;
}
