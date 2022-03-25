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
#include "write_combine/timeout_handler.h"

FSAPIAllocatorCtxArray g_fs_api_allocator_array;

static int task_slice_pair_alloc_init(FSAPIWaitingTaskSlicePair
        *task_slice_pair, struct fast_mblock_man *allocator)
{
    task_slice_pair->allocator = allocator;
    return 0;
}

static int waiting_task_alloc_init(FSAPIWaitingTask *task,
        struct fast_mblock_man *allocator)
{
    int result;

    if ((result=init_pthread_lock_cond_pair(&task->lcp)) != 0) {
        return result;
    }

    FC_INIT_LIST_HEAD(&task->waitings.head);
    task->allocator = allocator;
    return 0;
}

static int wcmb_slice_entry_alloc_init(FSAPISliceEntry *slice,
        FSAPIAllocatorContext *ctx)
{
    slice->buff = (char *)malloc(g_fs_api_allocator_array.
            api_ctx->write_combine.buffer_size);
    if (slice->buff == NULL) {
        return ENOMEM;
    }

    slice->allocator_ctx = ctx;
    slice->version = fs_api_next_slice_version(ctx);
    slice->timer.lock_index = slice->version %
        g_timer_ms_ctx.timer.entry_shares.count;
    return 0;
}

static int callback_arg_alloc_init(FSAPIWriteDoneCallbackArg
        *callback_arg, struct fast_mblock_man *allocator)
{
    callback_arg->allocator = allocator;
    return 0;
}

static int pread_block_entry_alloc_init(FSPrereadBlockHEntry *block,
        struct fast_mblock_man *allocator)
{
    block->allocator = allocator;
    return 0;
}

static int pread_slice_entry_alloc_init(FSPrereadSliceEntry *slice,
        struct fast_mblock_man *allocator)
{
    slice->allocator = allocator;
    return 0;
}

static int init_write_combine_allocators(FSAPIContext *api_ctx,
        FSAPIAllocatorContext *ctx)
{
    int result;
    int element_size;

    if ((result=fast_mblock_init_ex1(&ctx->write_combine.task_slice_pair,
                    "task-slice-pair", sizeof(FSAPIWaitingTaskSlicePair),
                    4096, 0, (fast_mblock_object_init_func)
                    task_slice_pair_alloc_init,
                    &ctx->write_combine.task_slice_pair, true)) != 0)
    {
        return result;
    }

    if ((result=fast_mblock_init_ex1(&ctx->write_combine.waiting_task,
                    "waiting-task", sizeof(FSAPIWaitingTask), 1024, 0,
                    (fast_mblock_object_init_func)waiting_task_alloc_init,
                    &ctx->write_combine.waiting_task, true)) != 0)
    {
        return result;
    }

    if ((result=fast_mblock_init_ex1(&ctx->write_combine.slice.allocator,
                    "wcombine-slice", sizeof(FSAPISliceEntry), 8, 0,
                    (fast_mblock_object_init_func)wcmb_slice_entry_alloc_init,
                    ctx, true)) != 0)
    {
        return result;
    }

    element_size = sizeof(FSAPIWriteDoneCallbackArg) +
        api_ctx->write_done_callback.arg_extra_size;
    if ((result=fast_mblock_init_ex1(&ctx->write_combine.callback_arg,
                    "write-done-callback-arg", element_size, 1024, 0,
                    (fast_mblock_object_init_func)callback_arg_alloc_init,
                    &ctx->write_combine.callback_arg, true)) != 0)
    {
        return result;
    }

    return 0;
}

static int init_read_ahead_allocators(FSAPIContext *api_ctx,
        FSAPIAllocatorContext *ctx)
{
    int result;

    if ((result=fs_api_buffer_pool_init(&ctx->read_ahead.buffer_pool,
                    api_ctx->read_ahead.min_buffer_size,
                    api_ctx->read_ahead.max_buffer_size)) != 0)
    {
        return result;
    }

    if ((result=fast_mblock_init_ex1(&ctx->read_ahead.block,
                    "preread-block", sizeof(FSPrereadBlockHEntry), 8192, 0,
                    (fast_mblock_object_init_func)pread_block_entry_alloc_init,
                    &ctx->read_ahead.block, true)) != 0)
    {
        return result;
    }

    if ((result=fast_mblock_init_ex1(&ctx->read_ahead.slice,
                    "preread-slice", sizeof(FSPrereadSliceEntry), 8192, 0,
                    (fast_mblock_object_init_func)pread_slice_entry_alloc_init,
                    &ctx->read_ahead.slice, true)) != 0)
    {
        return result;
    }

    return 0;
}

static int init_allocator_context(FSAPIContext *api_ctx,
        FSAPIAllocatorContext *ctx)
{
    int result;

    if (api_ctx->write_combine.enabled) {
        if ((result=init_write_combine_allocators(api_ctx, ctx)) != 0) {
            return result;
        }
    }

    if (api_ctx->read_ahead.enabled) {
        if ((result=init_read_ahead_allocators(api_ctx, ctx)) != 0) {
            return result;
        }
    }

    return 0;
}

int fs_api_allocator_init(FSAPIContext *api_ctx)
{
    int result;
    int bytes;
    FSAPIAllocatorContext *ctx;
    FSAPIAllocatorContext *end;

    g_fs_api_allocator_array.count = api_ctx->
        common.shared_allocator_count;
    bytes = sizeof(FSAPIAllocatorContext) * g_fs_api_allocator_array.count;
    g_fs_api_allocator_array.allocators = (FSAPIAllocatorContext *)
        fc_malloc(bytes);
    if (g_fs_api_allocator_array.allocators == NULL) {
        return ENOMEM;
    }
    memset(g_fs_api_allocator_array.allocators, 0, bytes);

    g_fs_api_allocator_array.api_ctx = api_ctx;
    end = g_fs_api_allocator_array.allocators + g_fs_api_allocator_array.count;
    for (ctx=g_fs_api_allocator_array.allocators; ctx<end; ctx++) {
        ctx->write_combine.slice.current_version = 0;
        ctx->write_combine.slice.version_mask = ((int64_t)(ctx -
                    g_fs_api_allocator_array.allocators) + 1) << 48;
        if ((result=init_allocator_context(api_ctx, ctx)) != 0) {
            return result;
        }
    }

    return 0;
}
