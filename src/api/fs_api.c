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
#include "timeout_handler.h"
#include "combine_handler.h"
#include "fs_api.h"

FSAPIContext g_fs_api_ctx;

int fs_api_init_ex(FSAPIContext *api_ctx, IniFullContext *ini_ctx)
{
    const int precision_ms = 10;
    const int max_timeout_ms = 10000;
    const int shared_lock_count = 163;
    const int allocator_count = 16;
    int64_t element_limit = 1000 * 1000;
    const int sharding_count = 163;
    const int64_t htable_capacity = 1403641;
    const int64_t min_ttl_ms = 600 * 1000;
    const int64_t max_ttl_ms = 86400 * 1000;
    int result;

    //TODO
    api_ctx->write_combine.enabled = true;

    g_timer_ms_ctx.current_time_ms = get_current_time_ms();
        if ((result=timeout_handler_init(precision_ms, max_timeout_ms,
                    shared_lock_count)) != 0)
    {
        return result;
    }

    if ((result=fs_api_allocator_init()) != 0) {
        return result;
    }

    if ((result=otid_htable_init(sharding_count, htable_capacity,
                    allocator_count, element_limit,
                    min_ttl_ms, max_ttl_ms)) != 0)
    {
        return result;
    }

    if ((result=obid_htable_init(sharding_count, htable_capacity,
                    allocator_count, element_limit,
                    min_ttl_ms, max_ttl_ms)) != 0)
    {
        return result;
    }


    api_ctx->write_combine.min_wait_time_ms = 10;
    api_ctx->write_combine.max_wait_time_ms = 1000;
    api_ctx->write_combine.skip_combine_on_slice_size = 256 * 1024;
    api_ctx->write_combine.skip_combine_on_last_merged_slices = 1;

    return 0;
}

int fs_api_combine_thread_start_ex(FSAPIContext *api_ctx)
{
    int result;
    const int thread_limit = 16;
    const int min_idle_count = 4;
    const int max_idle_time = 300;

    if (!api_ctx->write_combine.enabled) {
        return 0;
    }

    if ((result=combine_handler_init(thread_limit, min_idle_count,
                    max_idle_time)) != 0)
    {
        return result;
    }

    return 0;
}

void fs_api_destroy_ex(FSAPIContext *api_ctx)
{
}

void fs_api_terminate_ex(FSAPIContext *api_ctx)
{
    api_ctx->write_combine.enabled = false;
    timeout_handler_terminate();
    combine_handler_terminate();
}

#define FS_API_SET_BID_AND_ALLOCATOR_CTX(op_ctx) \
    op_ctx->bid = op_ctx->bs_key.block.offset / FS_FILE_BLOCK_SIZE; \
    op_ctx->allocator_ctx = fs_api_allocator_get(op_ctx->tid)

#define FS_API_CHECK_CONFLICT_AND_WAIT(op_ctx) \
    do {  \
        if (op_ctx->api_ctx->write_combine.enabled) {  \
            int conflict_count;  \
            FS_API_SET_BID_AND_ALLOCATOR_CTX(op_ctx);  \
            obid_htable_check_conflict_and_wait(op_ctx, &conflict_count); \
        } \
    } while (0)

int fs_api_slice_write(FSAPIOperationContext *op_ctx, const char *buff,
        bool *combined, int *write_bytes, int *inc_alloc)
{
    int result;
    int conflict_count;

    do {
        if (!op_ctx->api_ctx->write_combine.enabled) {
            *combined = false;
            break;
        }

        FS_API_SET_BID_AND_ALLOCATOR_CTX(op_ctx);
        if ((result=obid_htable_check_conflict_and_wait(
                        op_ctx, &conflict_count)) != 0)
        {
            *combined = false;
            break;
        }
        if (conflict_count > 0) {
            *combined = false;
            break;
        }

        if ((result=otid_htable_insert(op_ctx, buff, combined)) != 0) {
            break;
        }

        if (*combined) {  //already trigger write combine
            *write_bytes = op_ctx->bs_key.slice.length;
            *inc_alloc = 0;
            return 0;
        }
    } while (0);

    return fs_client_slice_write(op_ctx->api_ctx->fs,
            &op_ctx->bs_key, buff, write_bytes, inc_alloc);
}

int fs_api_slice_read(FSAPIOperationContext *op_ctx,
        char *buff, int *read_bytes)
{
    FS_API_CHECK_CONFLICT_AND_WAIT(op_ctx);
    return fs_client_slice_read(op_ctx->api_ctx->fs,
            &op_ctx->bs_key, buff, read_bytes);
}

int fs_api_slice_allocate_ex(FSAPIOperationContext *op_ctx,
        const int enoent_log_level, int *inc_alloc)
{
    FS_API_CHECK_CONFLICT_AND_WAIT(op_ctx);
    return fs_client_slice_allocate_ex(op_ctx->api_ctx->fs,
            &op_ctx->bs_key, enoent_log_level, inc_alloc);
}

int fs_api_slice_delete_ex(FSAPIOperationContext *op_ctx,
        const int enoent_log_level, int *dec_alloc)
{
    FS_API_CHECK_CONFLICT_AND_WAIT(op_ctx);
    return fs_client_slice_delete_ex(op_ctx->api_ctx->fs,
            &op_ctx->bs_key, enoent_log_level, dec_alloc);
}

int fs_api_block_delete_ex(FSAPIOperationContext *op_ctx,
        const int enoent_log_level, int *dec_alloc)
{
    FS_API_CHECK_CONFLICT_AND_WAIT(op_ctx);
    return fs_client_block_delete_ex(op_ctx->api_ctx->fs,
            &op_ctx->bs_key.block, enoent_log_level, dec_alloc);
}

int fs_api_unlink_file(FSAPIContext *api_ctx, const int64_t oid,
        const int64_t file_size, const uint64_t tid)
{
    FSAPIOperationContext op_ctx;
    int64_t remain;
    int result;
    int dec_alloc;

    if (file_size == 0) {
        return 0;
    }

    if (api_ctx->write_combine.enabled) {
        FS_API_SET_CTX_AND_TID_EX(op_ctx, api_ctx, tid);
    }

    op_ctx.bs_key.slice.offset = 0;
    op_ctx.bs_key.slice.length = FS_FILE_BLOCK_SIZE;
    fs_set_block_key(&op_ctx.bs_key.block, oid, 0);
    remain = file_size;
    while (1) {
        /*
        logInfo("block {oid: %"PRId64", offset: %"PRId64"}",
                bkey.oid, bkey.offset);
                */

        result = fs_api_block_delete(&op_ctx, &dec_alloc);
        if (result == ENOENT) {
            result = 0;
        } else if (result != 0) {
            break;
        }

        remain -= FS_FILE_BLOCK_SIZE;
        if (remain <= 0) {
            break;
        }

        fs_next_block_key(&op_ctx.bs_key.block);
    }

    return result;
}
