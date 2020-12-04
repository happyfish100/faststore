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

#define FS_API_MIN_WRITE_COMBINE_BUFFER_SIZE      ( 64 * 1024)
#define FS_API_MAX_WRITE_COMBINE_BUFFER_SIZE     FS_FILE_BLOCK_SIZE
#define FS_API_DEFAULT_WRITE_COMBINE_BUFFER_SIZE  (256 * 1024)

#define FS_API_MIN_WRITE_COMBINE_MIN_WAIT_TIME        10
#define FS_API_MAX_WRITE_COMBINE_MIN_WAIT_TIME       100
#define FS_API_DEFAULT_WRITE_COMBINE_MIN_WAIT_TIME    20

#define FS_API_MIN_WRITE_COMBINE_MAX_WAIT_TIME       100
#define FS_API_MAX_WRITE_COMBINE_MAX_WAIT_TIME     10000
#define FS_API_DEFAULT_WRITE_COMBINE_MAX_WAIT_TIME  1000

#define FS_API_MIN_SKIP_COMBINE_ON_SLICE_SIZE        ( 64 * 1024)
#define FS_API_MAX_SKIP_COMBINE_ON_SLICE_SIZE      FS_FILE_BLOCK_SIZE
#define FS_API_DEFAULT_SKIP_COMBINE_ON_SLICE_SIZE    (256 * 1024)

#define FS_API_DEFAULT_SKIP_COMBINE_ON_LAST_MERGED_SLICES  1

#define FS_API_MIN_TIMER_SHARED_LOCK_COUNT           1
#define FS_API_MAX_TIMER_SHARED_LOCK_COUNT     1000000
#define FS_API_DEFAULT_TIMER_SHARED_LOCK_COUNT     163

#define FS_API_MIN_SHARED_ALLOCATOR_COUNT           1
#define FS_API_MAX_SHARED_ALLOCATOR_COUNT        1000
#define FS_API_DEFAULT_SHARED_ALLOCATOR_COUNT      17

#define FS_API_MIN_HASHTABLE_SHARDING_COUNT           1
#define FS_API_MAX_HASHTABLE_SHARDING_COUNT       10000
#define FS_API_DEFAULT_HASHTABLE_SHARDING_COUNT     163

#define FS_API_MIN_HASHTABLE_TOTAL_CAPACITY          10949
#define FS_API_MAX_HASHTABLE_TOTAL_CAPACITY      100000000
#define FS_API_DEFAULT_HASHTABLE_TOTAL_CAPACITY    1403641

#define FS_API_MIN_THREAD_POOL_MAX_THREADS            1
#define FS_API_MAX_THREAD_POOL_MAX_THREADS         1024
#define FS_API_DEFAULT_THREAD_POOL_MAX_THREADS       16

#define FS_API_MIN_THREAD_POOL_MIN_IDLE_COUNT         0
#define FS_API_MAX_THREAD_POOL_MIN_IDLE_COUNT        64
#define FS_API_DEFAULT_THREAD_POOL_MIN_IDLE_COUNT     2

#define FS_API_MIN_THREAD_POOL_MAX_IDLE_TIME          0
#define FS_API_MAX_THREAD_POOL_MAX_IDLE_TIME      86400
#define FS_API_DEFAULT_THREAD_POOL_MAX_IDLE_TIME    300

static int fs_api_config_load(FSAPIContext *api_ctx, IniFullContext *ini_ctx)
{
    api_ctx->write_combine.enabled = iniGetBoolValue(ini_ctx->section_name,
            "enabled", ini_ctx->context, true);

    api_ctx->write_combine.buffer_size = iniGetByteCorrectValue(ini_ctx,
            "buffer_size", FS_API_DEFAULT_WRITE_COMBINE_BUFFER_SIZE,
            FS_API_MIN_WRITE_COMBINE_BUFFER_SIZE,
            FS_API_MAX_WRITE_COMBINE_BUFFER_SIZE);

    api_ctx->write_combine.min_wait_time_ms = iniGetIntCorrectValue(ini_ctx,
            "min_wait_time_ms", FS_API_DEFAULT_WRITE_COMBINE_MIN_WAIT_TIME,
            FS_API_MIN_WRITE_COMBINE_MIN_WAIT_TIME,
            FS_API_MAX_WRITE_COMBINE_MIN_WAIT_TIME);

    api_ctx->write_combine.max_wait_time_ms = iniGetIntCorrectValue(ini_ctx,
            "max_wait_time_ms", FS_API_DEFAULT_WRITE_COMBINE_MAX_WAIT_TIME,
            FS_API_MIN_WRITE_COMBINE_MAX_WAIT_TIME,
            FS_API_MAX_WRITE_COMBINE_MAX_WAIT_TIME);

    api_ctx->write_combine.skip_combine_on_slice_size = iniGetByteCorrectValue(
            ini_ctx, "skip_combine_on_slice_size",
            FS_API_DEFAULT_SKIP_COMBINE_ON_SLICE_SIZE,
            FS_API_MIN_SKIP_COMBINE_ON_SLICE_SIZE,
            FS_API_MAX_SKIP_COMBINE_ON_SLICE_SIZE);

    api_ctx->write_combine.skip_combine_on_last_merged_slices = iniGetIntValue(
            ini_ctx->section_name, "skip_combine_on_last_merged_slices",
            ini_ctx->context, FS_API_DEFAULT_SKIP_COMBINE_ON_LAST_MERGED_SLICES);

    api_ctx->write_combine.timer_shared_lock_count = iniGetIntCorrectValue(
            ini_ctx, "timer_shared_lock_count",
            FS_API_DEFAULT_TIMER_SHARED_LOCK_COUNT,
            FS_API_MIN_TIMER_SHARED_LOCK_COUNT,
            FS_API_MAX_TIMER_SHARED_LOCK_COUNT);

    api_ctx->write_combine.shared_allocator_count = iniGetIntCorrectValue(
            ini_ctx, "shared_allocator_count",
            FS_API_DEFAULT_SHARED_ALLOCATOR_COUNT,
            FS_API_MIN_SHARED_ALLOCATOR_COUNT,
            FS_API_MAX_SHARED_ALLOCATOR_COUNT);

    api_ctx->write_combine.hashtable_sharding_count = iniGetIntCorrectValue(
            ini_ctx, "hashtable_sharding_count",
            FS_API_DEFAULT_HASHTABLE_SHARDING_COUNT,
            FS_API_MIN_HASHTABLE_SHARDING_COUNT,
            FS_API_MAX_HASHTABLE_SHARDING_COUNT);

    api_ctx->write_combine.hashtable_total_capacity = iniGetInt64CorrectValue(
            ini_ctx, "hashtable_total_capacity",
            FS_API_DEFAULT_HASHTABLE_TOTAL_CAPACITY,
            FS_API_MIN_HASHTABLE_TOTAL_CAPACITY,
            FS_API_MAX_HASHTABLE_TOTAL_CAPACITY);

    api_ctx->write_combine.thread_pool_max_threads = iniGetIntCorrectValue(
            ini_ctx, "thread_pool_max_threads",
            FS_API_DEFAULT_THREAD_POOL_MAX_THREADS,
            FS_API_MIN_THREAD_POOL_MAX_THREADS,
            FS_API_MAX_THREAD_POOL_MAX_THREADS);

    api_ctx->write_combine.thread_pool_min_idle_count = iniGetIntCorrectValue(
            ini_ctx, "thread_pool_min_idle_count",
            FS_API_DEFAULT_THREAD_POOL_MIN_IDLE_COUNT,
            FS_API_MIN_THREAD_POOL_MIN_IDLE_COUNT,
            FS_API_MAX_THREAD_POOL_MIN_IDLE_COUNT);

    api_ctx->write_combine.thread_pool_max_idle_time = iniGetIntCorrectValue(
            ini_ctx, "thread_pool_max_idle_time",
            FS_API_DEFAULT_THREAD_POOL_MAX_IDLE_TIME,
            FS_API_MIN_THREAD_POOL_MAX_IDLE_TIME,
            FS_API_MAX_THREAD_POOL_MAX_IDLE_TIME);

    return 0;
}

void fs_api_config_to_string_ex(FSAPIContext *api_ctx,
        char *output, const int size)
{
    int len;

    len = snprintf(output, size, "write_combine enabled: %d",
            api_ctx->write_combine.enabled);
    if (!api_ctx->write_combine.enabled) {
        return;
    }

    snprintf(output + len, size - len, ", "
            "buffer_size=%d KB, "
            "min_wait_time_ms=%d ms, "
            "max_wait_time_ms=%d ms, "
            "skip_combine_on_slice_size=%d KB, "
            "skip_combine_on_last_merged_slices=%d, "
            "timer_shared_lock_count=%d, "
            "shared_allocator_count=%d, "
            "hashtable_sharding_count=%d, "
            "hashtable_total_capacity=%"PRId64", "
            "thread_pool_max_threads=%d, "
            "thread_pool_min_idle_count=%d, "
            "thread_pool_max_idle_time=%d s",
            api_ctx->write_combine.buffer_size / 1024,
            api_ctx->write_combine.min_wait_time_ms,
            api_ctx->write_combine.max_wait_time_ms,
            api_ctx->write_combine.skip_combine_on_slice_size / 1024,
            api_ctx->write_combine.skip_combine_on_last_merged_slices,
            api_ctx->write_combine.timer_shared_lock_count,
            api_ctx->write_combine.shared_allocator_count,
            api_ctx->write_combine.hashtable_sharding_count,
            api_ctx->write_combine.hashtable_total_capacity,
            api_ctx->write_combine.thread_pool_max_threads,
            api_ctx->write_combine.thread_pool_min_idle_count,
            api_ctx->write_combine.thread_pool_max_idle_time);
}

int fs_api_init_ex(FSAPIContext *api_ctx, IniFullContext *ini_ctx,
        fs_api_write_done_callback write_done_callback,
        const int write_done_arg_extra_size)
{
    const int precision_ms = 10;
    int64_t element_limit = 1000 * 1000;
    const int64_t min_ttl_ms = 600 * 1000;
    const int64_t max_ttl_ms = 86400 * 1000;
    int result;

    if ((result=fs_api_config_load(api_ctx, ini_ctx)) != 0) {
        return result;
    }
    if (!api_ctx->write_combine.enabled) {
        return 0;
    }

    api_ctx->write_done_callback.func = write_done_callback;
    api_ctx->write_done_callback.arg_extra_size = write_done_arg_extra_size;
    g_timer_ms_ctx.current_time_ms = get_current_time_ms();
    if ((result=timeout_handler_init(precision_ms, api_ctx->write_combine.
                    max_wait_time_ms, api_ctx->write_combine.
                    timer_shared_lock_count)) != 0)
    {
        return result;
    }

    if ((result=fs_api_allocator_init(api_ctx)) != 0) {
        return result;
    }

    if ((result=otid_htable_init(api_ctx->write_combine.
                    hashtable_sharding_count, api_ctx->write_combine.
                    hashtable_total_capacity, api_ctx->write_combine.
                    shared_allocator_count, element_limit, min_ttl_ms,
                    max_ttl_ms)) != 0)
    {
        return result;
    }

    if ((result=obid_htable_init(api_ctx->write_combine.
                    hashtable_sharding_count, api_ctx->write_combine.
                    hashtable_total_capacity, api_ctx->write_combine.
                    shared_allocator_count, element_limit, min_ttl_ms,
                    max_ttl_ms)) != 0)
    {
        return result;
    }

    return 0;
}

int fs_api_start_ex(FSAPIContext *api_ctx)
{
    int result;

    if (!api_ctx->write_combine.enabled) {
        return 0;
    }

    if ((result=timeout_handler_start()) != 0) {
        return result;
    }

    if ((result=combine_handler_init(api_ctx->write_combine.
                    thread_pool_max_threads, api_ctx->write_combine.
                    thread_pool_min_idle_count, api_ctx->write_combine.
                    thread_pool_max_idle_time)) != 0)
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
    if (api_ctx->write_combine.enabled) {
        api_ctx->write_combine.enabled = false;
        timeout_handler_terminate();
        combine_handler_terminate();
    }
}

#define FS_API_SET_BID_AND_ALLOCATOR_CTX(op_ctx) \
    op_ctx->bid = op_ctx->bs_key.block.offset;   \
    op_ctx->allocator_ctx = fs_api_allocator_get(op_ctx->tid)

#define FS_API_CHECK_CONFLICT_AND_WAIT(op_ctx) \
    do {  \
        if (op_ctx->api_ctx->write_combine.enabled) {  \
            int conflict_count;  \
            FS_API_SET_BID_AND_ALLOCATOR_CTX(op_ctx);  \
            obid_htable_check_conflict_and_wait(op_ctx, &conflict_count); \
        } \
    } while (0)

int fs_api_slice_write(FSAPIOperationContext *op_ctx,
        FSAPIWriteBuffer *wbuffer, int *write_bytes, int *inc_alloc)
{
    int result;
    int conflict_count;

    do {
        if (!op_ctx->api_ctx->write_combine.enabled) {
            wbuffer->combined = false;
            break;
        }

        FS_API_SET_BID_AND_ALLOCATOR_CTX(op_ctx);
        if ((result=obid_htable_check_conflict_and_wait(
                        op_ctx, &conflict_count)) != 0)
        {
            wbuffer->combined = false;
            break;
        }
        if (conflict_count > 0) {
            wbuffer->combined = false;
            break;
        }

        if ((result=otid_htable_insert(op_ctx, wbuffer)) != 0) {
            break;
        }

        if (wbuffer->combined) {  //already trigger write combine
            *write_bytes = op_ctx->bs_key.slice.length;
            *inc_alloc = 0;
            return 0;
        }
    } while (0);

    return fs_client_slice_write(op_ctx->api_ctx->fs, &op_ctx->bs_key,
            wbuffer->buff, write_bytes, inc_alloc);
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

    FS_API_SET_CTX_AND_TID_EX(op_ctx, api_ctx, tid);
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
