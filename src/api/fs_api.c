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
    //TODO
    api_ctx->write_combine.enabled = true;
    api_ctx->write_combine.min_wait_time_ms = 10;
    api_ctx->write_combine.max_wait_time_ms = 1000;
    api_ctx->write_combine.skip_combine_on_slice_size = 256 * 1024;
    api_ctx->write_combine.skip_combine_on_last_merged_slices = 1;

    return 0;
}

void fs_api_terminate_ex(FSAPIContext *api_ctx)
{
    api_ctx->write_combine.enabled = false;
    timeout_handler_terminate();
    combine_handler_terminate();
}

int fs_api_slice_write(FSAPIOperationContext *op_ctx,
        const char *buff, int *write_bytes, int *inc_alloc)
{
    int result;
    int conflict_count;
    bool combined;

    op_ctx->bid = op_ctx->bs_key.block.offset / FS_FILE_BLOCK_SIZE;
    op_ctx->allocator_ctx = fs_api_allocator_get(op_ctx->tid);
    if ((result=obid_htable_check_conflict_and_wait(
                    op_ctx, &conflict_count)) != 0)
    {
        return result;
    }

    if (conflict_count > 0) {
        return fs_client_slice_write(op_ctx->api_ctx->fs,
                &op_ctx->bs_key, buff, write_bytes, inc_alloc);
    }

    if ((result=otid_htable_insert(op_ctx, buff, &combined)) != 0) {
        return fs_client_slice_write(op_ctx->api_ctx->fs,
                &op_ctx->bs_key, buff, write_bytes, inc_alloc);
    }

    if (combined) {  //already trigger write combine
        *write_bytes = op_ctx->bs_key.slice.length;
        *inc_alloc = 0;  //TODO
        return 0;
    }

    return fs_client_slice_write(op_ctx->api_ctx->fs,
            &op_ctx->bs_key, buff, write_bytes, inc_alloc);
}
