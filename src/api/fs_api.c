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
#include "fs_api.h"

FSAPIContext g_fs_api_ctx;

int fs_api_slice_write(FSAPIContext *api_ctx, FSAPIOperationContext *op_ctx,
        const char *buff, int *write_bytes, int *inc_alloc)
{
    int result;
    int conflict_count;
    int successive_count;

    op_ctx->bid = op_ctx->bs_key.block.offset / FS_FILE_BLOCK_SIZE;
    op_ctx->allocator_ctx = fs_api_allocator_get(op_ctx->tid);
    if ((result=obid_htable_check_conflict_and_wait(
                    op_ctx, &conflict_count)) != 0)
    {
        return result;
    }

    if (conflict_count > 0) {
        return fs_client_slice_write(api_ctx->fs, &op_ctx->bs_key,
                buff, write_bytes, inc_alloc);
    }

    if ((result=otid_htable_insert(op_ctx, buff, &successive_count)) != 0) {
        return result;
    }

    if (successive_count > 0) {  //already trigger write combine
        *write_bytes = op_ctx->bs_key.slice.length;
        *inc_alloc = 0;  //TODO
        return 0;
    }

    return fs_client_slice_write(api_ctx->fs, &op_ctx->bs_key,
            buff, write_bytes, inc_alloc);
}
