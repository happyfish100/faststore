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
        const char *data, int *write_bytes, int *inc_alloc)
{
    //TODO
    return fs_client_slice_write(api_ctx->fs, &op_ctx->bs_key,
            data, write_bytes, inc_alloc);
}
