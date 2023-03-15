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

#ifndef _FS_TRUNK_WRITE_THREAD_H
#define _FS_TRUNK_WRITE_THREAD_H

#include "diskallocator/dio/trunk_write_thread.h"
#include "../binlog/binlog_types.h"

#ifdef __cplusplus
extern "C" {
#endif

    static inline int trunk_write_thread_push_cached_slice(
            FSSliceOpContext *op_ctx, const int op_type,
            const int64_t version, OBSliceEntry *slice, void *data)
    {
        int result;
        int inc_alloc;
        DASliceEntry se;

        slice->data_version = op_ctx->info.data_version;
        if ((result=ob_index_add_slice(&op_ctx->info.bs_key.block, slice,
                        &op_ctx->info.sn, &inc_alloc)) != 0)
        {
            return result;
        }
        op_ctx->update.space_changed += inc_alloc;

        se.timestamp = op_ctx->update.timestamp;
        se.source = op_ctx->info.source;
        se.bs_key.block = op_ctx->info.bs_key.block;
        se.bs_key.slice = slice->ssize;
        se.data_version = slice->data_version;
        se.sn = op_ctx->info.sn;
        return da_trunk_write_thread_push_cached_slice(&DA_CTX,
                op_type, version, &slice->space, data, &se);
    }

    static inline int trunk_write_thread_push_slice_by_buff(
            FSSliceOpContext *op_ctx, const int64_t version,
            OBSliceEntry *slice, char *buff,
            da_trunk_write_io_notify_func notify_func, void *notify_arg)
    {
        if (slice->type == DA_SLICE_TYPE_CACHE) {
            return trunk_write_thread_push_cached_slice(op_ctx,
                    DA_IO_TYPE_WRITE_SLICE_BY_BUFF, version, slice, buff);
        } else {
            return da_trunk_write_thread_push_slice_by_buff(&DA_CTX, version,
                    &slice->space, buff, notify_func, notify_arg, slice);
        }
    }

    static inline int trunk_write_thread_push_slice_by_iovec(
            FSSliceOpContext *op_ctx, const int64_t version,
            OBSliceEntry *slice, iovec_array_t *iovec_array,
            da_trunk_write_io_notify_func notify_func, void *notify_arg)
    {
        if (slice->type == DA_SLICE_TYPE_CACHE) {
            return trunk_write_thread_push_cached_slice(op_ctx,
                    DA_IO_TYPE_WRITE_SLICE_BY_IOVEC, version,
                    slice, iovec_array);
        } else {
            return da_trunk_write_thread_push_slice_by_iovec(&DA_CTX, version,
                    &slice->space, iovec_array, notify_func, notify_arg, slice);
        }
    }

#ifdef __cplusplus
}
#endif

#endif
