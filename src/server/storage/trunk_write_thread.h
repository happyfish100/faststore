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
#include "slice_space_log.h"

#ifdef __cplusplus
extern "C" {
#endif

    static inline int trunk_write_thread_push_cached_slice(
            FSSliceOpContext *op_ctx, const int op_type,
            FSSliceSNPair *slice_sn_pair, void *data)
    {
        int result;
        int inc_alloc;
        DASliceEntry se;
        FSSliceSpaceLogRecord *record;

        if ((record=slice_space_log_alloc_init_record()) == NULL) {
            return ENOMEM;
        }

        if ((result=ob_index_add_slice_no_db(slice_sn_pair->type, &op_ctx->
                        info.bs_key.block, &slice_sn_pair->ssize,
                        op_ctx->info.data_version, slice_sn_pair,
                        op_ctx->mbuffer, &slice_sn_pair->sn,
                        &inc_alloc, &record->space_chain)) != 0)
        {
            return result;
        }

        op_ctx->update.space_changed += inc_alloc;
        op_ctx->info.sn.last = slice_sn_pair->sn;

        se.timestamp = op_ctx->update.timestamp;
        se.source = op_ctx->info.source;
        se.bs_key.block = op_ctx->info.bs_key.block;
        se.bs_key.slice = slice_sn_pair->ssize;
        se.data_version = op_ctx->info.data_version;
        se.sn = slice_sn_pair->sn;
        return da_trunk_write_thread_push_cached_slice(&DA_CTX, op_type,
                slice_sn_pair->version, &slice_sn_pair->space, data,
                &se, record);
    }

    static inline int trunk_write_thread_push_slice_by_buff(
            FSSliceOpContext *op_ctx, FSSliceSNPair *slice_sn_pair, char *buff,
            da_trunk_write_io_notify_func notify_func, void *notify_arg)
    {
        if (slice_sn_pair->type == DA_SLICE_TYPE_CACHE) {
            return trunk_write_thread_push_cached_slice(op_ctx,
                    DA_IO_TYPE_WRITE_SLICE_BY_BUFF, slice_sn_pair, buff);
        } else {
            return da_trunk_write_thread_push_slice_by_buff(&DA_CTX,
                    slice_sn_pair->version, &slice_sn_pair->space,
                    buff, notify_func, notify_arg, slice_sn_pair);
        }
    }

    static inline int trunk_write_thread_push_slice_by_iovec(FSSliceOpContext
            *op_ctx, FSSliceSNPair *slice_sn_pair, iovec_array_t *iovec_array,
            da_trunk_write_io_notify_func notify_func, void *notify_arg)
    {
        if (slice_sn_pair->type == DA_SLICE_TYPE_CACHE) {
            return trunk_write_thread_push_cached_slice(op_ctx,
                    DA_IO_TYPE_WRITE_SLICE_BY_IOVEC, slice_sn_pair,
                    iovec_array);
        } else {
            return da_trunk_write_thread_push_slice_by_iovec(&DA_CTX,
                    slice_sn_pair->version, &slice_sn_pair->space,
                    iovec_array, notify_func, notify_arg,
                    slice_sn_pair);
        }
    }

#ifdef __cplusplus
}
#endif

#endif
