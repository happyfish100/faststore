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


#ifndef _SLICE_OP_H
#define _SLICE_OP_H

#include "../../common/fs_types.h"

#ifdef OS_LINUX
#include "../dio/read_buffer_pool.h"
#endif

#include "object_block_index.h"
#include "storage_config.h"

#ifdef __cplusplus
extern "C" {
#endif

    static inline int fs_slice_array_init(FSSliceSNPairArray *parray)
    {
        parray->alloc = FS_SLICE_SN_PARRAY_INIT_ALLOC_COUNT;
        parray->slice_sn_pairs = (FSSliceSNPair *)fc_malloc(
                sizeof(FSSliceSNPair) * parray->alloc);
        if (parray->slice_sn_pairs == NULL) {
            return ENOMEM;
        }

        return 0;
    }

    static inline void fs_slice_array_destroy(FSSliceSNPairArray *parray)
    {
        if (parray->slice_sn_pairs != NULL) {
            free(parray->slice_sn_pairs);
            parray->slice_sn_pairs = NULL;
            parray->alloc = 0;
        }
    }

    static inline void fs_slice_array_release(FSSliceSNPairArray *array)
    {
        FSSliceSNPair *slice_sn_pair;
        FSSliceSNPair *slice_sn_end;

        slice_sn_end = array->slice_sn_pairs + array->count;
        for (slice_sn_pair=array->slice_sn_pairs;
                slice_sn_pair<slice_sn_end; slice_sn_pair++)
        {
            ob_index_free_slice(slice_sn_pair->slice);
        }
        array->count = 0;
    }

    static inline void fs_log_rw_error(FSSliceOpContext *op_ctx,
            const int result, const int ignore_errno, const char *caption)
    {
        int log_level;
        log_level = (result == ignore_errno) ? LOG_DEBUG : LOG_ERR;
        log_it_ex(&g_log_context, log_level,
                "file: "__FILE__", line: %d, %s slice fail, "
                "oid: %"PRId64", block offset: %"PRId64", "
                "slice offset: %d, length: %d, "
                "errno: %d, error info: %s", __LINE__, caption,
                op_ctx->info.bs_key.block.oid,
                op_ctx->info.bs_key.block.offset,
                op_ctx->info.bs_key.slice.offset,
                op_ctx->info.bs_key.slice.length,
                result, STRERROR(result));
    }


#ifdef OS_LINUX
    static inline void fs_release_aio_buffers(FSSliceOpContext *op_ctx)
    {
        AlignedReadBuffer **aligned_buffer;
        AlignedReadBuffer **aligned_bend;

        aligned_bend = op_ctx->aio_buffer_parray.buffers +
            op_ctx->aio_buffer_parray.count;
        for (aligned_buffer=op_ctx->aio_buffer_parray.buffers;
                aligned_buffer<aligned_bend; aligned_buffer++)
        {
            read_buffer_pool_free(*aligned_buffer);
        }

        op_ctx->aio_buffer_parray.count = 0;
    }

    void fs_release_task_aio_buffers(struct fast_task_info *task);
    int fs_slice_read(FSSliceOpContext *op_ctx);
#else
#define fs_slice_read(op_ctx) fs_slice_normal_read(op_ctx)
#endif

    int fs_slice_write(FSSliceOpContext *op_ctx);
    void fs_write_finish(FSSliceOpContext *op_ctx);

    int fs_slice_allocate(FSSliceOpContext *op_ctx);

    int fs_slice_normal_read(FSSliceOpContext *op_ctx);

    int fs_delete_slices(FSSliceOpContext *op_ctx);
    int fs_delete_block(FSSliceOpContext *op_ctx);

    int fs_log_slice_write(FSSliceOpContext *op_ctx);
    int fs_log_slice_allocate(FSSliceOpContext *op_ctx);
    int fs_log_delete_slices(FSSliceOpContext *op_ctx);
    int fs_log_delete_block(FSSliceOpContext *op_ctx);

    int fs_slice_blocked_op_ctx_init(FSSliceBlockedOpContext *bctx);

    void fs_slice_blocked_op_ctx_destroy(FSSliceBlockedOpContext *bctx);

    int fs_slice_blocked_read(FSSliceBlockedOpContext *bctx,
            FSBlockSliceKeyInfo *bs_key, const int ignore_errno);

#ifdef __cplusplus
}
#endif

#endif
