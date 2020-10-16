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
#include "object_block_index.h"
#include "storage_config.h"

#ifdef __cplusplus
extern "C" {
#endif
    static inline int fs_init_slice_op_ctx(FSSliceSNPairArray *parray)
    {
        parray->alloc = FS_SLICE_SN_PARRAY_INIT_ALLOC_COUNT;
        parray->slice_sn_pairs = (FSSliceSNPair *)fc_malloc(
                sizeof(FSSliceSNPair) * parray->alloc);
        if (parray->slice_sn_pairs == NULL) {
            return ENOMEM;
        }

        return 0;
    }

    static inline void fs_free_slice_op_ctx(FSSliceSNPairArray *parray)
    {
        if (parray->slice_sn_pairs != NULL) {
            free(parray->slice_sn_pairs);
            parray->slice_sn_pairs = NULL;
            parray->alloc = 0;
        }
    }

    int fs_slice_write_ex(FSSliceOpContext *op_ctx, const bool reclaim_alloc);

    static inline int fs_slice_write(FSSliceOpContext *op_ctx)
    {
        const bool reclaim_alloc = false;
        return fs_slice_write_ex(op_ctx, reclaim_alloc);
    }

    int fs_slice_allocate_ex(FSSliceOpContext *op_ctx,
            OBSlicePtrArray *sarray);

    static inline int fs_slice_allocate(FSSliceOpContext *op_ctx)
    {
        OBSlicePtrArray sarray;
        int result;

        ob_index_init_slice_ptr_array(&sarray);
        result = fs_slice_allocate_ex(op_ctx, &sarray);
        ob_index_free_slice_ptr_array(&sarray);
        return result;
    }

    int fs_slice_read_ex(FSSliceOpContext *op_ctx, OBSlicePtrArray *sarray);

    static inline int fs_slice_read(FSSliceOpContext *op_ctx)
    {
        OBSlicePtrArray sarray;
        int result;

        ob_index_init_slice_ptr_array(&sarray);
        result = fs_slice_read_ex(op_ctx, &sarray);
        ob_index_free_slice_ptr_array(&sarray);
        return result;
    }

    int fs_delete_slices(FSSliceOpContext *op_ctx);
    int fs_delete_block(FSSliceOpContext *op_ctx);

    int fs_log_slice_write(FSSliceOpContext *op_ctx);
    int fs_log_slice_allocate(FSSliceOpContext *op_ctx);
    int fs_log_delete_slices(FSSliceOpContext *op_ctx);
    int fs_log_delete_block(FSSliceOpContext *op_ctx);

#ifdef __cplusplus
}
#endif

#endif
