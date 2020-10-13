
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

    int fs_slice_write_ex(FSSliceOpContext *op_ctx, char *buff,
            const bool reclaim_alloc);

    static inline int fs_slice_write(FSSliceOpContext *op_ctx, char *buff)
    {
        const bool reclaim_alloc = false;
        return fs_slice_write_ex(op_ctx, buff, reclaim_alloc);
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

    int fs_slice_read_ex(FSSliceOpContext *op_ctx, char *buff,
            OBSlicePtrArray *sarray);

    static inline int fs_slice_read(FSSliceOpContext *op_ctx, char *buff)
    {
        OBSlicePtrArray sarray;
        int result;

        ob_index_init_slice_ptr_array(&sarray);
        result = fs_slice_read_ex(op_ctx, buff, &sarray);
        ob_index_free_slice_ptr_array(&sarray);
        return result;
    }

    int fs_delete_slices(FSSliceOpContext *op_ctx);
    int fs_delete_block(FSSliceOpContext *op_ctx);

    int fs_log_slice_write(FSSliceOpContext *op_ctx);
    int fs_log_slice_allocate(FSSliceOpContext *op_ctx);
    int fs_log_delete_slices(FSSliceOpContext *op_ctx);
    int fs_log_delete_block(FSSliceOpContext *op_ctx);

    int fs_log_data_update(const unsigned char req_cmd,
            FSSliceOpContext *op_ctx, const int result);

#ifdef __cplusplus
}
#endif

#endif
