
#ifndef _SLICE_OP_H
#define _SLICE_OP_H

#include "../../common/fs_types.h"
#include "object_block_index.h"
#include "storage_config.h"

#ifdef __cplusplus
extern "C" {
#endif

    int fs_slice_write_ex(FSSliceOpContext *op_ctx, char *buff,
            const bool reclaim_alloc);

    static inline int fs_slice_write(FSSliceOpContext *op_ctx, char *buff)
    {
        const bool reclaim_alloc = false;
        return fs_slice_write_ex(op_ctx, buff, reclaim_alloc);
    }

    int fs_slice_allocate_ex(FSSliceOpContext *op_ctx,
            OBSlicePtrArray *sarray, int *inc_alloc);

    static inline int fs_slice_allocate(FSSliceOpContext *op_ctx,
            int *inc_alloc)
    {
        OBSlicePtrArray sarray;
        int result;

        ob_index_init_slice_ptr_array(&sarray);
        result = fs_slice_allocate_ex(op_ctx, &sarray, inc_alloc);
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

    int fs_delete_slices(FSSliceOpContext *op_ctx, int *dec_alloc);

    int fs_delete_block(FSSliceOpContext *op_ctx, int *dec_alloc);

#ifdef __cplusplus
}
#endif

#endif
