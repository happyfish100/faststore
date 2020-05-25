
#ifndef _SLICE_OP_H
#define _SLICE_OP_H

#include "../../common/fs_types.h"
#include "object_block_index.h"
#include "storage_config.h"

#ifdef __cplusplus
extern "C" {
#endif

    int fs_slice_write_ex(const FSBlockSliceKeyInfo *bs_key, char *buff,
            FSSliceOpNotify *notify, const bool reclaim_alloc);

    static inline int fs_slice_write(const FSBlockSliceKeyInfo *bs_key,
            char *buff, FSSliceOpNotify *notify)
    {
        const bool reclaim_alloc = false;
        return fs_slice_write_ex(bs_key, buff, notify, reclaim_alloc);
    }

    int fs_slice_allocate_ex(const FSBlockSliceKeyInfo *bs_key,
            const bool reclaim_alloc, int *inc_alloc);

    static inline int fs_slice_allocate(const FSBlockSliceKeyInfo *bs_key,
            int *inc_alloc)
    {
        const bool reclaim_alloc = false;
        return fs_slice_allocate_ex(bs_key, reclaim_alloc, inc_alloc);
    }

    int fs_slice_read_ex(const FSBlockSliceKeyInfo *bs_key, char *buff,
            FSSliceOpNotify *notify, OBSlicePtrArray *sarray);

    static inline int fs_slice_read(const FSBlockSliceKeyInfo *bs_key,
            char *buff, FSSliceOpNotify *notify)
    {
        OBSlicePtrArray sarray;
        int result;

        ob_index_init_slice_ptr_array(&sarray);
        result = fs_slice_read_ex(bs_key, buff, notify, &sarray);
        ob_index_free_slice_ptr_array(&sarray);
        return result;
    }

#ifdef __cplusplus
}
#endif

#endif
