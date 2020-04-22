
#ifndef _SLICE_OP_H
#define _SLICE_OP_H

#include "../../common/fs_types.h"
#include "storage_config.h"

#ifdef __cplusplus
extern "C" {
#endif

    int fs_slice_write_ex(const FSBlockKey *bkey, const int slice_offset,
            string_t *data, FSSliceOpNotify *notify, const bool reclaim_alloc);

    static inline int fs_slice_write(const FSBlockKey *bkey,
            const int slice_offset, string_t *data, FSSliceOpNotify *notify)
    {
        const bool reclaim_alloc = false;
        return fs_slice_write_ex(bkey, slice_offset, data,
                notify, reclaim_alloc);
    }

    int fs_slice_read(const FSBlockKey *bkey, const int slice_offset,
            const int length, string_t *data, FSSliceOpNotify *notify);

#ifdef __cplusplus
}
#endif

#endif
