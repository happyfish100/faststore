
#ifndef _SLICE_OP_H
#define _SLICE_OP_H

#include "../../common/fs_types.h"
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

    int fs_slice_read(const FSBlockSliceKeyInfo *bs_key,
            string_t *data, FSSliceOpNotify *notify);

#ifdef __cplusplus
}
#endif

#endif
