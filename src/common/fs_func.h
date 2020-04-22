
#ifndef _FS_FUNC_H
#define _FS_FUNC_H

#include "fs_types.h"

#ifdef __cplusplus
extern "C" {
#endif

    static inline void fs_calc_block_hashcode(FSBlockKey *bkey)
    {
        bkey->hash_code = bkey->inode + (bkey->offset / FS_FILE_BLOCK_SIZE);
    }

#ifdef __cplusplus
}
#endif

#endif
