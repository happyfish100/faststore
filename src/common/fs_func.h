
#ifndef _FS_FUNC_H
#define _FS_FUNC_H

#include "fastcommon/hash.h"
#include "fs_types.h"

#ifdef __cplusplus
extern "C" {
#endif

    static inline void fs_calc_block_hashcode(FSBlockKey *bkey)
    {
        bkey->hash.codes[FS_BLOCK_HASH_CODE_INDEX_DATA_GROUP] =
            bkey->inode + (bkey->offset / FS_FILE_BLOCK_SIZE);
        bkey->hash.codes[FS_BLOCK_HASH_CODE_INDEX_SERVER] =
            simple_hash((void *)bkey, 16);
    }

#ifdef __cplusplus
}
#endif

#endif
