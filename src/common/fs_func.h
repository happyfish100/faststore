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


#ifndef _FS_FUNC_H
#define _FS_FUNC_H

#include "fastcommon/hash.h"
#include "fastcommon/logger.h"
#include "fs_types.h"

#ifdef __cplusplus
extern "C" {
#endif

    static inline void fs_calc_block_hashcode(FSBlockKey *bkey,
            const int file_block_size, const bool use_hash_func)
    {
        if (use_hash_func) {
            struct {
                int64_t oid;
                int64_t offset;
            } hkey;

            hkey.oid = bkey->oid;
            hkey.offset = bkey->offset;
            bkey->hash_code = fc_simple_hash(&hkey, sizeof(hkey));
        } else {
            bkey->hash_code = bkey->oid + bkey->offset +
                (bkey->offset / file_block_size);
        }
    }

    static inline void fs_fill_padding_bkey(const int data_group_id,
            FSBlockKey *bkey)
    {
        bkey->oid = data_group_id - 1;
        bkey->offset = 0;
    }

    static inline bool fs_slice_is_overlap(const FSSliceSize *s1,
        const FSSliceSize *s2)
    {
        if (s1->offset < s2->offset) {
            return s1->offset + s1->length > s2->offset;
        } else {
            return s2->offset + s2->length > s1->offset;
        }
    }

    static inline int fs_rmdir(const char *filepath)
    {
        int result;

        if (rmdir(filepath) < 0) {
            result = errno != 0 ? errno : EPERM;
            if (result != ENOTEMPTY) {
                logError("file: "__FILE__", line: %d, "
                        "rmdir %s fail, errno: %d, error info: %s",
                        __LINE__, filepath, result, STRERROR(result));
                return result;
            }
        }

        return 0;
    }

#ifdef __cplusplus
}
#endif

#endif
