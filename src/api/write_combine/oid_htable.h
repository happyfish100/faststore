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

#ifndef _WRITE_COMBINE_OID_HTABLE_H
#define _WRITE_COMBINE_OID_HTABLE_H

#include "sf/sf_sharding_htable.h"
#include "../fs_api_types.h"
#include "obid_htable.h"

#define FS_WCOMBINE_ID_ARRAY_FIXED_SIZE  128

typedef struct fs_wcombine_oid_entry {
    SFShardingHashEntry hentry;  //must be the first
    struct fc_list_head writing_head;   //element: FSAPIBlockEntry
} FSWCombineOIDEntry;

typedef struct fs_wcombine_id_array {
    int64_t fixed[FS_WCOMBINE_ID_ARRAY_FIXED_SIZE];
    int64_t *elts;
    int count;
    int alloc;
} FSWCombineIDArray;

#ifdef __cplusplus
extern "C" {
#endif

    int wcombine_oid_htable_init(const int sharding_count,
            const int64_t htable_capacity,
            const int allocator_count, int64_t element_limit,
            const int64_t min_ttl_ms, const int64_t max_ttl_ms,
            const double low_water_mark_ratio);

    int wcombine_oid_htable_insert(FSAPIBlockEntry *block);

    int wcombine_oid_htable_find(const int64_t oid, FSWCombineIDArray *array);

    int wcombine_oid_htable_delete(FSAPIBlockEntry *block);

    static inline void wcombine_id_array_init(FSWCombineIDArray *array)
    {
        array->elts = array->fixed;
        array->alloc = FS_WCOMBINE_ID_ARRAY_FIXED_SIZE;
        array->count = 0;
    }

    static inline void wcombine_id_array_destroy(FSWCombineIDArray *array)
    {
        if (array->elts != array->fixed) {
            free(array->elts);
        }
    }

#ifdef __cplusplus
}
#endif

#endif
