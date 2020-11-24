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

#ifndef _OBID_HTABLE_H
#define _OBID_HTABLE_H

#include "fs_api_types.h"
#include "sharding_htable.h"

typedef struct fs_api_block_entry {
    FSAPIHashEntry hentry; //must be the first
    struct {
        struct fc_list_head head;  //element: FSAPISliceEntry
    } slices;
} FSAPIBlockEntry;

#ifdef __cplusplus
extern "C" {
#endif

    int obid_htable_init(const int sharding_count,
            const int64_t htable_capacity, const int allocator_count,
            int64_t element_limit, const int64_t min_ttl_ms,
            const int64_t max_ttl_ms);

    int obid_htable_insert(FSAPIOperationContext *op_ctx,
            FSAPISliceEntry *slice);

    int obid_htable_check_conflict_and_wait(FSAPIOperationContext *op_ctx,
            int *conflict_count);

    static inline void fs_api_set_slice_stage(FSAPISliceEntry *slice,
            const int stage)
    {
        PTHREAD_MUTEX_LOCK(&slice->block->hentry.sharding->lock);
        slice->stage = stage;
        PTHREAD_MUTEX_UNLOCK(&slice->block->hentry.sharding->lock);
    }

    static inline int fs_api_swap_slice_stage(FSAPISliceEntry *slice,
            const int old_stage, const int new_stage)
    {
        int result;
        PTHREAD_MUTEX_LOCK(&slice->block->hentry.sharding->lock);
        if (slice->stage == old_stage) {
            slice->stage = new_stage;
            result = 0;
        } else {
            result = EEXIST;
        }
        PTHREAD_MUTEX_UNLOCK(&slice->block->hentry.sharding->lock);

        return result;
    }

#ifdef __cplusplus
}
#endif

#endif
