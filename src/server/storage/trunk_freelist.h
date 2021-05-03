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


#ifndef _TRUNK_FREELIST_H
#define _TRUNK_FREELIST_H

#include "storage_types.h"

struct fs_trunk_allocator;
typedef struct {
    int count;
    int water_mark_trunks;
    FSTrunkFileInfo *head;  //allocate from head
    FSTrunkFileInfo *tail;  //push to tail
    pthread_lock_cond_pair_t lcp;  //for lock and notify
} FSTrunkFreelist;

#ifdef __cplusplus
extern "C" {
#endif

    int trunk_freelist_init(FSTrunkFreelist *freelist);

    void trunk_freelist_keep_water_mark(struct fs_trunk_allocator
            *allocator);

    void trunk_freelist_add(FSTrunkFreelist *freelist,
            FSTrunkFileInfo *trunk_info);

    int trunk_freelist_alloc_space(struct fs_trunk_allocator *allocator,
            FSTrunkFreelist *freelist, const uint32_t blk_hc, const int size,
            FSTrunkSpaceWithVersion *spaces, int *count, const bool is_normal);

#ifdef __cplusplus
}
#endif

#endif
