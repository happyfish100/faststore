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


#ifndef _TRUNK_RECLAIM_H
#define _TRUNK_RECLAIM_H

#include "fastcommon/uniq_skiplist.h"
#include "fastcommon/multi_skiplist.h"
#include "../../common/fs_types.h"
#include "storage_config.h"
#include "trunk_allocator.h"

struct trunk_reclaim_slice_info;

typedef struct trunk_reclaim_block_info {
    OBEntry *ob;
    struct trunk_reclaim_slice_info *head;
} TrunkReclaimBlockInfo;

typedef struct trunk_reclaim_slice_info {
    OBSliceType type;
    FSBlockSliceKeyInfo bs_key;
    struct trunk_reclaim_slice_info *next;
} TrunkReclaimSliceInfo;

typedef struct trunk_reclaim_block_array {
    int count;
    int alloc;
    TrunkReclaimBlockInfo *blocks;
} TrunkReclaimBlockArray;

typedef struct trunk_reclaim_slice_array {
    int count;
    int alloc;
    TrunkReclaimSliceInfo *slices;
} TrunkReclaimSliceArray;

typedef struct trunk_reclaim_context {
    TrunkReclaimBlockArray barray;
    TrunkReclaimSliceArray sarray;
    FSSliceOpContext op_ctx;
    int buffer_size;
    struct {
        bool finished;
        pthread_lock_cond_pair_t lcp; //for notify
    } notify;
} TrunkReclaimContext;


#ifdef __cplusplus
extern "C" {
#endif
    int trunk_reclaim_init_ctx(TrunkReclaimContext *rctx);

    int trunk_reclaim(FSTrunkAllocator *allocator, FSTrunkFileInfo *trunk,
            TrunkReclaimContext *rctx);

#ifdef __cplusplus
}
#endif

#endif
