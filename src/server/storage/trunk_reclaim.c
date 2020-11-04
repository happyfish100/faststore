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

#include <limits.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fast_mblock.h"
#include "fastcommon/fc_queue.h"
#include "fastcommon/common_blocked_queue.h"
#include "sf/sf_global.h"
#include "sf/sf_func.h"
#include "../server_global.h"
#include "../dio/trunk_io_thread.h"
#include "storage_allocator.h"
#include "trunk_reclaim.h"

static int realloc_rb_array(TrunkReclaimBlockArray *array)
{
    TrunkReclaimBlockInfo *blocks;
    int new_alloc;
    int bytes;

    new_alloc = (array->alloc > 0) ? 2 * array->alloc : 1024;
    bytes = sizeof(TrunkReclaimBlockInfo) * new_alloc;
    blocks = (TrunkReclaimBlockInfo *)fc_malloc(bytes);
    if (blocks == NULL) {
        return ENOMEM;
    }

    if (array->blocks != NULL) {
        if (array->count > 0) {
            memcpy(blocks, array->blocks, array->count *
                    sizeof(TrunkReclaimBlockInfo));
        }
        free(array->blocks);
    }

    array->alloc = new_alloc;
    array->blocks = blocks;
    return 0;
}

static int realloc_rs_array(TrunkReclaimSliceArray *array)
{
    TrunkReclaimSliceInfo *slices;
    int new_alloc;
    int bytes;

    new_alloc = (array->alloc > 0) ? 2 * array->alloc : 1024;
    bytes = sizeof(TrunkReclaimSliceInfo) * new_alloc;
    slices = (TrunkReclaimSliceInfo *)fc_malloc(bytes);
    if (slices == NULL) {
        return ENOMEM;
    }

    if (array->slices != NULL) {
        if (array->count > 0) {
            memcpy(slices, array->slices, array->count *
                    sizeof(TrunkReclaimSliceInfo));
        }
        free(array->slices);
    }

    array->alloc = new_alloc;
    array->slices = slices;
    return 0;
}

static int compare_by_block_slice_key(const TrunkReclaimSliceInfo *s1,
        const TrunkReclaimSliceInfo *s2)
{
    int sub;
    if ((sub=fc_compare_int64(s1->bs_key.block.oid,
                    s2->bs_key.block.oid)) != 0)
    {
        return sub;
    }

    if ((sub=fc_compare_int64(s1->bs_key.block.offset,
                    s2->bs_key.block.offset)) != 0)
    {
        return sub;
    }

    return (int)s1->bs_key.slice.offset - (int)s2->bs_key.slice.offset;
}

static int convert_to_rs_array(FSTrunkAllocator *allocator,
        FSTrunkFileInfo *trunk, TrunkReclaimSliceArray *rs_array)
{
    int result;
    OBSliceEntry *slice;
    TrunkReclaimSliceInfo *rs;

    result = 0;
    rs = rs_array->slices;
    PTHREAD_MUTEX_LOCK(&allocator->lock);
    fc_list_for_each_entry(slice, &trunk->used.slice_head, dlink) {
        if (rs_array->alloc <= rs - rs_array->slices) {
            rs_array->count = rs - rs_array->slices;
            if ((result=realloc_rs_array(rs_array)) != 0) {
                break;
            }
            rs = rs_array->slices +  rs_array->count;
        }

        rs->bs_key.block = slice->ob->bkey;
        rs->bs_key.slice = slice->ssize;
        rs++;
    }
    PTHREAD_MUTEX_UNLOCK(&allocator->lock);

    if (result != 0) {
        return result;
    }

    rs_array->count = rs - rs_array->slices;
    if (rs_array->count > 1) {
        qsort(rs_array->slices, rs_array->count,
                sizeof(TrunkReclaimSliceInfo),
                (int (*)(const void *, const void *))
                compare_by_block_slice_key);
    }

    return 0;
}

static int combine_to_rb_array(TrunkReclaimSliceArray *sarray,
        TrunkReclaimBlockArray *barray)
{
    int result;
    TrunkReclaimSliceInfo *slice;
    TrunkReclaimSliceInfo *send;
    TrunkReclaimSliceInfo *tail;
    TrunkReclaimBlockInfo *block;

    if (sarray->count == 0) {
        barray->count = 0;
        return 0;
    }

    if (barray->alloc < sarray->count) {
        if ((result=realloc_rb_array(barray)) != 0) {
            return result;
        }
    }

    send = sarray->slices + sarray->count;
    slice = sarray->slices;
    block = barray->blocks;
    if ((block->ob=ob_index_reclaim_lock(&slice->bs_key.block)) == NULL) {
        return ENOENT;
    }
    block->head = tail = slice++;
    while (slice < send) {
        if (ob_index_compare_block_key(&block->ob->bkey,
                    &slice->bs_key.block) != 0)
        {
            tail->next = NULL;
            block++;
            if ((block->ob=ob_index_reclaim_lock(&slice->
                            bs_key.block)) == NULL)
            {
                return ENOENT;
            }
            block->head = tail = slice++;
        } else {
            //TODO combine
            slice++;
        }
    }
    block++;

    barray->count = block - barray->blocks;
    return 0;
}

int trunk_reclaim(FSTrunkAllocator *allocator, FSTrunkFileInfo *trunk,
        TrunkReclaimContext *rctx)
{
    int result;

    if ((result=convert_to_rs_array(allocator, trunk, &rctx->sarray)) != 0) {
        return result;
    }

    if ((result=combine_to_rb_array(&rctx->sarray, &rctx->barray)) != 0) {
        return result;
    }

    return EINPROGRESS;
}
