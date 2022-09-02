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
#include <assert.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fast_mblock.h"
#include "fastcommon/fc_queue.h"
#include "fastcommon/common_blocked_queue.h"
#include "sf/sf_global.h"
#include "sf/sf_func.h"
#include "../common/fs_func.h"
#include "../server_global.h"
#include "../binlog/binlog_types.h"
#include "storage_allocator.h"
#include "slice_op.h"
#include "trunk_reclaim.h"

int trunk_reclaim_init_ctx(TrunkReclaimContext *rctx)
{
    int result;

    rctx->bctx.op_ctx.info.source = BINLOG_SOURCE_RECLAIM;
    rctx->bctx.op_ctx.info.write_binlog.log_replica = false;
    rctx->bctx.op_ctx.info.data_version = 0;
    rctx->bctx.op_ctx.info.myself = NULL;
    if ((result=fs_slice_blocked_op_ctx_init(&rctx->bctx)) != 0) {
        return result;
    }

    return fs_slice_array_init(&rctx->bctx.op_ctx.update.sarray);
}

static int realloc_rb_array(TrunkReclaimBlockArray *array,
        const int target_count)
{
    TrunkReclaimBlockInfo *blocks;
    int new_alloc;
    int bytes;

    new_alloc = (array->alloc > 0) ? 2 * array->alloc : 1024;
    while (new_alloc < target_count) {
        new_alloc *= 2;
    }
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
    PTHREAD_MUTEX_LOCK(&allocator->trunks.lock);
    fc_list_for_each_entry(slice, &trunk->used.slice_head, dlink) {
        if (rs_array->alloc <= rs - rs_array->slices) {
            rs_array->count = rs - rs_array->slices;
            if ((result=realloc_rs_array(rs_array)) != 0) {
                break;
            }
            rs = rs_array->slices + rs_array->count;
        }

        rs->type = slice->type;
        rs->bs_key.block = slice->ob->bkey;
        rs->bs_key.slice = slice->ssize;
        rs++;
    }
    PTHREAD_MUTEX_UNLOCK(&allocator->trunks.lock);

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

    barray->count = 0;
    if (barray->alloc < sarray->count) {
        if ((result=realloc_rb_array(barray, sarray->count)) != 0) {
            return result;
        }
    }

    send = sarray->slices + sarray->count;
    slice = sarray->slices;
    block = barray->blocks;
    while (slice < send) {
        if ((block->ob=ob_index_reclaim_lock(&slice->
                        bs_key.block)) == NULL)
        {
            TrunkReclaimBlockInfo *bend;
            bend = barray->blocks + (block - barray->blocks);
            for (block=barray->blocks; block<bend; block++) {
                ob_index_reclaim_unlock(block->ob);  //rollback
            }
            return ENOENT;
        }

        block->head = tail = slice;
        slice++;
        while (slice < send && ob_index_compare_block_key(
                    &block->ob->bkey, &slice->bs_key.block) == 0)
        {
            if (tail->bs_key.slice.offset + tail->bs_key.slice.length ==
                    slice->bs_key.slice.offset && tail->type == slice->type)
            {  //combine slices
                tail->bs_key.slice.length += slice->bs_key.slice.length;
            } else {
                tail->next = slice;
                tail = slice;
            }
            slice++;
        }

        block++;
        tail->next = NULL;  //end of slice chain
    }

    barray->count = block - barray->blocks;
    return 0;
}

static int migrate_one_slice(TrunkReclaimContext *rctx,
        TrunkReclaimSliceInfo *slice)
{
    int result;

    if (slice->type == OB_SLICE_TYPE_CACHE) {
        return 0;
    } else if (slice->type == OB_SLICE_TYPE_ALLOC) {
        rctx->bctx.op_ctx.info.bs_key = slice->bs_key;
        rctx->bctx.op_ctx.info.data_group_id = FS_DATA_GROUP_ID(
                slice->bs_key.block);
        if ((result=fs_slice_allocate(&rctx->bctx.op_ctx)) == 0) {
            return fs_log_slice_allocate(&rctx->bctx.op_ctx);
        } else {
            fs_log_rw_error(&rctx->bctx.op_ctx, result, 0, "allocate");
            return result;
        }
    }

    if ((result=fs_slice_blocked_read(&rctx->bctx,
                    &slice->bs_key, ENOENT)) != 0)
    {
        return result == ENOENT ? 0 : result;
    }

    rctx->bctx.op_ctx.info.bs_key.slice.length =
        FC_ATOMIC_GET(rctx->bctx.op_ctx.done_bytes);
    if ((result=fs_slice_write(&rctx->bctx.op_ctx)) == 0) {
        PTHREAD_MUTEX_LOCK(&rctx->bctx.notify.lcp.lock);
        while (!rctx->bctx.notify.finished && SF_G_CONTINUE_FLAG) {
            pthread_cond_wait(&rctx->bctx.notify.lcp.cond,
                    &rctx->bctx.notify.lcp.lock);
        }
        if (rctx->bctx.notify.finished) {
            rctx->bctx.notify.finished = false;  /* reset for next call */
        } else {
            rctx->bctx.op_ctx.result = EINTR;
        }
        PTHREAD_MUTEX_UNLOCK(&rctx->bctx.notify.lcp.lock);
    } else {
        rctx->bctx.op_ctx.result = result;
    }

    if (result == 0) {
        fs_write_finish(&rctx->bctx.op_ctx);  //for add slice index and cleanup
    }

#ifdef OS_LINUX
    fs_release_aio_buffers(&rctx->bctx.op_ctx);
#endif

    if (rctx->bctx.op_ctx.result != 0) {
        fs_log_rw_error(&rctx->bctx.op_ctx, rctx->bctx.
                op_ctx.result, 0, "write");
        return rctx->bctx.op_ctx.result;
    }

    return fs_log_slice_write(&rctx->bctx.op_ctx);
}

static int migrate_one_block(TrunkReclaimContext *rctx,
        TrunkReclaimBlockInfo *block)
{
    TrunkReclaimSliceInfo *slice;
    int result;

    slice = block->head;
    while (slice != NULL) {
        if ((result=migrate_one_slice(rctx, slice)) != 0) {
            return result;
        }
        slice = slice->next;
    }

    ob_index_reclaim_unlock(block->ob);
    return 0;
}

static int migrate_blocks(TrunkReclaimContext *rctx)
{
    TrunkReclaimBlockInfo *block;
    TrunkReclaimBlockInfo *bend;
    int result;

    bend = rctx->barray.blocks + rctx->barray.count;
    for (block=rctx->barray.blocks; block<bend; block++) {
        if ((result=migrate_one_block(rctx, block)) != 0) {
            do {
                ob_index_reclaim_unlock(block->ob);  //rollback
                block++;
            } while (block < bend);
            return result;
        }
    }

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

    if ((result=migrate_blocks(rctx)) != 0) {
        return result;
    }

    return 0;
}
