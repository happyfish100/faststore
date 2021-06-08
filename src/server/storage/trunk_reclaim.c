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

static void reclaim_slice_rw_done_callback(FSSliceOpContext *op_ctx,
        TrunkReclaimContext *rctx)
{
    PTHREAD_MUTEX_LOCK(&rctx->notify.lcp.lock);
    rctx->notify.finished = true;
    pthread_cond_signal(&rctx->notify.lcp.cond);
    PTHREAD_MUTEX_UNLOCK(&rctx->notify.lcp.lock);
}

int trunk_reclaim_init_ctx(TrunkReclaimContext *rctx)
{
    int result;

    ob_index_init_slice_ptr_array(&rctx->op_ctx.slice_ptr_array);
    rctx->op_ctx.info.source = BINLOG_SOURCE_RECLAIM;
    rctx->op_ctx.info.write_binlog.log_replica = false;
    rctx->op_ctx.info.data_version = 0;
    rctx->op_ctx.info.myself = NULL;

#ifdef OS_LINUX
    rctx->op_ctx.info.buffer_type = fs_buffer_type_array;
    rctx->buffer_size = 0;
    rctx->op_ctx.info.buff = NULL;
#else
    rctx->buffer_size = 256 * 1024;
    rctx->op_ctx.info.buff = (char *)fc_malloc(rctx->buffer_size);
    if (rctx->op_ctx.info.buff == NULL) {
        return ENOMEM;
    }
#endif

    if ((result=init_pthread_lock_cond_pair(&rctx->notify.lcp)) != 0) {
        return result;
    }

    rctx->notify.finished = false;
    rctx->op_ctx.rw_done_callback = (fs_rw_done_callback_func)
        reclaim_slice_rw_done_callback;
    rctx->op_ctx.arg = rctx;
    return fs_init_slice_op_ctx(&rctx->op_ctx.update.sarray);
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
                    slice->bs_key.slice.offset)
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

static int migrate_prepare(TrunkReclaimContext *rctx,
        FSBlockSliceKeyInfo *bs_key)
{
    rctx->op_ctx.info.bs_key = *bs_key;
    rctx->op_ctx.info.data_group_id = FS_DATA_GROUP_ID(bs_key->block);

#ifdef OS_LINUX
#else
    if (rctx->buffer_size < bs_key->slice.length) {
        char *buff;
        int buffer_size;

        buffer_size = rctx->buffer_size * 2;
        while (buffer_size < bs_key->slice.length) {
            buffer_size *= 2;
        }
        buff = (char *)fc_malloc(buffer_size);
        if (buff == NULL) {
            return ENOMEM;
        }

        free(rctx->op_ctx.info.buff);
        rctx->op_ctx.info.buff = buff;
        rctx->buffer_size = buffer_size;
    }
#endif

    return 0;
}

static inline void log_rw_error(FSSliceOpContext *op_ctx,
        const int result, const int ignore_errno, const char *caption)
{
    int log_level;
    log_level = (result == ignore_errno) ? LOG_DEBUG : LOG_ERR;
    log_it_ex(&g_log_context, log_level,
            "file: "__FILE__", line: %d, %s slice fail, "
            "oid: %"PRId64", block offset: %"PRId64", "
            "slice offset: %d, length: %d, "
            "errno: %d, error info: %s", __LINE__, caption,
            op_ctx->info.bs_key.block.oid,
            op_ctx->info.bs_key.block.offset,
            op_ctx->info.bs_key.slice.offset,
            op_ctx->info.bs_key.slice.length,
            result, STRERROR(result));
}

static int migrate_one_slice(TrunkReclaimContext *rctx,
        FSBlockSliceKeyInfo *bs_key)
{
    int result;

    if ((result=migrate_prepare(rctx, bs_key)) != 0) {
        return result;
    }

    if ((result=fs_slice_read(&rctx->op_ctx)) == 0) {
        PTHREAD_MUTEX_LOCK(&rctx->notify.lcp.lock);
        while (!rctx->notify.finished && SF_G_CONTINUE_FLAG) {
            pthread_cond_wait(&rctx->notify.lcp.cond,
                    &rctx->notify.lcp.lock);
        }
        result = rctx->notify.finished ? rctx->op_ctx.result : EINTR;
        rctx->notify.finished = false;  /* reset for next call */
        PTHREAD_MUTEX_UNLOCK(&rctx->notify.lcp.lock);
    }

    if (result != 0) {
        log_rw_error(&rctx->op_ctx, result, ENOENT, "read");
        return result == ENOENT ? 0 : result;
    }

    rctx->op_ctx.info.bs_key.slice.length = rctx->op_ctx.done_bytes;
    if ((result=fs_slice_write(&rctx->op_ctx)) == 0) {
        PTHREAD_MUTEX_LOCK(&rctx->notify.lcp.lock);
        while (!rctx->notify.finished && SF_G_CONTINUE_FLAG) {
            pthread_cond_wait(&rctx->notify.lcp.cond,
                    &rctx->notify.lcp.lock);
        }
        if (rctx->notify.finished) {
            rctx->notify.finished = false;  /* reset for next call */
        } else {
            rctx->op_ctx.result = EINTR;
        }
        PTHREAD_MUTEX_UNLOCK(&rctx->notify.lcp.lock);
    } else {
        rctx->op_ctx.result = result;
    }

    if (result == 0) {
        fs_write_finish(&rctx->op_ctx);  //for add slice index and cleanup
    }

#ifdef OS_LINUX
    fs_release_aio_buffers(&rctx->op_ctx);
#endif

    if (rctx->op_ctx.result != 0) {
        log_rw_error(&rctx->op_ctx, rctx->op_ctx.result, 0, "write");
        return rctx->op_ctx.result;
    }

    return fs_log_slice_write(&rctx->op_ctx);
}

static int migrate_one_block(TrunkReclaimContext *rctx,
        TrunkReclaimBlockInfo *block)
{
    TrunkReclaimSliceInfo *slice;
    int result;

    slice = block->head;
    while (slice != NULL) {
        if ((result=migrate_one_slice(rctx, &slice->bs_key)) != 0) {
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
