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
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "sf/sf_global.h"
#include "../common/fs_proto.h"
#include "../server_global.h"
#include "../data_thread.h"
#include "../dio/trunk_io_thread.h"
#include "../binlog/slice_binlog.h"
#include "../binlog/replica_binlog.h"
#include "storage_allocator.h"
#include "slice_op.h"

#define SLICE_OP_CHECK_LOCK(op_ctx) \
    do { \
        bool need_lock;  \
        do {  \
            need_lock = (op_ctx->info.data_version == 0 &&   \
                    op_ctx->info.write_binlog.log_replica);  \
            if (need_lock) {  \
                PTHREAD_MUTEX_LOCK(&op_ctx->info.myself->data.lock); \
            }  \
        } while (0)

#define SLICE_OP_CHECK_UNLOCK(op_ctx) \
        if (need_lock) {  \
            PTHREAD_MUTEX_UNLOCK(&op_ctx->info.myself->data.lock); \
        } \
    } while (0)

static int realloc_slice_sn_pairs(FSSliceSNPairArray *parray,
        const int capacity)
{
    FSSliceSNPair *slice_sn_pairs;

    slice_sn_pairs = (FSSliceSNPair *)fc_malloc(
            sizeof(FSSliceSNPair) * capacity);
    if (slice_sn_pairs == NULL) {
        return ENOMEM;
    }

    free(parray->slice_sn_pairs);
    parray->alloc = capacity;
    parray->slice_sn_pairs = slice_sn_pairs;
    return 0;
}

static inline void set_data_version(FSSliceOpContext *op_ctx)
{
    uint64_t old_version;

    if (!op_ctx->info.write_binlog.log_replica) {
        return;
    }

    if (op_ctx->info.data_version == 0) {
        op_ctx->info.data_version = __sync_add_and_fetch(
                &op_ctx->info.myself->data.version, 1);
    } else {
        while (1) {
            old_version = __sync_add_and_fetch(&op_ctx->info.
                    myself->data.version, 0);
            if (op_ctx->info.data_version <= old_version) {
                break;
            }
            if (__sync_bool_compare_and_swap(&op_ctx->info.myself->
                        data.version, old_version,
                        op_ctx->info.data_version))
            {
                break;
            }
        }
    }
}

static inline void free_slice_array(FSSliceSNPairArray *array)
{
    FSSliceSNPair *slice_sn_pair;
    FSSliceSNPair *slice_sn_end;

    slice_sn_end = array->slice_sn_pairs + array->count;
    for (slice_sn_pair=array->slice_sn_pairs;
            slice_sn_pair<slice_sn_end; slice_sn_pair++)
    {
        ob_index_free_slice(slice_sn_pair->slice);
    }
    array->count = 0;
}

int fs_log_slice_write(FSSliceOpContext *op_ctx)
{
    FSSliceSNPair *slice_sn_pair;
    FSSliceSNPair *slice_sn_end;
    time_t current_time;
    int result;

    result = 0;
    current_time = g_current_time;
    slice_sn_end = op_ctx->update.sarray.slice_sn_pairs +
        op_ctx->update.sarray.count;
    for (slice_sn_pair=op_ctx->update.sarray.slice_sn_pairs;
            slice_sn_pair<slice_sn_end; slice_sn_pair++)
    {
        if ((result=slice_binlog_log_add_slice(slice_sn_pair->slice,
                        current_time, slice_sn_pair->sn, op_ctx->info.
                        data_version, op_ctx->info.source)) != 0)
        {
            break;
        }
    }

    if (op_ctx->info.write_binlog.log_replica) {
        if ((result=replica_binlog_log_write_slice(current_time,
                        op_ctx->info.data_group_id, op_ctx->info.
                        data_version, &op_ctx->info.bs_key,
                        op_ctx->info.source)) != 0)
        {
            return result;
        }
    }

    free_slice_array(&op_ctx->update.sarray);
    return 0;
}

static void slice_write_finish(FSSliceOpContext *op_ctx)
{
    FSSliceSNPair *slice_sn_pair;
    FSSliceSNPair *slice_sn_end;
    int result;
    int inc_alloc;

    do {
        if (op_ctx->result != 0) {
            break;
        }

        SLICE_OP_CHECK_LOCK(op_ctx);
        slice_sn_end = op_ctx->update.sarray.slice_sn_pairs +
            op_ctx->update.sarray.count;
        for (slice_sn_pair=op_ctx->update.sarray.slice_sn_pairs;
                slice_sn_pair<slice_sn_end; slice_sn_pair++)
        {
            if ((result=ob_index_add_slice(slice_sn_pair->slice,
                            &slice_sn_pair->sn, &inc_alloc)) != 0)
            {
                op_ctx->result = result;
                break;
            }
            op_ctx->update.space_changed += inc_alloc;
        }

        if (op_ctx->result == 0) {
            set_data_version(op_ctx);
        }
        SLICE_OP_CHECK_UNLOCK(op_ctx);
    } while (0);

    if (op_ctx->result != 0) {
        free_slice_array(&op_ctx->update.sarray);
    }
}

static void slice_write_done(struct trunk_io_buffer *record, const int result)
{
    FSSliceOpContext *op_ctx;

    op_ctx = (FSSliceOpContext *)record->notify.arg;
    if (result == 0) {
        op_ctx->done_bytes += record->slice->ssize.length;
    } else {
        op_ctx->result = result;
    }

    /*
    logInfo("slice_write_done result: %d, offset: %d, length: %d, "
            "done_bytes: %d", result, record->slice->ssize.offset,
            record->slice->ssize.length, op_ctx->done_bytes);
            */

    if (__sync_sub_and_fetch(&op_ctx->counter, 1) == 0) {
        slice_write_finish(op_ctx);
        data_thread_notify(op_ctx->data_thread_ctx);
    }
}

static inline OBSliceEntry *alloc_init_slice(const FSBlockKey *bkey,
        FSTrunkSpaceInfo *space, const OBSliceType slice_type,
        const int offset, const int length)
{
    OBSliceEntry *slice;

    slice = ob_index_alloc_slice(bkey);
    if (slice == NULL) {
        return NULL;
    }

    slice->type = slice_type;
    slice->read_offset = 0;
    slice->space = *space;
    slice->ssize.offset = offset;
    slice->ssize.length = length;
    return slice;
}

static int fs_slice_alloc(const FSBlockSliceKeyInfo *bs_key,
        const OBSliceType slice_type, const bool reclaim_alloc,
        FSSliceSNPair *slice_sn_pairs, int *slice_count)
{
    int result;
    FSTrunkSpaceInfo spaces[FS_MAX_SPLIT_COUNT_PER_SPACE_ALLOC];

    if (reclaim_alloc) {
        result = storage_allocator_reclaim_alloc(
                FS_BLOCK_HASH_CODE(bs_key->block),
                bs_key->slice.length, spaces, slice_count);
    } else {
        result = storage_allocator_normal_alloc(
                FS_BLOCK_HASH_CODE(bs_key->block),
                bs_key->slice.length, spaces, slice_count);
    }

    if (result != 0) {
        return result;
    }

    /*
    logInfo("write slice_count: %d, block "
            "{ oid: %"PRId64", offset: %"PRId64" }, "
            "target slice offset: %d, length: %d", *slice_count,
            bs_key->block.oid, bs_key->block.offset,
            bs_key->slice.offset, bs_key->slice.length);
            */

    if (*slice_count == 1) {
        slice_sn_pairs[0].slice = alloc_init_slice(&bs_key->block,
                spaces + 0, slice_type, bs_key->slice.offset,
                bs_key->slice.length);
        if (slice_sn_pairs[0].slice == NULL) {
            return ENOMEM;
        }

        /*
        logInfo("slice %d. offset: %"PRId64", length: %"PRId64,
                0, spaces[0].offset, spaces[0].size);
                */
    } else {
        int offset;
        int remain;
        int i;

        offset = bs_key->slice.offset;
        remain = bs_key->slice.length;
        for (i=0; i<*slice_count; i++) {
            slice_sn_pairs[i].slice = alloc_init_slice(&bs_key->block,
                    spaces + i, slice_type, offset, (spaces[i].size <
                        remain ?  spaces[i].size : remain));
            if (slice_sn_pairs[i].slice == NULL) {
                return ENOMEM;
            }

            /*
            logInfo("slice %d. offset: %"PRId64", length: %"PRId64,
                    i, spaces[i].offset, spaces[i].size);
                    */

            offset += slice_sn_pairs[i].slice->ssize.length;
            remain -= slice_sn_pairs[i].slice->ssize.length;
        }
    }

    return result;
}

int fs_slice_write_ex(FSSliceOpContext *op_ctx, const bool reclaim_alloc)
{
    FSSliceSNPair *slice_sn_pair;
    FSSliceSNPair *slice_sn_end;
    int result;

    //TODO notify alloc fail
    if ((result=fs_slice_alloc(&op_ctx->info.bs_key, OB_SLICE_TYPE_FILE,
                    reclaim_alloc, op_ctx->update.sarray.slice_sn_pairs,
                    &op_ctx->update.sarray.count)) != 0)
    {
        return result;
    }

    op_ctx->result = 0;
    op_ctx->done_bytes = 0;
    op_ctx->update.space_changed = 0;
    op_ctx->counter = op_ctx->update.sarray.count;
    if (op_ctx->update.sarray.count == 1) {
        result = io_thread_push_slice_op(FS_IO_TYPE_WRITE_SLICE,
                            op_ctx->update.sarray.slice_sn_pairs[0].slice,
                            op_ctx->info.buff, slice_write_done, op_ctx);
    } else {
        int length;
        char *ps;

        ps = op_ctx->info.buff;
        slice_sn_end = op_ctx->update.sarray.slice_sn_pairs +
            op_ctx->update.sarray.count;
        for (slice_sn_pair=op_ctx->update.sarray.slice_sn_pairs;
                slice_sn_pair<slice_sn_end; slice_sn_pair++)
        {
            length = slice_sn_pair->slice->ssize.length;
            if ((result=io_thread_push_slice_op(FS_IO_TYPE_WRITE_SLICE,
                            slice_sn_pair->slice, ps, slice_write_done,
                            op_ctx)) != 0)
            {
                break;
            }
            ps += length;
        }
    }

    return result;
}

static int get_slice_index_holes(const FSBlockSliceKeyInfo *bs_key,
        OBSlicePtrArray *sarray, FSSliceSize *ssizes,
        const int max_size, int *count)
{
    int result;
    int offset;
    int hole_len;
    OBSliceEntry **pp;
    OBSliceEntry **end;

    if ((result=ob_index_get_slices(bs_key, sarray)) != 0) {
        if (result == ENOENT) {
            ssizes[0] = bs_key->slice;
            *count = 1;
            return 0;
        } else {
            *count = 0;
            return result;
        }
    }

    /*
    logInfo("file: "__FILE__", line: %d, "
            "read sarray->count: %"PRId64", target slice offset: %d, "
            "length: %d", __LINE__, sarray->count, bs_key->slice.offset,
            bs_key->slice.length);
            */

    *count = 0;
    offset = bs_key->slice.offset;
    end = sarray->slices + sarray->count;
    for (pp=sarray->slices; pp<end; pp++) {
        hole_len = (*pp)->ssize.offset - offset;
        if (hole_len > 0) {
            if (*count < max_size) {
                ssizes[*count].offset = offset;
                ssizes[*count].length = hole_len;
            }
            (*count)++;
        }

        /*
        logInfo("slice %d. type: %c (0x%02x), offset: %d, "
                "length: %d, hole_len: %d", (int)(pp - sarray->slices),
                (*pp)->type, (*pp)->type, (*pp)->ssize.offset,
                (*pp)->ssize.length, hole_len);
                */

        offset = (*pp)->ssize.offset + (*pp)->ssize.length;
        ob_index_free_slice(*pp);
    }

    if (offset < bs_key->slice.offset + bs_key->slice.length) {
        if (*count < max_size) {
            ssizes[*count].offset = offset;
            ssizes[*count].length = (bs_key->slice.offset +
                    bs_key->slice.length) - offset;
        }
        (*count)++;
    }

    return *count <= max_size ? 0 : -ENOSPC;
}

int fs_slice_allocate_ex(FSSliceOpContext *op_ctx,
        OBSlicePtrArray *sarray)
{
#define FS_SLICE_HOLES_FIXED_COUNT  256
    int result;
    FSSliceSize fixed_ssizes[FS_SLICE_HOLES_FIXED_COUNT];
    FSSliceSize *ssizes;
    FSBlockSliceKeyInfo new_bskey;
    int alloc;
    int count;
    int inc;
    int n;
    int k;
    FSSliceSNPair *slice_sn_pair;
    FSSliceSNPair *slice_sn_end;

    op_ctx->update.sarray.count = 0;
    op_ctx->update.space_changed = 0;
    ssizes = fixed_ssizes;
    if ((result=get_slice_index_holes(&op_ctx->info.bs_key, sarray,
                    ssizes, FS_SLICE_HOLES_FIXED_COUNT, &count)) != 0)
    {
        if (result != -ENOSPC) {
            return result;
        }

        while (1) {
            alloc = count + FS_SLICE_SN_PARRAY_INIT_ALLOC_COUNT;
            ssizes = (FSSliceSize *)fc_malloc(sizeof(FSSliceSize) * alloc);
            if (ssizes == NULL) {
                return ENOMEM;
            }
            if ((result=get_slice_index_holes(&op_ctx->info.bs_key,
                            sarray, ssizes, alloc, &count)) == 0)
            {
                break;
            }

            free(ssizes);
            if (result != -ENOSPC) {
                return result;
            }
        }
    }

    do {
        if (op_ctx->update.sarray.alloc < count *
                FS_MAX_SPLIT_COUNT_PER_SPACE_ALLOC)
        {
            if ((result=realloc_slice_sn_pairs(&op_ctx->update.sarray, count *
                            FS_MAX_SPLIT_COUNT_PER_SPACE_ALLOC)) != 0)
            {
                break;
            }
        }

        new_bskey.block = op_ctx->info.bs_key.block;
        for (k=0; k<count; k++) {
            new_bskey.slice = ssizes[k];
            if ((result=fs_slice_alloc(&new_bskey, OB_SLICE_TYPE_ALLOC,
                            false, op_ctx->update.sarray.slice_sn_pairs +
                            op_ctx->update.sarray.count, &n)) != 0)
            {
                break;
            }

            op_ctx->update.sarray.count += n;
        }
    } while (0);

    if (ssizes != fixed_ssizes) {
        free(ssizes);
    }
    if (result != 0) {
        return result;
    }

    slice_sn_end = op_ctx->update.sarray.slice_sn_pairs +
        op_ctx->update.sarray.count;
    SLICE_OP_CHECK_LOCK(op_ctx);
    for (slice_sn_pair=op_ctx->update.sarray.slice_sn_pairs;
            slice_sn_pair<slice_sn_end; slice_sn_pair++)
    {
        if ((result=ob_index_add_slice(slice_sn_pair->slice,
                        &slice_sn_pair->sn, &inc)) != 0)
        {
            break;
        }
        op_ctx->update.space_changed += inc;
    }
    if (result == 0) {
        set_data_version(op_ctx);
    }
    SLICE_OP_CHECK_UNLOCK(op_ctx);

    if (result != 0) {
        free_slice_array(&op_ctx->update.sarray);
    }

    /*
    logInfo("file: "__FILE__", line: %d, "
            "slice hole count: %d, inc_alloc: %d",
            __LINE__, count, op_ctx->update.space_changed);
            */
    return result;
}

int fs_log_slice_allocate(FSSliceOpContext *op_ctx)
{
    FSSliceSNPair *slice_sn_pair;
    FSSliceSNPair *slice_sn_end;
    int result;
    time_t current_time;

    current_time = g_current_time;
    slice_sn_end = op_ctx->update.sarray.slice_sn_pairs +
        op_ctx->update.sarray.count;
    for (slice_sn_pair=op_ctx->update.sarray.slice_sn_pairs;
            slice_sn_pair<slice_sn_end; slice_sn_pair++)
    {
        if ((result=slice_binlog_log_add_slice(slice_sn_pair->slice,
                        current_time, slice_sn_pair->sn, op_ctx->info.
                        data_version, op_ctx->info.source)) != 0)
        {
            return result;
        }

        ob_index_free_slice(slice_sn_pair->slice);
    }

    if (op_ctx->info.write_binlog.log_replica) {
        if ((result=replica_binlog_log_alloc_slice(current_time,
                        op_ctx->info.data_group_id, op_ctx->info.
                        data_version, &op_ctx->info.bs_key,
                        op_ctx->info.source)) != 0)
        {
            return result;
        }
    }

    if (op_ctx->update.sarray.alloc > FS_SLICE_SN_PARRAY_INIT_ALLOC_COUNT) {
        realloc_slice_sn_pairs(&op_ctx->update.sarray,
                FS_SLICE_SN_PARRAY_INIT_ALLOC_COUNT);
    }

    return 0;
}

static void do_read_done(OBSliceEntry *slice, FSSliceOpContext *op_ctx,
        const int result)
{
    if (result == 0) {
        op_ctx->done_bytes += slice->ssize.length;
    } else {
        op_ctx->result = result;
    }

    /*
    logInfo("slice_read_done result: %d, offset: %d, length: %d, "
            "done_bytes: %d", result, slice->ssize.offset,
            slice->ssize.length, op_ctx->done_bytes);
            */

    ob_index_free_slice(slice);
    if (__sync_sub_and_fetch(&op_ctx->counter, 1) == 0) {
        data_thread_notify(op_ctx->data_thread_ctx);
    }
}

static void slice_read_done(struct trunk_io_buffer *record, const int result)
{
    do_read_done(record->slice, (FSSliceOpContext *)record->notify.arg, result);
}

int fs_slice_read_ex(FSSliceOpContext *op_ctx, OBSlicePtrArray *sarray)
{
    int result;
    int offset;
    int hole_len;
    FSSliceSize ssize;
    char *ps;
    OBSliceEntry **pp;
    OBSliceEntry **end;

    if ((result=ob_index_get_slices(&op_ctx->info.bs_key, sarray)) != 0) {
        return result;
    }

    /*
    logInfo("read sarray->count: %"PRId64", target slice "
            "offset: %d, length: %d", sarray->count,
            op_ctx->info.bs_key.slice.offset,
            op_ctx->info.bs_key.slice.length);
            */

    op_ctx->done_bytes = 0;
    op_ctx->counter = sarray->count;
    ps = op_ctx->info.buff;
    offset = op_ctx->info.bs_key.slice.offset;
    end = sarray->slices + sarray->count;
    for (pp=sarray->slices; pp<end; pp++) {
        hole_len = (*pp)->ssize.offset - offset;
        if (hole_len > 0) {
            memset(ps, 0, hole_len);
            ps += hole_len;
            op_ctx->done_bytes += hole_len;
        }

        /*
        logInfo("slice %d. type: %c (0x%02x), offset: %d, length: %d, "
                "hole_len: %d", (int)(pp - sarray->slices), (*pp)->type,
                (*pp)->type, (*pp)->ssize.offset, (*pp)->ssize.length, hole_len);
                */

        ssize = (*pp)->ssize;
        if ((*pp)->type == OB_SLICE_TYPE_ALLOC) {
            memset(ps, 0, (*pp)->ssize.length);
            do_read_done(*pp, op_ctx, 0);
        } else if ((result=io_thread_push_slice_op(FS_IO_TYPE_READ_SLICE,
                        *pp, ps, slice_read_done, op_ctx)) != 0)
        {
            ob_index_free_slice(*pp);
            break;
        }

        ps += ssize.length;
        offset = ssize.offset + ssize.length;
    }

    return result;
}

int fs_delete_slices(FSSliceOpContext *op_ctx)
{
    int result;

    SLICE_OP_CHECK_LOCK(op_ctx);
    if ((result=ob_index_delete_slices(&op_ctx->info.bs_key,
                    &op_ctx->info.sn, &op_ctx->update.space_changed)) == 0)
    {
        set_data_version(op_ctx);
    }
    SLICE_OP_CHECK_UNLOCK(op_ctx);

    return result;
}

int fs_log_delete_slices(FSSliceOpContext *op_ctx)
{
    int result;
    time_t current_time;

    current_time = g_current_time;
    if ((result=slice_binlog_log_del_slice(&op_ctx->info.bs_key,
                    current_time, op_ctx->info.sn, op_ctx->info.
                    data_version, op_ctx->info.source)) != 0)
    {
        return result;
    }

    if (op_ctx->info.write_binlog.log_replica) {
        return replica_binlog_log_del_slice(current_time,
                op_ctx->info.data_group_id, op_ctx->info.
                data_version, &op_ctx->info.bs_key,
                op_ctx->info.source);
    }
    return 0;
}

int fs_delete_block(FSSliceOpContext *op_ctx)
{
    int result;

    SLICE_OP_CHECK_LOCK(op_ctx);
    if ((result=ob_index_delete_block(&op_ctx->info.bs_key.block,
                    &op_ctx->info.sn, &op_ctx->update.space_changed)) == 0)
    {
        set_data_version(op_ctx);
    }
    SLICE_OP_CHECK_UNLOCK(op_ctx);

    return result;
}

int fs_log_delete_block(FSSliceOpContext *op_ctx)
{
    int result;
    time_t current_time;

    current_time = g_current_time;
    if ((result=slice_binlog_log_del_block(&op_ctx->info.bs_key.block,
                    current_time, op_ctx->info.sn, op_ctx->info.data_version,
                    op_ctx->info.source)) != 0)
    {
        return result;
    }

    if (op_ctx->info.write_binlog.log_replica) {
        return replica_binlog_log_del_block(current_time,
                op_ctx->info.data_group_id, op_ctx->info.data_version,
                &op_ctx->info.bs_key.block, op_ctx->info.source);
    }

    return 0;
}
