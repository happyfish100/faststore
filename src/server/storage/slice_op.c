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
#include "fastcommon/pthread_func.h"
#include "sf/sf_global.h"
#include "diskallocator/dio/trunk_read_thread.h"
#include "../common/fs_proto.h"
#include "../server_global.h"
#include "../data_thread.h"
#include "../storage/trunk_write_thread.h"
#include "../storage/slice_space_log.h"
#include "../binlog/slice_binlog.h"
#include "../binlog/replica_binlog.h"
#include "slice_op.h"

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

static void fs_set_data_version(FSSliceOpContext *op_ctx)
{
    uint64_t old_version;

    if (op_ctx->info.set_dv_done) {
        return;
    }

    op_ctx->info.set_dv_done = true;
    op_ctx->update.timestamp = g_current_time;
    if (!op_ctx->info.write_binlog.log_replica) {
        return;
    }

    if (op_ctx->info.data_version == 0) {
        op_ctx->info.data_version = __sync_add_and_fetch(
                &op_ctx->info.myself->data.current_version, 1);
    } else {
        while (1) {
            old_version = __sync_add_and_fetch(&op_ctx->info.
                    myself->data.current_version, 0);
            if (op_ctx->info.data_version <= old_version) {
                break;
            }
            if (__sync_bool_compare_and_swap(&op_ctx->info.
                        myself->data.current_version, old_version,
                        op_ctx->info.data_version))
            {
                break;
            }
        }
    }
}

static int slice_add_log_to_queue(FSSliceOpContext *op_ctx)
{
    FSSliceSNPair *slice_sn_pair;
    FSSliceSNPair *slice_sn_end;
    FSSliceSpaceLogRecord *record;
    SFBinlogWriterBuffer *slice_tail;
    int result;

    if ((record=slice_space_log_alloc_record()) == NULL) {
        return ENOMEM;
    }

    record->slice_head = NULL;
    record->space_chain = op_ctx->update.space_chain;
    slice_tail = NULL;
    slice_sn_end = op_ctx->update.sarray.slice_sn_pairs +
        op_ctx->update.sarray.count;
    for (slice_sn_pair=op_ctx->update.sarray.slice_sn_pairs;
            slice_sn_pair<slice_sn_end; slice_sn_pair++)
    {
        if ((result=ob_index_add_slice_to_wbuffer_chain(record, &slice_tail,
                        slice_sn_pair->slice, op_ctx->update.timestamp,
                        slice_sn_pair->sn, op_ctx->info.source)) != 0)
        {
            return result;
        }
    }

    slice_space_log_push(record);
    return 0;
}

int fs_log_slice_write(FSSliceOpContext *op_ctx)
{
    int result;

    if (op_ctx->info.write_to_cache) {
        result = 0;
    } else {
        result = slice_add_log_to_queue(op_ctx);
    }
    fs_slice_array_release(&op_ctx->update.sarray);

    if (result == 0 && op_ctx->info.write_binlog.log_replica) {
        result = replica_binlog_log_write_slice(op_ctx->update.timestamp,
                op_ctx->info.data_group_id, op_ctx->info.data_version,
                &op_ctx->info.bs_key, op_ctx->info.source);
    }

    return result;
}

void fs_write_finish(FSSliceOpContext *op_ctx)
{
    FSSliceSNPair *slice_sn_pair;
    FSSliceSNPair *slice_sn_end;
    int result;
    int inc_alloc;

    do {
        if (op_ctx->result != 0) {
            break;
        }

        fs_set_data_version(op_ctx);

        slice_sn_end = op_ctx->update.sarray.slice_sn_pairs +
            op_ctx->update.sarray.count;
        for (slice_sn_pair=op_ctx->update.sarray.slice_sn_pairs;
                slice_sn_pair<slice_sn_end; slice_sn_pair++)
        {
            slice_sn_pair->sn = 0;
            slice_sn_pair->slice->data_version = op_ctx->info.data_version;
            if ((result=ob_index_add_slice(&op_ctx->info.bs_key.block,
                            slice_sn_pair->slice, &slice_sn_pair->sn,
                            &inc_alloc, &op_ctx->update.space_chain)) != 0)
            {
                op_ctx->result = result;
                break;
            }
            op_ctx->update.space_changed += inc_alloc;
        }
    } while (0);

    if (op_ctx->result != 0) {
        fs_slice_array_release(&op_ctx->update.sarray);
    }
}

static void slice_write_done(struct da_trunk_write_io_buffer
        *record, const int result)
{
    FSSliceOpContext *op_ctx;
    OBSliceEntry *slice;

    op_ctx = record->notify.arg1;
    slice = record->notify.arg2;
    if (result == 0) {
        __sync_add_and_fetch(&op_ctx->done_bytes, slice->ssize.length);
    } else {
        op_ctx->result = result;
    }

    /*
    logInfo("slice_write_done result: %d, offset: %d, length: %d, "
            "done_bytes: %d", result, slice->ssize.offset,
            slice->ssize.length, op_ctx->done_bytes);
            */

    if (__sync_sub_and_fetch(&op_ctx->counter, 1) == 0) {
        op_ctx->rw_done_callback(op_ctx, op_ctx->arg);
    }
}

static inline OBSliceEntry *alloc_init_slice(FSSliceOpContext *op_ctx,
        const FSBlockSliceKeyInfo *bs_key, DATrunkSpaceInfo *space,
        const DASliceType slice_type, const int offset, const int length)
{
    OBSliceEntry *slice;

    slice = ob_index_alloc_slice(&bs_key->block);
    if (slice == NULL) {
        return NULL;
    }

    slice->type = slice_type;
    slice->space = *space;
    slice->ssize.offset = offset;
    slice->ssize.length = length;
    if (slice_type == DA_SLICE_TYPE_CACHE) {
        slice->cache.mbuffer = op_ctx->mbuffer;
        slice->cache.buff = op_ctx->info.buff +
            (offset - bs_key->slice.offset);
        sf_shared_mbuffer_hold(op_ctx->mbuffer);
    }
    return slice;
}

static int fs_slice_alloc(FSSliceOpContext *op_ctx, const FSBlockSliceKeyInfo
        *bs_key, const DASliceType slice_type, const bool reclaim_alloc,
        FSSliceSNPair *slice_sn_pairs, int *slice_count)
{
    int result;
    DATrunkSpaceWithVersion spaces[FS_MAX_SPLIT_COUNT_PER_SPACE_ALLOC];

    *slice_count = 2;
    if (reclaim_alloc) {
        result = da_storage_allocator_reclaim_alloc(&DA_CTX, 
                FS_BLOCK_HASH_CODE(bs_key->block),
                bs_key->slice.length, spaces, slice_count);
    } else {
        result = da_storage_allocator_normal_alloc(&DA_CTX,
                FS_BLOCK_HASH_CODE(bs_key->block),
                bs_key->slice.length, spaces, slice_count);
    }

    if (result != 0) {
        logError("file: "__FILE__", line: %d, "
                "alloc disk space %d bytes fail, "
                "errno: %d, error info: %s",
                __LINE__, bs_key->slice.length,
                result, STRERROR(result));
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
        slice_sn_pairs[0].slice = alloc_init_slice(op_ctx,
                bs_key, &spaces[0].space, slice_type,
                bs_key->slice.offset, bs_key->slice.length);
        if (slice_sn_pairs[0].slice == NULL) {
            return ENOMEM;
        }
        slice_sn_pairs[0].version = spaces[0].version;

        /*
        logInfo("slice %d. offset: %"PRId64", length: %"PRId64,
                0, spaces[0].space.offset, spaces[0].space.size);
                */
    } else {
        int offset;
        int remain;
        int i;

        offset = bs_key->slice.offset;
        remain = bs_key->slice.length;
        for (i=0; i<*slice_count; i++) {
            slice_sn_pairs[i].slice = alloc_init_slice(op_ctx, bs_key,
                    &spaces[i].space, slice_type, offset,
                    (spaces[i].space.size < remain ?
                     spaces[i].space.size : remain));
            if (slice_sn_pairs[i].slice == NULL) {
                return ENOMEM;
            }
            slice_sn_pairs[i].version = spaces[i].version;

            /*
            logInfo("slice %d. offset: %"PRId64", length: %"PRId64,
                    i, spaces[i].space.offset, spaces[i].space.size);
                    */

            offset += slice_sn_pairs[i].slice->ssize.length;
            remain -= slice_sn_pairs[i].slice->ssize.length;
        }
    }

    return result;
}

void fs_release_task_send_buffer(struct fast_task_info *task)
{
#ifdef OS_LINUX
    fs_release_aio_buffers(&SLICE_OP_CTX);
#else
#endif
}

#ifdef OS_LINUX

static int write_iovec_array(FSSliceOpContext *op_ctx)
{
    FSSliceSNPair *slice_sn_pair;
    FSSliceSNPair *slice_sn_end;
    DAAlignedReadBuffer **aligned_buffer;
    DAAlignedReadBuffer **aligned_bend;
    struct iovec *iovc;
    int result;

    if (op_ctx->update.sarray.count == 1) {
        if ((result=fc_check_realloc_iovec_array(&op_ctx->iovec_array,
                        op_ctx->aio_buffer_parray.count)) != 0)
        {
            return result;
        }

        aligned_bend = op_ctx->aio_buffer_parray.buffers +
            op_ctx->aio_buffer_parray.count;
        for (aligned_buffer = op_ctx->aio_buffer_parray.buffers,
                iovc = op_ctx->iovec_array.iovs; aligned_buffer <
                aligned_bend; aligned_buffer++, iovc++)
        {
            FC_SET_IOVEC(*iovc, (*aligned_buffer)->buff +
                    (*aligned_buffer)->offset,
                    (*aligned_buffer)->length);
        }
        op_ctx->iovec_array.count = op_ctx->aio_buffer_parray.count;

        result = trunk_write_thread_push_slice_by_iovec(op_ctx,
                op_ctx->update.sarray.slice_sn_pairs[0].version,
                op_ctx->update.sarray.slice_sn_pairs[0].slice,
                &op_ctx->iovec_array, slice_write_done, op_ctx);
    } else {
        iovec_array_t iov_arr;
        int slice_total_length;
        int buffer_total_length;
        int len;
        int remain;

        if ((result=fc_check_realloc_iovec_array(&op_ctx->iovec_array,
                        op_ctx->aio_buffer_parray.count +
                        op_ctx->update.sarray.count)) != 0)
        {
            return result;
        }

        iov_arr.iovs = op_ctx->iovec_array.iovs;
        aligned_buffer = op_ctx->aio_buffer_parray.buffers;
        aligned_bend = op_ctx->aio_buffer_parray.buffers +
            op_ctx->aio_buffer_parray.count;

        slice_total_length = buffer_total_length = 0;
        slice_sn_end = op_ctx->update.sarray.slice_sn_pairs +
            op_ctx->update.sarray.count;
        for (slice_sn_pair=op_ctx->update.sarray.slice_sn_pairs;
                slice_sn_pair<slice_sn_end; slice_sn_pair++)
        {
            slice_total_length += slice_sn_pair->slice->ssize.length;
            iovc = iov_arr.iovs;
            while (aligned_buffer < aligned_bend) {
                buffer_total_length += (*aligned_buffer)->length;
                if (buffer_total_length <= slice_total_length) {
                    FC_SET_IOVEC(*iovc, (*aligned_buffer)->buff +
                            (*aligned_buffer)->offset,
                            (*aligned_buffer)->length);

                    iovc++;
                    aligned_buffer++;
                    if (buffer_total_length == slice_total_length) {
                        break;
                    }
                } else {
                    remain = buffer_total_length - slice_total_length;
                    len = (*aligned_buffer)->length - remain;
                    FC_SET_IOVEC(*iovc, (*aligned_buffer)->buff +
                            (*aligned_buffer)->offset, len);

                    iovc++;
                    (*aligned_buffer)->offset += len;
                    (*aligned_buffer)->length = remain;
                    buffer_total_length = slice_total_length;
                    break;
                }
            }

            iov_arr.count = iovc - iov_arr.iovs;
            if ((result=trunk_write_thread_push_slice_by_iovec(op_ctx,
                            slice_sn_pair->version, slice_sn_pair->slice,
                            &iov_arr, slice_write_done, op_ctx)) != 0)
            {
                break;
            }

            iov_arr.iovs = iovc;
        }
    }

    return result;
}
#endif

int fs_slice_write(FSSliceOpContext *op_ctx)
{
    FSSliceSNPair *slice_sn_pair;
    FSSliceSNPair *slice_sn_end;
    DASliceType slice_type;
    int result;

    op_ctx->info.set_dv_done = false;
    op_ctx->update.space_changed = 0;
    op_ctx->update.space_chain.head = op_ctx->update.space_chain.tail = NULL;
    if (WRITE_TO_CACHE && (op_ctx->info.source == BINLOG_SOURCE_RPC_MASTER ||
                op_ctx->info.source == BINLOG_SOURCE_RPC_SLAVE))
    {
        op_ctx->info.write_to_cache = true;
        op_ctx->done_bytes = op_ctx->info.bs_key.slice.length;
        slice_type = DA_SLICE_TYPE_CACHE;
    } else {
        op_ctx->info.write_to_cache = false;
        op_ctx->done_bytes = 0;
        slice_type = DA_SLICE_TYPE_FILE;
    }
    if ((result=fs_slice_alloc(op_ctx, &op_ctx->info.bs_key, slice_type,
                    op_ctx->info.source == BINLOG_SOURCE_RECLAIM,
                    op_ctx->update.sarray.slice_sn_pairs,
                    &op_ctx->update.sarray.count)) != 0)
    {
        op_ctx->result = result;
        return result;
    }

    op_ctx->result = 0;
    if (slice_type == DA_SLICE_TYPE_CACHE) {
        fs_set_data_version(op_ctx);
    } else {
        op_ctx->counter = op_ctx->update.sarray.count;
    }

#ifdef OS_LINUX
    if (op_ctx->info.buffer_type == fs_buffer_type_array) {
        return write_iovec_array(op_ctx);
    }
#endif

    if (op_ctx->update.sarray.count == 1) {
        result = trunk_write_thread_push_slice_by_buff(op_ctx,
                op_ctx->update.sarray.slice_sn_pairs[0].version,
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
            if ((result=trunk_write_thread_push_slice_by_buff(op_ctx,
                            slice_sn_pair->version, slice_sn_pair->slice,
                            ps, slice_write_done, op_ctx)) != 0)
            {
                break;
            }
            ps += length;
        }
    }

    return result;
}

static int get_slice_index_holes(FSSliceOpContext *op_ctx,
        FSSliceSize *ssizes, const int max_size, int *count)
{
    int result;
    int offset;
    int hole_len;
    OBSliceReadBufferPair *pair;
    OBSliceReadBufferPair *end;

    if ((result=ob_index_get_slices(&op_ctx->info.bs_key,
                    &op_ctx->slice_rbuffer_array)) != 0)
    {
        if (result == ENOENT) {
            ssizes[0] = op_ctx->info.bs_key.slice;
            *count = 1;
            return 0;
        } else {
            *count = 0;
            return result;
        }
    }

    /*
    logInfo("file: "__FILE__", line: %d, "
            "read sarray.count: %"PRId64", target slice offset: %d, "
            "length: %d", __LINE__, op_ctx->slice_rbuffer_array.count,
            op_ctx->info.bs_key.slice.offset,
            op_ctx->info.bs_key.slice.length);
            */

    *count = 0;
    offset = op_ctx->info.bs_key.slice.offset;
    end = op_ctx->slice_rbuffer_array.pairs + op_ctx->slice_rbuffer_array.count;
    for (pair=op_ctx->slice_rbuffer_array.pairs; pair<end; pair++) {
        hole_len = pair->slice->ssize.offset - offset;
        if (hole_len > 0) {
            if (*count < max_size) {
                ssizes[*count].offset = offset;
                ssizes[*count].length = hole_len;
            }
            (*count)++;
        }

        /*
        logInfo("slice %d. type: %c (0x%02x), offset: %d, "
                "length: %d, hole_len: %d", (int)(pair - op_ctx->
                slice_rbuffer_array.pairs), pair->slice->type, pair->slice->type,
                pair->slice->ssize.offset, pair->slice->ssize.length, hole_len);
                */

        offset = pair->slice->ssize.offset + pair->slice->ssize.length;
        ob_index_free_slice(pair->slice);
    }

    if (offset < op_ctx->info.bs_key.slice.offset +
            op_ctx->info.bs_key.slice.length)
    {
        if (*count < max_size) {
            ssizes[*count].offset = offset;
            ssizes[*count].length = (op_ctx->info.bs_key.slice.offset +
                    op_ctx->info.bs_key.slice.length) - offset;
        }
        (*count)++;
    }

    return *count <= max_size ? 0 : -ENOSPC;
}

int fs_slice_allocate(FSSliceOpContext *op_ctx)
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
    op_ctx->update.space_chain.head = op_ctx->update.space_chain.tail = NULL;
    ssizes = fixed_ssizes;
    if ((result=get_slice_index_holes(op_ctx, ssizes,
                    FS_SLICE_HOLES_FIXED_COUNT, &count)) != 0)
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
            if ((result=get_slice_index_holes(op_ctx, ssizes,
                            alloc, &count)) == 0)
            {
                break;
            }

            free(ssizes);
            if (result != -ENOSPC) {
                return result;
            }
        }
    }

    if (count == 0) {   //NOT need to allocate space
        return 0;
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
            if ((result=fs_slice_alloc(op_ctx, &new_bskey, DA_SLICE_TYPE_ALLOC,
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

    op_ctx->info.set_dv_done = false;
    fs_set_data_version(op_ctx);

    slice_sn_end = op_ctx->update.sarray.slice_sn_pairs +
        op_ctx->update.sarray.count;
    for (slice_sn_pair=op_ctx->update.sarray.slice_sn_pairs;
            slice_sn_pair<slice_sn_end; slice_sn_pair++)
    {
        slice_sn_pair->sn = 0;
        slice_sn_pair->slice->data_version = op_ctx->info.data_version;
        if ((result=ob_index_add_slice(&op_ctx->info.bs_key.block,
                        slice_sn_pair->slice, &slice_sn_pair->sn,
                        &inc, &op_ctx->update.space_chain)) != 0)
        {
            break;
        }
        op_ctx->update.space_changed += inc;
    }

    if (result != 0) {
        fs_slice_array_release(&op_ctx->update.sarray);
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
    int result;

    if (op_ctx->update.sarray.count == 0) {  //NOT log when no change
        return 0;
    }

    if ((result=slice_add_log_to_queue(op_ctx)) != 0) {
        return result;
    }
    fs_slice_array_release(&op_ctx->update.sarray);

    if (op_ctx->info.write_binlog.log_replica) {
        if ((result=replica_binlog_log_alloc_slice(op_ctx->update.timestamp,
                        op_ctx->info.data_group_id, op_ctx->info.data_version,
                        &op_ctx->info.bs_key, op_ctx->info.source)) != 0)
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
        __sync_add_and_fetch(&op_ctx->done_bytes, slice->ssize.length);
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
        op_ctx->rw_done_callback(op_ctx, op_ctx->arg);
    }
}

static void slice_read_done(struct da_trunk_read_io_buffer
        *record, const int result)
{
    do_read_done(record->rb->arg, (FSSliceOpContext *)
            record->notify.arg, result);
}

#ifdef OS_LINUX

static inline int check_realloc_buffer_ptr_array(AIOBufferPtrArray
        *barray, const int target_size)
{
    int new_alloc;
    DAAlignedReadBuffer **new_buffers;

    if (barray->alloc >= target_size) {
        return 0;
    }

    if (barray->alloc == 0) {
        new_alloc = 256;
    } else {
        new_alloc = barray->alloc * 2;
    }
    while (new_alloc < target_size) {
        new_alloc *= 2;
    }

    new_buffers = (DAAlignedReadBuffer **)fc_malloc(
            sizeof(DAAlignedReadBuffer *) * new_alloc);
    if (new_buffers == NULL) {
        return ENOMEM;
    }

    if (barray->buffers != NULL) {
        free(barray->buffers);
    }
    barray->buffers = new_buffers;
    barray->alloc = new_alloc;
    return 0;
}

static inline int add_zero_aligned_buffer(FSSliceOpContext *op_ctx,
        const int path_index, const int length)
{
    DAAlignedReadBuffer *aligned_buffer;

    aligned_buffer = da_aligned_buffer_new(&DA_CTX,
            path_index, 0, length, length);
    if (aligned_buffer == NULL) {
        return ENOMEM;
    }

    memset(aligned_buffer->buff, 0, length);
    op_ctx->aio_buffer_parray.buffers[op_ctx->
        aio_buffer_parray.count++] = aligned_buffer;
    return 0;
}

static inline int add_cached_aligned_buffer(FSSliceOpContext *op_ctx,
        OBSliceEntry *slice)
{
    DAAlignedReadBuffer *aligned_buffer;

    aligned_buffer = da_aligned_buffer_new(&DA_CTX, slice->space.store->
            index, 0, slice->ssize.length, slice->space.size);
    if (aligned_buffer == NULL) {
        return ENOMEM;
    }

    memcpy(aligned_buffer->buff, slice->cache.buff,
            slice->ssize.length);
    op_ctx->aio_buffer_parray.buffers[op_ctx->
        aio_buffer_parray.count++] = aligned_buffer;
    return 0;
}

int fs_slice_read(FSSliceOpContext *op_ctx)
{
    int result;
    int offset;
    int hole_len;
    FSSliceSize ssize;
    OBSliceReadBufferPair *pair;
    OBSliceReadBufferPair *end;

    if (READ_DIRECT_IO_PATHS == 0) {
        op_ctx->info.buffer_type = fs_buffer_type_direct;
        return fs_slice_normal_read(op_ctx);
    }

    if ((result=ob_index_get_slices(&op_ctx->info.bs_key,
                    &op_ctx->slice_rbuffer_array)) != 0)
    {
        return result;
    }

    if ((result=check_realloc_buffer_ptr_array(&op_ctx->aio_buffer_parray,
                    op_ctx->slice_rbuffer_array.count * 2)) != 0)
    {
        return result;
    }

    /*
    logInfo("read sarray->count: %"PRId64", target slice "
            "offset: %d, length: %d", op_ctx->slice_rbuffer_array.count,
            op_ctx->info.bs_key.slice.offset,
            op_ctx->info.bs_key.slice.length);
            */

    op_ctx->info.buffer_type = fs_buffer_type_array;
    op_ctx->result = 0;
    op_ctx->done_bytes = 0;
    op_ctx->counter = op_ctx->slice_rbuffer_array.count;
    op_ctx->aio_buffer_parray.count = 0;
    offset = op_ctx->info.bs_key.slice.offset;
    end = op_ctx->slice_rbuffer_array.pairs + op_ctx->slice_rbuffer_array.count;
    for (pair=op_ctx->slice_rbuffer_array.pairs; pair<end; pair++) {
        hole_len = pair->slice->ssize.offset - offset;
        if (hole_len > 0) {
            if ((result=add_zero_aligned_buffer(op_ctx, pair->slice->
                            space.store->index, hole_len)) != 0)
            {
                return result;
            }
            __sync_add_and_fetch(&op_ctx->done_bytes, hole_len);

            /*
            logInfo("slice %d. type: %c (0x%02x), ref_count: %d, "
                    "slice offset: %d, length: %d, total offset: %d, hole_len: %d",
                    (int)(pair - op_ctx->slice_rbuffer_array.pairs), pair->slice->type,
                    pair->slice->type, __sync_add_and_fetch(&pair->slice->ref_count, 0),
                    pair->slice->ssize.offset, pair->slice->ssize.length, offset, hole_len);
                    */
        }

        ssize = pair->slice->ssize;
        if (pair->slice->type == DA_SLICE_TYPE_ALLOC) {
            if ((result=add_zero_aligned_buffer(op_ctx, pair->slice->space.
                            store->index, pair->slice->ssize.length)) != 0)
            {
                return result;
            }

            do_read_done(pair->slice, op_ctx, 0);
        } else if (pair->slice->type == DA_SLICE_TYPE_CACHE) {
            if ((result=add_cached_aligned_buffer(op_ctx, pair->slice)) != 0) {
                return result;
            }

            do_read_done(pair->slice, op_ctx, 0);
        } else {
            DAAlignedReadBuffer **aligned_buffer;
            aligned_buffer = op_ctx->aio_buffer_parray.buffers +
                op_ctx->aio_buffer_parray.count++;
            *aligned_buffer = da_aligned_buffer_new(&DA_CTX, pair->
                    slice->space.store->index, 0, pair->slice->
                    ssize.length, pair->slice->space.size);
            if (*aligned_buffer == NULL) {
                return ENOMEM;
            }

            if (STORAGE_CFG.paths_by_index.paths[pair->slice->space.
                    store->index]->read_direct_io)
            {
                pair->rb.type = da_buffer_type_aio;
                pair->rb.aio_buffer = *aligned_buffer;
            } else {
                pair->rb.type = da_buffer_type_direct;
                pair->rb.buffer.buff = (*aligned_buffer)->buff;
            }
            pair->rb.arg = pair->slice;
            if ((result=da_trunk_read_thread_push(&DA_CTX, &pair->slice->
                            space, pair->slice->ssize.length, &pair->rb,
                            slice_read_done, op_ctx)) != 0)
            {
                break;
            }
        }

        offset = ssize.offset + ssize.length;
    }

    return result;
}

#endif

int fs_slice_normal_read(FSSliceOpContext *op_ctx)
{
    int result;
    int offset;
    int hole_len;
    FSSliceSize ssize;
    char *ps;
    OBSliceReadBufferPair *pair;
    OBSliceReadBufferPair *end;

    if ((result=ob_index_get_slices(&op_ctx->info.bs_key,
                    &op_ctx->slice_rbuffer_array)) != 0)
    {
        return result;
    }

    /*
    logInfo("read sarray->count: %"PRId64", target slice "
            "offset: %d, length: %d", op_ctx->slice_rbuffer_array.count,
            op_ctx->info.bs_key.slice.offset,
            op_ctx->info.bs_key.slice.length);
            */

    op_ctx->result = 0;
    op_ctx->done_bytes = 0;
    op_ctx->counter = op_ctx->slice_rbuffer_array.count;
    ps = op_ctx->info.buff;
    offset = op_ctx->info.bs_key.slice.offset;
    end = op_ctx->slice_rbuffer_array.pairs + op_ctx->slice_rbuffer_array.count;
    for (pair=op_ctx->slice_rbuffer_array.pairs; pair<end; pair++) {
        hole_len = pair->slice->ssize.offset - offset;
        if (hole_len > 0) {
            memset(ps, 0, hole_len);
            ps += hole_len;
            __sync_add_and_fetch(&op_ctx->done_bytes, hole_len);

            /*
            logInfo("slice %d. type: %c (0x%02x), ref_count: %d, "
                    "slice offset: %d, length: %d, total offset: %d, hole_len: %d",
                    (int)(pair - op_ctx->slice_rbuffer_array.pairs), pair->slice->type,
                    pair->slice->type, __sync_add_and_fetch(&pair->slice->ref_count, 0),
                    pair->slice->ssize.offset, pair->slice->ssize.length, offset, hole_len);
                    */
        }

        ssize = pair->slice->ssize;
        if (pair->slice->type == DA_SLICE_TYPE_ALLOC) {
            memset(ps, 0, pair->slice->ssize.length);
            do_read_done(pair->slice, op_ctx, 0);
        } else if (pair->slice->type == DA_SLICE_TYPE_CACHE) {
            memcpy(ps, pair->slice->cache.buff, pair->slice->ssize.length);
            do_read_done(pair->slice, op_ctx, 0);
        } else {
#ifdef OS_LINUX
            pair->rb.type = da_buffer_type_direct;
#endif
            pair->rb.buffer.buff = ps;
            pair->rb.arg = pair->slice;
            if ((result=da_trunk_read_thread_push(&DA_CTX, &pair->slice->
                            space, pair->slice->ssize.length, &pair->rb,
                            slice_read_done, op_ctx)) != 0)
            {
                break;
            }
        }

        ps += ssize.length;
        offset = ssize.offset + ssize.length;
    }

    return result;
}

int fs_delete_slice(FSSliceOpContext *op_ctx)
{
    int result;

    op_ctx->info.sn = 0;
    op_ctx->update.space_chain.head = op_ctx->update.space_chain.tail = NULL;
    if ((result=ob_index_delete_slice(&op_ctx->info.bs_key, &op_ctx->info.sn,
                    &op_ctx->update.space_changed, &op_ctx->
                    update.space_chain)) == 0)
    {
        op_ctx->info.set_dv_done = false;
        fs_set_data_version(op_ctx);
    }

    return result;
}

int fs_log_delete_slice(FSSliceOpContext *op_ctx)
{
    int result;

    if ((result=slice_binlog_del_slice_push(&op_ctx->info.bs_key,
                    op_ctx->update.timestamp, op_ctx->info.sn,
                    op_ctx->info.data_version, op_ctx->info.source,
                    &op_ctx->update.space_chain)) != 0)
    {
        return result;
    }

    if (op_ctx->info.write_binlog.log_replica) {
        return replica_binlog_log_del_slice(op_ctx->update.timestamp,
                op_ctx->info.data_group_id, op_ctx->info.data_version,
                &op_ctx->info.bs_key, op_ctx->info.source);
    }

    return 0;
}

int fs_delete_block(FSSliceOpContext *op_ctx)
{
    int result;

    op_ctx->info.sn = 0;
    op_ctx->update.space_chain.head = op_ctx->update.space_chain.tail = NULL;
    if ((result=ob_index_delete_block(&op_ctx->info.bs_key.block, &op_ctx->
                    info.sn, &op_ctx->update.space_changed,
                    &op_ctx->update.space_chain)) == 0)
    {
        op_ctx->info.set_dv_done = false;
        fs_set_data_version(op_ctx);
    }

    return result;
}

int fs_log_delete_block(FSSliceOpContext *op_ctx)
{
    FSSliceSpaceLogRecord *record;
    int result;

    if ((record=slice_space_log_alloc_record()) == NULL) {
        return ENOMEM;
    }

    record->space_chain = op_ctx->update.space_chain;
    if ((result=ob_index_del_block_to_wbuffer_chain(record, &op_ctx->info.
                    bs_key.block, op_ctx->update.timestamp, op_ctx->info.sn,
                    op_ctx->info.data_version, op_ctx->info.source)) != 0)
    {
        return result;
    }
    slice_space_log_push(record);

    if (op_ctx->info.write_binlog.log_replica) {
        return replica_binlog_log_del_block(op_ctx->update.timestamp,
                op_ctx->info.data_group_id, op_ctx->info.data_version,
                &op_ctx->info.bs_key.block, op_ctx->info.source);
    }

    return 0;
}

static void fs_slice_rw_done_callback(FSSliceOpContext *op_ctx,
        FSSliceBlockedOpContext *bctx)
{
    PTHREAD_MUTEX_LOCK(&bctx->notify.lcp.lock);
    bctx->notify.finished = true;
    pthread_cond_signal(&bctx->notify.lcp.cond);
    PTHREAD_MUTEX_UNLOCK(&bctx->notify.lcp.lock);
}

int fs_slice_blocked_op_ctx_init(FSSliceBlockedOpContext *bctx)
{
    int result;
    bool malloc_buff;

    ob_index_init_slice_rbuffer_array(&bctx->op_ctx.slice_rbuffer_array);

#ifdef OS_LINUX
    malloc_buff = (READ_DIRECT_IO_PATHS == 0);
#else
    malloc_buff = true;
#endif

    if (malloc_buff) {
        bctx->buffer_size = 256 * 1024;
        bctx->op_ctx.info.buff = (char *)fc_malloc(bctx->buffer_size);
        if (bctx->op_ctx.info.buff == NULL) {
            return ENOMEM;
        }
    } else {
        bctx->buffer_size = 0;
        bctx->op_ctx.info.buff = NULL;
    }

    if ((result=init_pthread_lock_cond_pair(&bctx->notify.lcp)) != 0) {
        return result;
    }

    bctx->notify.finished = false;
    bctx->op_ctx.rw_done_callback = (fs_rw_done_callback_func)
        fs_slice_rw_done_callback;
    bctx->op_ctx.arg = bctx;
    return 0;
}

void fs_slice_blocked_op_ctx_destroy(FSSliceBlockedOpContext *bctx)
{
    ob_index_free_slice_rbuffer_array(&bctx->op_ctx.slice_rbuffer_array);
    destroy_pthread_lock_cond_pair(&bctx->notify.lcp);

    if (bctx->op_ctx.info.buff != NULL) {
        free(bctx->op_ctx.info.buff);
        bctx->op_ctx.info.buff = NULL;
    }
}

int fs_slice_blocked_read(FSSliceBlockedOpContext *bctx,
        FSBlockSliceKeyInfo *bs_key, const int ignore_errno)
{
    int result;
    bool check_malloc;

    bctx->op_ctx.info.bs_key = *bs_key;
    bctx->op_ctx.info.data_group_id = FS_DATA_GROUP_ID(bs_key->block);

#ifdef OS_LINUX
    check_malloc = (READ_DIRECT_IO_PATHS == 0);
#else
    check_malloc = true;
#endif

    if (check_malloc && bctx->buffer_size < bs_key->slice.length) {
        char *buff;
        int buffer_size;

        buffer_size = bctx->buffer_size * 2;
        while (buffer_size < bs_key->slice.length) {
            buffer_size *= 2;
        }
        buff = (char *)fc_malloc(buffer_size);
        if (buff == NULL) {
            return ENOMEM;
        }

        free(bctx->op_ctx.info.buff);
        bctx->op_ctx.info.buff = buff;
        bctx->buffer_size = buffer_size;
    }

    if ((result=fs_slice_read(&bctx->op_ctx)) == 0) {
        PTHREAD_MUTEX_LOCK(&bctx->notify.lcp.lock);
        while (!bctx->notify.finished && SF_G_CONTINUE_FLAG) {
            pthread_cond_wait(&bctx->notify.lcp.cond,
                    &bctx->notify.lcp.lock);
        }
        result = bctx->notify.finished ? bctx->op_ctx.result : EINTR;
        bctx->notify.finished = false;  /* reset for next call */
        PTHREAD_MUTEX_UNLOCK(&bctx->notify.lcp.lock);
    }

    if (result != 0) {
        fs_log_rw_error(&bctx->op_ctx, result, ignore_errno, "read");
    }

    return result;
}
