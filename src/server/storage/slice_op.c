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
#include "../common/fs_proto.h"
#include "../server_global.h"
#include "../data_thread.h"
#include "../dio/trunk_write_thread.h"
#include "../dio/trunk_read_thread.h"
#include "../binlog/slice_binlog.h"
#include "../binlog/replica_binlog.h"
#include "storage_allocator.h"
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
            if (__sync_bool_compare_and_swap(&op_ctx->info.
                        myself->data.version, old_version,
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

    if (result == 0 && op_ctx->info.write_binlog.log_replica) {
        result = replica_binlog_log_write_slice(current_time,
                op_ctx->info.data_group_id, op_ctx->info.
                data_version, &op_ctx->info.bs_key,
                op_ctx->info.source);
    }

    free_slice_array(&op_ctx->update.sarray);
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

        slice_sn_end = op_ctx->update.sarray.slice_sn_pairs +
            op_ctx->update.sarray.count;
        for (slice_sn_pair=op_ctx->update.sarray.slice_sn_pairs;
                slice_sn_pair<slice_sn_end; slice_sn_pair++)
        {
            if ((result=ob_index_add_slice(slice_sn_pair->slice,
                            &slice_sn_pair->sn, &inc_alloc, op_ctx->
                            info.source == BINLOG_SOURCE_RECLAIM)) != 0)
            {
                op_ctx->result = result;
                break;
            }
            op_ctx->update.space_changed += inc_alloc;
        }

        if (op_ctx->result == 0) {
            set_data_version(op_ctx);
        }
    } while (0);

    if (op_ctx->result != 0) {
        free_slice_array(&op_ctx->update.sarray);
    }
}

static void slice_write_done(struct trunk_write_io_buffer
        *record, const int result)
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
        op_ctx->rw_done_callback(op_ctx, op_ctx->arg);
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
    FSTrunkSpaceWithVersion spaces[FS_MAX_SPLIT_COUNT_PER_SPACE_ALLOC];

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
        slice_sn_pairs[0].slice = alloc_init_slice(&bs_key->block,
                &spaces[0].space, slice_type, bs_key->slice.offset,
                bs_key->slice.length);
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
            slice_sn_pairs[i].slice = alloc_init_slice(&bs_key->block,
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

#ifdef OS_LINUX

void fs_release_task_aio_buffers(struct fast_task_info *task)
{
    fs_release_aio_buffers(&SLICE_OP_CTX);
}

static int write_iovec_array(FSSliceOpContext *op_ctx)
{
    FSSliceSNPair *slice_sn_pair;
    FSSliceSNPair *slice_sn_end;
    AlignedReadBuffer **aligned_buffer;
    AlignedReadBuffer **aligned_bend;
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

        result = trunk_write_thread_push_slice_by_iovec(
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
            if ((result=trunk_write_thread_push_slice_by_iovec(
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
    int result;

    op_ctx->done_bytes = 0;
    op_ctx->update.space_changed = 0;
    if ((result=fs_slice_alloc(&op_ctx->info.bs_key, OB_SLICE_TYPE_FILE,
                    op_ctx->info.source == BINLOG_SOURCE_RECLAIM,
                    op_ctx->update.sarray.slice_sn_pairs,
                    &op_ctx->update.sarray.count)) != 0)
    {
        op_ctx->result = result;
        op_ctx->rw_done_callback(op_ctx, op_ctx->arg);
        return result;
    }

    op_ctx->result = 0;
    op_ctx->counter = op_ctx->update.sarray.count;
#ifdef OS_LINUX
    if (op_ctx->info.buffer_type == fs_buffer_type_array) {
        return write_iovec_array(op_ctx);
    }
#endif

    if (op_ctx->update.sarray.count == 1) {
        result = trunk_write_thread_push_slice_by_buff(
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
            if ((result=trunk_write_thread_push_slice_by_buff(
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
    OBSliceEntry **pp;
    OBSliceEntry **end;

    if ((result=ob_index_get_slices(&op_ctx->info.bs_key,
                    &op_ctx->slice_ptr_array, op_ctx->info.
                    source == BINLOG_SOURCE_RECLAIM)) != 0)
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
            "length: %d", __LINE__, op_ctx->slice_ptr_array.count,
            op_ctx->info.bs_key.slice.offset,
            op_ctx->info.bs_key.slice.length);
            */

    *count = 0;
    offset = op_ctx->info.bs_key.slice.offset;
    end = op_ctx->slice_ptr_array.slices + op_ctx->slice_ptr_array.count;
    for (pp=op_ctx->slice_ptr_array.slices; pp<end; pp++) {
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
                "length: %d, hole_len: %d", (int)(pp - op_ctx->
                slice_ptr_array.slices), (*pp)->type, (*pp)->type,
                (*pp)->ssize.offset, (*pp)->ssize.length, hole_len);
                */

        offset = (*pp)->ssize.offset + (*pp)->ssize.length;
        ob_index_free_slice(*pp);
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
    for (slice_sn_pair=op_ctx->update.sarray.slice_sn_pairs;
            slice_sn_pair<slice_sn_end; slice_sn_pair++)
    {
        if ((result=ob_index_add_slice(slice_sn_pair->slice,
                        &slice_sn_pair->sn, &inc, op_ctx->info.
                        source == BINLOG_SOURCE_RECLAIM)) != 0)
        {
            break;
        }
        op_ctx->update.space_changed += inc;
    }
    if (result == 0) {
        set_data_version(op_ctx);
    }

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
        op_ctx->rw_done_callback(op_ctx, op_ctx->arg);
    }
}

static void slice_read_done(struct trunk_read_io_buffer
        *record, const int result)
{
    do_read_done(record->slice, (FSSliceOpContext *)
            record->notify.arg, result);
}

#ifdef OS_LINUX

static inline int check_realloc_buffer_ptr_array(AIOBufferPtrArray
        *barray, const int target_size)
{
    int new_alloc;
    AlignedReadBuffer **new_buffers;

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

    new_buffers = (AlignedReadBuffer **)fc_malloc(
            sizeof(AlignedReadBuffer *) * new_alloc);
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
    AlignedReadBuffer *aligned_buffer;

    aligned_buffer = aligned_buffer_new(path_index, 0, length, length);
    if (aligned_buffer == NULL) {
        return ENOMEM;
    }

    memset(aligned_buffer->buff, 0, length);
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
    OBSliceEntry **pp;
    OBSliceEntry **end;

    if ((result=ob_index_get_slices(&op_ctx->info.bs_key,
                    &op_ctx->slice_ptr_array, op_ctx->info.
                    source == BINLOG_SOURCE_RECLAIM)) != 0)
    {
        return result;
    }

    if ((result=check_realloc_buffer_ptr_array(&op_ctx->aio_buffer_parray,
                    op_ctx->slice_ptr_array.count * 2)) != 0)
    {
        return result;
    }

    /*
    logInfo("read sarray->count: %"PRId64", target slice "
            "offset: %d, length: %d", op_ctx->slice_ptr_array.count,
            op_ctx->info.bs_key.slice.offset,
            op_ctx->info.bs_key.slice.length);
            */

    op_ctx->result = 0;
    op_ctx->done_bytes = 0;
    op_ctx->counter = op_ctx->slice_ptr_array.count;
    op_ctx->aio_buffer_parray.count = 0;
    offset = op_ctx->info.bs_key.slice.offset;
    end = op_ctx->slice_ptr_array.slices + op_ctx->slice_ptr_array.count;
    for (pp=op_ctx->slice_ptr_array.slices; pp<end; pp++) {
        hole_len = (*pp)->ssize.offset - offset;
        if (hole_len > 0) {
            if ((result=add_zero_aligned_buffer(op_ctx, (*pp)->
                            space.store->index, hole_len)) != 0)
            {
                return result;
            }
            op_ctx->done_bytes += hole_len;

            /*
            logInfo("slice %d. type: %c (0x%02x), ref_count: %d, "
                    "slice offset: %d, length: %d, total offset: %d, hole_len: %d",
                    (int)(pp - op_ctx->slice_ptr_array.slices), (*pp)->type,
                    (*pp)->type, __sync_add_and_fetch(&(*pp)->ref_count, 0),
                    (*pp)->ssize.offset, (*pp)->ssize.length, offset, hole_len);
                    */
        }

        ssize = (*pp)->ssize;
        if ((*pp)->type == OB_SLICE_TYPE_ALLOC) {
            if ((result=add_zero_aligned_buffer(op_ctx, (*pp)->space.
                            store->index, (*pp)->ssize.length)) != 0)
            {
                return result;
            }

            do_read_done(*pp, op_ctx, 0);
        } else {
            AlignedReadBuffer **aligned_buffer;
            aligned_buffer = op_ctx->aio_buffer_parray.buffers +
                op_ctx->aio_buffer_parray.count++;
            if ((result=trunk_read_thread_push(*pp, aligned_buffer,
                            slice_read_done, op_ctx)) != 0)
            {
                ob_index_free_slice(*pp);
                break;
            }
        }

        offset = ssize.offset + ssize.length;
    }

    return result;
}

#else

int fs_slice_read(FSSliceOpContext *op_ctx)
{
    int result;
    int offset;
    int hole_len;
    FSSliceSize ssize;
    char *ps;
    OBSliceEntry **pp;
    OBSliceEntry **end;

    if ((result=ob_index_get_slices(&op_ctx->info.bs_key,
                    &op_ctx->slice_ptr_array, op_ctx->info.
                    source == BINLOG_SOURCE_RECLAIM)) != 0)
    {
        return result;
    }

    /*
    logInfo("read sarray->count: %"PRId64", target slice "
            "offset: %d, length: %d", op_ctx->slice_ptr_array.count,
            op_ctx->info.bs_key.slice.offset,
            op_ctx->info.bs_key.slice.length);
            */

    op_ctx->result = 0;
    op_ctx->done_bytes = 0;
    op_ctx->counter = op_ctx->slice_ptr_array.count;
    ps = op_ctx->info.buff;
    offset = op_ctx->info.bs_key.slice.offset;
    end = op_ctx->slice_ptr_array.slices + op_ctx->slice_ptr_array.count;
    for (pp=op_ctx->slice_ptr_array.slices; pp<end; pp++) {
        hole_len = (*pp)->ssize.offset - offset;
        if (hole_len > 0) {
            memset(ps, 0, hole_len);
            ps += hole_len;
            op_ctx->done_bytes += hole_len;

            /*
            logInfo("slice %d. type: %c (0x%02x), ref_count: %d, "
                    "slice offset: %d, length: %d, total offset: %d, hole_len: %d",
                    (int)(pp - op_ctx->slice_ptr_array.slices), (*pp)->type,
                    (*pp)->type, __sync_add_and_fetch(&(*pp)->ref_count, 0),
                    (*pp)->ssize.offset, (*pp)->ssize.length, offset, hole_len);
                    */
        }

        ssize = (*pp)->ssize;
        if ((*pp)->type == OB_SLICE_TYPE_ALLOC) {
            memset(ps, 0, (*pp)->ssize.length);
            do_read_done(*pp, op_ctx, 0);
        } else if ((result=trunk_read_thread_push(*pp, ps,
                        slice_read_done, op_ctx)) != 0)
        {
            ob_index_free_slice(*pp);
            break;
        }

        ps += ssize.length;
        offset = ssize.offset + ssize.length;
    }

    return result;
}

#endif

int fs_delete_slices(FSSliceOpContext *op_ctx)
{
    int result;

    if ((result=ob_index_delete_slices(&op_ctx->info.bs_key,
                    &op_ctx->info.sn, &op_ctx->update.space_changed,
                    op_ctx->info.source == BINLOG_SOURCE_RECLAIM)) == 0)
    {
        set_data_version(op_ctx);
    }

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

    if ((result=ob_index_delete_block(&op_ctx->info.bs_key.block,
                    &op_ctx->info.sn, &op_ctx->update.space_changed,
                    op_ctx->info.source == BINLOG_SOURCE_RECLAIM)) == 0)
    {
        set_data_version(op_ctx);
    }

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
