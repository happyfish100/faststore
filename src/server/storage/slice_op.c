#include <limits.h>
#include <sys/stat.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "../dio/trunk_io_thread.h"
#include "../binlog/slice_binlog.h"
#include "../binlog/replica_binlog.h"
#include "storage_allocator.h"
#include "slice_op.h"

static void set_data_version(FSSliceOpContext *op_ctx)
{
    uint64_t old_version;

    if (op_ctx->info.data_version <= 0) {
        op_ctx->info.data_version = __sync_add_and_fetch(
                &op_ctx->info.myself->replica.data_version, 1);
    } else {
        while (1) {
            old_version = __sync_add_and_fetch(&op_ctx->info.
                    myself->replica.data_version, 0);
            if (op_ctx->info.data_version <= old_version) {
                break;
            }
            if (__sync_bool_compare_and_swap(&op_ctx->info.myself->
                        replica.data_version, old_version,
                        op_ctx->info.data_version))
            {
                break;
            }
        }
    }
}

static void slice_write_finish(FSSliceOpContext *op_ctx)
{
    uint64_t sns[FS_MAX_SPLIT_COUNT_PER_SPACE_ALLOC];
    time_t current_time;
    int inc_alloc;
    int r;
    int i;

    if (op_ctx->result == 0) {
        for (i=0; i<op_ctx->write.sarray.count; i++) {
            if ((r=ob_index_add_slice(op_ctx->write.sarray.slices[i],
                            sns + i, &inc_alloc)) != 0)
            {
                op_ctx->result = r;
                return;
            }
            op_ctx->write.inc_alloc += inc_alloc;
        }

        current_time = g_current_time;
        set_data_version(op_ctx);
        for (i=0; i<op_ctx->write.sarray.count; i++) {
            if ((r=slice_binlog_log_add_slice(op_ctx->write.
                            sarray.slices[i], current_time, sns[i],
                            op_ctx->info.data_version)) != 0)
            {
                op_ctx->result = r;
                return;
            }
        }

        if (op_ctx->info.write_data_binlog) {
            if ((r=replica_binlog_log_write_slice(current_time,
                            op_ctx->info.data_group_id, op_ctx->info.
                            data_version, &op_ctx->info.bs_key)) != 0)
            {
                op_ctx->result = r;
                return;
            }
        }
    }

    for (i=0; i<op_ctx->write.sarray.count; i++) {
        ob_index_free_slice(op_ctx->write.sarray.slices[i]);
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

    logInfo("slice_write_done result: %d, offset: %d, length: %d, "
            "done_bytes: %d", result, record->slice->ssize.offset,
            record->slice->ssize.length, op_ctx->done_bytes);

    if (__sync_sub_and_fetch(&op_ctx->counter, 1) == 0) {
        slice_write_finish(op_ctx);
        if (op_ctx->notify.func != NULL) {
            op_ctx->notify.func(op_ctx);
        }
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
        OBSliceEntry **slices, int *slice_count)
{
    int result;
    FSTrunkSpaceInfo spaces[FS_MAX_SPLIT_COUNT_PER_SPACE_ALLOC];

    if (reclaim_alloc) {
        result = storage_allocator_reclaim_alloc(
                FS_BLOCK_HASH_CODE(bs_key->block),
                bs_key->slice.length, spaces, &*slice_count);
    } else {
        result = storage_allocator_normal_alloc(
                FS_BLOCK_HASH_CODE(bs_key->block),
                bs_key->slice.length, spaces, &*slice_count);
    }

    if (result != 0) {
        return result;
    }

    logInfo("write *slice_count: %d, block { oid: %"PRId64", offset: %"PRId64" }, "
            "target slice offset: %d, length: %d", *slice_count,
            bs_key->block.oid, bs_key->block.offset,
            bs_key->slice.offset, bs_key->slice.length);

    if (*slice_count == 1) {
        slices[0] = alloc_init_slice(&bs_key->block, spaces + 0, slice_type,
                bs_key->slice.offset, bs_key->slice.length);
        if (slices[0] == NULL) {
            return ENOMEM;
        }

        logInfo("slice %d. offset: %"PRId64", length: %"PRId64,
                0, spaces[0].offset, spaces[0].size);
    } else {
        int offset;
        int remain;
        int i;

        offset = bs_key->slice.offset;
        remain = bs_key->slice.length;
        for (i=0; i<*slice_count; i++) {
            slices[i] = alloc_init_slice(&bs_key->block, spaces + i,
                    slice_type, offset, (spaces[i].size < remain ?
                    spaces[i].size : remain));
            if (slices[i] == NULL) {
                return ENOMEM;
            }

            logInfo("slice %d. offset: %"PRId64", length: %"PRId64,
                    i, spaces[i].offset, spaces[i].size);

            offset += slices[i]->ssize.length;
            remain -= slices[i]->ssize.length;
        }
    }

    return result;
}

int fs_slice_write_ex(FSSliceOpContext *op_ctx, char *buff,
        const bool reclaim_alloc)
{
    int result;
    int i;

    //TODO notify alloc fail
    if ((result=fs_slice_alloc(&op_ctx->info.bs_key, OB_SLICE_TYPE_FILE,
                    reclaim_alloc, op_ctx->write.sarray.slices,
                    &op_ctx->write.sarray.count)) != 0)
    {
        return result;
    }

    op_ctx->result = 0;
    op_ctx->done_bytes = 0;
    op_ctx->write.inc_alloc = 0;
    op_ctx->counter = op_ctx->write.sarray.count;
    if (op_ctx->write.sarray.count == 1) {
        result = io_thread_push_slice_op(FS_IO_TYPE_WRITE_SLICE,
                            op_ctx->write.sarray.slices[0], buff,
                            slice_write_done, op_ctx);
    } else {
        int length;
        char *ps;

        ps = buff;
        for (i=0; i<op_ctx->write.sarray.count; i++) {
            length = op_ctx->write.sarray.slices[i]->ssize.length;
            if ((result=io_thread_push_slice_op(FS_IO_TYPE_WRITE_SLICE,
                            op_ctx->write.sarray.slices[i], ps,
                            slice_write_done, op_ctx)) != 0)
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

    logInfo("file: "__FILE__", line: %d, "
            "read sarray->count: %"PRId64", target slice offset: %d, length: %d",
            __LINE__, sarray->count, bs_key->slice.offset, bs_key->slice.length);

    *count = 0;
    offset = bs_key->slice.offset;
    end = sarray->slices + sarray->count;
    for (pp=sarray->slices; pp<end; pp++) {
        hole_len = (*pp)->ssize.offset - offset;
        if (hole_len > 0) {
            if (*count >= max_size) {
                return ENOSPC;
            }
            ssizes[*count].offset = offset;
            ssizes[*count].length = hole_len;
            (*count)++;
        }

        logInfo("slice %d. type: %c (0x%02x), offset: %d, length: %d, "
                "hole_len: %d", (int)(pp - sarray->slices), (*pp)->type,
                (*pp)->type, (*pp)->ssize.offset, (*pp)->ssize.length, hole_len);

        offset = (*pp)->ssize.offset + (*pp)->ssize.length;
        ob_index_free_slice(*pp);
    }

    if (offset < bs_key->slice.offset + bs_key->slice.length) {
        if (*count >= max_size) {
            return ENOSPC;
        }
        ssizes[*count].offset = offset;
        ssizes[*count].length = (bs_key->slice.offset +
            bs_key->slice.length) - offset;
        (*count)++;
    }

    return 0;
}

int fs_slice_allocate_ex(FSSliceOpContext *op_ctx,
        OBSlicePtrArray *sarray, int *inc_alloc)
{
#define SLICE_MAX_HOLES  256
    int result;
    int r;
    FSSliceSize ssizes[SLICE_MAX_HOLES];
    FSBlockSliceKeyInfo new_bskey;
    int count;
    int slice_count;
    int inc;
    int n;
    int i;
    int k;
    time_t current_time;
    OBSliceEntry *slices[2 * SLICE_MAX_HOLES];
    uint64_t sns[2 * SLICE_MAX_HOLES];

    *inc_alloc = 0;
    if ((result=get_slice_index_holes(&op_ctx->info.bs_key, sarray,
                    ssizes, SLICE_MAX_HOLES, &count)) != 0)
    {
        return result;
    }

    slice_count = 0;
    new_bskey.block = op_ctx->info.bs_key.block;
    for (k=0; k<count; k++) {
        new_bskey.slice = ssizes[k];
        if ((result=fs_slice_alloc(&new_bskey, OB_SLICE_TYPE_ALLOC,
                        false, slices + slice_count, &n)) != 0)
        {
            return result;
        }

        slice_count += n;
    }

    for (i=0; i<slice_count; i++) {
        if ((r=ob_index_add_slice(slices[i], sns + i, &inc)) != 0) {
            return r;
        }
        *inc_alloc += inc;
    }

    current_time = g_current_time;
    set_data_version(op_ctx);
    for (i=0; i<slice_count; i++) {
        if ((r=slice_binlog_log_add_slice(slices[i], current_time,
                        sns[i], op_ctx->info.data_version)) != 0)
        {
            return r;
        }

        ob_index_free_slice(slices[i]);
    }

    if (op_ctx->info.write_data_binlog) {
        if ((r=replica_binlog_log_alloc_slice(current_time,
                        op_ctx->info.data_group_id, op_ctx->info.
                        data_version, &op_ctx->info.bs_key)) != 0)
        {
            return r;
        }
    }

    logInfo("file: "__FILE__", line: %d, "
            "slice hole count: %d, inc_alloc: %d",
            __LINE__, count, *inc_alloc);

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
        if (op_ctx->notify.func != NULL) {
            op_ctx->notify.func(op_ctx);
        }
    }
}

static void slice_read_done(struct trunk_io_buffer *record, const int result)
{
    do_read_done(record->slice, (FSSliceOpContext *)record->notify.arg, result);
}

int fs_slice_read_ex(FSSliceOpContext *op_ctx, char *buff,
        OBSlicePtrArray *sarray)
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

    logInfo("read sarray->count: %"PRId64", target slice offset: %d, length: %d",
            sarray->count, op_ctx->info.bs_key.slice.offset,
            op_ctx->info.bs_key.slice.length);

    op_ctx->done_bytes = 0;
    op_ctx->counter = sarray->count;
    ps = buff;
    offset = op_ctx->info.bs_key.slice.offset;
    end = sarray->slices + sarray->count;
    for (pp=sarray->slices; pp<end; pp++) {
        hole_len = (*pp)->ssize.offset - offset;
        if (hole_len > 0) {
            memset(ps, 0, hole_len);
            ps += hole_len;
            op_ctx->done_bytes += hole_len;
        }

        logInfo("slice %d. type: %c (0x%02x), offset: %d, length: %d, "
                "hole_len: %d", (int)(pp - sarray->slices), (*pp)->type,
                (*pp)->type, (*pp)->ssize.offset, (*pp)->ssize.length, hole_len);

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

int fs_delete_slices(FSSliceOpContext *op_ctx, int *dec_alloc)
{
    uint64_t sn;
    time_t current_time;
    int result;

    if ((result=ob_index_delete_slices(&op_ctx->info.bs_key,
                    &sn, dec_alloc)) != 0)
    {
        return result;
    }

    current_time = g_current_time;
    set_data_version(op_ctx);
    if ((result=slice_binlog_log_del_slice(&op_ctx->info.bs_key,
                    current_time, sn, op_ctx->info.data_version)) != 0)
    {
        return result;
    }

    if (op_ctx->info.write_data_binlog) {
        return replica_binlog_log_del_slice(current_time,
                op_ctx->info.data_group_id, op_ctx->info.
                data_version, &op_ctx->info.bs_key);
    }
    return 0;
}

int fs_delete_block(FSSliceOpContext *op_ctx, int *dec_alloc)
{
    uint64_t sn;
    time_t current_time;
    int result;

    if ((result=ob_index_delete_block(&op_ctx->info.bs_key.block,
                    &sn, dec_alloc)) != 0)
    {
        return result;
    }

    current_time = g_current_time;
    set_data_version(op_ctx);
    if ((result=slice_binlog_log_del_block(&op_ctx->info.bs_key.block,
                    current_time, sn, op_ctx->info.data_version)) != 0)
    {
        return result;
    }

    if (op_ctx->info.write_data_binlog) {
        return replica_binlog_log_del_block(current_time,
                op_ctx->info.data_group_id, op_ctx->info.
                data_version, &op_ctx->info.bs_key.block);
    }
    return 0;
}
