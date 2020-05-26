#include <limits.h>
#include <sys/stat.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "../dio/trunk_io_thread.h"
#include "storage_allocator.h"
#include "slice_op.h"

static void slice_write_done(struct trunk_io_buffer *record, const int result)
{
    FSSliceOpNotify *notify;
    int inc_alloc;
    int r;

    notify = (FSSliceOpNotify *)record->notify.args;
    if (result == 0) {
        notify->done_bytes += record->slice->ssize.length;
        if ((r=ob_index_add_slice(record->slice, &inc_alloc)) == 0) {
            notify->inc_alloc += inc_alloc;
        }
    } else {
        r = result;
    }
    if (r != 0) {
        notify->result = r;
        ob_index_free_slice(record->slice);
    }

    logInfo("slice_write_done result: %d, offset: %d, length: %d, "
            "done_bytes: %d", result, record->slice->ssize.offset,
            record->slice->ssize.length, notify->done_bytes);

    if (__sync_sub_and_fetch(&notify->counter, 1) == 0) {
        if (notify->notify.func != NULL) {
            notify->notify.func(notify);
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
    FSTrunkSpaceInfo spaces[2];

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

int fs_slice_write_ex(const FSBlockSliceKeyInfo *bs_key, char *buff,
        FSSliceOpNotify *notify, const bool reclaim_alloc)
{
    int result;
    int slice_count;
    int i;
    OBSliceEntry *slices[2];

    if ((result=fs_slice_alloc(bs_key, OB_SLICE_TYPE_FILE,
                    reclaim_alloc, slices, &slice_count)) != 0)
    {
        return result;
    }

    notify->result = 0;
    notify->done_bytes = 0;
    notify->inc_alloc = 0;
    notify->counter = slice_count;
    if (slice_count == 1) {
        result = io_thread_push_slice_op(FS_IO_TYPE_WRITE_SLICE,
                            slices[0], buff, slice_write_done,
                            notify);
    } else {
        int length;
        char *ps;

        ps = buff;
        for (i=0; i<slice_count; i++) {
            length = slices[i]->ssize.length;
            if ((result=io_thread_push_slice_op(FS_IO_TYPE_WRITE_SLICE,
                            slices[i], ps, slice_write_done,
                            notify)) != 0)
            {
                break;
            }
            ps += length;
        }
    }

    return result;
}

int fs_slice_allocate_ex(const FSBlockSliceKeyInfo *bs_key,
        const bool reclaim_alloc, int *inc_alloc)
{
    int result;
    int r;
    int slice_count;
    int inc;
    int i;
    OBSliceEntry *slices[2];

    *inc_alloc = 0;
    if ((result=fs_slice_alloc(bs_key, OB_SLICE_TYPE_ALLOC,
                    reclaim_alloc, slices, &slice_count)) != 0)
    {
        return result;
    }

    for (i=0; i<slice_count; i++) {
        if ((r=ob_index_add_slice(slices[i], &inc)) == 0) {
            *inc_alloc += inc;
        } else {
            result = r;
        }
    }
    return result;
}

static void do_read_done(OBSliceEntry *slice, FSSliceOpNotify *notify,
        const int result)
{
    if (result == 0) {
        notify->done_bytes += slice->ssize.length;
    } else {
        notify->result = result;
    }

    /*
    logInfo("slice_read_done result: %d, offset: %d, length: %d, "
            "done_bytes: %d", result, slice->ssize.offset,
            slice->ssize.length, notify->done_bytes);
            */

    ob_index_free_slice(slice);
    if (__sync_sub_and_fetch(&notify->counter, 1) == 0) {
        if (notify->notify.func != NULL) {
            notify->notify.func(notify);
        }
    }
}

static void slice_read_done(struct trunk_io_buffer *record, const int result)
{
    do_read_done(record->slice, (FSSliceOpNotify *)record->notify.args, result);
}

int fs_slice_read_ex(const FSBlockSliceKeyInfo *bs_key, char *buff,
        FSSliceOpNotify *notify, OBSlicePtrArray *sarray)
{
    int result;
    int offset;
    int hole_len;
    FSSliceSize ssize;
    char *ps;
    OBSliceEntry **pp;
    OBSliceEntry **end;

    if ((result=ob_index_get_slices(bs_key, sarray)) != 0) {
        return result;
    }

    logInfo("read sarray->count: %d, target slice offset: %d, length: %d",
            sarray->count, bs_key->slice.offset, bs_key->slice.length);

    notify->done_bytes = 0;
    notify->counter = sarray->count;
    ps = buff;
    offset = bs_key->slice.offset;
    end = sarray->slices + sarray->count;
    for (pp=sarray->slices; pp<end; pp++) {

        hole_len = (*pp)->ssize.offset - offset;
        if (hole_len > 0) {
            memset(ps, 0, hole_len);
            ps += hole_len;
        }

        logInfo("slice %d. type: %c (0x%02x), offset: %d, length: %d, "
                "hole_len: %d", (int)(pp - sarray->slices), (*pp)->type,
                (*pp)->type, (*pp)->ssize.offset, (*pp)->ssize.length, hole_len);

        ssize = (*pp)->ssize;
        if ((*pp)->type == OB_SLICE_TYPE_ALLOC) {
            memset(ps, 0, (*pp)->ssize.length);
            do_read_done(*pp, notify, 0);
        } else if ((result=io_thread_push_slice_op(FS_IO_TYPE_READ_SLICE,
                        *pp, ps, slice_read_done, notify)) != 0)
        {
            break;
        }

        ps += ssize.length;
        offset = ssize.offset + ssize.length;
    }

    return result;
}
