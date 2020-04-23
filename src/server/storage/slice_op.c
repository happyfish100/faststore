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

    notify = (FSSliceOpNotify *)record->notify.args;
    if (result == 0) {
        notify->done_bytes += record->slice->ssize.length;
        ob_index_add_slice(record->slice);
    } else {
        notify->result = result;
        ob_index_free_slice(record->slice);
    }

    if (__sync_sub_and_fetch(&notify->counter, 1) == 0) {
        if (notify->notify.func != NULL) {
            notify->notify.func(notify);
        }
    }
}

int fs_slice_write_ex(const FSBlockSliceKeyInfo *bs_key, char *buff,
        FSSliceOpNotify *notify, const bool reclaim_alloc)
{
    int result;
    int slice_count;
    FSTrunkSpaceInfo spaces[2];

    if (reclaim_alloc) {
        result = storage_allocator_reclaim_alloc(bs_key->block.hash_code,
                bs_key->slice.length, spaces, &slice_count);
    } else {
        result = storage_allocator_normal_alloc(bs_key->block.hash_code,
                bs_key->slice.length, spaces, &slice_count);
    }

    if (result != 0) {
        return result;
    }

    notify->result = 0;
    notify->done_bytes = 0;
    notify->counter = slice_count;
    if (slice_count == 1) {
        OBSliceEntry *slice;

        slice = ob_index_alloc_slice(&bs_key->block);
        if (slice == NULL) {
            return ENOMEM;
        }

        slice->space = spaces[0];
        slice->ssize.offset = bs_key->slice.offset;
        slice->ssize.length = bs_key->slice.length;
        result = io_thread_push_slice_op(FS_IO_TYPE_WRITE_SLICE,
                slice, buff, slice_write_done, notify);
    } else {
        int offset;
        int remain;
        int i;
        OBSliceEntry *slices[2];
        char *ps;

        offset = bs_key->slice.offset;
        remain = bs_key->slice.length;
        for (i=0; i<slice_count; i++) {
            slices[i] = ob_index_alloc_slice(&bs_key->block);
            if (slices[i] == NULL) {
                return ENOMEM;
            }

            slices[i]->space = spaces[i];
            slices[i]->ssize.offset = offset;
            if (spaces[i].size > remain) {
                slices[i]->ssize.length = remain;
            } else {
                slices[i]->ssize.length = spaces[i].size;
            }

            offset += slices[i]->ssize.length;
            remain -= slices[i]->ssize.length;
        }

        ps = buff;
        for (i=0; i<slice_count; i++) {
            if ((result=io_thread_push_slice_op(FS_IO_TYPE_WRITE_SLICE,
                            slices[i], ps, slice_write_done,
                            notify)) != 0)
            {
                break;
            }
            ps += slices[i]->ssize.length;
        }
    }

    return result;
}
