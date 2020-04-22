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
        notify->done_bytes += record->slice->length;
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

int fs_slice_write_ex(const FSBlockKey *bkey, const int slice_offset,
        string_t *data, FSSliceOpNotify *notify, const bool reclaim_alloc)
{
    int result;
    int slice_count;
    FSTrunkSpaceInfo spaces[2];

    if (reclaim_alloc) {
        result = storage_allocator_reclaim_alloc(bkey->hash_code,
                data->len, spaces, &slice_count);
    } else {
        result = storage_allocator_normal_alloc(bkey->hash_code,
                data->len, spaces, &slice_count);
    }

    if (result != 0) {
        return result;
    }

    notify->result = 0;
    notify->done_bytes = 0;
    notify->counter = slice_count;
    if (slice_count == 1) {
        OBSliceEntry *slice;

        slice = ob_index_alloc_slice(bkey);
        if (slice == NULL) {
            return ENOMEM;
        }

        slice->space = spaces[0];
        slice->offset = slice_offset;
        slice->length = data->len;
        result = io_thread_push_slice_op(FS_IO_TYPE_WRITE_SLICE,
                slice, data, slice_write_done, notify);
    } else {
        int offset;
        int remain;
        int i;
        OBSliceEntry *slices[2];
        char *ps;
        string_t new_data;

        offset = slice_offset;
        remain = data->len;
        for (i=0; i<slice_count; i++) {
            slices[i] = ob_index_alloc_slice(bkey);
            if (slices[i] == NULL) {
                return ENOMEM;
            }

            slices[i]->space = spaces[i];
            slices[i]->offset = offset;
            if (spaces[i].size > remain) {
                slices[i]->length = remain;
            } else {
                slices[i]->length = spaces[i].size;
            }

            offset += slices[i]->length;
            remain -= slices[i]->length;
        }

        ps = data->str;
        for (i=0; i<slice_count; i++) {
            new_data.str = ps;
            new_data.len = slices[i]->length;
            ps += slices[i]->length;

            if ((result=io_thread_push_slice_op(FS_IO_TYPE_WRITE_SLICE,
                            slices[i], &new_data, slice_write_done,
                            notify)) != 0)
            {
                break;
            }
        }
    }

    return result;
}
