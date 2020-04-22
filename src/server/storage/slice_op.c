#include <limits.h>
#include <sys/stat.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "../dio/trunk_io_thread.h"
#include "storage_allocator.h"
#include "slice_op.h"

int slice_write(const FSBlockKey *bkey, const int slice_offset,
        string_t *data)
{
    int result;
    int slice_count;
    FSTrunkSpaceInfo spaces[2];

    if ((result=storage_allocator_alloc(bkey->hash_code, data->len,
            spaces, &slice_count)) != 0)
    {
        return result;
    }

    if (slice_count == 1) {
        OBSliceEntry *slice;

        slice = ob_index_alloc_slice(bkey);
        if (slice == NULL) {
            return ENOMEM;
        }

        slice->space = spaces[0];
        slice->offset = slice_offset;
        slice->length = data->len;
        //TODO notify func
        result = io_thread_push_slice_op(FS_IO_TYPE_WRITE_SLICE,
                slice, data, NULL, NULL);
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

            //TODO notify func
            if ((result=io_thread_push_slice_op(FS_IO_TYPE_WRITE_SLICE,
                            slices[i], &new_data, NULL, NULL)) != 0)
            {
                break;
            }
        }
    }

    return result;
}
