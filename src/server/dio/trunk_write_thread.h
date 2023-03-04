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


#ifndef _TRUNK_WRITE_THREAD_H
#define _TRUNK_WRITE_THREAD_H

#include "../../common/fs_types.h"
#include "../storage/storage_config.h"
#include "../storage/object_block_index.h"
#include "../storage/trunk_allocator.h"
#include "../storage/storage_allocator.h"

#define FS_IO_TYPE_CREATE_TRUNK           'C'
#define FS_IO_TYPE_DELETE_TRUNK           'D'
#define FS_IO_TYPE_WRITE_SLICE_BY_BUFF    'W'
#define FS_IO_TYPE_WRITE_SLICE_BY_IOVEC   'V'

struct trunk_write_io_buffer;

//Note: the record can NOT be persisted
typedef void (*trunk_write_io_notify_func)(struct trunk_write_io_buffer
        *record, const int result);

typedef struct trunk_write_io_buffer {
    int type;

    union {
        DATrunkSpaceInfo space;  //for trunk op
        struct {
            OBSliceEntry *slice;
            FSBlockKey bkey;
        };  //for slice op
    };

    int64_t version; //for write in order

    union {
        char *buff;
        iovec_array_t iovec_array;
    };

    union {
        struct {
            trunk_write_io_notify_func func;
            void *arg;
        } notify;

        struct {
            uint32_t timestamp;
            char source;
            uint64_t sn;            //for slice binlog
        } binlog;
    };

    struct trunk_write_io_buffer *next;
} TrunkWriteIOBuffer;

#ifdef __cplusplus
extern "C" {
#endif

    int trunk_write_thread_init();
    void trunk_write_thread_terminate();

    int trunk_write_thread_push(const int type, const int64_t version,
            const int path_index, const uint64_t hash_code, void *entry,
            void *data, trunk_write_io_notify_func notify_func,
            void *notify_arg);

    static inline int trunk_write_thread_push_trunk_op(const int type,
            const DATrunkSpaceInfo *space, trunk_write_io_notify_func
            notify_func, void *notify_arg)
    {
        FSTrunkAllocator *allocator;
        allocator = g_allocator_mgr->allocator_ptr_array.
            allocators[space->store->index];
        return trunk_write_thread_push(type, __sync_add_and_fetch(&allocator->
                    allocate.current_version, 1), space->store->index,
                space->id_info.id, (void *)space, NULL,
                notify_func, notify_arg);
    }

    int trunk_write_thread_push_cached_slice(FSSliceOpContext *op_ctx,
            const int type, const int64_t version,
            OBSliceEntry *slice, void *data);

    static inline int trunk_write_thread_push_slice_by_buff(
            FSSliceOpContext *op_ctx, const int64_t version,
            OBSliceEntry *slice, char *buff,
            trunk_write_io_notify_func notify_func, void *notify_arg)
    {
        if (slice->type == DA_SLICE_TYPE_CACHE) {
            return trunk_write_thread_push_cached_slice(op_ctx,
                    FS_IO_TYPE_WRITE_SLICE_BY_BUFF, version, slice, buff);
        } else {
            return trunk_write_thread_push(FS_IO_TYPE_WRITE_SLICE_BY_BUFF,
                    version, slice->space.store->index, slice->space.id_info.id,
                    slice, buff, notify_func, notify_arg);
        }
    }

    static inline int trunk_write_thread_push_slice_by_iovec(
            FSSliceOpContext *op_ctx, const int64_t version,
            OBSliceEntry *slice, iovec_array_t *iovec_array,
            trunk_write_io_notify_func notify_func, void *notify_arg)
    {
        if (slice->type == DA_SLICE_TYPE_CACHE) {
            return trunk_write_thread_push_cached_slice(op_ctx,
                    FS_IO_TYPE_WRITE_SLICE_BY_IOVEC, version,
                    slice, iovec_array);
        } else {
            return trunk_write_thread_push(FS_IO_TYPE_WRITE_SLICE_BY_IOVEC,
                    version, slice->space.store->index, slice->space.id_info.id,
                    slice, iovec_array, notify_func, notify_arg);
        }
    }

#ifdef __cplusplus
}
#endif

#endif
