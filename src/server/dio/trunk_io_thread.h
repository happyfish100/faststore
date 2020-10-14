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


#ifndef _TRUNK_IO_THREAD_H
#define _TRUNK_IO_THREAD_H

#include "../../common/fs_types.h"
#include "../storage/storage_config.h"
#include "../storage/object_block_index.h"

#define FS_IO_TYPE_CREATE_TRUNK   'C'
#define FS_IO_TYPE_DELETE_TRUNK   'D'
#define FS_IO_TYPE_READ_SLICE     'R'
#define FS_IO_TYPE_WRITE_SLICE    'W'

struct trunk_io_buffer;

//Note: the record can NOT be persisted
typedef void (*trunk_io_notify_func)(struct trunk_io_buffer *record,
        const int result);

typedef struct trunk_io_buffer {
    int type;

    union {
        FSTrunkSpaceInfo space;  //for trunk op
        OBSliceEntry *slice;     //for slice op
    };

    string_t data;
    struct {
        trunk_io_notify_func func;
        void *arg;
    } notify;
    struct trunk_io_buffer *next;
} TrunkIOBuffer;

#ifdef __cplusplus
extern "C" {
#endif

    int trunk_io_thread_init();
    void trunk_io_thread_terminate();

    int trunk_io_thread_push(const int type, const int path_index,
            const uint32_t hash_code, void *entry, char *buff,
            trunk_io_notify_func notify_func, void *notify_arg);

    static inline int io_thread_push_trunk_op(const int type,
            const FSTrunkSpaceInfo *space, trunk_io_notify_func
            notify_func, void *notify_arg)
    {
        return trunk_io_thread_push(type, space->store->index,
                space->id_info.id, (void *)space, NULL,
                notify_func, notify_arg);
    }

    static inline int io_thread_push_slice_op(const int type,
            OBSliceEntry *slice, char *buff, trunk_io_notify_func
            notify_func, void *notify_arg)
    {
        return trunk_io_thread_push(type, slice->space.store->index,
                FS_BLOCK_HASH_CODE(slice->ob->bkey), slice, buff,
                notify_func, notify_arg);
    }

#ifdef __cplusplus
}
#endif

#endif
