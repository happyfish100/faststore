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


#ifndef _TRUNK_READ_THREAD_H
#define _TRUNK_READ_THREAD_H

#include "fastcommon/common_define.h"
#ifdef OS_LINUX
#include <libaio.h>
#endif
#include "../../common/fs_types.h"
#include "../storage/storage_config.h"
#include "../storage/object_block_index.h"
#include "../storage/trunk_allocator.h"
#include "../storage/storage_allocator.h"
#ifdef OS_LINUX
#include "read_buffer_pool.h"
#endif

struct trunk_read_io_buffer;

//Note: the record can NOT be persisted
typedef void (*trunk_read_io_notify_func)(struct trunk_read_io_buffer
        *record, const int result);

typedef struct trunk_read_io_buffer {
    OBSliceEntry *slice;     //for slice op
    char *data;

#ifdef OS_LINUX
    AlignedReadBuffer **aligned_buffer;
    struct iocb iocb;
#endif

    struct {
        trunk_read_io_notify_func func;
        void *arg;
    } notify;

    struct trunk_read_io_buffer *next;
} TrunkReadIOBuffer;

#ifdef __cplusplus
extern "C" {
#endif

    int trunk_read_thread_init();
    void trunk_read_thread_terminate();

#ifdef OS_LINUX
    int trunk_read_thread_push(OBSliceEntry *slice, AlignedReadBuffer
            **aligned_buffer, trunk_read_io_notify_func notify_func,
            void *notify_arg);

    static inline AlignedReadBuffer *aligned_buffer_new(const short pindex,
            const int offset, const int length, const int read_bytes)
    {
        AlignedReadBuffer *aligned_buffer;

        aligned_buffer = read_buffer_pool_alloc(pindex, read_bytes);
        if (aligned_buffer == NULL) {
            return NULL;
        }

        aligned_buffer->offset = offset;
        aligned_buffer->length = length;
        aligned_buffer->read_bytes = read_bytes;
        return aligned_buffer;
    }

#else

    int trunk_read_thread_push(OBSliceEntry *slice, char *buff,
            trunk_read_io_notify_func notify_func, void *notify_arg);

#endif

#ifdef __cplusplus
}
#endif

#endif
