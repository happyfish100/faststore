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


#ifndef _FS_BLOCK_SERIALIZER_H
#define _FS_BLOCK_SERIALIZER_H

#include "sf/sf_serializer.h"
#include "../server_types.h"
#include "change_notify.h"

typedef struct {
    struct {
        char *buff;
        int alloc;
    } buffer;

    struct {
        string_t *strings;
        int alloc;
    } array;
} BlockSerializerPacker;

typedef struct {
    struct fast_mblock_man buffer_allocator;
} BlockSerializerContext;

#ifdef __cplusplus
extern "C" {
#endif

    extern BlockSerializerContext g_serializer_ctx;

    int block_serializer_init();
    void block_serializer_destroy();

    int block_serializer_init_packer(BlockSerializerPacker *packer,
            const int init_alloc);

    static inline FastBuffer *block_serializer_alloc_buffer(
            const int capacity)
    {
        FastBuffer *buffer;
        if ((buffer=(FastBuffer *)fast_mblock_alloc_object(
                        &g_serializer_ctx.buffer_allocator)) == NULL)
        {
            return NULL;
        }

        buffer->length = 0;
        if (fast_buffer_check_capacity(buffer, capacity) != 0) {
            fast_mblock_free_object(&g_serializer_ctx.
                    buffer_allocator, buffer);
            return NULL;
        }
        return buffer;
    }

    static inline FastBuffer *block_serializer_to_buffer(const string_t *s)
    {
        FastBuffer *buffer;

        if ((buffer=block_serializer_alloc_buffer(s->len)) == NULL) {
            return NULL;
        }

        memcpy(buffer->data, s->str, s->len);
        buffer->length = s->len;
        return buffer;
    }

    void block_serializer_batch_free_buffer(FastBuffer **buffers,
            const int count);

    int block_serializer_pack(BlockSerializerPacker *packer,
            const OBEntry *ob, FastBuffer **buffer);

    int block_serializer_unpack(SFSerializerIterator *it,
            const string_t *content, OBEntry *ob);

#ifdef __cplusplus
}
#endif

#endif
