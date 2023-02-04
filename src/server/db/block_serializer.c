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


#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/pthread_func.h"
#include "../binlog/binlog_types.h"
#include "../server_global.h"
#include "block_serializer.h"

#define BLOCK_FIELD_ID                 1
#define SLICE_RECORD_MAX_SIZE        128
#define DEFAULT_PACKED_BUFFER_SIZE   (8 * 1024)

BlockSerializerContext g_serializer_ctx;

static int buffer_init_func(void *element, void *init_args)
{
    FastBuffer *buffer;
    buffer = (FastBuffer *)element;
    return fast_buffer_init_ex(buffer, DEFAULT_PACKED_BUFFER_SIZE);
}

int block_serializer_init()
{
    int result;

    if ((result=fast_mblock_init_ex1(&g_serializer_ctx.buffer_allocator,
                    "packed-buffer", sizeof(FastBuffer), 1024, 0,
                    buffer_init_func, NULL, true)) != 0)
    {
        return result;
    }

    return 0;
}

void block_serializer_batch_free_buffer(FastBuffer **buffers,
            const int count)
{
    FastBuffer **buf;
    FastBuffer **end;

    end = buffers + count;
    for (buf=buffers; buf<end; buf++) {
        if ((*buf)->alloc_size > DEFAULT_PACKED_BUFFER_SIZE) {
            (*buf)->length = 0;  //reset data length
            fast_buffer_set_capacity(*buf, DEFAULT_PACKED_BUFFER_SIZE);
        }
    }

    fast_mblock_free_objects(&g_serializer_ctx.buffer_allocator,
            (void **)buffers, count);
}

int block_serializer_init_packer(BlockSerializerPacker *packer,
        const int init_alloc)
{
    packer->array.alloc = init_alloc;
    packer->array.strings = (string_t *)fc_malloc(
            sizeof(string_t) * packer->array.alloc);
    if (packer->array.strings == NULL) {
        return ENOMEM;
    }

    packer->buffer.alloc = packer->array.alloc * SLICE_RECORD_MAX_SIZE;
    packer->buffer.buff = (char *)fc_malloc(packer->buffer.alloc);
    if (packer->buffer.buff == NULL) {
        return ENOMEM;
    }

    return 0;
}

static int block_serializer_realloc_packer(BlockSerializerPacker *packer,
        const int target_count)
{
    int result;
    int alloc_count;
    BlockSerializerPacker new_packer;

    alloc_count = packer->array.alloc * 2;
    while (alloc_count < target_count) {
        alloc_count *= 2;
    }
    if ((result=block_serializer_init_packer(&new_packer,
                    alloc_count)) != 0)
    {
        return result;
    }

    free(packer->array.strings);
    free(packer->buffer.buff);
    *packer = new_packer;
    return 0;
}

static int pack_to_str_array(BlockSerializerPacker *packer,
        const OBEntry *ob, int *count)
{
    UniqSkiplistIterator it;
    OBSliceEntry *slice;
    string_t *s;
    char *p;

    s = packer->array.strings;
    p = packer->buffer.buff;
    *count = 0;
    uniq_skiplist_iterator(ob->db_args->slices, &it);
    while ((slice=uniq_skiplist_next(&it)) != NULL) {
        ++(*count);
        if (*count > packer->array.alloc) {
            while (uniq_skiplist_next(&it) != NULL) {
                ++(*count);
            }
            return EOVERFLOW;
        }

        s->str = p;
        s->len = sprintf(p, "%c %d %d %d %"PRId64" "
                "%"PRId64" %"PRId64" %"PRId64"\n",
                slice->type == OB_SLICE_TYPE_ALLOC ?
                BINLOG_OP_TYPE_ALLOC_SLICE :
                BINLOG_OP_TYPE_WRITE_SLICE,
                slice->ssize.offset, slice->ssize.length,
                slice->space.store->index, slice->space.id_info.id,
                slice->space.id_info.subdir, slice->space.offset,
                slice->space.size);
        p += s->len;
        ++s;
    }

    return 0;
}

int block_serializer_pack(BlockSerializerPacker *packer,
        const OBEntry *ob, FastBuffer **buffer)
{
    int result;
    int count;

    result = pack_to_str_array(packer, ob, &count);
    switch (result) {
        case 0:
            break;
        case EOVERFLOW:
            if ((result=block_serializer_realloc_packer(
                            packer, count)) != 0)
            {
                return result;
            }

            if ((result=pack_to_str_array(packer, ob, &count)) == 0) {
                break;
            }
        default:
            return result;
    }

    *buffer = (FastBuffer *)fast_mblock_alloc_object(
            &g_serializer_ctx.buffer_allocator);
    if (*buffer == NULL) {
        return ENOMEM;
    }

    sf_serializer_pack_begin(*buffer);
    if ((result=sf_serializer_pack_string_array(*buffer, BLOCK_FIELD_ID,
                    packer->array.strings, count)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "pack block {oid: %"PRId64", offset: %"PRId64"} fail, "
                "errno: %d, error info: %s", __LINE__, ob->bkey.oid,
                ob->bkey.offset, result, STRERROR(result));
        fast_mblock_free_object(&g_serializer_ctx.buffer_allocator, *buffer);
        *buffer = NULL;
        return result;
    }
    sf_serializer_pack_end(*buffer);

    return 0;
}

#define BLOCK_SERIALIZER_PARSE_INTEGER(var, endchr) \
    do { \
        var = strtol(p + 1, &p, 10); \
        if (*p != endchr) { \
            logError("file: "__FILE__", line: %d, " \
                    "offset: %d, char: 0x%02X != expected: 0x%02X, " \
                    "slice content: %.*s", __LINE__, (int)(p - line->str), \
                    *p, endchr, line->len, line->str); \
            return EINVAL; \
        } \
    } while (0)

int block_serializer_parse_slice(const string_t *line, OBSliceEntry *slice)
{
    char *p;

    if (line->str[0] == BINLOG_OP_TYPE_ALLOC_SLICE) {
        slice->type = OB_SLICE_TYPE_ALLOC;
    } else if (line->str[0] == BINLOG_OP_TYPE_WRITE_SLICE) {
        slice->type = OB_SLICE_TYPE_FILE;
    } else {
        logError("file: "__FILE__", line: %d, "
                "unkown op_type: 0x%02X, slice content: %.*s",
                __LINE__, line->str[0], line->len, line->str);
        return EINVAL;
    }

    p = line->str + 1;
    if (*p != ' ') {
        logError("file: "__FILE__", line: %d, "
                "offset: 1, char: 0x%02X != expected: 0x%02X, "
                "slice content: %.*s", __LINE__, *p, ' ',
                line->len, line->str);
        return EINVAL;
    }

    BLOCK_SERIALIZER_PARSE_INTEGER(slice->ssize.offset, ' ');
    BLOCK_SERIALIZER_PARSE_INTEGER(slice->ssize.length, ' ');
    BLOCK_SERIALIZER_PARSE_INTEGER(slice->space.store->index, ' ');
    BLOCK_SERIALIZER_PARSE_INTEGER(slice->space.id_info.id, ' ');
    BLOCK_SERIALIZER_PARSE_INTEGER(slice->space.id_info.subdir, ' ');
    BLOCK_SERIALIZER_PARSE_INTEGER(slice->space.offset, ' ');
    BLOCK_SERIALIZER_PARSE_INTEGER(slice->space.size, '\n');
    return 0;
}

int block_serializer_unpack(OBSegment *segment, OBEntry *ob,
        const string_t *content)
{
    int result;
    const SFSerializerFieldValue *fv;

    if ((result=sf_serializer_unpack(&segment->
                    db_fetch_ctx.it, content)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "block {oid: %"PRId64", offset: %"PRId64"}, unpack fail, "
                "error info: %s", __LINE__, ob->bkey.oid, ob->bkey.offset,
                segment->db_fetch_ctx.it.error_info);
        return result;
    }

    if ((fv=sf_serializer_next(&segment->db_fetch_ctx.it)) == NULL) {
        logError("file: "__FILE__", line: %d, "
                "block {oid: %"PRId64", offset: %"PRId64"}, no entry",
                __LINE__, ob->bkey.oid, ob->bkey.offset);
        return EINVAL;
    }

    if (fv->fid != BLOCK_FIELD_ID) {
        logError("file: "__FILE__", line: %d, "
                "block {oid: %"PRId64", offset: %"PRId64"}, invalid "
                "fid: %d != expect: %d", __LINE__, ob->bkey.oid,
                ob->bkey.offset, fv->fid, BLOCK_FIELD_ID);
        return EINVAL;
    }

    if (fv->type != sf_serializer_value_type_string_array) {
        logError("file: "__FILE__", line: %d, "
                "block {oid: %"PRId64", offset: %"PRId64"}, "
                "invalid type: %d != expect: %d", __LINE__,
                ob->bkey.oid, ob->bkey.offset, fv->type,
                sf_serializer_value_type_string_array);
        return EINVAL;
    }

    return ob_index_unpack_ob_entry(segment, ob, fv);
}
