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
    const int init_capacity = DEFAULT_PACKED_BUFFER_SIZE;
    const bool binary_mode = true;
    const bool check_capacity = true;
    FastBuffer *buffer;
    buffer = (FastBuffer *)element;
    return fast_buffer_init_ex(buffer, init_capacity,
            binary_mode, check_capacity);
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
        *p++ = slice->type == DA_SLICE_TYPE_ALLOC ?
                BINLOG_OP_TYPE_ALLOC_SLICE :
                BINLOG_OP_TYPE_WRITE_SLICE;
        *p++ = ' ';
        p += fc_itoa(slice->data_version, p);
        *p++ = ' ';
        p += fc_itoa(slice->ssize.offset, p);
        *p++ = ' ';
        p += fc_itoa(slice->ssize.length, p);
        *p++ = ' ';
        p += fc_itoa(slice->space.store->index, p);
        *p++ = ' ';
        p += fc_itoa(slice->space.id_info.id, p);
        *p++ = ' ';
        p += fc_itoa(slice->space.id_info.subdir, p);
        *p++ = ' ';
        p += fc_itoa(slice->space.offset, p);
        *p++ = ' ';
        p += fc_itoa(slice->space.size, p);
        *p++ = '\n';

        s->len = p - s->str;
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

int block_serializer_parse_slice_ex(const string_t *line,
        int64_t *data_version, DASliceType *slice_type,
        FSSliceSize *ssize, DATrunkSpaceInfo *space)
{
    char *p;
    int path_index;

    if (line->str[0] == BINLOG_OP_TYPE_ALLOC_SLICE) {
        *slice_type = DA_SLICE_TYPE_ALLOC;
    } else if (line->str[0] == BINLOG_OP_TYPE_WRITE_SLICE) {
        *slice_type = DA_SLICE_TYPE_FILE;
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

    BLOCK_SERIALIZER_PARSE_INTEGER(*data_version, ' ');
    BLOCK_SERIALIZER_PARSE_INTEGER(ssize->offset, ' ');
    BLOCK_SERIALIZER_PARSE_INTEGER(ssize->length, ' ');
    BLOCK_SERIALIZER_PARSE_INTEGER(path_index, ' ');
    if (path_index < 0 || path_index > STORAGE_CFG.max_store_path_index) {
        logError("file: "__FILE__", line: %d, "
                "invalid path_index: %d, max_store_path_index: %d",
                __LINE__, path_index, STORAGE_CFG.max_store_path_index);
        return EINVAL;
    }
    if (PATHS_BY_INDEX_PPTR[path_index] == NULL) {
        logError("file: "__FILE__", line: %d, "
                "path_index: %d not exist",
                __LINE__, path_index);
        return ENOENT;
    }
    space->store = &PATHS_BY_INDEX_PPTR[path_index]->store;

    BLOCK_SERIALIZER_PARSE_INTEGER(space->id_info.id, ' ');
    BLOCK_SERIALIZER_PARSE_INTEGER(space->id_info.subdir, ' ');
    BLOCK_SERIALIZER_PARSE_INTEGER(space->offset, ' ');
    BLOCK_SERIALIZER_PARSE_INTEGER(space->size, '\n');
    return 0;
}

static int block_serializer_unpack(FSDBFetchContext *db_fetch_ctx,
        const FSBlockKey *bkey, const string_t *content,
        const SFSerializerFieldValue **fv)
{
    int result;

    if ((result=sf_serializer_unpack(&db_fetch_ctx->it, content)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "block {oid: %"PRId64", offset: %"PRId64"}, unpack fail, "
                "error info: %s", __LINE__, bkey->oid, bkey->offset,
                db_fetch_ctx->it.error_info);
        return result;
    }

    if ((*fv=sf_serializer_next(&db_fetch_ctx->it)) == NULL) {
        logError("file: "__FILE__", line: %d, "
                "block {oid: %"PRId64", offset: %"PRId64"}, no entry",
                __LINE__, bkey->oid, bkey->offset);
        return EINVAL;
    }

    if ((*fv)->fid != BLOCK_FIELD_ID) {
        logError("file: "__FILE__", line: %d, "
                "block {oid: %"PRId64", offset: %"PRId64"}, invalid "
                "fid: %d != expect: %d", __LINE__, bkey->oid,
                bkey->offset, (*fv)->fid, BLOCK_FIELD_ID);
        return EINVAL;
    }

    if ((*fv)->type != sf_serializer_value_type_string_array) {
        logError("file: "__FILE__", line: %d, "
                "block {oid: %"PRId64", offset: %"PRId64"}, "
                "invalid type: %d != expect: %d", __LINE__,
                bkey->oid, bkey->offset, (*fv)->type,
                sf_serializer_value_type_string_array);
        return EINVAL;
    }

    return 0;
}

int block_serializer_fetch_and_unpack(FSDBFetchContext *db_fetch_ctx,
        const FSBlockKey *bkey, const SFSerializerFieldValue **fv)
{
    int result;
    string_t content;

    if ((result=STORAGE_ENGINE_FETCH_API(bkey,
                    &db_fetch_ctx->read_ctx)) != 0)
    {
        if (result == ENOENT) {
            *fv = NULL;
            return 0;
        }

        logError("file: "__FILE__", line: %d, "
                "block {oid: %"PRId64", offset: %"PRId64"}, "
                "load slices from db fail, result: %d", __LINE__,
                bkey->oid, bkey->offset, result);
        return result;
    }

    FC_SET_STRING_EX(content, DA_OP_CTX_BUFFER_PTR(db_fetch_ctx->read_ctx.
                op_ctx), DA_OP_CTX_BUFFER_LEN(db_fetch_ctx->read_ctx.op_ctx));
    return block_serializer_unpack(db_fetch_ctx, bkey, &content, fv);
}
