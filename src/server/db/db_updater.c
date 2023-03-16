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
#include "../../common/fs_func.h"
#include "../server_global.h"
#include "block_serializer.h"
#include "event_dealer.h"
#include "db_updater.h"

#define REDO_TMP_FILENAME  ".dbstore.tmp"
#define REDO_LOG_FILENAME  "dbstore.redo"

#define REDO_HEADER_FIELD_ID_RECORD_COUNT         1
#define REDO_HEADER_FIELD_ID_LAST_FIELD_VERSION   2
#define REDO_HEADER_FIELD_ID_LAST_BLOCK_VERSION   3
#define REDO_HEADER_FIELD_ID_OB_COUNT             4
#define REDO_HEADER_FIELD_ID_SLICE_COUNT          5

#define REDO_ENTRY_FIELD_ID_VERSION               1
#define REDO_ENTRY_FIELD_ID_BLOCK_OID             2
#define REDO_ENTRY_FIELD_ID_BLOCK_OFFSET          3
#define REDO_ENTRY_FIELD_ID_FIELD_BUFFER          4

typedef struct db_updater_ctx {
    SafeWriteFileInfo redo;
} DBUpdaterCtx;

static DBUpdaterCtx db_updater_ctx;

int db_updater_realloc_block_array(FSDBUpdateBlockArray *array)
{
    FSDBUpdateBlockInfo *entries;

    if (array->alloc == 0) {
        array->alloc = 8 * 1024;
    } else {
        array->alloc *= 2;
    }

    entries = (FSDBUpdateBlockInfo *)fc_malloc(
            sizeof(FSDBUpdateBlockInfo) * array->alloc);
    if (entries == NULL) {
        return ENOMEM;
    }

    if (array->entries != NULL) {
        memcpy(entries, array->entries, sizeof(
                    FSDBUpdateBlockInfo) * array->count);
        free(array->entries);
    }

    array->entries = entries;
    return 0;
}

static inline int write_buffer_to_file(const FastBuffer *buffer)
{
    int result;

    if (fc_safe_write(db_updater_ctx.redo.fd, buffer->data,
                buffer->length) != buffer->length)
    {
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "write file %s fail, errno: %d, error info: %s",
                __LINE__, db_updater_ctx.redo.tmp_filename,
                result, STRERROR(result));
        return result;
    }
    return 0;
}

static int write_header(FSDBUpdaterContext *ctx)
{
    int result;

    sf_serializer_pack_begin(&ctx->buffer);
    if ((result=sf_serializer_pack_integer(&ctx->buffer,
                    REDO_HEADER_FIELD_ID_RECORD_COUNT,
                    ctx->array.count)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_int64(&ctx->buffer,
                    REDO_HEADER_FIELD_ID_LAST_FIELD_VERSION,
                    ctx->last_versions.field)) != 0)
    {
        return result;
    }
    if ((result=sf_serializer_pack_int64(&ctx->buffer,
                    REDO_HEADER_FIELD_ID_LAST_BLOCK_VERSION,
                    ctx->last_versions.block.prepare)) != 0)
    {
        return result;
    }

    if ((result=sf_serializer_pack_int64(&ctx->buffer,
                    REDO_HEADER_FIELD_ID_OB_COUNT,
                    STORAGE_ENGINE_OB_COUNT)) != 0)
    {
        return result;
    }
    if ((result=sf_serializer_pack_int64(&ctx->buffer,
                    REDO_HEADER_FIELD_ID_SLICE_COUNT,
                    STORAGE_ENGINE_SLICE_COUNT)) != 0)
    {
        return result;
    }

    sf_serializer_pack_end(&ctx->buffer);

    /*
    logInfo("count: %d, last_versions {field: %"PRId64", block: %"PRId64"}, "
            "buffer length: %d", ctx->array.count, ctx->last_versions.field,
            ctx->last_versions.block.prepare, ctx->buffer.length);
            */

    return write_buffer_to_file(&ctx->buffer);
}

static int write_one_entry(FSDBUpdaterContext *ctx,
        const FSDBUpdateBlockInfo *entry)
{
    int result;

    sf_serializer_pack_begin(&ctx->buffer);

    if ((result=sf_serializer_pack_int64(&ctx->buffer,
                    REDO_ENTRY_FIELD_ID_VERSION,
                    entry->version)) != 0)
    {
        return result;
    }
    if ((result=sf_serializer_pack_int64(&ctx->buffer,
                    REDO_ENTRY_FIELD_ID_BLOCK_OID,
                    entry->bkey.oid)) != 0)
    {
        return result;
    }
    if ((result=sf_serializer_pack_int64(&ctx->buffer,
                    REDO_ENTRY_FIELD_ID_BLOCK_OFFSET,
                    entry->bkey.offset)) != 0)
    {
        return result;
    }

    if (entry->buffer != NULL) {
        if ((result=sf_serializer_pack_buffer(&ctx->buffer,
                        REDO_ENTRY_FIELD_ID_FIELD_BUFFER,
                        entry->buffer)) != 0)
        {
            return result;
        }
    }

    sf_serializer_pack_end(&ctx->buffer);
    return write_buffer_to_file(&ctx->buffer);
}

static int do_write(FSDBUpdaterContext *ctx)
{
    int result;
    FSDBUpdateBlockInfo *entry;
    FSDBUpdateBlockInfo *end;

    if ((result=write_header(ctx)) != 0) {
        return result;
    }

    end = ctx->array.entries + ctx->array.count;
    for (entry=ctx->array.entries; entry<end; entry++) {
        if ((result=write_one_entry(ctx, entry)) != 0) {
            return result;
        }
    }

    if (fsync(db_updater_ctx.redo.fd) != 0) {
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "fsync file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, db_updater_ctx.redo.tmp_filename,
                result, STRERROR(result));
        return result;
    }

    return 0;
}

static int write_redo_log(FSDBUpdaterContext *ctx)
{
    int result;

    if ((result=fc_safe_write_file_open(&db_updater_ctx.redo)) != 0) {
        return result;
    }

    //logInfo("write redo log count =====: %d", ctx->array.count);
    if ((result=do_write(ctx)) != 0) {
        return result;
    }

    return fc_safe_write_file_close(&db_updater_ctx.redo);
}

static int unpack_from_file(SFSerializerIterator *it,
        const char *caption, BufferInfo *buffer)
{
    const int max_size = 256 * 1024 * 1024;
    int result;
    string_t content;

    if ((result=sf_serializer_read_message(db_updater_ctx.
                    redo.fd, buffer, max_size)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "read %s message from file %s fail, "
                "errno: %d, error info: %s", __LINE__, caption,
                db_updater_ctx.redo.filename, result, STRERROR(result));
        return result;
    }

    FC_SET_STRING_EX(content, buffer->buff, buffer->length);
    if ((result=sf_serializer_unpack(it, &content)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "file: %s, unpack %s fail, errno: %d, error info: %s",
                __LINE__, db_updater_ctx.redo.filename, caption,
                it->error_no, it->error_info);
        return result;
    }

    return 0;
}

static int unpack_header(SFSerializerIterator *it,
        FSDBUpdaterContext *ctx, BufferInfo *buffer,
        int *record_count)
{
    int result;
    const SFSerializerFieldValue *fv;

    *record_count = 0;
    ctx->last_versions.field = ctx->last_versions.block.prepare = 0;
    if ((result=unpack_from_file(it, "header", buffer)) != 0) {
        return result;
    }

    while ((fv=sf_serializer_next(it)) != NULL) {
        switch (fv->fid) {
            case REDO_HEADER_FIELD_ID_RECORD_COUNT:
                *record_count = fv->value.n;
                break;
            case REDO_HEADER_FIELD_ID_LAST_FIELD_VERSION:
                ctx->last_versions.field = fv->value.n;
                break;
            case REDO_HEADER_FIELD_ID_LAST_BLOCK_VERSION:
                ctx->last_versions.block.prepare = fv->value.n;
                break;
            case REDO_HEADER_FIELD_ID_OB_COUNT:
                STORAGE_ENGINE_OB_COUNT = fv->value.n;
                break;
            case REDO_HEADER_FIELD_ID_SLICE_COUNT:
                STORAGE_ENGINE_SLICE_COUNT = fv->value.n;
                break;
            default:
                break;
        }
    }
    if (*record_count == 0 || ctx->last_versions.field == 0 ||
            ctx->last_versions.block.prepare == 0)
    {
        logError("file: "__FILE__", line: %d, "
                "file: %s, invalid packed header, record_count: %d, "
                "last_versions {field: %"PRId64", block: %"PRId64"}",
                __LINE__, db_updater_ctx.redo.filename, *record_count,
                ctx->last_versions.field, ctx->last_versions.block.prepare);
        return EINVAL;
    }

    return 0;
}

static int unpack_one_block(SFSerializerIterator *it,
        FSDBUpdaterContext *ctx, BufferInfo *buffer,
        const int rowno)
{
    int result;
    char caption[32];
    FSDBUpdateBlockInfo *entry;
    const SFSerializerFieldValue *fv;

    sprintf(caption, "block #%d", rowno);
    if ((result=unpack_from_file(it, caption, buffer)) != 0) {
        return result;
    }

    if (ctx->array.count >= ctx->array.alloc) {
        if ((result=db_updater_realloc_block_array(&ctx->array)) != 0) {
            return result;
        }
    }

    entry = ctx->array.entries + ctx->array.count;
    entry->buffer = NULL;
    while ((fv=sf_serializer_next(it)) != NULL) {
        switch (fv->fid) {
            case REDO_ENTRY_FIELD_ID_VERSION:
                entry->version = fv->value.n;
                break;
            case REDO_ENTRY_FIELD_ID_BLOCK_OID:
                entry->bkey.oid = fv->value.n;
                break;
            case REDO_ENTRY_FIELD_ID_BLOCK_OFFSET:
                entry->bkey.offset = fv->value.n;
                break;
            case REDO_ENTRY_FIELD_ID_FIELD_BUFFER:
                if ((entry->buffer=block_serializer_to_buffer(
                                &fv->value.s)) == NULL)
                {
                    return ENOMEM;
                }
            default:
                break;
        }
    }

    if (it->error_no != 0) {
        logError("file: "__FILE__", line: %d, "
                "file: %s, unpack entry fail, "
                "errno: %d, error info: %s",
                __LINE__, db_updater_ctx.redo.filename,
                it->error_no, it->error_info);
        return it->error_no;
    }
    fs_calc_block_hashcode(&entry->bkey);

    ctx->array.count++;
    return 0;
}

static int do_load(FSDBUpdaterContext *ctx)
{
    int result;
    int i;
    int record_count;
    BufferInfo buffer;
    SFSerializerIterator it;

    if ((result=fc_init_buffer(&buffer, 4 * 1024)) != 0) {
        return result;
    }

    sf_serializer_iterator_init(&it);
    ctx->array.count = 0;
    if ((result=unpack_header(&it, ctx, &buffer, &record_count)) != 0) {
        return result;
    }

    for (i=0; i<record_count; i++) {
        if ((result=unpack_one_block(&it, ctx, &buffer, i + 1)) != 0) {
            break;
        }
    }

    fc_free_buffer(&buffer);
    sf_serializer_iterator_destroy(&it);
    return result;
}

static int resume_from_redo_log(FSDBUpdaterContext *ctx)
{
    int result;

    if ((db_updater_ctx.redo.fd=open(db_updater_ctx.redo.
                    filename, O_RDONLY | O_CLOEXEC)) < 0)
    {
        result = errno != 0 ? errno : EIO;
        if (result == ENOENT) {
            return 0;
        }
        logError("file: "__FILE__", line: %d, "
                "open file %s fail, errno: %d, error info: %s",
                __LINE__, db_updater_ctx.redo.filename,
                result, STRERROR(result));
        return result;
    }

    result = do_load(ctx);
    close(db_updater_ctx.redo.fd);

    logInfo("last_versions {field: %"PRId64", block: %"PRId64"}",
            ctx->last_versions.field, ctx->last_versions.block.prepare);

    if (result != 0) {
        return result;
    }

    if ((result=STORAGE_ENGINE_REDO_API(&ctx->array)) != 0) {
        return result;
    }

    ctx->last_versions.block.commit = ctx->last_versions.block.prepare;
    event_dealer_free_buffers(&ctx->array);
    return 0;
}

int db_updater_init(FSDBUpdaterContext *ctx)
{
    int result;

    if ((result=fc_safe_write_file_init(&db_updater_ctx.redo,
                    STORAGE_PATH_STR, REDO_LOG_FILENAME,
                    REDO_TMP_FILENAME)) != 0)
    {
        return result;
    }

    return resume_from_redo_log(ctx);
}

void db_updater_destroy()
{
}

int db_updater_deal(FSDBUpdaterContext *ctx)
{
    int result;

    if ((result=write_redo_log(ctx)) != 0) {
        return result;
    }

    return STORAGE_ENGINE_STORE_API(&ctx->array);
}
