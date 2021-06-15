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

#include <stdlib.h>
#include "../fs_api_allocator.h"
#include "../fs_api.h"
#include "../write_combine/obid_htable.h"
#include "otid_htable.h"

//FSAPIBuffer
typedef struct fs_preread_otid_entry {
    SFShardingHashEntry hentry;  //must be the first
    int successive_count;
    int64_t last_read_offset;
    FSAPIBuffer *buffer;
} FSPrereadOTIDEntry;

typedef struct fs_api_insert_buffer_context {
    FSAPIOperationContext *op_ctx;
    FSAPIBuffer *buffer;
    char *out_buff;
    int *read_bytes;
    int *ahead_bytes;
    struct {
        int successive_count;
        struct fs_preread_otid_entry *entry;
    } otid;
} FSAPIInsertBufferContext;

static SFHtableShardingContext otid_ctx;

static int otid_htable_insert_callback(SFShardingHashEntry *he,
        void *arg, const bool new_create)
{
    FSPrereadOTIDEntry *entry;
    FSAPIInsertBufferContext *ictx;
    int64_t offset;

    entry = (FSPrereadOTIDEntry *)he;
    ictx = (FSAPIInsertBufferContext *)arg;
    offset = ictx->op_ctx->bs_key.block.offset +
        ictx->op_ctx->bs_key.slice.offset;
    if (new_create) {
        entry->buffer = NULL;
        entry->successive_count = 0;
    } else {
        if (offset == entry->last_read_offset) {
            entry->successive_count++;
        } else {
            entry->successive_count = 0;
        }
    }

    //TODO
    //*read_bytes = ictx.cache_hit ? op_ctx->bs_key.slice.length : 0;
    //ictx.ahead_bytes
    //memcpy(buff, ictx.buffer->buff + ictx.offset, *read_bytes);

    entry->last_read_offset = offset + ictx->op_ctx->bs_key.slice.length;
    ictx->otid.entry = entry;
    ictx->otid.successive_count = entry->successive_count;
    return 0;
}

static bool otid_htable_accept_reclaim_callback(SFShardingHashEntry *he)
{
    //TODO
    return ((FSPrereadOTIDEntry *)he)->buffer == NULL;
}

int preread_otid_htable_init(const int sharding_count,
        const int64_t htable_capacity,
        const int allocator_count, int64_t element_limit,
        const int64_t min_ttl_ms, const int64_t max_ttl_ms,
        const double low_water_mark_ratio)
{
    return sf_sharding_htable_init_ex(&otid_ctx,
            sf_sharding_htable_key_ids_two,
            otid_htable_insert_callback, NULL,
            otid_htable_accept_reclaim_callback, sharding_count,
            htable_capacity, allocator_count, sizeof(FSPrereadOTIDEntry),
            element_limit, min_ttl_ms, max_ttl_ms, low_water_mark_ratio);
}

static FSAPIBuffer *preread_fetch_and_predict(FSAPIOperationContext
        *op_ctx, char *buff, int *read_bytes, int *ahead_bytes)
{
    SFTwoIdsHashKey key;
    FSAPIInsertBufferContext ictx;
    int result;

    key.oid = op_ctx->bs_key.block.oid;
    key.tid = op_ctx->tid;
    ictx.op_ctx = op_ctx;
    ictx.out_buff = buff;
    ictx.read_bytes = read_bytes;
    ictx.ahead_bytes = ahead_bytes;
    ictx.buffer = NULL;
    if ((result=sf_sharding_htable_insert(&otid_ctx, &key, &ictx)) != 0) {
        *read_bytes = *ahead_bytes = 0;
        return NULL;
    }

    return ictx.buffer;
}

int preread_otid_htable_insert(FSAPIOperationContext *op_ctx,
        FSAPIBuffer *buffer)
{
    SFTwoIdsHashKey key;
    FSAPIInsertBufferContext ictx;
    int result;

    key.oid = op_ctx->bs_key.block.oid;
    key.tid = op_ctx->tid;
    ictx.op_ctx = op_ctx;
    ictx.buffer = buffer;
    if ((result=sf_sharding_htable_insert(&otid_ctx, &key, &ictx)) != 0) {
        return result;
    }

    if (ictx.otid.successive_count > 0) {
        //TODO
        /*
           result = obid_htable_check_combine_slice(&ictx);
           */
    }

    /*
    logInfo("file: "__FILE__", line: %d, "
            "tid: %"PRId64", block {oid: %"PRId64", offset: %"PRId64"}, "
            "slice {offset: %d, length: %d}, successive_count: %d, "
            "combined: %d, reason: %d", __LINE__, op_ctx->tid,
            op_ctx->bs_key.block.oid, op_ctx->bs_key.block.offset,
            op_ctx->bs_key.slice.offset, op_ctx->bs_key.slice.length,
            ictx.otid.successive_count, buffer->combined, buffer->reason);
            */

    return result;
}

int preread_slice_read(FSAPIOperationContext *op_ctx,
            char *buff, int *read_bytes)
{
    //int result;
    int ahead_bytes;
    FSAPIBuffer *buffer;

    buffer = preread_fetch_and_predict(op_ctx, buff,
            read_bytes, &ahead_bytes);
    if (*read_bytes > 0) {
        return 0;
    }

    if (buffer == NULL) {
        FS_API_CHECK_CONFLICT_AND_WAIT(op_ctx, 'r');
        return fs_client_slice_read(op_ctx->api_ctx->fs,
                &op_ctx->bs_key, buff, read_bytes);
    }

    //TODO
    return 0;
}
