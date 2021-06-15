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
    pthread_mutex_t *lock;
    char *out_buff;
    int *read_bytes;
    int ahead_bytes;
} FSAPIInsertBufferContext;

static SFHtableShardingContext otid_ctx;

static void process_on_successive(FSAPIInsertBufferContext *ictx,
        FSPrereadOTIDEntry *entry)
{
    int bytes;
    int block_remain;
    int min_bytes;
    int i;

    if (entry->buffer != NULL && !entry->buffer->dirty &&
            (ictx->op_ctx->bs_key.slice.length <=
             (entry->buffer->length - entry->buffer->offset)))
    {
        *(ictx->read_bytes) = ictx->op_ctx->bs_key.slice.length;
        memcpy(ictx->out_buff, entry->buffer->buff +
                entry->buffer->offset, *(ictx->read_bytes));
        entry->buffer->offset += *(ictx->read_bytes);
    } else if (ictx->op_ctx->bs_key.slice.length < ictx->op_ctx->
            api_ctx->read_ahead.skip_preread_on_slice_size)
    {
        block_remain = FS_FILE_BLOCK_SIZE - (ictx->op_ctx->bs_key.
                slice.offset + ictx->op_ctx->bs_key.slice.length);
        min_bytes = FC_MIN(block_remain, ictx->op_ctx->
                api_ctx->read_ahead.preread_max_size);
        if (entry->buffer != NULL) {
            bytes = entry->buffer->allocator->buffer_size * 2;
        } else {
            bytes = ictx->op_ctx->api_ctx->read_ahead.preread_min_size;
            while (bytes < ictx->op_ctx->bs_key.slice.length) {
                bytes *= 2;
            }

            i = 0;
            while (i++ < entry->successive_count && bytes < min_bytes) {
                bytes *= 2;
            }
        }

        ictx->ahead_bytes = FC_MIN(bytes, min_bytes) -
            ictx->op_ctx->bs_key.slice.length;
    }
}

static int otid_htable_insert_callback(SFShardingHashEntry *he,
        void *arg, const bool new_create)
{
    FSPrereadOTIDEntry *entry;
    FSAPIInsertBufferContext *ictx;
    int64_t offset;
    bool release_buffer;

    entry = (FSPrereadOTIDEntry *)he;
    ictx = (FSAPIInsertBufferContext *)arg;
    offset = ictx->op_ctx->bs_key.block.offset +
        ictx->op_ctx->bs_key.slice.offset;
    if (new_create) {
        entry->buffer = NULL;
        entry->successive_count = 0;
        release_buffer = false;
    } else {
        if (offset == entry->last_read_offset) {
            entry->successive_count++;
            if (entry->buffer != NULL) {
                release_buffer = entry->buffer->dirty ||
                    (ictx->op_ctx->bs_key.slice.length >= entry->
                     buffer->length - entry->buffer->offset);
            } else {
                release_buffer = false;
            }
        } else {
            entry->successive_count = 0;
            release_buffer = (entry->buffer != NULL);
        }
    }

    if (entry->successive_count > 0) {
        process_on_successive(ictx, entry);
    }

    entry->last_read_offset = offset + ictx->op_ctx->bs_key.slice.length;
    if (release_buffer) {
        fs_api_buffer_release(entry->buffer);
        entry->buffer = NULL;
    }

    if (ictx->ahead_bytes > 0) {
        int read_bytes;

        read_bytes = ictx->op_ctx->bs_key.slice.length +
            ictx->ahead_bytes;
        entry->buffer = fs_api_buffer_alloc(&ictx->op_ctx->
                allocator_ctx->buffer_pool, read_bytes, 2);
        if (entry->buffer == NULL) {
            ictx->ahead_bytes = 0;
            return ENOMEM;
        }

        entry->buffer->dirty = true;
        entry->buffer->offset = ictx->op_ctx->bs_key.slice.length;
        entry->buffer->length = read_bytes;
        ictx->buffer = entry->buffer;
        ictx->lock = &he->sharding->lock;
    }

    return 0;
}

static bool otid_htable_accept_reclaim_callback(SFShardingHashEntry *he)
{
    FSPrereadOTIDEntry *entry;

    entry = (FSPrereadOTIDEntry *)he;
    if (entry->buffer != NULL) {
        fs_api_buffer_release(entry->buffer);
        entry->buffer = NULL;
    }

    return true;
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

int preread_slice_read(FSAPIOperationContext *op_ctx,
            char *buff, int *read_bytes)
{
    int result;
    int old_slice_len;
    int bytes;
    SFTwoIdsHashKey key;
    FSAPIInsertBufferContext ictx;

    *read_bytes = 0;
    key.oid = op_ctx->bs_key.block.oid;
    key.tid = op_ctx->tid;
    ictx.op_ctx = op_ctx;
    ictx.out_buff = buff;
    ictx.read_bytes = read_bytes;
    ictx.ahead_bytes = 0;
    ictx.buffer = NULL;
    ictx.lock = NULL;

    sf_sharding_htable_insert(&otid_ctx, &key, &ictx);
    if (*read_bytes > 0) {
        return 0;
    }

    if (ictx.buffer == NULL) {
        FS_API_CHECK_CONFLICT_AND_WAIT(op_ctx, 'r');
        return fs_client_slice_read(op_ctx->api_ctx->fs,
                &op_ctx->bs_key, buff, read_bytes);
    }

    old_slice_len = op_ctx->bs_key.slice.length;
    op_ctx->bs_key.slice.length = ictx.buffer->length;

    FS_API_CHECK_CONFLICT_AND_WAIT(op_ctx, 'r');
    result = fs_client_slice_read(op_ctx->api_ctx->fs,
            &op_ctx->bs_key, ictx.buffer->buff, &bytes);
    if (result == 0) {
        *read_bytes = FC_MIN(bytes, old_slice_len);
        memcpy(buff, ictx.buffer->buff, *read_bytes);
        if (bytes > old_slice_len) {
            PTHREAD_MUTEX_LOCK(ictx.lock);
            ictx.buffer->dirty = false;
            if (bytes < ictx.buffer->length) {
                ictx.buffer->length = bytes;
            }
            PTHREAD_MUTEX_UNLOCK(ictx.lock);
        }
    } else {
        *read_bytes = 0;
    }

    fs_api_buffer_release(ictx.buffer);
    op_ctx->bs_key.slice.length = old_slice_len;  //restore length
    return result;
}
