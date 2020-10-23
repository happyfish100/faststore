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


#ifndef _FS_CLIENT_H
#define _FS_CLIENT_H

#include "fs_proto.h"
#include "fs_types.h"
#include "fs_func.h"
#include "client_types.h"
#include "client_func.h"
#include "client_global.h"
#include "client_proto.h"
#include "simple_connection_manager.h"

#ifdef __cplusplus
extern "C" {
#endif

static inline void fs_set_block_key(FSBlockKey *bkey,
        const int64_t oid, const int64_t offset)
{
    bkey->oid = oid;
    bkey->offset = FS_FILE_BLOCK_ALIGN(offset);
    fs_calc_block_hashcode(bkey);
}

static inline void fs_set_slice_size(FSBlockSliceKeyInfo *bs_key,
        const int64_t offset, const int current_size)
{
    bs_key->slice.offset = offset - bs_key->block.offset;
    if (bs_key->slice.offset + current_size <= FS_FILE_BLOCK_SIZE) {
        bs_key->slice.length = current_size;
    } else {
        bs_key->slice.length = FS_FILE_BLOCK_SIZE - bs_key->slice.offset;
    }
}

static inline void fs_set_block_slice(FSBlockSliceKeyInfo *bs_key,
        const int64_t oid, const int64_t offset, const int current_size)
{
    fs_set_block_key(&bs_key->block, oid, offset);
    fs_set_slice_size(bs_key, offset, current_size);
}

static inline void fs_next_block_key(FSBlockKey *bkey)
{
    bkey->offset += FS_FILE_BLOCK_SIZE;
    fs_calc_block_hashcode(bkey);
}

static inline void fs_next_block_slice_key(FSBlockSliceKeyInfo *bs_key,
        const int current_size)
{
    fs_next_block_key(&bs_key->block);

    bs_key->slice.offset = 0;
    if (current_size <= FS_FILE_BLOCK_SIZE) {
        bs_key->slice.length = current_size;
    } else {
        bs_key->slice.length = FS_FILE_BLOCK_SIZE;
    }
}

int fs_unlink_file(FSClientContext *client_ctx, const int64_t oid,
        const int64_t file_size);

int fs_cluster_stat(FSClientContext *client_ctx, const int data_group_id,
        FSClientClusterStatEntry *stats, const int size, int *count);

int fs_client_slice_write(FSClientContext *client_ctx,
        const FSBlockSliceKeyInfo *bs_key, const char *data,
        int *write_bytes, int *inc_alloc);

int fs_client_slice_read_ex(FSClientContext *client_ctx,
        const int slave_id, const int req_cmd, const int resp_cmd,
        const FSBlockSliceKeyInfo *bs_key, char *buff, int *read_bytes);

int fs_client_bs_operate(FSClientContext *client_ctx,
        const void *key, const uint32_t hash_code,
        const int req_cmd, const int resp_cmd,
        const int enoent_log_level, int *inc_alloc);

#define fs_client_slice_allocate_ex(client_ctx, bs_key, \
        enoent_log_level, inc_alloc) \
    fs_client_bs_operate(client_ctx, bs_key,       \
            (bs_key)->block.hash_code,             \
            FS_SERVICE_PROTO_SLICE_ALLOCATE_REQ,   \
            FS_SERVICE_PROTO_SLICE_ALLOCATE_RESP,  \
            enoent_log_level, inc_alloc)

#define fs_client_slice_read(client_ctx, bs_key, buff, read_bytes) \
    fs_client_slice_read_ex(client_ctx, 0, FS_SERVICE_PROTO_SLICE_READ_REQ, \
            FS_SERVICE_PROTO_SLICE_READ_RESP, bs_key, buff, read_bytes)

#define fs_client_slice_read_by_slave(client_ctx, \
        slave_id, bs_key, buff, read_bytes) \
    fs_client_slice_read_ex(client_ctx, slave_id, \
            FS_REPLICA_PROTO_SLICE_READ_REQ,  \
            FS_REPLICA_PROTO_SLICE_READ_RESP, \
            bs_key, buff, read_bytes)

#define fs_client_slice_delete_ex(client_ctx, bs_key, \
        enoent_log_level, dec_alloc) \
    fs_client_bs_operate(client_ctx, bs_key,    \
            (bs_key)->block.hash_code,          \
            FS_SERVICE_PROTO_SLICE_DELETE_REQ,  \
            FS_SERVICE_PROTO_SLICE_DELETE_RESP, \
            enoent_log_level, dec_alloc)

#define fs_client_block_delete_ex(client_ctx, bkey, \
        enoent_log_level, dec_alloc)   \
    fs_client_bs_operate(client_ctx, bkey, (bkey)->hash_code, \
            FS_SERVICE_PROTO_BLOCK_DELETE_REQ,  \
            FS_SERVICE_PROTO_BLOCK_DELETE_RESP, \
            enoent_log_level, dec_alloc)


#define fs_client_slice_allocate(client_ctx, bs_key, inc_alloc) \
    fs_client_slice_allocate_ex(client_ctx, bs_key, LOG_DEBUG, inc_alloc)

#define fs_client_slice_delete(client_ctx, bs_key, dec_alloc) \
    fs_client_slice_delete_ex(client_ctx, bs_key, LOG_DEBUG, dec_alloc)

#define fs_client_block_delete(client_ctx, bkey, dec_alloc) \
    fs_client_block_delete_ex(client_ctx, bkey, LOG_DEBUG, dec_alloc)

int fs_client_server_group_space_stat(FSClientContext *client_ctx,
        FCServerInfo *server, FSClientServerSpaceStat *stats,
        const int size, int *count);

int fs_client_cluster_space_stat(FSClientContext *client_ctx,
        FSClusterSpaceStat *stat);

#ifdef __cplusplus
}
#endif

#endif
