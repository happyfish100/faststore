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


#ifndef _FS_API_H
#define _FS_API_H

#include "fs_api_types.h"
#include "fs_api_allocator.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FS_API_SET_CTX_AND_TID_EX(op_ctx, _api_ctx, thread_id) \
    do { \
        (op_ctx).api_ctx = _api_ctx; \
        (op_ctx).tid = thread_id;    \
    } while (0)

#define FS_API_SET_CTX_AND_TID(op_ctx, thread_id) \
    FS_API_SET_CTX_AND_TID_EX(op_ctx, &g_fs_api_ctx, thread_id)

#define FS_API_SET_BID_AND_ALLOCATOR_CTX(op_ctx) \
    op_ctx->bid = op_ctx->bs_key.block.offset;   \
    op_ctx->allocator_ctx = fs_api_allocator_get(op_ctx->tid)

#define FS_API_CHECK_CONFLICT_AND_WAIT(op_ctx) \
    do {  \
        if (op_ctx->api_ctx->write_combine.enabled) {  \
            int conflict_count;  \
            wcombine_obid_htable_check_conflict_and_wait( \
                    op_ctx, &conflict_count); \
        } \
    } while (0)

#define fs_api_init(ini_ctx, write_done_callback, write_done_arg_extra_size) \
    fs_api_init_ex(&g_fs_api_ctx, ini_ctx, write_done_callback, \
            write_done_arg_extra_size)

#define fs_api_config_to_string(output, size) \
    fs_api_config_to_string_ex(&g_fs_api_ctx, output, size)

void fs_api_config_to_string_ex(FSAPIContext *api_ctx,
        char *output, const int size);

#define fs_api_start() fs_api_start_ex(&g_fs_api_ctx)

#define fs_api_terminate()   fs_api_terminate_ex(&g_fs_api_ctx);

int fs_api_init_ex(FSAPIContext *api_ctx, IniFullContext *ini_ctx,
        fs_api_write_done_callback write_done_callback,
        const int write_done_arg_extra_size);

void fs_api_destroy_ex(FSAPIContext *api_ctx);

int fs_api_start_ex(FSAPIContext *api_ctx);

void fs_api_terminate_ex(FSAPIContext *api_ctx);

int fs_api_unlink_file(FSAPIContext *api_ctx, const int64_t oid,
        const int64_t file_size, const uint64_t tid);

int fs_api_slice_write(FSAPIOperationContext *op_ctx,
        FSAPIWriteBuffer *wbuffer, int *write_bytes, int *inc_alloc);

int fs_api_slice_read_ex(FSAPIOperationContext *op_ctx,
        char *buff, int *read_bytes, const bool is_prefetch);

static inline int fs_api_slice_read(FSAPIOperationContext *op_ctx,
        char *buff, int *read_bytes)
{
    const bool is_prefetch = false;
    return fs_api_slice_read_ex(op_ctx, buff, read_bytes, is_prefetch);
}

int fs_api_slice_readv(FSAPIOperationContext *op_ctx,
        const struct iovec *iov, const int iovcnt, int *read_bytes);

int fs_api_slice_allocate_ex(FSAPIOperationContext *op_ctx,
        const int enoent_log_level, int *inc_alloc);

int fs_api_slice_delete_ex(FSAPIOperationContext *op_ctx,
        const int enoent_log_level, int *dec_alloc);

int fs_api_block_delete_ex(FSAPIOperationContext *op_ctx,
        const int enoent_log_level, int *dec_alloc);

#define fs_api_slice_allocate(op_ctx, inc_alloc) \
    fs_api_slice_allocate_ex(op_ctx, LOG_DEBUG, inc_alloc)

#define fs_api_slice_delete(op_ctx, dec_alloc) \
    fs_api_slice_delete_ex(op_ctx, LOG_DEBUG, dec_alloc)

#define fs_api_block_delete(op_ctx, dec_alloc) \
    fs_api_block_delete_ex(op_ctx, LOG_DEBUG, dec_alloc)

#define fs_api_cluster_stat(api_ctx, data_group_id, stats, size, count) \
    fs_cluster_stat(api_ctx->fs, data_group_id, stats, size, count)

#define fs_api_server_group_space_stat(api_ctx, server, stats, size, count) \
    fs_server_group_space_stat(api_ctx->fs, server, stats, size, count)

#define fs_api_cluster_space_stat(api_ctx, stat) \
    fs_client_cluster_space_stat(api_ctx->fs, stat)

#ifdef __cplusplus
}
#endif

#endif
