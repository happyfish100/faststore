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

//replication_callee.h

#ifndef _REPLICATION_CALLEE_H_
#define _REPLICATION_CALLEE_H_

#include "replication_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int replication_callee_init();
void replication_callee_destroy();
void replication_callee_terminate();

int replication_callee_init_allocator(FSServerContext *server_context);

static inline FSSliceOpBufferContext *replication_callee_alloc_op_buffer_ctx(
        FSServerContext *server_context)
{
    return (FSSliceOpBufferContext *)fast_mblock_alloc_object(
            &server_context->replica.op_ctx_allocator);
}

static inline void replication_callee_free_op_buffer_ctx(
        FSServerContext *server_context,
        FSSliceOpBufferContext *op_buffer)
{
    fast_mblock_free_object(&server_context->replica.
            op_ctx_allocator, op_buffer);
}

static inline SharedBuffer *replication_callee_alloc_shared_buffer(
        FSServerContext *server_context)
{
    return shared_buffer_alloc_ex(&server_context->
            replica.shared_buffer_ctx, 1);
}

int replication_callee_push_to_rpc_result_queue(FSReplication *replication,
        const int data_group_id, const uint64_t data_version, const int err_no);

int replication_callee_deal_rpc_result_queue(FSReplication *replication);

#ifdef __cplusplus
}
#endif

#endif
