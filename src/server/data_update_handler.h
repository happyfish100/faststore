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

//data_update_handler.h

#ifndef FS_DATA_UPDATE_HANDLER_H
#define FS_DATA_UPDATE_HANDLER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fastcommon/fast_task_queue.h"
#include "server_types.h"
#include "data_thread.h"

#ifdef __cplusplus
extern "C" {
#endif

FSServerContext *du_handler_alloc_server_context();

#define du_handler_parse_check_readable_block_slice(task, bs) \
    du_handler_parse_check_block_slice(task, &SLICE_OP_CTX, bs, false)

int du_handler_parse_check_block_slice(struct fast_task_info *task,
        FSSliceOpContext *op_ctx, const FSProtoBlockSlice *bs,
        const bool master_only);

void du_handler_fill_slice_update_response(struct fast_task_info *task,
        const int inc_alloc);

void du_handler_idempotency_request_finish(struct fast_task_info *task,
        const int result);

void du_handler_slice_read_done_callback(FSSliceOpContext *op_ctx,
        struct fast_task_info *task);

void du_handler_slice_read_done_notify(FSDataOperation *op);

int du_handler_deal_slice_write(struct fast_task_info *task,
        FSSliceOpContext *op_ctx);

int du_handler_deal_slice_allocate(struct fast_task_info *task,
        FSSliceOpContext *op_ctx);

int du_handler_deal_slice_delete(struct fast_task_info *task,
        FSSliceOpContext *op_ctx);

int du_handler_deal_block_delete(struct fast_task_info *task,
        FSSliceOpContext *op_ctx);

int du_handler_deal_client_join(struct fast_task_info *task);

int du_handler_deal_get_readable_server(struct fast_task_info *task,
        const int group_index);

int du_handler_deal_get_group_servers(struct fast_task_info *task);

static inline void du_handler_set_slice_op_error_msg(struct fast_task_info *
        task, FSSliceOpContext *op_ctx, const char *caption, const int result)
{
    RESPONSE.error.length = sprintf(RESPONSE.error.message,
            "%s fail, result: %d, block {oid: %"PRId64", "
            "offset: %"PRId64"}, slice {offset: %d, length: %d}",
            caption, result, op_ctx->info.bs_key.block.oid,
            op_ctx->info.bs_key.block.offset,
            op_ctx->info.bs_key.slice.offset,
            op_ctx->info.bs_key.slice.length);
}

#ifdef __cplusplus
}
#endif

#endif
