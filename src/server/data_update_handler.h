//data_update_handler.h

#ifndef FS_DATA_UPDATE_HANDLER_H
#define FS_DATA_UPDATE_HANDLER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fastcommon/fast_task_queue.h"
#include "server_types.h"

#ifdef __cplusplus
extern "C" {
#endif

#define du_handler_parse_check_readable_block_slice(task, bs) \
    du_handler_parse_check_block_slice(task, &SLICE_OP_CTX, bs, false)

int du_handler_parse_check_block_slice(struct fast_task_info *task,
        FSSliceOpContext *op_ctx, const FSProtoBlockSlice *bs,
        const bool master_only);

int du_handler_deal_slice_write(struct fast_task_info *task,
        FSSliceOpContext *op_ctx);

int du_handler_deal_slice_allocate(struct fast_task_info *task,
        FSSliceOpContext *op_ctx);

int du_handler_deal_slice_delete(struct fast_task_info *task,
        FSSliceOpContext *op_ctx);

int du_handler_deal_block_delete(struct fast_task_info *task,
        FSSliceOpContext *op_ctx);

static inline void du_handler_set_slice_op_error_msg(struct fast_task_info *
        task, FSSliceOpContext *op_ctx, const char *caption, const int result)
{
    RESPONSE.error.length = sprintf(RESPONSE.error.message,
            "slice %s fail, result: %d, block {oid: %"PRId64", "
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
