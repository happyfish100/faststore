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

//data_update_handler.c

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/ioevent_loop.h"
#include "sf/sf_util.h"
#include "sf/sf_func.h"
#include "sf/sf_nio.h"
#include "sf/sf_global.h"
#include "sf/idempotency/server/server_channel.h"
#include "common/fs_proto.h"
#include "common/fs_func.h"
#include "binlog/replica_binlog.h"
#include "server_replication.h"
#include "server_global.h"
#include "server_func.h"
#include "server_group_info.h"
#include "server_storage.h"
#include "data_thread.h"
#include "data_update_handler.h"

static int parse_check_block_key_ex(struct fast_task_info *task,
        FSSliceOpContext *op_ctx, const FSProtoBlockKey *bkey,
        const bool master_only)
{
    op_ctx->info.bs_key.block.oid = buff2long(bkey->oid);
    op_ctx->info.bs_key.block.offset = buff2long(bkey->offset);
    if (op_ctx->info.bs_key.block.offset % FS_FILE_BLOCK_SIZE != 0) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message, "block offset: %"PRId64" "
                "NOT the multiple of the block size %d",
                op_ctx->info.bs_key.block.offset, FS_FILE_BLOCK_SIZE);
        return EINVAL;
    }

    fs_calc_block_hashcode(&op_ctx->info.bs_key.block);
    op_ctx->info.data_group_id = FS_DATA_GROUP_ID(op_ctx->info.bs_key.block);

    op_ctx->info.myself = fs_get_my_data_server(op_ctx->info.data_group_id);
    if (op_ctx->info.myself == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data group id: %d NOT belongs to me",
                op_ctx->info.data_group_id);
        return ENOENT;
    }

    if (master_only) {
        if (!__sync_add_and_fetch(&op_ctx->info.myself->is_master, 0)) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "data group id: %d, i am NOT master",
                    op_ctx->info.data_group_id);
            return SF_RETRIABLE_ERROR_NOT_MASTER;
        }
    } else {
        if (op_ctx->info.myself->status != FS_SERVER_STATUS_ACTIVE) {
            int status;
            status = __sync_add_and_fetch(&op_ctx->info.myself->status, 0);
            if (status != FS_SERVER_STATUS_ACTIVE) {
                RESPONSE.error.length = sprintf(RESPONSE.error.message,
                        "data group id: %d, i am NOT active, "
                        "my status: %d (%s)", op_ctx->info.data_group_id,
                        status, fs_get_server_status_caption(status));
                return SF_RETRIABLE_ERROR_NOT_ACTIVE;
            }
        }
    }

    return 0;
}

int du_handler_parse_check_block_slice(struct fast_task_info *task,
        FSSliceOpContext *op_ctx, const FSProtoBlockSlice *bs,
        const bool master_only)
{
    int result;

    if ((result=parse_check_block_key_ex(task, op_ctx,
                    &bs->bkey, master_only)) != 0)
    {
        return result;
    }

    op_ctx->info.bs_key.slice.offset = buff2int(bs->slice_size.offset);
    op_ctx->info.bs_key.slice.length = buff2int(bs->slice_size.length);
    if (op_ctx->info.bs_key.slice.offset < 0 || op_ctx->info.bs_key.slice.offset >=
            FS_FILE_BLOCK_SIZE)
    {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message, "slice offset: %d "
                "is invalid which < 0 or exceeds the block size %d",
                op_ctx->info.bs_key.slice.offset, FS_FILE_BLOCK_SIZE);
        return EINVAL;
    }
    if (op_ctx->info.bs_key.slice.length <= 0 || op_ctx->info.bs_key.slice.offset +
            op_ctx->info.bs_key.slice.length > FS_FILE_BLOCK_SIZE)
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "slice offset: %d, length: %d is invalid which <= 0, "
                "or offset + length exceeds the block size %d",
                op_ctx->info.bs_key.slice.offset,
                op_ctx->info.bs_key.slice.length, FS_FILE_BLOCK_SIZE);
        return EINVAL;
    }

    return 0;
}

void du_handler_fill_slice_update_response(struct fast_task_info *task,
        const int inc_alloc)
{
    FSProtoSliceUpdateResp *resp;
    resp = (FSProtoSliceUpdateResp *)REQUEST.body;
    int2buff(inc_alloc, resp->inc_alloc);

    RESPONSE.header.body_len = sizeof(FSProtoSliceUpdateResp);
    TASK_ARG->context.response_done = true;
}

void du_handler_idempotency_request_finish(struct fast_task_info *task,
        const int result)
{
    if (SERVER_TASK_TYPE == SF_SERVER_TASK_TYPE_CHANNEL_USER &&
            IDEMPOTENCY_REQUEST != NULL)
    {
        IDEMPOTENCY_REQUEST->finished = true;
        IDEMPOTENCY_REQUEST->output.result = result;
        ((FSUpdateOutput *)IDEMPOTENCY_REQUEST->output.response)->
            inc_alloc = SLICE_OP_CTX.update.space_changed;
        idempotency_request_release(IDEMPOTENCY_REQUEST);

        /* server task type for channel ONLY, do NOT set task type to NONE!!! */
        IDEMPOTENCY_REQUEST = NULL;
    }
}

static void master_data_update_done_notify(FSDataOperation *op)
{
    struct fast_task_info *task;
    const char *caption;

    task = (struct fast_task_info *)op->arg;
    if (op->ctx->result != 0) {

        RESPONSE.error.length = snprintf(RESPONSE.error.message,
                sizeof(RESPONSE.error.message),
                "%s", STRERROR(op->ctx->result));

        caption = fs_get_data_operation_caption(op->operation);
        logError("file: "__FILE__", line: %d, "
                "client ip: %s, %s fail, "
                "oid: %"PRId64", block offset: %"PRId64", "
                "slice offset: %d, length: %d, "
                "errno: %d, error info: %s",
                __LINE__, task->client_ip, caption,
                op->ctx->info.bs_key.block.oid,
                op->ctx->info.bs_key.block.offset,
                op->ctx->info.bs_key.slice.offset,
                op->ctx->info.bs_key.slice.length,
                op->ctx->result, STRERROR(op->ctx->result));
        TASK_ARG->context.log_level = LOG_NOTHING;
    } else {
        switch (op->operation) {
            case DATA_OPERATION_SLICE_WRITE:
                RESPONSE.header.cmd = FS_SERVICE_PROTO_SLICE_WRITE_RESP;
                break;
            case DATA_OPERATION_SLICE_ALLOCATE:
                RESPONSE.header.cmd = FS_SERVICE_PROTO_SLICE_ALLOCATE_RESP;
                break;
            case DATA_OPERATION_SLICE_DELETE:
                RESPONSE.header.cmd = FS_SERVICE_PROTO_SLICE_DELETE_RESP;
                break;
            case DATA_OPERATION_BLOCK_DELETE:
                RESPONSE.header.cmd = FS_SERVICE_PROTO_BLOCK_DELETE_RESP;
                break;
        }
        du_handler_fill_slice_update_response(task,
                SLICE_OP_CTX.update.space_changed);
        /*
           logInfo("file: "__FILE__", line: %d, "
           "which_side: %c, data_group_id: %d, "
           "op->ctx->info.data_version: %"PRId64", result: %d, "
           "done_bytes: %d, inc_alloc: %d", __LINE__,
           TASK_CTX.which_side, op->ctx->info.data_group_id,
           op->ctx->info.data_version, result, SLICE_OP_CTX.done_bytes,
           SLICE_OP_CTX.update.space_changed);
         */
    }

    du_handler_idempotency_request_finish(task, op->ctx->result);
    RESPONSE_STATUS = op->ctx->result;
    sf_nio_notify(task, SF_NIO_STAGE_CONTINUE);
}

static void slave_data_update_done_notify(FSDataOperation *op)
{
    FSSliceOpBufferContext *op_buffer_ctx;
    struct fast_task_info *task;

    task = (struct fast_task_info *)op->arg;
    if (op->ctx->result != 0) {
        logError("file: "__FILE__", line: %d, "
                "client ip: %s, write slice fail, "
                "oid: %"PRId64", block offset: %"PRId64", "
                "slice offset: %d, length: %d, "
                "errno: %d, error info: %s",
                __LINE__, task->client_ip,
                op->ctx->info.bs_key.block.oid, op->ctx->info.bs_key.block.offset,
                op->ctx->info.bs_key.slice.offset, op->ctx->info.bs_key.slice.length,
                op->ctx->result, STRERROR(op->ctx->result));
    } else {
        /*
           logInfo("file: "__FILE__", line: %d, "
           "which_side: %c, data_group_id: %d, "
           "op->ctx->info.data_version: %"PRId64", result: %d",
           __LINE__, TASK_CTX.which_side, op->ctx->info.data_group_id,
           op->ctx->info.data_version, op->ctx->result);
         */
    }

    if (SERVER_TASK_TYPE == FS_SERVER_TASK_TYPE_REPLICATION &&
            REPLICA_REPLICATION != NULL)
    {
        FSReplication *replication;
        replication = REPLICA_REPLICATION;
        if (replication != NULL && replication->task_version ==
                TASK_ARG->task_version)
        {
            replication_callee_push_to_rpc_result_queue(replication,
                    op->ctx->info.data_group_id, op->ctx->info.data_version,
                    op->ctx->result);
        }
    }

    if (op->operation == DATA_OPERATION_SLICE_WRITE) {
        op_buffer_ctx = fc_list_entry(op->ctx, FSSliceOpBufferContext, op_ctx);
        shared_buffer_release(op_buffer_ctx->buffer);
        replication_callee_free_op_buffer_ctx(SERVER_CTX, op_buffer_ctx);
    }
}

static inline void set_block_op_error_msg(struct fast_task_info *task,
        FSSliceOpContext *op_ctx, const char *caption, const int result)
{
    RESPONSE.error.length = sprintf(RESPONSE.error.message,
            "%s fail, result: %d, block {oid: %"PRId64", "
            "offset: %"PRId64"}", caption, result,
            op_ctx->info.bs_key.block.oid,
            op_ctx->info.bs_key.block.offset);
}

#define SLAVE_CHECK_DATA_VERSION(op_ctx) \
    do {  \
        if (TASK_CTX.which_side == FS_WHICH_SIDE_SLAVE) { \
            if (op_ctx->info.data_version < op_ctx->info. \
                    myself->replica.rpc_start_version)    \
            {  \
                logWarning("file: "__FILE__", line: %d, "  \
                        "data group id: %d, current data version: %"PRId64 \
                        " < rpc start version: %"PRId64", skip it!", \
                        __LINE__, op_ctx->info.data_group_id, \
                        op_ctx->info.data_version, op_ctx->info. \
                        myself->replica.rpc_start_version); \
                return 0;  \
            }  \
        }  \
    } while (0)


static inline int du_push_to_data_queue(struct fast_task_info *task,
        FSSliceOpContext *op_ctx, const int operation)
{
    int result;

    if (TASK_CTX.which_side == FS_WHICH_SIDE_MASTER) {
        op_ctx->notify_func = master_data_update_done_notify;
    } else {
        op_ctx->notify_func = slave_data_update_done_notify;
    }

    op_ctx->info.write_binlog.log_replica = true;
    if ((result=push_to_data_thread_queue(operation,
                   TASK_CTX.which_side == FS_WHICH_SIDE_MASTER ?
                   DATA_SOURCE_MASTER_SERVICE : DATA_SOURCE_SLAVE_REPLICA,
                   task, op_ctx)) != 0)
    {
        const char *caption;
        caption = fs_get_data_operation_caption(operation);
        if (operation == DATA_OPERATION_BLOCK_DELETE) {
            set_block_op_error_msg(task, op_ctx, caption, result);
        } else {
            du_handler_set_slice_op_error_msg(task, op_ctx, caption, result);
        }
        return result;
    }

    return TASK_STATUS_CONTINUE;
}

int du_handler_deal_slice_write(struct fast_task_info *task,
        FSSliceOpContext *op_ctx)
{
    FSProtoSliceWriteReqHeader *req_header;
    int result;

    if ((result=sf_server_check_min_body_length(&RESPONSE,
                    op_ctx->info.body_len,
                    sizeof(FSProtoSliceWriteReqHeader))) != 0)
    {
        return result;
    }

    req_header = (FSProtoSliceWriteReqHeader *)op_ctx->info.body;
    if ((result=du_handler_parse_check_block_slice(task, op_ctx,
                    &req_header->bs, TASK_CTX.which_side ==
                    FS_WHICH_SIDE_MASTER)) != 0)
    {
        return result;
    }
    SLAVE_CHECK_DATA_VERSION(op_ctx);

    /*
    logInfo("file: "__FILE__", line: %d, func: %s, "
            "data_group_id: %d", __LINE__, __FUNCTION__,
            op_ctx->info.data_group_id);
            */

    if (sizeof(FSProtoSliceWriteReqHeader) + op_ctx->info.bs_key.
            slice.length != op_ctx->info.body_len)
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "body header length: %d + slice length: %d"
                " != body length: %d", (int)sizeof
                (FSProtoSliceWriteReqHeader), op_ctx->info.bs_key.
                slice.length, op_ctx->info.body_len);
        return EINVAL;
    }

    op_ctx->info.buff = op_ctx->info.body + sizeof(FSProtoSliceWriteReqHeader);
    /*
    {
        int64_t offset = op_ctx->info.bs_key.block.offset + op_ctx->info.bs_key.slice.offset;
        int size = op_ctx->info.bs_key.slice.length;
        if (lseek(write_fd, 0, SEEK_CUR) != offset) {
            logError("file: "__FILE__", line: %d, func: %s, "
                    "lseek file offset: %"PRId64" != %"PRId64,
                    __LINE__, __FUNCTION__, (int64_t)lseek(write_fd, 0, SEEK_CUR),
                    (int64_t)offset);
            return EIO;
        }
        if (write(write_fd, buff, size) != size) {
            result = errno != 0 ? errno : EIO;
            logError("file: "__FILE__", line: %d, func: %s, "
                    "write to file fail, errno: %d, error info: %s",
                    __LINE__, __FUNCTION__, errno, strerror(errno));
            return result;
        }
    }
    */

    return du_push_to_data_queue(task, op_ctx, DATA_OPERATION_SLICE_WRITE);
}

int du_handler_deal_slice_allocate(struct fast_task_info *task,
        FSSliceOpContext *op_ctx)
{
    int result;
    FSProtoSliceAllocateReq *req;

    if ((result=sf_server_expect_body_length(&RESPONSE, op_ctx->info.body_len,
                    sizeof(FSProtoSliceAllocateReq))) != 0)
    {
        return result;
    }

    req = (FSProtoSliceAllocateReq *)op_ctx->info.body;
    if ((result=du_handler_parse_check_block_slice(task, op_ctx, &req->bs,
                    TASK_CTX.which_side == FS_WHICH_SIDE_MASTER)) != 0)
    {
        return result;
    }
    SLAVE_CHECK_DATA_VERSION(op_ctx);

    return du_push_to_data_queue(task, op_ctx, DATA_OPERATION_SLICE_ALLOCATE);
}

int du_handler_deal_slice_delete(struct fast_task_info *task,
        FSSliceOpContext *op_ctx)
{
    int result;
    FSProtoSliceDeleteReq *req;

    if ((result=sf_server_expect_body_length(&RESPONSE, op_ctx->info.body_len,
                    sizeof(FSProtoSliceDeleteReq))) != 0)
    {
        return result;
    }

    req = (FSProtoSliceDeleteReq *)op_ctx->info.body;
    if ((result=du_handler_parse_check_block_slice(task, op_ctx, &req->bs,
                    TASK_CTX.which_side == FS_WHICH_SIDE_MASTER)) != 0)
    {
        return result;
    }
    SLAVE_CHECK_DATA_VERSION(op_ctx);

    return du_push_to_data_queue(task, op_ctx, DATA_OPERATION_SLICE_DELETE);
}

int du_handler_deal_block_delete(struct fast_task_info *task,
        FSSliceOpContext *op_ctx)
{
    int result;
    FSProtoBlockDeleteReq *req;

    if ((result=sf_server_expect_body_length(&RESPONSE, op_ctx->info.body_len,
                    sizeof(FSProtoBlockDeleteReq))) != 0)
    {
        return result;
    }

    req = (FSProtoBlockDeleteReq *)op_ctx->info.body;
    if ((result=parse_check_block_key_ex(task, op_ctx, &req->bkey,
                    TASK_CTX.which_side == FS_WHICH_SIDE_MASTER)) != 0)
    {
        return result;
    }
    SLAVE_CHECK_DATA_VERSION(op_ctx);

    return du_push_to_data_queue(task, op_ctx, DATA_OPERATION_BLOCK_DELETE);
}

FSServerContext *du_handler_alloc_server_context()
{
    FSServerContext *server_context;

    server_context = (FSServerContext *)fc_malloc(sizeof(FSServerContext));
    if (server_context == NULL) {
        return NULL;
    }
    memset(server_context, 0, sizeof(FSServerContext));
    return server_context;
}
