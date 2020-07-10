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
#include "common/fs_proto.h"
#include "common/fs_func.h"
#include "binlog/replica_binlog.h"
#include "server_replication.h"
#include "server_global.h"
#include "server_func.h"
#include "server_group_info.h"
#include "server_storage.h"
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
    op_ctx->info.data_group_id = FS_BLOCK_HASH_CODE(op_ctx->info.bs_key.block) %
        FS_DATA_GROUP_COUNT(CLUSTER_CONFIG_CTX) + 1;

    op_ctx->info.myself = fs_get_my_data_server(op_ctx->info.data_group_id);
    if (op_ctx->info.myself == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data group id: %d NOT belongs to me",
                op_ctx->info.data_group_id);
        return ENOENT;
    }

    logInfo("data_group_id: %d, master_only: %d",
            op_ctx->info.data_group_id, master_only);

    if (master_only) {
        if (!op_ctx->info.myself->is_master) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "data group id: %d, i am NOT master",
                    op_ctx->info.data_group_id);
            return EINVAL;
        }
    } else {
        if (op_ctx->info.myself->status != FS_SERVER_STATUS_ACTIVE) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "data group id: %d, i am NOT active, my status: %d",
                    op_ctx->info.data_group_id, op_ctx->info.myself->status);
            return EINVAL;
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

static inline void fill_slice_update_response(struct fast_task_info *task,
        const int inc_alloc)
{
    FSProtoSliceUpdateResp *resp;
    resp = (FSProtoSliceUpdateResp *)REQUEST.body;
    int2buff(inc_alloc, resp->inc_alloc);

    RESPONSE.header.body_len = sizeof(FSProtoSliceUpdateResp);
    TASK_ARG->context.response_done = true;
}

static int handle_slice_write_replica_done(struct fast_task_info *task)
{
    TASK_ARG->context.deal_func = NULL;
    RESPONSE.header.cmd = FS_SERVICE_PROTO_SLICE_WRITE_RESP;
    fill_slice_update_response(task, SLICE_OP_CTX.write.inc_alloc);
    return RESPONSE_STATUS;
}

static void master_slice_write_done_notify(FSSliceOpContext *op_ctx)
{
    struct fast_task_info *task;
    int result;

    task = (struct fast_task_info *)op_ctx->notify.args;
    if (op_ctx->result != 0) {
        RESPONSE.error.length = snprintf(RESPONSE.error.message,
                sizeof(RESPONSE.error.message),
                "%s", STRERROR(op_ctx->result));

        logError("file: "__FILE__", line: %d, "
                "client ip: %s, write slice fail, "
                "oid: %"PRId64", block offset: %"PRId64", "
                "slice offset: %d, length: %d, "
                "errno: %d, error info: %s",
                __LINE__, task->client_ip,
                op_ctx->info.bs_key.block.oid, op_ctx->info.bs_key.block.offset,
                op_ctx->info.bs_key.slice.offset, op_ctx->info.bs_key.slice.length,
                op_ctx->result, STRERROR(op_ctx->result));
        TASK_ARG->context.log_error = false;
        result = op_ctx->result;
    } else {
        if ((result=replication_caller_push_to_slave_queues(task)) ==
                TASK_STATUS_CONTINUE)
        {
            TASK_ARG->context.deal_func = handle_slice_write_replica_done;
        } else {
            RESPONSE.header.cmd = FS_SERVICE_PROTO_SLICE_WRITE_RESP;
            fill_slice_update_response(task, op_ctx->write.inc_alloc);
        }

        result = 0;
        logInfo("file: "__FILE__", line: %d, "
                "which_side: %c, data_group_id: %d, "
                "op_ctx->info.data_version: %"PRId64", result: %d",
                __LINE__, TASK_CTX.which_side, op_ctx->info.data_group_id,
                op_ctx->info.data_version, result);
    }

    RESPONSE_STATUS = op_ctx->result;
    if (result != TASK_STATUS_CONTINUE) {
        sf_nio_notify(task, SF_NIO_STAGE_CONTINUE);
    }
}

static void slave_slice_write_done_notify(FSSliceOpContext *op_ctx)
{
    struct fast_task_info *task;
    FSSliceOpBufferContext *op_buffer_ctx;

    task = (struct fast_task_info *)op_ctx->notify.args;
    if (op_ctx->result != 0) {
        logError("file: "__FILE__", line: %d, "
                "client ip: %s, write slice fail, "
                "oid: %"PRId64", block offset: %"PRId64", "
                "slice offset: %d, length: %d, "
                "errno: %d, error info: %s",
                __LINE__, task->client_ip,
                op_ctx->info.bs_key.block.oid, op_ctx->info.bs_key.block.offset,
                op_ctx->info.bs_key.slice.offset, op_ctx->info.bs_key.slice.length,
                op_ctx->result, STRERROR(op_ctx->result));
    } else {
        logInfo("file: "__FILE__", line: %d, "
                "which_side: %c, data_group_id: %d, "
                "op_ctx->info.data_version: %"PRId64", result: %d",
                __LINE__, TASK_CTX.which_side, op_ctx->info.data_group_id,
                op_ctx->info.data_version, op_ctx->result);
    }

    if (CLUSTER_TASK_TYPE == FS_CLUSTER_TASK_TYPE_REPLICATION &&
            CLUSTER_REPLICA != NULL)
    {
        replication_callee_push_to_rpc_result_queue(CLUSTER_REPLICA,
                op_ctx->info.data_version, op_ctx->result);
    }

    op_buffer_ctx = fc_list_entry(op_ctx, FSSliceOpBufferContext, op_ctx);
    shared_buffer_release(op_buffer_ctx->buffer);
}

static inline void set_block_op_error_msg(struct fast_task_info *task,
        FSSliceOpContext *op_ctx, const char *caption, const int result)
{
    RESPONSE.error.length = sprintf(RESPONSE.error.message,
            "block %s fail, result: %d, block {oid: %"PRId64", "
            "offset: %"PRId64"}", caption, result,
            op_ctx->info.bs_key.block.oid,
            op_ctx->info.bs_key.block.offset);
}

int du_handler_deal_slice_write(struct fast_task_info *task,
        FSSliceOpContext *op_ctx)
{
    FSProtoSliceWriteReqHeader *req_header;
    char *buff;
    int result;

    if ((result=server_check_min_body_length(task,
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

    if (sizeof(FSProtoSliceWriteReqHeader) + op_ctx->info.bs_key.slice.length
            != REQUEST.header.body_len)
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "body header length: %d + slice length: %d"
                " != body length: %d", (int)sizeof(FSProtoSliceWriteReqHeader),
                op_ctx->info.bs_key.slice.length, REQUEST.header.body_len);
        return EINVAL;
    }

    logInfo("file: "__FILE__", line: %d, func: %s, "
            "data_group_id: %d", __LINE__, __FUNCTION__,
            op_ctx->info.data_group_id);

    buff = op_ctx->info.body + sizeof(FSProtoSliceWriteReqHeader);
    if (TASK_CTX.which_side == FS_WHICH_SIDE_MASTER) {
        op_ctx->notify.func = master_slice_write_done_notify;
    } else {
        op_ctx->notify.func = slave_slice_write_done_notify;
    }
    op_ctx->notify.args = task;

    //TODO
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

    op_ctx->info.write_data_binlog = true;
    if ((result=fs_slice_write(op_ctx, buff)) != 0) {
        du_handler_set_slice_op_error_msg(task, op_ctx, "write", result);
        return result;
    }

    return TASK_STATUS_CONTINUE;
}

int du_handler_deal_slice_allocate(struct fast_task_info *task,
        FSSliceOpContext *op_ctx)
{
    int result;
    int inc_alloc;
    FSProtoSliceAllocateReq *req;

    if ((result=server_expect_body_length(task,
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

    op_ctx->info.write_data_binlog = true;
    if ((result=fs_slice_allocate_ex(op_ctx, ((FSServerContext *)
                        task->thread_data->arg)->service.slice_ptr_array,
                    &inc_alloc)) != 0)
    {
        du_handler_set_slice_op_error_msg(task, op_ctx, "allocate", result);
        return result;
    }

    RESPONSE.header.cmd = FS_SERVICE_PROTO_SLICE_ALLOCATE_RESP;
    fill_slice_update_response(task, inc_alloc);
    return 0;
}

int du_handler_deal_slice_delete(struct fast_task_info *task,
        FSSliceOpContext *op_ctx)
{
    int result;
    int dec_alloc;
    FSProtoSliceDeleteReq *req;

    if ((result=server_expect_body_length(task,
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

    op_ctx->info.write_data_binlog = true;
    if ((result=fs_delete_slices(op_ctx, &dec_alloc)) != 0) {
        du_handler_set_slice_op_error_msg(task, op_ctx, "delete", result);
        return result;
    }

    RESPONSE.header.cmd = FS_SERVICE_PROTO_SLICE_DELETE_RESP;
    fill_slice_update_response(task, dec_alloc);
    return 0;
}

int du_handler_deal_block_delete(struct fast_task_info *task,
        FSSliceOpContext *op_ctx)
{
    int result;
    int dec_alloc;
    FSProtoBlockDeleteReq *req;

    if ((result=server_expect_body_length(task,
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

    op_ctx->info.write_data_binlog = true;
    if ((result=fs_delete_block(op_ctx, &dec_alloc)) != 0) {
        set_block_op_error_msg(task, op_ctx, "delete", result);
        return result;
    }

    RESPONSE.header.cmd = FS_SERVICE_PROTO_BLOCK_DELETE_RESP;
    fill_slice_update_response(task, dec_alloc);
    return 0;
}
