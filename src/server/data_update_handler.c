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
#include "replication/replication_producer.h"
#include "server_global.h"
#include "server_func.h"
#include "server_group_info.h"
#include "server_storage.h"
#include "data_update_handler.h"

static int parse_check_block_key_ex(struct fast_task_info *task,
        const FSProtoBlockKey *bkey, const bool master_only)
{
    OP_CTX_INFO.bs_key.block.oid = buff2long(bkey->oid);
    OP_CTX_INFO.bs_key.block.offset = buff2long(bkey->offset);
    if (OP_CTX_INFO.bs_key.block.offset % FS_FILE_BLOCK_SIZE != 0) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message, "block offset: %"PRId64" "
                "NOT the multiple of the block size %d",
                OP_CTX_INFO.bs_key.block.offset, FS_FILE_BLOCK_SIZE);
        return EINVAL;
    }

    fs_calc_block_hashcode(&OP_CTX_INFO.bs_key.block);
    OP_CTX_INFO.data_group_id = FS_BLOCK_HASH_CODE(OP_CTX_INFO.bs_key.block) %
        FS_DATA_GROUP_COUNT(CLUSTER_CONFIG_CTX) + 1;

    OP_CTX_INFO.myself = fs_get_my_data_server(OP_CTX_INFO.data_group_id);
    if (OP_CTX_INFO.myself == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data group id: %d NOT belongs to me",
                OP_CTX_INFO.data_group_id);
        return ENOENT;
    }

    logInfo("data_group_id: %d, master_only: %d",
            OP_CTX_INFO.data_group_id, master_only);

    if (master_only) {
        if (!OP_CTX_INFO.myself->is_master) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "data group id: %d, i am NOT master",
                    OP_CTX_INFO.data_group_id);
            return EINVAL;
        }
    } else {
        if (OP_CTX_INFO.myself->status != FS_SERVER_STATUS_ACTIVE) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "data group id: %d, i am NOT active, my status: %d",
                    OP_CTX_INFO.data_group_id, OP_CTX_INFO.myself->status);
            return EINVAL;
        }
    }

    return 0;
}

int du_handler_parse_check_block_slice(struct fast_task_info *task,
        const FSProtoBlockSlice *bs, const bool master_only)
{
    int result;

    if ((result=parse_check_block_key_ex(task, &bs->bkey, master_only)) != 0) {
        return result;
    }

    OP_CTX_INFO.bs_key.slice.offset = buff2int(bs->slice_size.offset);
    OP_CTX_INFO.bs_key.slice.length = buff2int(bs->slice_size.length);
    if (OP_CTX_INFO.bs_key.slice.offset < 0 || OP_CTX_INFO.bs_key.slice.offset >=
            FS_FILE_BLOCK_SIZE)
    {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message, "slice offset: %d "
                "is invalid which < 0 or exceeds the block size %d",
                OP_CTX_INFO.bs_key.slice.offset, FS_FILE_BLOCK_SIZE);
        return EINVAL;
    }
    if (OP_CTX_INFO.bs_key.slice.length <= 0 || OP_CTX_INFO.bs_key.slice.offset +
            OP_CTX_INFO.bs_key.slice.length > FS_FILE_BLOCK_SIZE)
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "slice offset: %d, length: %d is invalid which <= 0, "
                "or offset + length exceeds the block size %d",
                OP_CTX_INFO.bs_key.slice.offset,
                OP_CTX_INFO.bs_key.slice.length, FS_FILE_BLOCK_SIZE);
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
                OP_CTX_INFO.bs_key.block.oid, OP_CTX_INFO.bs_key.block.offset,
                OP_CTX_INFO.bs_key.slice.offset, OP_CTX_INFO.bs_key.slice.length,
                op_ctx->result, STRERROR(op_ctx->result));
        TASK_ARG->context.log_error = false;
        result = op_ctx->result;
    } else {
        /*
        if ((result=replication_producer_push_to_slave_queues(task)) ==
                TASK_STATUS_CONTINUE)
        {
            TASK_ARG->context.deal_func = handle_slice_write_replica_done;
        } else {
        }
        */
        {
            RESPONSE.header.cmd = FS_SERVICE_PROTO_SLICE_WRITE_RESP;
            fill_slice_update_response(task, op_ctx->write.inc_alloc);
        }

        result = 0;
        logInfo("file: "__FILE__", line: %d, "
                "which_side: %c, data_group_id: %d, "
                "OP_CTX_INFO.data_version: %"PRId64", result: %d",
                __LINE__, TASK_CTX.which_side, OP_CTX_INFO.data_group_id,
                OP_CTX_INFO.data_version, result);
    }

    RESPONSE_STATUS = op_ctx->result;
    if (result != TASK_STATUS_CONTINUE) {
        sf_nio_notify(task, SF_NIO_STAGE_CONTINUE);
    }
}

static void slave_slice_write_done_notify(FSSliceOpContext *op_ctx)
{
    ReplicationRPCResult *r;
    struct fast_task_info *task;
    int result;

    r = (ReplicationRPCResult *)op_ctx->notify.args;
    task = r->replication->task;
    if (task == NULL) {
        return;
    }

    if (op_ctx->result != 0) {
        logError("file: "__FILE__", line: %d, "
                "client ip: %s, write slice fail, "
                "oid: %"PRId64", block offset: %"PRId64", "
                "slice offset: %d, length: %d, "
                "errno: %d, error info: %s",
                __LINE__, task->client_ip,
                OP_CTX_INFO.bs_key.block.oid, OP_CTX_INFO.bs_key.block.offset,
                OP_CTX_INFO.bs_key.slice.offset, OP_CTX_INFO.bs_key.slice.length,
                op_ctx->result, STRERROR(op_ctx->result));
        result = op_ctx->result;
    } else {
        result = 0;
        logInfo("file: "__FILE__", line: %d, "
                "which_side: %c, data_group_id: %d, "
                "OP_CTX_INFO.data_version: %"PRId64", result: %d",
                __LINE__, TASK_CTX.which_side, OP_CTX_INFO.data_group_id,
                OP_CTX_INFO.data_version, result);
    }

    /*
    int replication_producer_push_to_rpc_result_queue(FSReplication *replication,
        const uint64_t data_version, const int err_no);
        */
}

static inline void set_block_op_error_msg(struct fast_task_info *task,
        const char *caption, const int result)
{
    RESPONSE.error.length = sprintf(RESPONSE.error.message,
            "block %s fail, result: %d, block {oid: %"PRId64", "
            "offset: %"PRId64"}", caption, result,
            OP_CTX_INFO.bs_key.block.oid,
            OP_CTX_INFO.bs_key.block.offset);
}

int du_handler_deal_slice_write_ex(struct fast_task_info *task, char *body)
{
    FSProtoSliceWriteReqHeader *req_header;
    char *buff;
    int result;

    if ((result=server_check_min_body_length(task,
                    sizeof(FSProtoSliceWriteReqHeader))) != 0)
    {
        return result;
    }

    req_header = (FSProtoSliceWriteReqHeader *)body;
    if ((result=du_handler_parse_check_block_slice(task, &req_header->bs,
                    TASK_CTX.which_side == FS_WHICH_SIDE_MASTER)) != 0)
    {
        return result;
    }

    if (sizeof(FSProtoSliceWriteReqHeader) + OP_CTX_INFO.bs_key.slice.length
        != REQUEST.header.body_len)
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "body header length: %d + slice length: %d"
                " != body length: %d", (int)sizeof(FSProtoSliceWriteReqHeader),
                OP_CTX_INFO.bs_key.slice.length, REQUEST.header.body_len);
        return EINVAL;
    }

    logInfo("file: "__FILE__", line: %d, func: %s, "
            "data_group_id: %d", __LINE__, __FUNCTION__,
            OP_CTX_INFO.data_group_id);

    buff = body + sizeof(FSProtoSliceWriteReqHeader);
    if (TASK_CTX.which_side == FS_WHICH_SIDE_MASTER) {
        OP_CTX_NOTIFY.func = master_slice_write_done_notify;
        OP_CTX_NOTIFY.args = task;
    } else {
        OP_CTX_NOTIFY.func = slave_slice_write_done_notify;
        //OP_CTX_NOTIFY.args = task;
        //TODO
    }

    //TODO
    /*
    {
        int64_t offset = OP_CTX_INFO.bs_key.block.offset + OP_CTX_INFO.bs_key.slice.offset;
        int size = OP_CTX_INFO.bs_key.slice.length;
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

    OP_CTX_INFO.write_data_binlog = true;
    if ((result=fs_slice_write(&SLICE_OP_CTX, buff)) != 0) {
        du_handler_set_slice_op_error_msg(task, "write", result);
        return result;
    }

    return TASK_STATUS_CONTINUE;
}

int du_handler_deal_slice_allocate_ex(struct fast_task_info *task, char *body)
{
    int result;
    int inc_alloc;
    FSProtoSliceAllocateReq *req;

    if ((result=server_expect_body_length(task,
                    sizeof(FSProtoSliceAllocateReq))) != 0)
    {
        return result;
    }

    req = (FSProtoSliceAllocateReq *)body;
    if ((result=du_handler_parse_check_block_slice(task, &req->bs,
                    TASK_CTX.which_side == FS_WHICH_SIDE_MASTER)) != 0)
    {
        return result;
    }

    OP_CTX_INFO.write_data_binlog = true;
    if ((result=fs_slice_allocate_ex(&SLICE_OP_CTX, ((FSServerContext *)
                        task->thread_data->arg)->service.slice_ptr_array,
                    &inc_alloc)) != 0)
    {
        du_handler_set_slice_op_error_msg(task, "allocate", result);
        return result;
    }

    RESPONSE.header.cmd = FS_SERVICE_PROTO_SLICE_ALLOCATE_RESP;
    fill_slice_update_response(task, inc_alloc);
    return 0;
}

int du_handler_deal_slice_delete_ex(struct fast_task_info *task, char *body)
{
    int result;
    int dec_alloc;
    FSProtoSliceDeleteReq *req;

    if ((result=server_expect_body_length(task,
                    sizeof(FSProtoSliceDeleteReq))) != 0)
    {
        return result;
    }

    req = (FSProtoSliceDeleteReq *)body;
    if ((result=du_handler_parse_check_block_slice(task, &req->bs,
                    TASK_CTX.which_side == FS_WHICH_SIDE_MASTER)) != 0)
    {
        return result;
    }

    OP_CTX_INFO.write_data_binlog = true;
    if ((result=fs_delete_slices(&SLICE_OP_CTX, &dec_alloc)) != 0) {
        du_handler_set_slice_op_error_msg(task, "delete", result);
        return result;
    }

    RESPONSE.header.cmd = FS_SERVICE_PROTO_SLICE_DELETE_RESP;
    fill_slice_update_response(task, dec_alloc);
    return 0;
}

int du_handler_deal_block_delete_ex(struct fast_task_info *task, char *body)
{
    int result;
    int dec_alloc;
    FSProtoBlockDeleteReq *req;

    if ((result=server_expect_body_length(task,
                    sizeof(FSProtoBlockDeleteReq))) != 0)
    {
        return result;
    }

    req = (FSProtoBlockDeleteReq *)body;
    if ((result=parse_check_block_key_ex(task, &req->bkey,
                    TASK_CTX.which_side == FS_WHICH_SIDE_MASTER)) != 0)
    {
        return result;
    }

    OP_CTX_INFO.write_data_binlog = true;
    if ((result=fs_delete_block(&SLICE_OP_CTX, &dec_alloc)) != 0) {
        set_block_op_error_msg(task, "delete", result);
        return result;
    }

    RESPONSE.header.cmd = FS_SERVICE_PROTO_BLOCK_DELETE_RESP;
    fill_slice_update_response(task, dec_alloc);
    return 0;
}
