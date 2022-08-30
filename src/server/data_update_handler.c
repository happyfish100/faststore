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
#include "sf/sf_service.h"
#include "sf/sf_global.h"
#include "sf/sf_configs.h"
#include "sf/idempotency/server/server_channel.h"
#include "common/fs_proto.h"
#include "common/fs_func.h"
#include "binlog/replica_binlog.h"
#include "server_replication.h"
#include "server_global.h"
#include "server_func.h"
#include "server_group_info.h"
#include "server_storage.h"
#include "data_update_handler.h"

void du_handler_fill_slice_update_response(struct fast_task_info *task,
        const int inc_alloc)
{
    FSProtoSliceUpdateResp *resp;
    resp = (FSProtoSliceUpdateResp *)SF_PROTO_RESP_BODY(task);
    int2buff(inc_alloc, resp->inc_alloc);
    RESPONSE.header.body_len = sizeof(FSProtoSliceUpdateResp);
    TASK_CTX.common.response_done = true;
}

static void slice_update_response(struct fast_task_info *task,
        const int inc_alloc)
{
    switch (REQUEST.header.cmd) {
        case FS_SERVICE_PROTO_SLICE_WRITE_REQ:
            RESPONSE.header.cmd = FS_SERVICE_PROTO_SLICE_WRITE_RESP;
            break;
        case FS_SERVICE_PROTO_SLICE_ALLOCATE_REQ:
            RESPONSE.header.cmd = FS_SERVICE_PROTO_SLICE_ALLOCATE_RESP;
            break;
        case FS_SERVICE_PROTO_SLICE_DELETE_REQ:
            RESPONSE.header.cmd = FS_SERVICE_PROTO_SLICE_DELETE_RESP;
            break;
        case FS_SERVICE_PROTO_BLOCK_DELETE_REQ:
            RESPONSE.header.cmd = FS_SERVICE_PROTO_BLOCK_DELETE_RESP;
            break;
        default:
            return;
    }
    du_handler_fill_slice_update_response(task, inc_alloc);
}

void du_handler_idempotency_request_finish(struct fast_task_info *task,
        const int result)
{
    if (IDEMPOTENCY_REQUEST != NULL) {
        if (SF_IS_SERVER_RETRIABLE_ERROR(result)) {
            if (IDEMPOTENCY_CHANNEL != NULL) {
                idempotency_channel_remove_request(IDEMPOTENCY_CHANNEL,
                        IDEMPOTENCY_REQUEST->req_id);
            }
        } else {
            IDEMPOTENCY_REQUEST->finished = true;
            IDEMPOTENCY_REQUEST->output.result = result;
            if (result == 0) {
                ((FSUpdateOutput *)IDEMPOTENCY_REQUEST->output.response)->
                    inc_alloc = SLICE_OP_CTX.update.space_changed;
            }
        }
        idempotency_request_release(IDEMPOTENCY_REQUEST);

        /* server task type for channel ONLY, do NOT set task type to NONE!!! */
        IDEMPOTENCY_REQUEST = NULL;
    }
}

static inline int wait_recovery_done(FSClusterDataServerInfo *ds,
        FSSliceOpContext *op_ctx)
{
    int i;
    bool checked;
    int64_t until_version;

    for (i=0; i<3; i++) {
        until_version = FC_ATOMIC_GET(ds->recovery.until_version);
        checked = (until_version != 0);
        if (checked && (op_ctx->info.data_version <= until_version)) {
            logDebug("file: "__FILE__", line: %d, "
                    "rpc data group id: %d, data version: %"PRId64" <= "
                    "until_version: %"PRId64", skipped", __LINE__,
                    op_ctx->info.data_group_id, op_ctx->info.data_version,
                    until_version);
            op_ctx->info.deal_done = true;
            return 0;
        }

        while (FC_ATOMIC_GET(ds->status) == FS_DS_STATUS_ONLINE
                && SF_G_CONTINUE_FLAG)
        {
            PTHREAD_MUTEX_LOCK(&ds->replica.notify.lock);
            if (FC_ATOMIC_GET(ds->status) == FS_DS_STATUS_ONLINE) {
                pthread_cond_wait(&ds->replica.notify.cond,
                        &ds->replica.notify.lock);
            }
            PTHREAD_MUTEX_UNLOCK(&ds->replica.notify.lock);
        }

        if (checked) {
            break;
        }
    }

    return EAGAIN;
}

static int parse_check_block_key(struct fast_task_info *task,
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
        SFProtoIdempotencyAdditionalHeader *adheader;
        int64_t req_id;
        int64_t data_version;
        int active_count;

        if (!FC_ATOMIC_GET(op_ctx->info.myself->is_master)) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "[service] data group id: %d, i am NOT master",
                    op_ctx->info.data_group_id);
            return SF_RETRIABLE_ERROR_NOT_MASTER;
        }

        if (!(op_ctx->info.source == BINLOG_SOURCE_RPC_MASTER &&
                    op_ctx->info.is_update))
        {
            return 0;
        }

        if (SERVER_TASK_TYPE == SF_SERVER_TASK_TYPE_CHANNEL_USER &&
                IDEMPOTENCY_CHANNEL != NULL)
        {
            adheader = (SFProtoIdempotencyAdditionalHeader *)(
                    REQUEST.body - sizeof(*adheader));
            req_id = buff2long(adheader->req_id);
            if (SF_IDEMPOTENCY_EXTRACT_SERVER_ID(req_id) !=
                    CLUSTER_MY_SERVER_ID)
            {
                /*
                logInfo("req_id: %"PRId64", server_id: %d", req_id,
                        SF_IDEMPOTENCY_EXTRACT_SERVER_ID(req_id));
                        */
                if (idempotency_request_metadata_get(&op_ctx->info.
                            myself->dg->req_meta_ctx, req_id,
                            &data_version, &SLICE_OP_CTX.
                            update.space_changed) == 0)
                {
                    op_ctx->info.deal_done = true;

                    /* clear idempotency request */
                    du_handler_idempotency_request_finish(task, EAGAIN);

                    /*
                    logInfo("req_id: %"PRId64", data_version: %"PRId64", "
                            "my current dv: %"PRId64", inc alloc: %d",
                            req_id, data_version, FC_ATOMIC_GET(op_ctx->info.
                                myself->data.confirmed_version),
                            SLICE_OP_CTX.update.space_changed);
                            */

                    if (data_version <= FC_ATOMIC_GET(op_ctx->info.
                                myself->data.confirmed_version))
                    {
                        slice_update_response(task, SLICE_OP_CTX.
                                update.space_changed);
                        return 0;
                    } else {
                        return EAGAIN;
                    }
                }
            }
        }

        if (FC_ATOMIC_GET(op_ctx->info.myself->dg->replica_quorum.need_majority)) {
            active_count = FC_ATOMIC_GET(op_ctx->
                    info.myself->dg->active_count);
            if (!SF_REPLICATION_QUORUM_MAJORITY(op_ctx->info.myself->
                        dg->ds_ptr_array.count, active_count))
            {
                RESPONSE.error.length = sprintf(RESPONSE.error.message,
                        "active server count: %d < half of servers: %d, "
                        "should try again later", active_count, op_ctx->
                        info.myself->dg->ds_ptr_array.count / 2 + 1);
                TASK_CTX.common.log_level = LOG_NOTHING;
                du_handler_idempotency_request_finish(task, EAGAIN);
                return EAGAIN;
            }
        }
    } else {
        int status;
        status = FC_ATOMIC_GET(op_ctx->info.myself->status);
        if (op_ctx->info.is_update) {
            while (status == FS_DS_STATUS_ONLINE && SF_G_CONTINUE_FLAG) {
                if (!FC_ATOMIC_GET(op_ctx->info.myself->
                            recovery.in_progress))
                {
                    status = FC_ATOMIC_GET(op_ctx->info.myself->status);
                    logInfo("file: "__FILE__", line: %d, "
                            "rpc data group id: %d, recovery in "
                            "progress: 0, data version: %"PRId64", "
                            "until_version: %"PRId64", status: %d",
                            __LINE__, op_ctx->info.data_group_id,
                            op_ctx->info.data_version,
                            FC_ATOMIC_GET(op_ctx->info.myself->
                                recovery.until_version), status);
                    break;
                }

                if (wait_recovery_done(op_ctx->info.myself, op_ctx) == 0) {
                    break;
                }
                status = FC_ATOMIC_GET(op_ctx->info.myself->status);
            }
        }

        if (!(status == FS_DS_STATUS_ACTIVE || op_ctx->info.deal_done)) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "data group id: %d, i am NOT active or online, "
                    "my status: %d (%s)", op_ctx->info.data_group_id,
                    status, fs_get_server_status_caption(status));
            return SF_RETRIABLE_ERROR_NOT_ACTIVE;
        }
    }

    return 0;
}

int du_handler_parse_check_block_slice(struct fast_task_info *task,
        FSSliceOpContext *op_ctx, const FSProtoBlockSlice *bs,
        const bool master_only)
{
    int result;

    if ((result=parse_check_block_key(task, op_ctx,
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

#ifdef OS_LINUX
static int buffer_to_iovec_array(struct fast_task_info *task)
{
    AlignedReadBuffer **aligned_buffer;
    AlignedReadBuffer **end;
    struct iovec *iov;
    //int total;
    int result;

    if ((result=fc_check_realloc_iovec_array(&SLICE_OP_CTX.iovec_array,
                    SLICE_OP_CTX.aio_buffer_parray.count + 1)) != 0)
    {
        return result;
    }

    iov = SLICE_OP_CTX.iovec_array.iovs;
    FC_SET_IOVEC(*iov, task->data, sizeof(FSProtoHeader));
    iov++;

    //total = 0;
    end = SLICE_OP_CTX.aio_buffer_parray.buffers +
        SLICE_OP_CTX.aio_buffer_parray.count;
    for (aligned_buffer=SLICE_OP_CTX.aio_buffer_parray.buffers;
            aligned_buffer<end; aligned_buffer++, iov++)
    {
        FC_SET_IOVEC(*iov, (*aligned_buffer)->buff +
                (*aligned_buffer)->offset,
                (*aligned_buffer)->length);

        //total += (*aligned_buffer)->length;
    }

    task->iovec_array.iovs = SLICE_OP_CTX.iovec_array.iovs;
    task->iovec_array.count = iov - SLICE_OP_CTX.iovec_array.iovs;

    /*
    logInfo("buffer {count: %d, bytes: %d}, done_bytes: %d",
            SLICE_OP_CTX.aio_buffer_parray.count, total, SLICE_OP_CTX.done_bytes);
            */
    return 0;
}
#endif

void du_handler_slice_read_done_callback(FSSliceOpContext *op_ctx,
        struct fast_task_info *task)
{
    int log_level;

    if (op_ctx->result != 0) {
        RESPONSE.error.length = snprintf(RESPONSE.error.message,
                sizeof(RESPONSE.error.message),
                "%s", STRERROR(op_ctx->result));

        log_level = (op_ctx->result == ENOENT) ? LOG_DEBUG : LOG_ERR;
        log_it_ex(&g_log_context, log_level,
                "file: "__FILE__", line: %d, "
                "client ip: %s, read slice fail, "
                "oid: %"PRId64", block offset: %"PRId64", "
                "slice offset: %d, length: %d, "
                "errno: %d, error info: %s",
                __LINE__, task->client_ip,
                OP_CTX_INFO.bs_key.block.oid,
                OP_CTX_INFO.bs_key.block.offset,
                OP_CTX_INFO.bs_key.slice.offset,
                OP_CTX_INFO.bs_key.slice.length,
                op_ctx->result, STRERROR(op_ctx->result));
        TASK_CTX.common.log_level = LOG_NOTHING;
    } else {

#ifdef OS_LINUX
        if (op_ctx->info.buffer_type == fs_buffer_type_direct || (op_ctx->
                    result=buffer_to_iovec_array(task)) == 0)
        {
#endif

            RESPONSE.header.body_len = FC_ATOMIC_GET(op_ctx->done_bytes);
            TASK_CTX.common.response_done = true;

#ifdef OS_LINUX
        }
#endif

    }

    RESPONSE_STATUS = op_ctx->result;
    sf_nio_notify(task, SF_NIO_STAGE_CONTINUE);
    sf_release_task(task);
}

void du_handler_slice_read_done_notify(FSDataOperation *op)
{
    du_handler_slice_read_done_callback(op->ctx, op->arg);
}

static void log_data_operation_error(struct fast_task_info *task,
        FSDataOperation *op)
{
    const char *caption;
    char buff[512];
    int len;

    caption = fs_get_data_operation_caption(op->operation);
    len = sprintf(buff, "file: "__FILE__", line: %d, "
            "client ip: %s, %s %s fail, oid: %"PRId64", "
            "block offset: %"PRId64, __LINE__, task->client_ip,
            (TASK_CTX.which_side == FS_WHICH_SIDE_MASTER ?
             "master" : "slave"), caption,
            op->ctx->info.bs_key.block.oid,
            op->ctx->info.bs_key.block.offset
            );

    if (op->operation != DATA_OPERATION_BLOCK_DELETE) {
        len += sprintf(buff + len, ", slice offset: %d, length: %d",
                op->ctx->info.bs_key.slice.offset,
                op->ctx->info.bs_key.slice.length);
    }

    len += snprintf(buff + len, sizeof(buff) - len,
            ", errno: %d, error info: %s",
            op->ctx->result, sf_strerror(op->ctx->result));
    log_it1(LOG_ERR, buff, len);
}

static void master_data_update_done_notify(FSDataOperation *op)
{
    struct fast_task_info *task;

    task = (struct fast_task_info *)op->arg;

    FC_ATOMIC_DEC(op->ctx->info.myself->master_dealing_count);
    if (op->ctx->result != 0) {
        RESPONSE.error.length = snprintf(RESPONSE.error.message,
                sizeof(RESPONSE.error.message),
                "%s", sf_strerror(op->ctx->result));

        if (!((op->operation == DATA_OPERATION_BLOCK_DELETE ||
                        op->operation == DATA_OPERATION_SLICE_DELETE) &&
                    op->ctx->result == ENOENT)) //ignore error on delete file
        {
            log_data_operation_error(task, op);
        }
        TASK_CTX.common.log_level = LOG_NOTHING;
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
    sf_release_task(task);
}

static void slave_data_update_done_notify(FSDataOperation *op)
{
    FSSliceOpBufferContext *op_buffer_ctx;
    struct fast_task_info *task;

    task = (struct fast_task_info *)op->arg;
    if (op->ctx->result != 0) {
        log_data_operation_error(task, op);
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
        if (replication != NULL) {
            replication_callee_push_to_rpc_result_queue(replication,
                    op->ctx->info.data_group_id, op->ctx->info.data_version,
                    op->ctx->result);
        }
    }

    op_buffer_ctx = fc_list_entry(op->ctx, FSSliceOpBufferContext, op_ctx);
    if (op->operation == DATA_OPERATION_SLICE_WRITE) {
        sf_shared_mbuffer_release(op_buffer_ctx->op_ctx.mbuffer);
    }
    replication_callee_free_op_buffer_ctx(SERVER_CTX, op_buffer_ctx);
    sf_release_task(task);
}

static inline void set_block_op_error_msg(struct fast_task_info *task,
        FSSliceOpContext *op_ctx, const char *caption, const int result)
{
    RESPONSE.error.length = sprintf(RESPONSE.error.message,
            "%s fail, result: %d, data_group_id: %d, block {oid: %"PRId64", "
            "offset: %"PRId64"}", caption, result, op_ctx->info.data_group_id,
            op_ctx->info.bs_key.block.oid, op_ctx->info.bs_key.block.offset);
}

static inline int du_slave_check_data_version(struct fast_task_info *task,
        FSSliceOpContext *op_ctx, bool *skipped)
{
    uint64_t rpc_last_version;

    *skipped = false;
    rpc_last_version = __sync_fetch_and_add(&op_ctx->info.
            myself->replica.rpc_last_version, 0);
    if (op_ctx->info.data_version != rpc_last_version + 1) {
        if (op_ctx->info.data_version <= rpc_last_version) {
            logWarning("file: "__FILE__", line: %d, "
                    "data group id: %d, rpc data version: %"PRId64
                    " <= current data version: %"PRId64", skip it!",
                    __LINE__, op_ctx->info.data_group_id,
                    op_ctx->info.data_version, rpc_last_version);
            *skipped = true;
            return 0;
        }  else {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "rpc data version: %"PRId64" is too large, expect: %"
                    PRId64, op_ctx->info.data_version, rpc_last_version + 1);
            return EINVAL;
        }
    }

    __sync_fetch_and_add(&op_ctx->info.myself->replica.rpc_last_version, 1);
    return 0;
}

static inline int du_push_to_data_queue(struct fast_task_info *task,
        FSSliceOpContext *op_ctx, const int operation)
{
    int result;
    bool skipped;

    if (op_ctx->info.deal_done) {
        return 0;
    }

    if (TASK_CTX.which_side == FS_WHICH_SIDE_MASTER) {
        if (!FC_ATOMIC_GET(op_ctx->info.myself->is_master)) { //check again
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "[service] data group id: %d, i am NOT master",
                    op_ctx->info.data_group_id);
            TASK_CTX.common.log_level = LOG_NOTHING;
            return SF_RETRIABLE_ERROR_NOT_MASTER;
        }

        if (FC_ATOMIC_GET(op_ctx->info.myself->dg->master_swapping)) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "[service] data group id: %d, master swapping "
                    "in progress", op_ctx->info.data_group_id);
            TASK_CTX.common.log_level = LOG_NOTHING;
            return SF_RETRIABLE_ERROR_NOT_ACTIVE;
        }

        FC_ATOMIC_INC(op_ctx->info.myself->master_dealing_count);
        op_ctx->notify_func = master_data_update_done_notify;
    } else {
        result = du_slave_check_data_version(task, op_ctx, &skipped);
        if (result != 0 || skipped) {
            return result;
        }
        op_ctx->notify_func = slave_data_update_done_notify;
    }

    sf_hold_task(task);
    op_ctx->info.write_binlog.log_replica = true;
    if ((result=push_to_data_thread_queue(operation,
                   TASK_CTX.which_side == FS_WHICH_SIDE_MASTER ?
                   DATA_SOURCE_MASTER_SERVICE : DATA_SOURCE_SLAVE_REPLICA,
                   task, op_ctx)) != 0)
    {
        const char *caption;

        if (TASK_CTX.which_side == FS_WHICH_SIDE_MASTER) {
            FC_ATOMIC_DEC(op_ctx->info.myself->master_dealing_count);
        }
        caption = fs_get_data_operation_caption(operation);
        if (operation == DATA_OPERATION_BLOCK_DELETE) {
            set_block_op_error_msg(task, op_ctx, caption, result);
        } else {
            du_handler_set_slice_op_error_msg(task, op_ctx, caption, result);
        }
        sf_release_task(task);
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

    op_ctx->mbuffer = fc_list_entry(task->recv_body, SFSharedMBuffer, buff);
    op_ctx->info.buff = op_ctx->info.body + sizeof(FSProtoSliceWriteReqHeader);
    result = du_push_to_data_queue(task, op_ctx, DATA_OPERATION_SLICE_WRITE);
    if (result == TASK_STATUS_CONTINUE) {
        task->recv_body = NULL;
    }
    return result;
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
    if ((result=parse_check_block_key(task, op_ctx, &req->bkey,
                    TASK_CTX.which_side == FS_WHICH_SIDE_MASTER)) != 0)
    {
        return result;
    }

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

int du_handler_deal_client_join(struct fast_task_info *task)
{
    int result;
    int data_group_count;
    int file_block_size;
    int flags;
    int my_auth_enabled;
    int req_auth_enabled;
    FSProtoClientJoinReq *req;
    FSProtoClientJoinResp *join_resp;

    if ((result=server_expect_body_length(sizeof(
                        FSProtoClientJoinReq))) != 0)
    {
        return result;
    }

    req = (FSProtoClientJoinReq *)REQUEST.body;
    data_group_count = buff2int(req->data_group_count);
    file_block_size = buff2int(req->file_block_size);
    flags = buff2int(req->flags);
    if (data_group_count != FS_DATA_GROUP_COUNT(CLUSTER_CONFIG_CTX)) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "client data group count: %d != mine: %d",
                data_group_count, FS_DATA_GROUP_COUNT(CLUSTER_CONFIG_CTX));
        return EINVAL;
    }
    if (file_block_size != FS_FILE_BLOCK_SIZE) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "client file block size: %d != mine: %d",
                file_block_size, FS_FILE_BLOCK_SIZE);
        return EINVAL;
    }

    my_auth_enabled = (AUTH_ENABLED ? 1 : 0);
    req_auth_enabled = (req->auth_enabled ? 1 : 0);
    if (req_auth_enabled != my_auth_enabled) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "client auth enabled: %d != mine: %d",
                req_auth_enabled, my_auth_enabled);
        return EINVAL;
    }

    if (memcmp(req->cluster_cfg_signs.servers, SERVERS_CONFIG_SIGN_BUF,
                SF_CLUSTER_CONFIG_SIGN_LEN) != 0)
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "client's servers part of cluster.conf "
                "is not consistent with mine");
        return EINVAL;
    }
    if (memcmp(req->cluster_cfg_signs.cluster, CLUSTER_CONFIG_SIGN_BUF,
                SF_CLUSTER_CONFIG_SIGN_LEN) != 0)
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "client's topology of cluster.conf "
                "is not consistent with mine");
        return EINVAL;
    }

    if ((flags & FS_CLIENT_JOIN_FLAGS_IDEMPOTENCY_REQUEST) != 0) {
        uint32_t channel_id;
        int key;

        if (IDEMPOTENCY_CHANNEL != NULL) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "channel already exist, the channel id: %d",
                    IDEMPOTENCY_CHANNEL->id);
            return EEXIST;
        }

        channel_id = buff2int(req->idempotency.channel_id);
        key = buff2int(req->idempotency.key);
        IDEMPOTENCY_CHANNEL = idempotency_channel_find_and_hold(
                channel_id, key, &result);
        if (IDEMPOTENCY_CHANNEL == NULL) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "find channel fail, channel id: %d, result: %d",
                    channel_id, result);
            return SF_RETRIABLE_ERROR_NO_CHANNEL;
        }

        SERVER_TASK_TYPE = SF_SERVER_TASK_TYPE_CHANNEL_USER;
    }

    join_resp = (FSProtoClientJoinResp *)SF_PROTO_RESP_BODY(task);
    int2buff(g_sf_global_vars.min_buff_size -
            FS_TASK_BUFFER_FRONT_PADDING_SIZE,
            join_resp->buffer_size);
    RESPONSE.header.body_len = sizeof(FSProtoClientJoinResp);
    RESPONSE.header.cmd = FS_COMMON_PROTO_CLIENT_JOIN_RESP;
    TASK_CTX.common.response_done = true;
    return 0;
}

static FSClusterDataServerInfo *get_readable_server(
        FSClusterDataGroupInfo *group, const SFDataReadRule read_rule)
{
    int index;
    int acc_index;
    int active_count;
    int i;
    FSClusterDataServerInfo *ds;
    FSClusterDataServerInfo *send;

    if (group->data_server_array.count == 1) {
        return (FSClusterDataServerInfo *)__sync_fetch_and_add(
                &group->master, 0);
    }

    index = rand() % group->data_server_array.count;
    if (__sync_add_and_fetch(&group->data_server_array.servers[index].
                status, 0) == FS_DS_STATUS_ACTIVE)
    {
        if (read_rule != sf_data_read_rule_slave_first ||
                !__sync_add_and_fetch(&group->data_server_array.
                    servers[index].is_master, 0))
        {
            return group->data_server_array.servers + index;
        }
    }

    acc_index = 0;
    send = group->data_server_array.servers + group->data_server_array.count;
    for (i=0; i<group->data_server_array.count; i++) {
        active_count = 0;
        for (ds=group->data_server_array.servers; ds<send; ds++) {
            if (__sync_add_and_fetch(&ds->status, 0) !=
                    FS_DS_STATUS_ACTIVE)
            {
                continue;
            }

            active_count++;
            if (read_rule == sf_data_read_rule_slave_first &&
                    __sync_add_and_fetch(&ds->is_master, 0)) {
                continue;
            }

            if (acc_index++ == index) {
                return ds;
            }
        }

        if (active_count == 0) {
            return NULL;
        } else if (active_count == 1) {
            break;
        }
    }

    return (FSClusterDataServerInfo *)__sync_fetch_and_add(
            &group->master, 0);
}

int du_handler_deal_get_readable_server(struct fast_task_info *task,
        const int group_index)
{
    int result;
    int data_group_id;
    SFDataReadRule read_rule;
    FSClusterDataGroupInfo *group;
    FSClusterDataServerInfo *ds;
    FSProtoGetReadableServerReq *req;
    FSProtoGetServerResp *resp;
    const FCAddressInfo *addr;

    if ((result=server_expect_body_length(sizeof(
                        FSProtoGetReadableServerReq))) != 0)
    {
        return result;
    }

    req = (FSProtoGetReadableServerReq *)REQUEST.body;
    data_group_id = buff2int(req->data_group_id);
    read_rule = req->read_rule;
    if ((group=fs_get_data_group(data_group_id)) == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data_group_id: %d not exist", data_group_id);
        return ENOENT;
    }

    if (read_rule == sf_data_read_rule_master_only) {
        ds = (FSClusterDataServerInfo *)__sync_fetch_and_add(
                &group->master, 0);
    } else {
        ds = get_readable_server(group, read_rule);
    }

    if (ds == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "no active server, read rule: %d", read_rule);
        return SF_RETRIABLE_ERROR_NO_SERVER;
    }

    resp = (FSProtoGetServerResp *)SF_PROTO_RESP_BODY(task);
    addr = fc_server_get_address_by_peer(&(ds->cs->server->
                group_addrs[group_index].address_array), task->client_ip);

    int2buff(ds->cs->server->id, resp->server_id);
    snprintf(resp->ip_addr, sizeof(resp->ip_addr), "%s",
            addr->conn.ip_addr);
    short2buff(addr->conn.port, resp->port);

    RESPONSE.header.body_len = sizeof(FSProtoGetServerResp);
    RESPONSE.header.cmd = FS_COMMON_PROTO_GET_READABLE_SERVER_RESP;
    TASK_CTX.common.response_done = true;
    return 0;
}

int du_handler_deal_get_group_servers(struct fast_task_info *task)
{
    int result;
    int data_group_id;
    FSClusterDataGroupInfo *group;
    FSClusterDataServerInfo *ds;
    FSClusterDataServerInfo *end;
    SFProtoGetGroupServersReq *req;
    SFProtoGetGroupServersRespBodyHeader *body_header;
    SFProtoGetGroupServersRespBodyPart *body_part;

    if ((result=server_expect_body_length(sizeof(
                        SFProtoGetGroupServersReq))) != 0)
    {
        return result;
    }

    req = (SFProtoGetGroupServersReq *)REQUEST.body;
    data_group_id = buff2int(req->group_id);
    if ((group=fs_get_data_group(data_group_id)) == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data_group_id: %d not exist", data_group_id);
        return ENOENT;
    }

    body_header = (SFProtoGetGroupServersRespBodyHeader *)
        SF_PROTO_RESP_BODY(task);
    body_part = (SFProtoGetGroupServersRespBodyPart *)(body_header + 1);
    end = group->data_server_array.servers + group->data_server_array.count;
    for (ds=group->data_server_array.servers; ds<end; ds++, body_part++) {
        int2buff(ds->cs->server->id, body_part->server_id);
        body_part->is_master = FC_ATOMIC_GET(ds->is_master);
        body_part->is_active = (FC_ATOMIC_GET(ds->status) ==
                FS_DS_STATUS_ACTIVE) ? 1 : 0;
    }
    int2buff(group->data_server_array.count, body_header->count);

    RESPONSE.header.body_len = (char *)body_part - SF_PROTO_RESP_BODY(task);
    RESPONSE.header.cmd = SF_SERVICE_PROTO_GET_GROUP_SERVERS_RESP;
    TASK_CTX.common.response_done = true;
    return 0;
}
