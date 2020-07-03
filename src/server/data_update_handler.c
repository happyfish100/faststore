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
    TASK_CTX.bs_key.block.oid = buff2long(bkey->oid);
    TASK_CTX.bs_key.block.offset = buff2long(bkey->offset);
    if (TASK_CTX.bs_key.block.offset % FS_FILE_BLOCK_SIZE != 0) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message, "block offset: %"PRId64" "
                "NOT the multiple of the block size %d",
                TASK_CTX.bs_key.block.offset, FS_FILE_BLOCK_SIZE);
        return EINVAL;
    }

    fs_calc_block_hashcode(&TASK_CTX.bs_key.block);
    TASK_CTX.data_group_id = FS_BLOCK_HASH_CODE(TASK_CTX.bs_key.block) %
        FS_DATA_GROUP_COUNT(CLUSTER_CONFIG_CTX) + 1;

    TASK_CTX.myself = fs_get_my_data_server(TASK_CTX.data_group_id);
    if (TASK_CTX.myself == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data group id: %d NOT belongs to me",
                TASK_CTX.data_group_id);
        return ENOENT;
    }

    logInfo("data_group_id: %d, master_only: %d",
            TASK_CTX.data_group_id, master_only);

    if (master_only) {
        if (!TASK_CTX.myself->is_master) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "data group id: %d, i am NOT master",
                    TASK_CTX.data_group_id);
            return EINVAL;
        }
    } else {
        if (TASK_CTX.myself->status != FS_SERVER_STATUS_ACTIVE) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "data group id: %d, i am NOT active, my status: %d",
                    TASK_CTX.data_group_id, TASK_CTX.myself->status);
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

    TASK_CTX.bs_key.slice.offset = buff2int(bs->slice_size.offset);
    TASK_CTX.bs_key.slice.length = buff2int(bs->slice_size.length);
    if (TASK_CTX.bs_key.slice.offset < 0 || TASK_CTX.bs_key.slice.offset >=
            FS_FILE_BLOCK_SIZE)
    {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message, "slice offset: %d "
                "is invalid which < 0 or exceeds the block size %d",
                TASK_CTX.bs_key.slice.offset, FS_FILE_BLOCK_SIZE);
        return EINVAL;
    }
    if (TASK_CTX.bs_key.slice.length <= 0 || TASK_CTX.bs_key.slice.offset +
            TASK_CTX.bs_key.slice.length > FS_FILE_BLOCK_SIZE)
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "slice offset: %d, length: %d is invalid which <= 0, "
                "or offset + length exceeds the block size %d",
                TASK_CTX.bs_key.slice.offset,
                TASK_CTX.bs_key.slice.length, FS_FILE_BLOCK_SIZE);
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

static int handle_slice_write_done(struct fast_task_info *task)
{
    TASK_ARG->context.deal_func = NULL;
    RESPONSE.header.cmd = FS_SERVICE_PROTO_SLICE_WRITE_RESP;
    fill_slice_update_response(task, TASK_CTX.slice_notify.inc_alloc);
    return RESPONSE_STATUS;
}

static void slice_write_done_notify(FSSliceOpNotify *notify)
{
    struct fast_task_info *task;
    int result;

    task = (struct fast_task_info *)notify->notify.args;
    if (notify->result != 0) {
        RESPONSE.error.length = snprintf(RESPONSE.error.message,
                sizeof(RESPONSE.error.message),
                "%s", STRERROR(notify->result));

        logError("file: "__FILE__", line: %d, "
                "client ip: %s, write slice fail, "
                "oid: %"PRId64", block offset: %"PRId64", "
                "slice offset: %d, length: %d, "
                "errno: %d, error info: %s",
                __LINE__, task->client_ip,
                TASK_CTX.bs_key.block.oid, TASK_CTX.bs_key.block.offset,
                TASK_CTX.bs_key.slice.offset, TASK_CTX.bs_key.slice.length,
                notify->result, STRERROR(notify->result));
        TASK_ARG->context.log_error = false;
        result = notify->result;
    } else {
        if (TASK_CTX.data_version == 0) {
            TASK_CTX.data_version = __sync_add_and_fetch(
                    &TASK_CTX.myself->data_version, 1);
        }

        logInfo("data_group_id: %d, TASK_CTX.data_version: %"PRId64,
                TASK_CTX.data_group_id, TASK_CTX.data_version);

        replica_binlog_log_write_slice(TASK_CTX.data_group_id,
                TASK_CTX.data_version, &TASK_CTX.bs_key);

        if (TASK_CTX.which_side == FS_WHICH_SIDE_MASTER) {
            if ((result=replication_producer_push_to_slave_queues(task)) ==
                    TASK_STATUS_CONTINUE)
            {
                TASK_ARG->context.deal_func = handle_slice_write_done;
            }
            else {
                RESPONSE.header.cmd = FS_SERVICE_PROTO_SLICE_WRITE_RESP;
                fill_slice_update_response(task, notify->inc_alloc);
            }
        } else {
            RESPONSE.header.cmd = FS_REPLICA_PROTO_OP_RESP;
            result = 0;
        }
    }

    RESPONSE_STATUS = notify->result;
    if (result != TASK_STATUS_CONTINUE) {
        sf_nio_notify(task, SF_NIO_STAGE_CONTINUE);
    }
}

static inline void set_block_op_error_msg(struct fast_task_info *task,
        const char *caption, const int result)
{
    RESPONSE.error.length = sprintf(RESPONSE.error.message,
            "block %s fail, result: %d, block {oid: %"PRId64", "
            "offset: %"PRId64"}", caption, result,
            TASK_CTX.bs_key.block.oid,
            TASK_CTX.bs_key.block.offset);
}

#define DH_SET_EXTRA_PKG_LENGTH(which) \
    int footer_len;  \
    TASK_CTX.which_side = which;  \
    footer_len = which == FS_WHICH_SIDE_MASTER ? \
    0 : sizeof(FSProtoReplicaFooter)

#define DH_SET_DATA_VERSION(task, which) \
    do { \
        if (which == FS_WHICH_SIDE_MASTER) {   \
            TASK_CTX.data_version = 0;  \
        } else {  \
            FSProtoReplicaFooter *footer;  \
            footer = (FSProtoReplicaFooter *)(REQUEST.body + (REQUEST.header. \
                        body_len - sizeof(FSProtoReplicaFooter)));   \
            TASK_CTX.data_version = buff2long(footer->data_version); \
            if (TASK_CTX.data_version <= 0) {  \
                RESPONSE.error.length = sprintf(RESPONSE.error.message, \
                        "invalid data version: %"PRId64, \
                        TASK_CTX.data_version);  \
                return EINVAL; \
            }  \
        }  \
    } while (0)


int du_handler_deal_slice_write(struct fast_task_info *task,
        const int which_side)
{
    FSProtoSliceWriteReqHeader *req_header;
    char *buff;
    int result;

    DH_SET_EXTRA_PKG_LENGTH(which_side);
    if ((result=server_check_min_body_length(task,
                    sizeof(FSProtoSliceWriteReqHeader) + footer_len)) != 0)
    {
        return result;
    }

    req_header = (FSProtoSliceWriteReqHeader *)REQUEST.body;
    if ((result=du_handler_parse_check_block_slice(task,&req_header->bs,
                    which_side == FS_WHICH_SIDE_MASTER)) != 0)
    {
        return result;
    }

    if (sizeof(FSProtoSliceWriteReqHeader) + TASK_CTX.bs_key.slice.length
            + footer_len != REQUEST.header.body_len)
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "body header length: %d + slice length: %d + footer length: %d"
                " != body length: %d", (int)sizeof(FSProtoSliceWriteReqHeader),
                TASK_CTX.bs_key.slice.length, footer_len, REQUEST.header.body_len);
        return EINVAL;
    }

    logInfo("file: "__FILE__", line: %d, func: %s, "
            "data_group_id: %d", __LINE__, __FUNCTION__,
            TASK_CTX.data_group_id);

    buff = REQUEST.body + sizeof(FSProtoSliceWriteReqHeader);
    TASK_CTX.slice_notify.notify.func = slice_write_done_notify;
    TASK_CTX.slice_notify.notify.args = task;

    //TODO
    /*
    {
        int64_t offset = TASK_CTX.bs_key.block.offset + TASK_CTX.bs_key.slice.offset;
        int size = TASK_CTX.bs_key.slice.length;
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

    DH_SET_DATA_VERSION(task, which_side);
    if ((result=fs_slice_write(&TASK_CTX.bs_key, buff,
                    &TASK_CTX.slice_notify)) != 0)
    {
        du_handler_set_slice_op_error_msg(task, "write", result);
        return result;
    }

    return TASK_STATUS_CONTINUE;
}

int du_handler_deal_slice_allocate(struct fast_task_info *task,
        const int which_side)
{
    int result;
    int inc_alloc;
    FSProtoSliceAllocateReq *req;

    DH_SET_EXTRA_PKG_LENGTH(which_side);
    if ((result=server_expect_body_length(task,
                    sizeof(FSProtoSliceAllocateReq) + footer_len)) != 0)
    {
        return result;
    }

    req = (FSProtoSliceAllocateReq *)REQUEST.body;
    if ((result=du_handler_parse_check_block_slice(task, &req->bs,
                    which_side == FS_WHICH_SIDE_MASTER)) != 0)
    {
        return result;
    }

    DH_SET_DATA_VERSION(task, which_side);
    if ((result=fs_slice_allocate_ex(&TASK_CTX.bs_key, ((FSServerContext *)
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

int du_handler_deal_slice_delete(struct fast_task_info *task,
        const int which_side)
{
    int result;
    int dec_alloc;
    FSProtoSliceDeleteReq *req;

    DH_SET_EXTRA_PKG_LENGTH(which_side);
    if ((result=server_expect_body_length(task,
                    sizeof(FSProtoSliceDeleteReq) + footer_len)) != 0)
    {
        return result;
    }

    req = (FSProtoSliceDeleteReq *)REQUEST.body;
    if ((result=du_handler_parse_check_block_slice(task, &req->bs,
                    which_side == FS_WHICH_SIDE_MASTER)) != 0)
    {
        return result;
    }

    DH_SET_DATA_VERSION(task, which_side);
    if ((result=ob_index_delete_slices(&TASK_CTX.bs_key, &dec_alloc)) != 0) {
        du_handler_set_slice_op_error_msg(task, "delete", result);
        return result;
    }

    RESPONSE.header.cmd = FS_SERVICE_PROTO_SLICE_DELETE_RESP;
    fill_slice_update_response(task, dec_alloc);
    return 0;
}

int du_handler_deal_block_delete(struct fast_task_info *task,
        const int which_side)
{
    int result;
    int dec_alloc;
    FSProtoBlockDeleteReq *req;

    DH_SET_EXTRA_PKG_LENGTH(which_side);
    if ((result=server_expect_body_length(task,
                    sizeof(FSProtoBlockDeleteReq) + footer_len)) != 0)
    {
        return result;
    }

    req = (FSProtoBlockDeleteReq *)REQUEST.body;
    if ((result=parse_check_block_key_ex(task, &req->bkey,
                    which_side == FS_WHICH_SIDE_MASTER)) != 0)
    {
        return result;
    }

    DH_SET_DATA_VERSION(task, which_side);
    if ((result=ob_index_delete_block(&TASK_CTX.bs_key.block,
                    &dec_alloc)) != 0)
    {
        set_block_op_error_msg(task, "delete", result);
        return result;
    }

    RESPONSE.header.cmd = FS_SERVICE_PROTO_BLOCK_DELETE_RESP;
    fill_slice_update_response(task, dec_alloc);
    return 0;
}
