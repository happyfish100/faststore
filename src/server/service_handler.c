//service_handler.c

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
#include "binlog/data_binlog.h"
#include "server_global.h"
#include "server_func.h"
#include "server_group_info.h"
#include "server_storage.h"
#include "service_handler.h"

//TODO
static int write_fd =  -1;

int service_handler_init()
{
    //TODO
    {
    const char *filename = "/tmp/fuse.dat";
    write_fd = open(filename, O_CREAT | O_WRONLY | O_TRUNC, 0775);
    if (write_fd < 0) {
        logInfo("file: "__FILE__", line: %d, func: %s, "
                "open file %s fail, errno: %d, error info: %s",
                __LINE__, __FUNCTION__, filename, errno, strerror(errno));
        return errno != 0 ? errno : EPERM;
    }
    }

    return 0;
}

int service_handler_destroy()
{   
    return 0;
}

void service_task_finish_cleanup(struct fast_task_info *task)
{
    __sync_add_and_fetch(&((FSServerTaskArg *)task->arg)->task_version, 1);
    sf_task_finish_clean_up(task);
}

static int service_deal_actvie_test(struct fast_task_info *task)
{
    return server_expect_body_length(task, 0);
}

static int service_deal_client_join(struct fast_task_info *task)
{
    int result;
    int data_group_count;
    FSProtoClientJoinReq *req;
    FSProtoClientJoinResp *join_resp;

    if ((result=server_expect_body_length(task,
                    sizeof(FSProtoClientJoinReq))) != 0)
    {
        return result;
    }

    req = (FSProtoClientJoinReq *)REQUEST.body;
    data_group_count = buff2int(req->data_group_count);
    if (data_group_count != FS_DATA_GROUP_COUNT(CLUSTER_CONFIG_CTX)) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "client data group count: %d != mine: %d",
                data_group_count, FS_DATA_GROUP_COUNT(CLUSTER_CONFIG_CTX));
        return EINVAL;
    }

    join_resp = (FSProtoClientJoinResp *)REQUEST.body;
    int2buff(g_sf_global_vars.min_buff_size - (sizeof(FSProtoHeader) +
            4 * sizeof(FSProtoSliceWriteReqHeader)), join_resp->buffer_size);
    RESPONSE.header.body_len = sizeof(FSProtoClientJoinResp);
    RESPONSE.header.cmd = FS_SERVICE_PROTO_CLIENT_JOIN_RESP;
    TASK_ARG->context.response_done = true;
    return 0;
}

static int service_deal_service_stat(struct fast_task_info *task)
{
    int result;
    FSProtoServiceStatResp *stat_resp;

    if ((result=server_expect_body_length(task, 0)) != 0) {
        return result;
    }

    stat_resp = (FSProtoServiceStatResp *)REQUEST.body;

    /*
    stat_resp->is_master = CLUSTER_MYSELF_PTR == CLUSTER_MASTER_PTR ? 1 : 0;
    stat_resp->status = CLUSTER_MYSELF_PTR->status;
    int2buff(CLUSTER_MYSELF_PTR->server->id, stat_resp->server_id);

    int2buff(SF_G_CONN_CURRENT_COUNT, stat_resp->connection.current_count);
    int2buff(SF_G_CONN_MAX_COUNT, stat_resp->connection.max_count);
    long2buff(CLUSTER_CURRENT_VERSION, stat_resp->dentry.current_data_version);
    long2buff(CURRENT_INODE_SN, stat_resp->dentry.current_oid_sn);
    long2buff(counters.ns, stat_resp->dentry.counters.ns);
    long2buff(counters.dir, stat_resp->dentry.counters.dir);
    long2buff(counters.file, stat_resp->dentry.counters.file);
    */

    RESPONSE.header.body_len = sizeof(FSProtoServiceStatResp);
    RESPONSE.header.cmd = FS_SERVICE_PROTO_SERVICE_STAT_RESP;
    TASK_ARG->context.response_done = true;

    return 0;
}

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

    TASK_CTX.myself = fs_get_data_server(TASK_CTX.data_group_id,
            CLUSTER_MYSELF_PTR->server->id);
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

#define parse_check_block_key(task, bkey) \
    parse_check_block_key_ex(task, bkey, true)

static int parse_check_block_slice(struct fast_task_info *task,
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
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message, "slice offset: %d, length: %d "
                "is invalid which <= 0, or offset + length exceeds "
                "the block size %d", TASK_CTX.bs_key.slice.offset,
                TASK_CTX.bs_key.slice.length, FS_FILE_BLOCK_SIZE);
        return EINVAL;
    }

    return 0;
}

#define parse_check_writable_block_slice(task, bs) \
    parse_check_block_slice(task, bs, true)

#define parse_check_readable_block_slice(task, bs) \
    parse_check_block_slice(task, bs, false)

static inline void fill_slice_update_response(struct fast_task_info *task,
        const int inc_alloc)
{
    FSProtoSliceUpdateResp *resp;
    resp = (FSProtoSliceUpdateResp *)REQUEST.body;
    int2buff(inc_alloc, resp->inc_alloc);

    RESPONSE.header.body_len = sizeof(FSProtoSliceUpdateResp);
    TASK_ARG->context.response_done = true;
}

static void slice_write_done_notify(FSSliceOpNotify *notify)
{
    struct fast_task_info *task;

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
    } else {
        //TODO
        TASK_CTX.data_version = __sync_add_and_fetch(
                &TASK_CTX.myself->data_version, 1);

        logInfo("data_group_id: %d, TASK_CTX.data_version: %"PRId64,
                TASK_CTX.data_group_id, TASK_CTX.data_version);

        data_binlog_log_write_slice(TASK_CTX.data_group_id,
            TASK_CTX.data_version, &TASK_CTX.bs_key);

        RESPONSE.header.cmd = FS_SERVICE_PROTO_SLICE_WRITE_RESP;
        fill_slice_update_response(task, notify->inc_alloc);
    }

    RESPONSE_STATUS = notify->result;
    sf_nio_notify(task, SF_NIO_STAGE_CONTINUE);
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

static inline void set_slice_op_error_msg(struct fast_task_info *task,
        const char *caption, const int result)
{
    RESPONSE.error.length = sprintf(RESPONSE.error.message,
            "slice %s fail, result: %d, block {oid: %"PRId64", "
            "offset: %"PRId64"}, slice {offset: %d, length: %d}",
            caption, result, TASK_CTX.bs_key.block.oid,
            TASK_CTX.bs_key.block.offset,
            TASK_CTX.bs_key.slice.offset,
            TASK_CTX.bs_key.slice.length);
}

static int service_deal_slice_write(struct fast_task_info *task)
{
    int result;
    FSProtoSliceWriteReqHeader *req_header;
    char *buff;

    if ((result=server_check_min_body_length(task,
                    sizeof(FSProtoSliceWriteReqHeader))) != 0)
    {
        return result;
    }

    req_header = (FSProtoSliceWriteReqHeader *)REQUEST.body;
    if ((result=parse_check_writable_block_slice(task, &req_header->bs)) != 0) {
        return result;
    }

    if (sizeof(FSProtoSliceWriteReqHeader) + TASK_CTX.bs_key.slice.length !=
            REQUEST.header.body_len)
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "body header length: %d + slice length: %d != body length: %d",
                (int)sizeof(FSProtoSliceWriteReqHeader),
                TASK_CTX.bs_key.slice.length, REQUEST.header.body_len);
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

    if ((result=fs_slice_write(&TASK_CTX.bs_key, buff,
                    &TASK_CTX.slice_notify)) != 0)
    {
        set_slice_op_error_msg(task, "write", result);
        return result;
    }

    return TASK_STATUS_CONTINUE;
}

static int service_deal_slice_allocate(struct fast_task_info *task)
{
    int result;
    int inc_alloc;
    FSProtoSliceAllocateReq *req;

    if ((result=server_expect_body_length(task,
                    sizeof(FSProtoSliceAllocateReq))) != 0)
    {
        return result;
    }

    req = (FSProtoSliceAllocateReq *)REQUEST.body;
    if ((result=parse_check_writable_block_slice(task, &req->bs)) != 0) {
        return result;
    }

    if ((result=fs_slice_allocate_ex(&TASK_CTX.bs_key, ((FSServerContext *)
                        task->thread_data->arg)->service.slice_ptr_array,
                    &inc_alloc)) != 0)
    {
        set_slice_op_error_msg(task, "allocate", result);
        return result;
    }

    RESPONSE.header.cmd = FS_SERVICE_PROTO_SLICE_ALLOCATE_RESP;
    fill_slice_update_response(task, inc_alloc);
    return 0;
}

static int service_deal_slice_delete(struct fast_task_info *task)
{
    int result;
    int dec_alloc;
    FSProtoSliceDeleteReq *req;

    if ((result=server_expect_body_length(task,
                    sizeof(FSProtoSliceDeleteReq))) != 0)
    {
        return result;
    }

    req = (FSProtoSliceDeleteReq *)REQUEST.body;
    if ((result=parse_check_writable_block_slice(task, &req->bs)) != 0) {
        return result;
    }

    if ((result=ob_index_delete_slices(&TASK_CTX.bs_key, &dec_alloc)) != 0) {
        set_slice_op_error_msg(task, "delete", result);
        return result;
    }

    RESPONSE.header.cmd = FS_SERVICE_PROTO_SLICE_DELETE_RESP;
    fill_slice_update_response(task, dec_alloc);
    return 0;
}

static int service_deal_block_delete(struct fast_task_info *task)
{
    int result;
    int dec_alloc;
    FSProtoBlockDeleteReq *req;

    if ((result=server_expect_body_length(task,
                    sizeof(FSProtoBlockDeleteReq))) != 0)
    {
        return result;
    }

    req = (FSProtoBlockDeleteReq *)REQUEST.body;
    if ((result=parse_check_block_key(task, &req->bkey)) != 0) {
        return result;
    }

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

static void slice_read_done_notify(FSSliceOpNotify *notify)
{
    struct fast_task_info *task;

    task = (struct fast_task_info *)notify->notify.args;
    if (notify->result != 0) {
        RESPONSE.error.length = snprintf(RESPONSE.error.message,
                sizeof(RESPONSE.error.message),
                "%s", STRERROR(notify->result));

        logError("file: "__FILE__", line: %d, "
                "client ip: %s, read slice fail, "
                "oid: %"PRId64", block offset: %"PRId64", "
                "slice offset: %d, length: %d, "
                "errno: %d, error info: %s",
                __LINE__, task->client_ip,
                TASK_CTX.bs_key.block.oid, TASK_CTX.bs_key.block.offset,
                TASK_CTX.bs_key.slice.offset, TASK_CTX.bs_key.slice.length,
                notify->result, STRERROR(notify->result));
        TASK_ARG->context.log_error = false;
    } else {
        RESPONSE.header.cmd = FS_SERVICE_PROTO_SLICE_READ_RESP;
        RESPONSE.header.body_len = notify->done_bytes;
        TASK_ARG->context.response_done = true;
    }

    RESPONSE_STATUS = notify->result;
    sf_nio_notify(task, SF_NIO_STAGE_CONTINUE);
}

static int service_deal_slice_read(struct fast_task_info *task)
{
    int result;
    FSProtoSliceReadReqHeader *req_header;
    char *buff;

    RESPONSE.header.cmd = FS_SERVICE_PROTO_SLICE_READ_RESP;
    if ((result=server_expect_body_length(task,
                    sizeof(FSProtoSliceReadReqHeader))) != 0)
    {
        return result;
    }

    req_header = (FSProtoSliceReadReqHeader *)REQUEST.body;
    if ((result=parse_check_readable_block_slice(task, &req_header->bs)) != 0) {
        return result;
    }

    if (TASK_CTX.bs_key.slice.length > task->size - sizeof(FSProtoHeader)) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "read slice length: %d > task buffer size: %d",
                TASK_CTX.bs_key.slice.length, (int)(
                    task->size - sizeof(FSProtoHeader)));
        return EOVERFLOW;
    }

    buff = REQUEST.body;
    TASK_CTX.slice_notify.notify.func = slice_read_done_notify;
    TASK_CTX.slice_notify.notify.args = task;
    if ((result=fs_slice_read_ex(&TASK_CTX.bs_key, buff,
                    &TASK_CTX.slice_notify, ((FSServerContext *)task->
                        thread_data->arg)->service.slice_ptr_array)) != 0)
    {
        set_slice_op_error_msg(task, "read", result);
        return result;
    }
    
    return TASK_STATUS_CONTINUE;
}

static inline void init_task_context(struct fast_task_info *task)
{
    TASK_ARG->req_start_time = get_current_time_us();
    RESPONSE.header.cmd = FS_PROTO_ACK;
    RESPONSE.header.body_len = 0;
    RESPONSE.header.status = 0;
    RESPONSE.error.length = 0;
    RESPONSE.error.message[0] = '\0';
    TASK_ARG->context.log_error = true;
    TASK_ARG->context.response_done = false;

    REQUEST.header.cmd = ((FSProtoHeader *)task->data)->cmd;
    REQUEST.header.body_len = task->length - sizeof(FSProtoHeader);
    REQUEST.body = task->data + sizeof(FSProtoHeader);
}

static inline int service_check_master(struct fast_task_info *task)
{
    /*
    if (CLUSTER_MYSELF_PTR != CLUSTER_MASTER_PTR) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am not master");
        return EINVAL;
    }
    */

    return 0;
}

static inline int service_check_readable(struct fast_task_info *task)
{
    /*
    if (!(CLUSTER_MYSELF_PTR == CLUSTER_MASTER_PTR ||
                CLUSTER_MYSELF_PTR->status == FS_SERVER_STATUS_ACTIVE))
    {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am not active");
        return EINVAL;
    }
    */

    return 0;
}

static int deal_task_done(struct fast_task_info *task)
{
    FSProtoHeader *proto_header;
    int r;
    int time_used;
    char time_buff[32];

    if (TASK_ARG->context.log_error && RESPONSE.error.length > 0) {
        logError("file: "__FILE__", line: %d, "
                "client ip: %s, cmd: %d (%s), req body length: %d, %s",
                __LINE__, task->client_ip, REQUEST.header.cmd,
                fs_get_cmd_caption(REQUEST.header.cmd),
                REQUEST.header.body_len,
                RESPONSE.error.message);
    }

    proto_header = (FSProtoHeader *)task->data;
    if (!TASK_ARG->context.response_done) {
        RESPONSE.header.body_len = RESPONSE.error.length;
        if (RESPONSE.error.length > 0) {
            memcpy(task->data + sizeof(FSProtoHeader),
                    RESPONSE.error.message, RESPONSE.error.length);
        }
    }

    short2buff(RESPONSE_STATUS >= 0 ? RESPONSE_STATUS : -1 * RESPONSE_STATUS,
            proto_header->status);
    proto_header->cmd = RESPONSE.header.cmd;
    int2buff(RESPONSE.header.body_len, proto_header->body_len);
    task->length = sizeof(FSProtoHeader) + RESPONSE.header.body_len;

    r = sf_send_add_event(task);
    time_used = (int)(get_current_time_us() - TASK_ARG->req_start_time);
    if (time_used > 20 * 1000) {
        lwarning("process a request timed used: %s us, "
                "cmd: %d (%s), req body len: %d, resp body len: %d",
                long_to_comma_str(time_used, time_buff),
                REQUEST.header.cmd, fs_get_cmd_caption(REQUEST.header.cmd),
                REQUEST.header.body_len, RESPONSE.header.body_len);
    }

    logInfo("file: "__FILE__", line: %d, "
            "client ip: %s, req cmd: %d (%s), req body_len: %d, "
            "resp cmd: %d (%s), status: %d, resp body_len: %d, "
            "time used: %s us", __LINE__,
            task->client_ip, REQUEST.header.cmd,
            fs_get_cmd_caption(REQUEST.header.cmd),
            REQUEST.header.body_len, RESPONSE.header.cmd,
            fs_get_cmd_caption(RESPONSE.header.cmd),
            RESPONSE_STATUS, RESPONSE.header.body_len,
            long_to_comma_str(time_used, time_buff));

    return r == 0 ? RESPONSE_STATUS : r;
}

int service_deal_task(struct fast_task_info *task)
{
    int result;

    /*
    logInfo("file: "__FILE__", line: %d, "
            "nio_stage: %d, SF_NIO_STAGE_CONTINUE: %d", __LINE__,
            task->nio_stage, SF_NIO_STAGE_CONTINUE);
            */

    if (task->nio_stage == SF_NIO_STAGE_CONTINUE) {
        task->nio_stage = SF_NIO_STAGE_SEND;
        if (TASK_ARG->context.deal_func != NULL) {
            result = TASK_ARG->context.deal_func(task);
        } else {
            result = RESPONSE_STATUS;
            if (result == TASK_STATUS_CONTINUE) {
                logError("file: "__FILE__", line: %d, "
                        "unexpect status: %d", __LINE__, result);
                result = EBUSY;
            }
        }
    } else {
        init_task_context(task);

        switch (REQUEST.header.cmd) {
            case FS_PROTO_ACTIVE_TEST_REQ:
                RESPONSE.header.cmd = FS_PROTO_ACTIVE_TEST_RESP;
                result = service_deal_actvie_test(task);
                break;
            case FS_SERVICE_PROTO_CLIENT_JOIN_REQ:
                result = service_deal_client_join(task);
                break;
            case FS_SERVICE_PROTO_SERVICE_STAT_REQ:
                result = service_deal_service_stat(task);
                break;
            case FS_SERVICE_PROTO_SLICE_WRITE_REQ:
                result = service_deal_slice_write(task);
                break;
            case FS_SERVICE_PROTO_SLICE_ALLOCATE_REQ:
                result = service_deal_slice_allocate(task);
                break;
            case FS_SERVICE_PROTO_SLICE_DELETE_REQ:
                result = service_deal_slice_delete(task);
                break;
            case FS_SERVICE_PROTO_BLOCK_DELETE_REQ:
                result = service_deal_block_delete(task);
                break;
            case FS_SERVICE_PROTO_SLICE_READ_REQ:
                result = service_deal_slice_read(task);
                break;
            default:
                RESPONSE.error.length = sprintf(
                        RESPONSE.error.message,
                        "unkown cmd: %d", REQUEST.header.cmd);
                result = -EINVAL;
                break;
        }
    }

    if (result == TASK_STATUS_CONTINUE) {
        return 0;
    } else {
        RESPONSE_STATUS = result;
        return deal_task_done(task);
    }
}

void *service_alloc_thread_extra_data(const int thread_index)
{
    int bytes;
    FSServerContext *server_context;

    bytes = sizeof(FSServerContext) + sizeof(struct ob_slice_ptr_array);
    server_context = (FSServerContext *)malloc(bytes);
    if (server_context == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail, errno: %d, error info: %s",
                __LINE__, bytes, errno, strerror(errno));
        return NULL;
    }
    memset(server_context, 0, bytes);

    server_context->service.slice_ptr_array = (struct ob_slice_ptr_array *)
        (server_context + 1);
    return server_context;
}
