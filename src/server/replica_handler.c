//replica_handler.c

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
#include "fastcommon/json_parser.h"
#include "sf/sf_util.h"
#include "sf/sf_func.h"
#include "sf/sf_nio.h"
#include "sf/sf_global.h"
#include "common/fs_proto.h"
#include "server_global.h"
#include "server_func.h"
#include "server_binlog.h"
#include "server_group_info.h"
#include "server_replication.h"
#include "cluster_topology.h"
#include "cluster_relationship.h"
#include "common_handler.h"
#include "data_update_handler.h"
#include "replica_handler.h"

int replica_handler_init()
{
    return 0;
}

int replica_handler_destroy()
{   
    return 0;
}

static inline int replica_alloc_reader(struct fast_task_info *task)
{
    REPLICA_READER = (ServerBinlogReader *)fc_malloc(sizeof(ServerBinlogReader));
    if (REPLICA_READER == NULL) {
        return ENOMEM;
    }
    SERVER_TASK_TYPE = FS_SERVER_TASK_TYPE_FETCH_BINLOG;
    return 0;
}

static inline void replica_release_reader(struct fast_task_info *task)
{
    if (REPLICA_READER != NULL) {
        binlog_reader_destroy(REPLICA_READER);
        free(REPLICA_READER);
        REPLICA_READER = NULL;
    }
    SERVER_TASK_TYPE = FS_SERVER_TASK_TYPE_NONE;
}

int replica_recv_timeout_callback(struct fast_task_info *task)
{
    if (SERVER_TASK_TYPE == FS_SERVER_TASK_TYPE_REPLICATION &&
            REPLICA_REPLICATION != NULL)
    {
        logError("file: "__FILE__", line: %d, "
                "client %s:%d, sock: %d, server id: %d, recv timeout",
                __LINE__, task->client_ip, task->port, task->event.fd,
                REPLICA_REPLICATION->peer->server->id);
        return ETIMEDOUT;
    }

    return 0;
}

void replica_task_finish_cleanup(struct fast_task_info *task)
{
    /*
    FSServerTaskArg *task_arg;
    task_arg = (FSServerTaskArg *)task->arg;
    */

    switch (SERVER_TASK_TYPE) {
        case FS_SERVER_TASK_TYPE_REPLICATION:
            if (REPLICA_REPLICATION != NULL) {
                replication_processor_unbind(REPLICA_REPLICATION);
                REPLICA_REPLICATION = NULL;
            }
            SERVER_TASK_TYPE = FS_SERVER_TASK_TYPE_NONE;
            break;
        case FS_SERVER_TASK_TYPE_FETCH_BINLOG:
            replica_release_reader(task);
            break;
        default:
            break;
    }

    __sync_add_and_fetch(&((FSServerTaskArg *)task->arg)->task_version, 1);
    sf_task_finish_clean_up(task);
}

static int replica_check_master(struct fast_task_info *task,
        const int data_group_id)
{
    FSClusterDataServerInfo *myself;

    myself = fs_get_my_data_server(data_group_id);
    if (myself == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data group id: %d NOT belongs to me", data_group_id);
        return ENOENT;
    }

    if (!myself->is_master) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data group id: %d, i am NOT master", data_group_id);
        return EINVAL;
    }

    return 0;
}

static int replica_fetch_binlog_output(struct fast_task_info *task)
{
    FSProtoReplicaFetchBinlogRespBodyHeader *body_header;
    char *buff;
    int result;
    int size;
    int read_bytes;

    body_header = (FSProtoReplicaFetchBinlogRespBodyHeader *)REQUEST.body;
    buff = (char *)(body_header + 1);
    size = (task->data + task->size) - buff;
    result = binlog_reader_integral_read(REPLICA_READER,
            buff, size, &read_bytes);
    if (!(result == 0 || result == ENOENT)) {
        return result;
    }

    int2buff(read_bytes, body_header->binlog_length);
    if (size - read_bytes < FS_REPLICA_BINLOG_MAX_RECORD_SIZE) {
        body_header->is_last = false;
    } else {
        body_header->is_last = binlog_reader_is_last_file(REPLICA_READER);
    }

    RESPONSE.header.body_len = sizeof(*body_header) + read_bytes;
    RESPONSE.header.cmd = FS_REPLICA_PROTO_FETCH_BINLOG_RESP;
    TASK_ARG->context.response_done = true;
    return 0;
}

static int replica_deal_fetch_binlog_first(struct fast_task_info *task)
{
    FSProtoReplicaFetchBinlogFirstReq *req;
    uint64_t last_data_version;
    int data_group_id;
    int result;

    if ((result=server_expect_body_length(task,
                    sizeof(FSProtoReplicaFetchBinlogFirstReq))) != 0)
    {
        return result;
    }

    req = (FSProtoReplicaFetchBinlogFirstReq *)REQUEST.body;
    last_data_version = buff2long(req->last_data_version);
    data_group_id = buff2int(req->data_group_id);
    //req->catch_up

    if ((result=replica_check_master(task, data_group_id)) != 0) {
        return result;
    }

    if (SERVER_TASK_TYPE != FS_SERVER_TASK_TYPE_NONE ||
            REPLICA_READER != NULL)
    {
        return EALREADY;
    }

    if ((result=replica_alloc_reader(task)) != 0) {
        return result;
    }

    if ((result=replica_binlog_reader_init(REPLICA_READER,
                    data_group_id, last_data_version)) != 0)
    {
        replica_release_reader(task);
        return result;
    }

    return replica_fetch_binlog_output(task);
}

static int replica_deal_fetch_binlog_next(struct fast_task_info *task)
{
    int result;

    if ((result=server_expect_body_length(task, 0)) != 0) {
        return result;
    }

    if (!(SERVER_TASK_TYPE == FS_SERVER_TASK_TYPE_FETCH_BINLOG &&
            REPLICA_READER != NULL))
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "please send cmd %d (%s) first",
                FS_REPLICA_PROTO_FETCH_BINLOG_FIRST_REQ,
                fs_get_cmd_caption(FS_REPLICA_PROTO_FETCH_BINLOG_FIRST_REQ));
        return EINVAL;
    }

    return replica_fetch_binlog_output(task);
}

static int replica_deal_join_server_req(struct fast_task_info *task)
{
    int result;
    int server_id;
    int buffer_size;
    int replica_channels_between_two_servers;
    FSProtoJoinServerReq *req;
    FSClusterServerInfo *peer;
    //FSProtoJoinServerResp *resp;
    FSReplication *replication;

    if ((result=server_expect_body_length(task,
                    sizeof(FSProtoJoinServerReq))) != 0)
    {
        return result;
    }

    req = (FSProtoJoinServerReq *)REQUEST.body;
    server_id = buff2int(req->server_id);
    buffer_size = buff2int(req->buffer_size);
    replica_channels_between_two_servers = buff2int(
            req->replica_channels_between_two_servers);
    if (buffer_size != task->size) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "peer task buffer size: %d != mine: %d",
                buffer_size, task->size);
        return EINVAL;
    }
    if (replica_channels_between_two_servers !=
            REPLICA_CHANNELS_BETWEEN_TWO_SERVERS)
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "replica_channels_between_two_servers: %d != mine: %d",
                replica_channels_between_two_servers,
                REPLICA_CHANNELS_BETWEEN_TWO_SERVERS);
        return EINVAL;
    }

    if ((result=handler_check_config_signs(task, server_id,
                    &req->config_signs)) != 0)
    {
        return result;
    }

    peer = fs_get_server_by_id(server_id);
    if (peer == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "peer server id: %d not exist", server_id);
        return ENOENT;
    }
    if (SERVER_TASK_TYPE != FS_SERVER_TASK_TYPE_NONE) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "server id: %d already joined", server_id);
        return EEXIST;
    }

    if ((replication=fs_get_idle_replication_by_peer(server_id)) == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "peer server id: %d, NO replication slot", server_id);
        return ENOENT;
    }

    replication_processor_bind_task(replication, task);
    RESPONSE.header.cmd = FS_REPLICA_PROTO_JOIN_SERVER_RESP;
    return 0;
}

static int replica_deal_join_server_resp(struct fast_task_info *task)
{
    if (!(SERVER_TASK_TYPE == FS_SERVER_TASK_TYPE_REPLICATION &&
                REPLICA_REPLICATION != NULL))
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "unexpect cmd: %d", REQUEST.header.cmd);
        return EINVAL;
    }

    set_replication_stage(REPLICA_REPLICATION, FS_REPLICATION_STAGE_SYNCING);
    return 0;
}

static inline int replica_check_replication_task(struct fast_task_info *task)
{
    if (SERVER_TASK_TYPE != FS_SERVER_TASK_TYPE_REPLICATION) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "invalid task type: %d != %d", SERVER_TASK_TYPE,
                FS_SERVER_TASK_TYPE_REPLICATION);
        return EINVAL;
    }

    if (REPLICA_REPLICATION == NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "cluster replication ptr is null");
        return EINVAL;
    }

    if (REPLICA_REPLICATION->last_net_comm_time != g_current_time) {
        REPLICA_REPLICATION->last_net_comm_time = g_current_time;
    }

    return 0;
}

static inline int replica_deal_actvie_test_req(struct fast_task_info *task)
{
    int result;

    if ((result=replica_check_replication_task(task)) != 0) {
        return result;
    }
    return handler_deal_actvie_test(task);
}

static int handle_rpc_req(struct fast_task_info *task, SharedBuffer *buffer,
        const int count)
{
    FSProtoReplicaRPCReqBodyPart *body_part;
    FSSliceOpBufferContext *op_buffer_ctx;
    FSSliceOpContext *op_ctx;
    int result;
    int current_len;
    int last_index;
    int blen;
    int i;

    TASK_CTX.which_side = FS_WHICH_SIDE_SLAVE;
    last_index = count - 1;
    current_len = sizeof(FSProtoReplicaRPCReqBodyHeader);
    for (i=0; i<count; i++) {
        body_part = (FSProtoReplicaRPCReqBodyPart *)
            (buffer->buff + current_len);
        blen = buff2int(body_part->body_len);
        if (blen <= 0) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "rpc body length: %d <= 0", blen);
            return EINVAL;
        }
        current_len += sizeof(*body_part) + blen;
        if (i < last_index) {
            if (REQUEST.header.body_len < current_len) {
                RESPONSE.error.length = sprintf(RESPONSE.error.message,
                        "body length: %d < %d, rpc count: %d, current: %d",
                        REQUEST.header.body_len, current_len, count, i + 1);
                return EINVAL;
            }
        } else {
            if (REQUEST.header.body_len != current_len) {
                RESPONSE.error.length = sprintf(RESPONSE.error.message,
                        "body length: %d != %d, rpc count: %d",
                        REQUEST.header.body_len, current_len, count);
                return EINVAL;
            }
        }

        if (body_part->cmd == FS_SERVICE_PROTO_SLICE_WRITE_REQ) {
            if ((op_buffer_ctx=replication_callee_alloc_op_buffer_ctx(
                            SERVER_CTX)) == NULL)
            {
                return ENOMEM;
            }

            shared_buffer_hold(buffer);
            op_buffer_ctx->buffer = buffer;
            op_ctx = &op_buffer_ctx->op_ctx;
        } else {
            op_ctx = &SLICE_OP_CTX;
        }

        op_ctx->info.data_version = buff2long(body_part->data_version);
        if (op_ctx->info.data_version <= 0) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "invalid data version: %"PRId64, op_ctx->info.data_version);
            return EINVAL;
        }
        
        op_ctx->info.body = (char *)(body_part + 1);
        switch (body_part->cmd) {
            case FS_SERVICE_PROTO_SLICE_WRITE_REQ:
                result = du_handler_deal_slice_write(task, op_ctx);
                break;
            case FS_SERVICE_PROTO_SLICE_ALLOCATE_REQ:
                result = du_handler_deal_slice_allocate(task, op_ctx);
                break;
            case FS_SERVICE_PROTO_SLICE_DELETE_REQ:
                result = du_handler_deal_slice_delete(task, op_ctx);
                break;
            case FS_SERVICE_PROTO_BLOCK_DELETE_REQ:
                result = du_handler_deal_block_delete(task, op_ctx);
                break;
            default:
                RESPONSE.error.length = sprintf(RESPONSE.error.message,
                        "unkown cmd: %d", body_part->cmd);
                return EINVAL;
        }

        if (result != TASK_STATUS_CONTINUE) {
            int r;
            r = replication_callee_push_to_rpc_result_queue(
                    REPLICA_REPLICATION, op_ctx->info.data_version, result);
            if (r != 0) {
                return r;
            }
        }
    }

    return 0;
}

static int replica_deal_rpc_req(struct fast_task_info *task)
{
    FSProtoReplicaRPCReqBodyHeader *body_header;
    SharedBuffer *buffer;
    int result;
    int min_body_len;
    int count;

    if ((result=replica_check_replication_task(task)) != 0) {
        return result;
    }

    if ((result=server_check_min_body_length(task,
                    sizeof(FSProtoReplicaRPCReqBodyHeader) +
                    sizeof(FSProtoReplicaRPCReqBodyPart))) != 0)
    {
        return result;
    }

    body_header = (FSProtoReplicaRPCReqBodyHeader *)REQUEST.body;
    count = buff2int(body_header->count);
    if (count <= 0) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "rpc count: %d <= 0", count);
        return EINVAL;
    }

    min_body_len = sizeof(FSProtoReplicaRPCReqBodyHeader) +
        sizeof(FSProtoReplicaRPCReqBodyPart) * count;
    if (REQUEST.header.body_len < min_body_len) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "body length: %d < min length: %d, rpc count: %d",
                REQUEST.header.body_len, min_body_len, count);
        return EINVAL;
    }

    if ((buffer=replication_callee_alloc_shared_buffer(SERVER_CTX)) == NULL) {
        return ENOMEM;
    }

    memcpy(buffer->buff, REQUEST.body, task->length - sizeof(FSProtoHeader));
    result = handle_rpc_req(task, buffer, count);
    shared_buffer_release(buffer);

    return result;
}

static int replica_deal_rpc_resp(struct fast_task_info *task)
{
    int result;
    int count;
    int expect_body_len;
    short err_no;
    uint64_t data_version;
    FSProtoReplicaRPCRespBodyHeader *body_header;
    FSProtoReplicaRPCRespBodyPart *body_part;
    FSProtoReplicaRPCRespBodyPart *bp_end;

    if ((result=replica_check_replication_task(task)) != 0) {
        return result;
    }

    if ((result=server_check_min_body_length(task,
                    sizeof(FSProtoReplicaRPCRespBodyHeader) +
                    sizeof(FSProtoReplicaRPCRespBodyPart))) != 0)
    {
        return result;
    }

    body_header = (FSProtoReplicaRPCRespBodyHeader *)REQUEST.body;
    count = buff2int(body_header->count);

    expect_body_len = sizeof(FSProtoReplicaRPCRespBodyHeader) +
        sizeof(FSProtoReplicaRPCRespBodyPart) * count;
    if (REQUEST.header.body_len != expect_body_len) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "body length: %d != expected: %d, results count: %d",
                REQUEST.header.body_len, expect_body_len, count);
        return EINVAL;
    }

    body_part = (FSProtoReplicaRPCRespBodyPart *)(REQUEST.body +
            sizeof(FSProtoReplicaRPCRespBodyHeader));
    bp_end = body_part + count;
    for (; body_part<bp_end; body_part++) {
        data_version = buff2long(body_part->data_version);
        err_no = buff2short(body_part->err_no);
        if (err_no != 0) {
            result = err_no;
            RESPONSE.error.length = sprintf(
                    RESPONSE.error.message,
                    "replica fail, data_version: %"PRId64
                    ", result: %d", data_version, err_no);
            break;
        }

        //logInfo("push_binlog_resp data_version: %"PRId64", errno: %d", data_version, err_no);

        if ((result=replication_processors_deal_rpc_response(
                        REPLICA_REPLICATION, data_version)) != 0)
        {
            RESPONSE.error.length = sprintf(
                    RESPONSE.error.message,
                    "rpc_result_ring_remove fail, "
                    "data_version: %"PRId64", result: %d",
                    data_version, result);
            break;
        }
    }

    return result;
}

int replica_deal_task(struct fast_task_info *task)
{
    int result;

    /*
    logInfo("file: "__FILE__", line: %d, "
            "cmd: %d, nio_stage: %d, SF_NIO_STAGE_CONTINUE: %d",
            __LINE__, ((FSProtoHeader *)task->data)->cmd,
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
        handler_init_task_context(task);

        switch (REQUEST.header.cmd) {
            case FS_PROTO_ACTIVE_TEST_REQ:
                RESPONSE.header.cmd = FS_PROTO_ACTIVE_TEST_RESP;
                result = replica_deal_actvie_test_req(task);
                break;
            case FS_PROTO_ACTIVE_TEST_RESP:
                result = replica_check_replication_task(task);
                TASK_ARG->context.need_response = false;
                break;
            case FS_PROTO_ACK:
                result = handler_deal_ack(task);
                TASK_ARG->context.need_response = false;
                break;
            case FS_REPLICA_PROTO_JOIN_SERVER_REQ:
                logInfo("file: "__FILE__", line: %d, "
                        "client ip: %s, cmd: %d", __LINE__,
                        task->client_ip, REQUEST.header.cmd);
                result = replica_deal_join_server_req(task);
                break;
            case FS_REPLICA_PROTO_JOIN_SERVER_RESP:
                result = replica_deal_join_server_resp(task);
                TASK_ARG->context.need_response = false;
                break;
            case FS_REPLICA_PROTO_RPC_REQ:
                if ((result=replica_deal_rpc_req(task)) == 0) {
                    TASK_ARG->context.need_response = false;
                }
                break;
            case FS_REPLICA_PROTO_RPC_RESP:
                result = replica_deal_rpc_resp(task);
                TASK_ARG->context.need_response = false;
                break;
            case FS_REPLICA_PROTO_FETCH_BINLOG_FIRST_REQ:
                result = replica_deal_fetch_binlog_first(task);
                break;
            case FS_REPLICA_PROTO_FETCH_BINLOG_NEXT_REQ:
                result = replica_deal_fetch_binlog_next(task);
                break;
            default:
                RESPONSE.error.length = sprintf(RESPONSE.error.message,
                        "unkown cmd: %d", REQUEST.header.cmd);
                result = -EINVAL;
                break;
        }
    }

    if (result == TASK_STATUS_CONTINUE) {
        return 0;
    } else {
        RESPONSE_STATUS = result;
        return handler_deal_task_done(task);
    }
}

void *replica_alloc_thread_extra_data(const int thread_index)
{
    FSServerContext *server_context;

    server_context = (FSServerContext *)fc_malloc(sizeof(FSServerContext));
    if (server_context == NULL) {
        return NULL;
    }
    memset(server_context, 0, sizeof(FSServerContext));

    if (replication_alloc_connection_ptr_arrays(server_context) != 0) {
        return NULL;
    }

    if (replication_callee_init_allocator(server_context) != 0) {
        return NULL;
    }

    return server_context;
}

int replica_thread_loop_callback(struct nio_thread_data *thread_data)
{
    FSServerContext *server_ctx;
    static int count = 0;

    server_ctx = (FSServerContext *)thread_data->arg;

    if (count++ % 100 == 0) {
        logInfo("thread index: %d, connectings.count: %d, "
                "connected.count: %d",
                SF_THREAD_INDEX(REPLICA_SF_CTX, thread_data),
                server_ctx->replica.connectings.count,
                server_ctx->replica.connected.count);
    }

    replication_processor_process(server_ctx);
    return 0;
}
