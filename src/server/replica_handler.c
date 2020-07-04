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
#include "server_group_info.h"
#include "replication/replication_processor.h"
#include "replication/replication_producer.h"
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

void replica_task_finish_cleanup(struct fast_task_info *task)
{
    /*
    FSServerTaskArg *task_arg;
    task_arg = (FSServerTaskArg *)task->arg;
    */

    switch (CLUSTER_TASK_TYPE) {
        case  FS_CLUSTER_TASK_TYPE_REPLICATION:
            if (CLUSTER_REPLICA != NULL) {
                replication_processor_unbind(CLUSTER_REPLICA);
                CLUSTER_REPLICA = NULL;
            }
            CLUSTER_TASK_TYPE = FS_CLUSTER_TASK_TYPE_NONE;
            break;
        default:
            break;
    }

    __sync_add_and_fetch(&((FSServerTaskArg *)task->arg)->task_version, 1);
    sf_task_finish_clean_up(task);
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
    if (CLUSTER_TASK_TYPE != FS_CLUSTER_TASK_TYPE_NONE) {
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
    if (!(CLUSTER_TASK_TYPE == FS_CLUSTER_TASK_TYPE_REPLICATION &&
                CLUSTER_REPLICA != NULL))
    {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "unexpect cmd: %d", REQUEST.header.cmd);
        return EINVAL;
    }

    set_replication_stage(CLUSTER_REPLICA, FS_REPLICATION_STAGE_SYNCING);
    return 0;
}

static inline int replica_check_replication_task(struct fast_task_info *task)
{
    if (CLUSTER_TASK_TYPE != FS_CLUSTER_TASK_TYPE_REPLICATION) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "invalid task type: %d != %d", CLUSTER_TASK_TYPE,
                FS_CLUSTER_TASK_TYPE_REPLICATION);
        return EINVAL;
    }

    if (CLUSTER_REPLICA == NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "cluster replication ptr is null");
        return EINVAL;
    }
    return 0;
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
                        CLUSTER_REPLICA, data_version)) != 0)
        {
            RESPONSE.error.length = sprintf(
                    RESPONSE.error.message,
                    "push_result_ring_remove fail, "
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

    logInfo("file: "__FILE__", line: %d, "
            "cmd: %d, nio_stage: %d, SF_NIO_STAGE_CONTINUE: %d",
            __LINE__, ((FSProtoHeader *)task->data)->cmd,
            task->nio_stage, SF_NIO_STAGE_CONTINUE);

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
                result = handler_deal_actvie_test(task);
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

                logInfo("file: "__FILE__", line: %d, "
                        "client ip: %s, cmd: %d, result: %d", __LINE__,
                        task->client_ip, REQUEST.header.cmd, result);
                break;
            case FS_REPLICA_PROTO_JOIN_SERVER_RESP:
                result = replica_deal_join_server_resp(task);
                TASK_ARG->context.need_response = false;
                break;
            case FS_REPLICA_PROTO_SLICE_WRITE:
                result = du_handler_deal_slice_write(task,
                        FS_WHICH_SIDE_SLAVE);
                break;
            case FS_REPLICA_PROTO_SLICE_ALLOCATE:
                result = du_handler_deal_slice_allocate(task,
                        FS_WHICH_SIDE_SLAVE);
                break;
            case FS_REPLICA_PROTO_SLICE_DELETE:
                result = du_handler_deal_slice_delete(task,
                        FS_WHICH_SIDE_SLAVE);
                break;
            case FS_REPLICA_PROTO_BLOCK_DELETE:
                result = du_handler_deal_block_delete(task,
                        FS_WHICH_SIDE_SLAVE);
                break;
            case FS_REPLICA_PROTO_RPC_RESP:
                result = replica_deal_rpc_resp(task);
                TASK_ARG->context.need_response = false;
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

static int alloc_replication_ptr_array(FSReplicationPtrArray *array)
{
    int bytes;

    bytes = sizeof(FSReplication *) * fs_get_replication_count();
    array->replications = (FSReplication **)malloc(bytes);
    if (array->replications == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(array->replications, 0, bytes);
    return 0;
}

void *replica_alloc_thread_extra_data(const int thread_index)
{
    FSServerContext *server_context;

    server_context = (FSServerContext *)malloc(sizeof(FSServerContext));
    if (server_context == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail, errno: %d, error info: %s",
                __LINE__, (int)sizeof(FSServerContext),
                errno, strerror(errno));
        return NULL;
    }
    memset(server_context, 0, sizeof(FSServerContext));

    if (alloc_replication_ptr_array(&server_context->
                cluster.connectings) != 0)
    {
        return NULL;
    }
    if (alloc_replication_ptr_array(&server_context->
                cluster.connected) != 0)
    {
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
                server_ctx->cluster.connectings.count,
                server_ctx->cluster.connected.count);
    }

    replication_processor_process(server_ctx);
    return 0;
}
