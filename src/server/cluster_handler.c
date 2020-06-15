//cluster_handler.c

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
#include "replication/binlog_replication.h"
#include "replication/binlog_local_consumer.h"
#include "cluster_relationship.h"
#include "cluster_handler.h"

int cluster_handler_init()
{
    return 0;
}

int cluster_handler_destroy()
{   
    return 0;
}

void cluster_task_finish_cleanup(struct fast_task_info *task)
{
    /*
    FSServerTaskArg *task_arg;

    task_arg = (FSServerTaskArg *)task->arg;
    */

    switch (CLUSTER_TASK_TYPE) {
        case FS_CLUSTER_TASK_TYPE_RELATIONSHIP:
            if (CLUSTER_PEER != NULL) {
                CLUSTER_PEER = NULL;
            }
            CLUSTER_TASK_TYPE = FS_CLUSTER_TASK_TYPE_NONE;
            break;
        case  FS_CLUSTER_TASK_TYPE_REPLICATION:
            if (CLUSTER_REPLICA != NULL) {
                binlog_replication_unbind(CLUSTER_REPLICA);
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

static int cluster_deal_actvie_test(struct fast_task_info *task)
{
    return server_expect_body_length(task, 0);
}

static int cluster_check_config_sign(struct fast_task_info *task,
        const int server_id, const unsigned char *config_sign,
        const unsigned char *my_sign, const int sign_len,
        const char *caption)
{
    if (memcmp(config_sign, my_sign, sign_len) != 0) {
        char peer_hex[2 * CLUSTER_CONFIG_SIGN_LEN + 1];
        char my_hex[2 * CLUSTER_CONFIG_SIGN_LEN + 1];

        bin2hex((const char *)config_sign, sign_len, peer_hex);
        bin2hex((const char *)my_sign, sign_len, my_hex);

        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "server #%d 's %s config md5: %s != mine: %s",
                server_id, caption, peer_hex, my_hex);
        return EFAULT;
    }

    return 0;
}

static int cluster_check_config_signs(struct fast_task_info *task,
        const int server_id, FSProtoConfigSigns *config_signs)
{
    int result;
    if ((result=cluster_check_config_sign(task, server_id,
                    config_signs->servers, SERVERS_CONFIG_SIGN_BUF,
                    SERVERS_CONFIG_SIGN_LEN, "servers")) != 0)
    {
        return result;
    }

    if ((result=cluster_check_config_sign(task, server_id,
                    config_signs->cluster, CLUSTER_CONFIG_SIGN_BUF,
                    CLUSTER_CONFIG_SIGN_LEN, "cluster")) != 0)
    {
        return result;
    }

    return 0;
}

static int cluster_deal_get_server_status(struct fast_task_info *task)
{
    int result;
    int server_id;
    FSProtoGetServerStatusReq *req;
    FSProtoGetServerStatusResp *resp;

    if ((result=server_expect_body_length(task,
                    sizeof(FSProtoGetServerStatusReq))) != 0)
    {
        return result;
    }

    req = (FSProtoGetServerStatusReq *)REQUEST.body;
    server_id = buff2int(req->server_id);
    if ((result=cluster_check_config_signs(task, server_id,
                    &req->config_signs)) != 0)
    {
        return result;
    }

    resp = (FSProtoGetServerStatusResp *)REQUEST.body;

    resp->is_leader = MYSELF_IS_LEADER;
    int2buff(CLUSTER_MY_SERVER_ID, resp->server_id);
    int2buff(g_sf_global_vars.up_time, resp->up_time);
    int2buff(fs_get_last_shutdown_time(), resp->last_shutdown_time);
    long2buff(CLUSTER_CURRENT_VERSION, resp->version);

    RESPONSE.header.body_len = sizeof(FSProtoGetServerStatusResp);
    RESPONSE.header.cmd = FS_CLUSTER_PROTO_GET_SERVER_STATUS_RESP;
    TASK_ARG->context.response_done = true;
    return 0;
}

static int cluster_deal_join_leader(struct fast_task_info *task)
{
    int result;
    int server_id;
    FSProtoJoinLeaderReq *req;
    FSClusterServerInfo *peer;

    if ((result=server_expect_body_length(task,
                    sizeof(FSProtoJoinLeaderReq))) != 0)
    {
        return result;
    }

    req = (FSProtoJoinLeaderReq *)REQUEST.body;
    server_id = buff2int(req->server_id);
    peer = fs_get_server_by_id(server_id);
    if (peer == NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "peer server id: %d not exist", server_id);
        return ENOENT;
    }

    if ((result=cluster_check_config_signs(task, server_id,
                    &req->config_signs)) != 0)
    {
        return result;
    }

    if (CLUSTER_MYSELF_PTR != CLUSTER_LEADER_PTR) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am not leader");
        return EINVAL;
    }

    if (CLUSTER_PEER != NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "peer server id: %d already joined", server_id);
        return EEXIST;
    }

    RESPONSE.header.cmd = FS_CLUSTER_PROTO_JOIN_LEADER_RESP;
    CLUSTER_TASK_TYPE = FS_CLUSTER_TASK_TYPE_RELATIONSHIP;
    CLUSTER_PEER = peer;
    return 0;
}

static int cluster_deal_ping_leader(struct fast_task_info *task)
{
    int result;
    FSProtoPingLeaderRespHeader *resp_header;
    FSProtoPingLeaderRespBodyPart *body_part;
    FSClusterServerInfo *cs;
    FSClusterServerInfo *end;

    if ((result=server_expect_body_length(task, 0)) != 0) {
        return result;
    }

    if (CLUSTER_PEER == NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "please join first");
        return EINVAL;
    }

    if (CLUSTER_MYSELF_PTR != CLUSTER_LEADER_PTR) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am not leader");
        return EINVAL;
    }

    resp_header = (FSProtoPingLeaderRespHeader *)REQUEST.body;
    body_part = (FSProtoPingLeaderRespBodyPart *)(REQUEST.body +
            sizeof(FSProtoPingLeaderRespHeader));
    /*
    if (CLUSTER_PEER->last_change_version < CLUSTER_SERVER_ARRAY.change_version) {
        CLUSTER_PEER->last_change_version = CLUSTER_SERVER_ARRAY.change_version;
        int2buff(CLUSTER_SERVER_ARRAY.count, resp_header->server_count);

        end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
        for (cs=CLUSTER_SERVER_ARRAY.servers; cs<end; cs++, body_part++) {
            int2buff(cs->server->id, body_part->server_id);
            body_part->status = cs->status;
        }
    } else {
        int2buff(0, resp_header->server_count);
    }
    */
    int2buff(0, resp_header->server_count);

    TASK_ARG->context.response_done = true;
    RESPONSE.header.cmd = FS_CLUSTER_PROTO_PING_LEADER_RESP;
    RESPONSE.header.body_len = (char *)body_part - REQUEST.body;
    return 0;
}

static int cluster_deal_next_leader(struct fast_task_info *task)
{
    int result;
    int leader_id;
    FSClusterServerInfo *leader;

    if ((result=server_expect_body_length(task, 4)) != 0) {
        return result;
    }

    if (CLUSTER_MYSELF_PTR == CLUSTER_LEADER_PTR) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "i am already leader");
        return EEXIST;
    }

    leader_id = buff2int(REQUEST.body);
    leader = fs_get_server_by_id(leader_id);
    if (leader == NULL) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "leader server id: %d not exist", leader_id);
        return ENOENT;
    }

    if (REQUEST.header.cmd == FS_CLUSTER_PROTO_PRE_SET_NEXT_LEADER) {
        return cluster_relationship_pre_set_leader(leader);
    } else {
        return cluster_relationship_commit_leader(leader);
    }
}

static int cluster_deal_join_server_req(struct fast_task_info *task)
{
    int result;
    int server_id;
    int buffer_size;
    int replica_channels_between_two_servers;
    FSProtoJoinServerReq *req;
    FSClusterServerInfo *peer;
    FSProtoJoinServerResp *resp;
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

    if ((result=cluster_check_config_signs(task, server_id,
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

    binlog_replication_bind_task(replication, task);
    RESPONSE.header.cmd = FS_REPLICA_PROTO_JOIN_SERVER_RESP;
    return 0;
}

static int cluster_deal_join_server_resp(struct fast_task_info *task)
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

static int cluster_deal_ack(struct fast_task_info *task)
{
    if (REQUEST_STATUS != 0) {
        if (REQUEST.header.body_len > 0) {
            int remain_size;
            int len;

            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "message from peer %s:%u => ",
                    task->client_ip, task->port);
            remain_size = sizeof(RESPONSE.error.message) -
                RESPONSE.error.length;
            if (REQUEST.header.body_len >= remain_size) {
                len = remain_size - 1;
            } else {
                len = REQUEST.header.body_len;
            }

            memcpy(RESPONSE.error.message + RESPONSE.error.length,
                    REQUEST.body, len);
            RESPONSE.error.length += len;
            *(RESPONSE.error.message + RESPONSE.error.length) = '\0';
        }

        return REQUEST_STATUS;
    }

    if (REQUEST.header.body_len > 0) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "ACK body length: %d != 0",
                REQUEST.header.body_len);
        return -EINVAL;
    }

    return 0;
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
    TASK_ARG->context.need_response = true;

    REQUEST.header.cmd = ((FSProtoHeader *)task->data)->cmd;
    REQUEST.header.body_len = task->length - sizeof(FSProtoHeader);
    REQUEST.header.status = buff2short(((FSProtoHeader *)task->data)->status);
    REQUEST.body = task->data + sizeof(FSProtoHeader);
}

static int deal_task_done(struct fast_task_info *task)
{
    FSProtoHeader *proto_header;
    int r;
    int time_used;

    if (TASK_ARG->context.log_error && RESPONSE.error.length > 0) {
        logError("file: "__FILE__", line: %d, "
                "peer %s:%u, cmd: %d (%s), req body length: %d, %s",
                __LINE__, task->client_ip, task->port, REQUEST.header.cmd,
                fs_get_cmd_caption(REQUEST.header.cmd),
                REQUEST.header.body_len, RESPONSE.error.message);
    }

    if (!TASK_ARG->context.need_response) {
        if (RESPONSE_STATUS == 0) {
            task->offset = task->length = 0;
            return sf_set_read_event(task);
        }
        return RESPONSE_STATUS > 0 ? -1 * RESPONSE_STATUS : RESPONSE_STATUS;
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
    if (time_used > 50 * 1000) {
        lwarning("process a request timed used: %d us, "
                "cmd: %d (%s), req body len: %d, resp body len: %d",
                time_used, REQUEST.header.cmd,
                fs_get_cmd_caption(REQUEST.header.cmd),
                REQUEST.header.body_len,
                RESPONSE.header.body_len);
    }

    if (REQUEST.header.cmd != FS_CLUSTER_PROTO_PING_LEADER_REQ) {
    logInfo("file: "__FILE__", line: %d, "
            "client ip: %s, req cmd: %d (%s), req body_len: %d, "
            "resp cmd: %d (%s), status: %d, resp body_len: %d, "
            "time used: %d us", __LINE__,
            task->client_ip, REQUEST.header.cmd,
            fs_get_cmd_caption(REQUEST.header.cmd),
            REQUEST.header.body_len, RESPONSE.header.cmd,
            fs_get_cmd_caption(RESPONSE.header.cmd),
            RESPONSE_STATUS, RESPONSE.header.body_len, time_used);
    }

    return r == 0 ? RESPONSE_STATUS : r;
}

int cluster_deal_task(struct fast_task_info *task)
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
                result = cluster_deal_actvie_test(task);
                break;
            case FS_PROTO_ACK:
                result = cluster_deal_ack(task);
                TASK_ARG->context.need_response = false;
                break;
            case FS_CLUSTER_PROTO_GET_SERVER_STATUS_REQ:
                result = cluster_deal_get_server_status(task);
                break;
            case FS_CLUSTER_PROTO_PRE_SET_NEXT_LEADER:
            case FS_CLUSTER_PROTO_COMMIT_NEXT_LEADER:
                result = cluster_deal_next_leader(task);
                break;
            case FS_CLUSTER_PROTO_JOIN_LEADER_REQ:
                result = cluster_deal_join_leader(task);
                break;
            case FS_CLUSTER_PROTO_PING_LEADER_REQ:
                result = cluster_deal_ping_leader(task);
                break;
            case FS_REPLICA_PROTO_JOIN_SERVER_REQ:
                result = cluster_deal_join_server_req(task);
                break;
            case FS_REPLICA_PROTO_JOIN_SERVER_RESP:
                result = cluster_deal_join_server_resp(task);
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
        return deal_task_done(task);
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

void *cluster_alloc_thread_extra_data(const int thread_index)
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

int cluster_thread_loop_callback(struct nio_thread_data *thread_data)
{
    FSServerContext *server_ctx;
    static int count = 0;

    server_ctx = (FSServerContext *)thread_data->arg;

    if (count++ % 1000 == 0) {
        logInfo("thread index: %d, connectings.count: %d, connected.count: %d",
                SF_THREAD_INDEX(CLUSTER_SF_CTX, thread_data),
                server_ctx->cluster.connectings.count,
                server_ctx->cluster.connected.count);
    }

    binlog_replication_process(server_ctx);
    return 0;
}
