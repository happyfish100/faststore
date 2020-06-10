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
#include "replication/binlog_replication.h"
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

    /*
    switch (CLUSTER_TASK_TYPE) {
        case FS_CLUSTER_TASK_TYPE_RELATIONSHIP:
            if (CLUSTER_PEER != NULL) {
                CLUSTER_PEER = NULL;
            }
            CLUSTER_TASK_TYPE = FS_CLUSTER_TASK_TYPE_NONE;
            break;
        case  FS_CLUSTER_TASK_TYPE_REPLICA_MASTER:
            if (CLUSTER_REPLICA != NULL) {
                binlog_replication_rebind_thread(CLUSTER_REPLICA);
                CLUSTER_REPLICA = NULL;
            }
            CLUSTER_TASK_TYPE = FS_CLUSTER_TASK_TYPE_NONE;
            break;
        case FS_CLUSTER_TASK_TYPE_REPLICA_SLAVE:
            if (CLUSTER_CONSUMER_CTX != NULL) {
                replica_consumer_thread_terminate(CLUSTER_CONSUMER_CTX);
                CLUSTER_CONSUMER_CTX = NULL;
                ((FSServerContext *)task->thread_data->arg)->
                    cluster.consumer_ctx = NULL;
            }
            CLUSTER_TASK_TYPE = FS_CLUSTER_TASK_TYPE_NONE;
            break;
        default:
            break;
    }
    */

    __sync_add_and_fetch(&((FSServerTaskArg *)task->arg)->task_version, 1);
    sf_task_finish_clean_up(task);
}

static int cluster_deal_actvie_test(struct fast_task_info *task)
{
    return server_expect_body_length(task, 0);
}

static int cluster_deal_slave_ack(struct fast_task_info *task)
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

    if (REQUEST.header.cmd != FS_CLUSTER_PROTO_PING_MASTER_REQ) {
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
                result = cluster_deal_slave_ack(task);
                TASK_ARG->context.need_response = false;
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
    return server_context;
}

int cluster_thread_loop_callback(struct nio_thread_data *thread_data)
{
    FSServerContext *server_ctx;
    static int count = 0;

    server_ctx = (FSServerContext *)thread_data->arg;

    if (count++ % 10000 == 0) {
        logInfo("connectings.count: %d, connected.count: %d",
                server_ctx->cluster.connectings.count,
                server_ctx->cluster.connected.count);
    }

    binlog_replication_process(server_ctx);
    return 0;
}
