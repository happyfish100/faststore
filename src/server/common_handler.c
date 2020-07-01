//common_handler.c

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

static int handler_check_config_sign(struct fast_task_info *task,
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

int handler_check_config_signs(struct fast_task_info *task,
        const int server_id, FSProtoConfigSigns *config_signs)
{
    int result;
    if ((result=handler_check_config_sign(task, server_id,
                    config_signs->servers, SERVERS_CONFIG_SIGN_BUF,
                    SERVERS_CONFIG_SIGN_LEN, "servers")) != 0)
    {
        return result;
    }

    if ((result=handler_check_config_sign(task, server_id,
                    config_signs->cluster, CLUSTER_CONFIG_SIGN_BUF,
                    CLUSTER_CONFIG_SIGN_LEN, "cluster")) != 0)
    {
        return result;
    }

    return 0;
}

int handler_deal_ack(struct fast_task_info *task)
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

int handler_deal_task_done(struct fast_task_info *task)
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
