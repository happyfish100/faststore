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
#include "sf/sf_func.h"
#include "sf/sf_nio.h"
#include "sf/sf_global.h"
#include "sf/sf_util.h"
#include "common/fs_proto.h"
#include "server_global.h"
#include "common_handler.h"

#define LOG_LEVEL_FOR_DEBUG  LOG_DEBUG

static int fs_get_cmd_log_level(const int cmd)
{
    switch (cmd) {
        case SF_PROTO_ACTIVE_TEST_REQ:
        case SF_PROTO_ACTIVE_TEST_RESP:
        case FS_CLUSTER_PROTO_PING_LEADER_REQ:
            return LOG_NOTHING;
        case SF_SERVICE_PROTO_REPORT_REQ_RECEIPT_REQ:
            return LOG_DEBUG;
        default:
            return LOG_LEVEL_FOR_DEBUG;
    }
}

void common_handler_init()
{
    SFHandlerContext handler_ctx;

    fs_proto_init();

    handler_ctx.slow_log = &SLOW_LOG;
    handler_ctx.callbacks.get_cmd_caption = fs_get_cmd_caption;
    if (FC_LOG_BY_LEVEL(LOG_LEVEL_FOR_DEBUG)) {
        handler_ctx.callbacks.get_cmd_log_level = fs_get_cmd_log_level;
    } else {
        handler_ctx.callbacks.get_cmd_log_level = NULL;
    }
    sf_proto_set_handler_context(&handler_ctx);
}

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
