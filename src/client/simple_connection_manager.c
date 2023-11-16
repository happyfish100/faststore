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

#include <sys/stat.h>
#include <limits.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "sf/idempotency/client/client_channel.h"
#include "client_global.h"
#include "client_func.h"
#include "client_proto.h"
#include "simple_connection_manager.h"

static int connect_done_callback(ConnectionInfo *conn, void *args)
{
    SFConnectionParameters *params;
    int result;

    params = (SFConnectionParameters *)conn->args;
    if (((FSClientContext *)args)->idempotency_enabled) {
        params->channel = idempotency_client_channel_get(conn->comm_type,
                conn->ip_addr, conn->port, ((FSClientContext *)args)->
                common_cfg.connect_timeout, &result);
        if (params->channel == NULL) {
            logError("file: "__FILE__", line: %d, "
                    "server %s:%u, idempotency channel get fail, "
                    "result: %d, error info: %s", __LINE__, conn->ip_addr,
                    conn->port, result, STRERROR(result));
            return result;
        }
    } else {
        params->channel = NULL;
    }

    result = fs_client_proto_join_server((FSClientContext *)args, conn, params);
    if (result == SF_RETRIABLE_ERROR_NO_CHANNEL && params->channel != NULL) {
        idempotency_client_channel_check_reconnect(params->channel);
    }
    return result;
}

static int init_data_group_array(FSClientContext *client_ctx,
        SFConnectionManager *cm)
{
    int result;
    FSDataServerMapping *mapping;
    FSDataServerMapping *end;

    end = client_ctx->cluster_cfg.ptr->data_groups.mappings + 
        client_ctx->cluster_cfg.ptr->data_groups.count;
    for (mapping=client_ctx->cluster_cfg.ptr->data_groups.mappings;
            mapping<end; mapping++)
    {
        if ((result=sf_connection_manager_add(cm, mapping->data_group_id,
                        mapping->server_group->server_array.servers,
                        mapping->server_group->server_array.count)) != 0)
        {
            return result;
        }
    }

    return 0;
}

int fs_simple_connection_manager_init_ex(FSClientContext *client_ctx,
        SFConnectionManager *cm, const int max_count_per_entry,
        const int max_idle_time, const bool bg_thread_enabled)
{
    int dg_count;
    int server_count;
    int result;

    dg_count = FS_DATA_GROUP_COUNT(*client_ctx->cluster_cfg.ptr);
    server_count = FC_SID_SERVER_COUNT(client_ctx->
            cluster_cfg.ptr->server_cfg);
    if ((result=sf_connection_manager_init_ex(cm, "fstore",
                    &client_ctx->common_cfg, dg_count, client_ctx->
                    cluster_cfg.group_index, server_count,
                    max_count_per_entry, max_idle_time,
                    connect_done_callback, client_ctx, &client_ctx->
                    cluster_cfg.ptr->server_cfg, bg_thread_enabled)) != 0)
    {
        return result;
    }

    if ((result=init_data_group_array(client_ctx, cm)) != 0) {
        return result;
    }

    return sf_connection_manager_prepare(cm);
}

void fs_simple_connection_manager_destroy(SFConnectionManager *cm)
{
}
