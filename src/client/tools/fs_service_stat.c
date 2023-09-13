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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "fastcommon/logger.h"
#include "faststore/client/fs_client.h"

static void usage(char *argv[])
{
    fprintf(stderr, "Usage: %s [-c config_filename=%s] "
            "[-s server_id] [-G server_group_id=0] [-g data_group_id=0] "
            "host[:port]|all\n", argv[0], FS_CLIENT_DEFAULT_CONFIG_FILENAME);
}

static const char *get_server_group_ids(const int server_id,
        char *server_group_ids)
{
#define MAX_SERVER_GROUPS 8
    int count;
    int len;
    int i;
    FSServerGroup *server_groups[MAX_SERVER_GROUPS];
    char *p;

    count = fs_cluster_cfg_get_my_server_groups(g_fs_client_vars.client_ctx.
            cluster_cfg.ptr, server_id, server_groups, MAX_SERVER_GROUPS);

    p = server_group_ids;
    for (i=0; i<count; i++) {
        if (i > 0) {
            *p++ = ',';
        }
        len = sprintf(p, "%d", server_groups[i]->server_group_id);
        p += len;
    }
    *p = '\0';
    return server_group_ids;
}

static void output(const ConnectionInfo *conn,
        const FSClientServiceStat *stat)
{
    double avg_slices;
    char server_group_ids[64];
    char storage_engine_buff[128];
    int len;

    if (stat->data.ob.total_count > 0) {
        avg_slices = (double)stat->data.slice.total_count /
            (double)stat->data.ob.total_count;
    } else {
        avg_slices = 0.00;
    }

    len = sprintf(storage_engine_buff, "enabled: %s", stat->
            storage_engine.enabled ? "true" : "false");
    if (stat->storage_engine.enabled) {
        sprintf(storage_engine_buff + len, ", current_version: %"PRId64,
                stat->storage_engine.current_version);
    }

    printf( "\tserver_id: %d\n"
            "\thost: %s:%u\n"
            "\tversion: %.*s\n"
            "\tis_leader: %s\n"
            "\tauth_enabled: %s\n"
            "\tstorage_engine: {%s}\n"
            "\tserver_group_id: %s\n"
            "\tconnection : {current: %d, max: %d}\n"
            "\tbinlog : {current_version: %"PRId64", "
            "writer: {next_version: %"PRId64", \n"
            "\t\t  total_count: %"PRId64", "
            "waiting_count: %d, max_waitings: %d}}\n"
            "\tdata : {ob : {total_count: %"PRId64", "
            "cached_count: %"PRId64", "
            "element_used: %"PRId64"},\n"
            "\t\tslice : {total_count: %"PRId64", "
            "cached_count: %"PRId64", "
            "element_used: %"PRId64"},\n"
            "\t\tavg slices/OB: %.2f}\n\n",
            stat->server_id, conn->ip_addr, conn->port,
            stat->version.len, stat->version.str,
            (stat->is_leader ?  "true" : "false"),
            (stat->auth_enabled ?  "true" : "false"),
            storage_engine_buff, get_server_group_ids(
                    stat->server_id, server_group_ids),
            stat->connection.current_count,
            stat->connection.max_count,
            stat->binlog.current_version,
            stat->binlog.writer.next_version,
            stat->binlog.writer.total_count,
            stat->binlog.writer.waiting_count,
            stat->binlog.writer.max_waitings,
            stat->data.ob.total_count,
            stat->data.ob.cached_count,
            stat->data.ob.element_used,
            stat->data.slice.total_count,
            stat->data.slice.cached_count,
            stat->data.slice.element_used,
            avg_slices);
}

int main(int argc, char *argv[])
{
#define EMPTY_POOL_NAME SF_G_EMPTY_STRING
#define MAX_CONNECTIONS  64

    const bool publish = false;
    const char *config_filename = FS_CLIENT_DEFAULT_CONFIG_FILENAME;
	int ch;
    int server_id;
    int server_group_id;
    int data_group_id;
    int count;
    int i;
	int result;
    char *host;
    FCServerInfo *server;
    FCAddressPtrArray *addr_parray;
    struct {
        ConnectionInfo **ptr;
        ConnectionInfo *holder[MAX_CONNECTIONS];
    } connections;
    ConnectionInfo conn;
    FSClientServiceStat stat;

    if (argc < 2) {
        usage(argv);
        return 1;
    }

    log_init();
    //g_log_context.log_level = LOG_DEBUG;

    server_id = 0;
    server_group_id = 0;
    data_group_id = 0;
    while ((ch=getopt(argc, argv, "hc:s:G:g:")) != -1) {
        switch (ch) {
            case 'h':
                usage(argv);
                return 0;
            case 'c':
                config_filename = optarg;
                break;
            case 's':
                server_id = strtol(optarg, NULL, 10);
                break;
            case 'G':
                server_group_id = strtol(optarg, NULL, 10);
                break;
            case 'g':
                data_group_id = strtol(optarg, NULL, 10);
                break;
            default:
                usage(argv);
                return 1;
        }
    }

    if ((result=fs_client_init_with_auth_ex1(&g_fs_client_vars.client_ctx,
                    &g_fcfs_auth_client_vars.client_ctx, config_filename,
                    NULL, NULL, false, &EMPTY_POOL_NAME, publish)) != 0)
    {
        return result;
    }

    connections.ptr = connections.holder;
    count = 0;
    if (server_id > 0) {
        if ((server=fc_server_get_by_id(&g_fs_client_vars.client_ctx.
                        cluster_cfg.ptr->server_cfg, server_id)) == NULL)
        {
            logError("file: "__FILE__", line: %d, "
                    "server id: %d not exist",
                    __LINE__, server_id);
            return ENOENT;
        }

        addr_parray = &FS_CFG_SERVICE_ADDRESS_ARRAY(
                &g_fs_client_vars.client_ctx, server);
        connections.ptr[count++] = &addr_parray->addrs[0]->conn;
    } else if (server_group_id > 0) {
        const FSServerGroup *server_group;

        if ((server_group=fs_cluster_cfg_get_server_group_by_id(
                        g_fs_client_vars.client_ctx.cluster_cfg.ptr,
                        server_group_id)) == NULL)
        {
            logError("file: "__FILE__", line: %d, "
                    "server group id: %d not exist",
                    __LINE__, server_group_id);
            return ENOENT;
        }

        for (i=0; i<server_group->server_array.count; i++) {
            addr_parray = &FS_CFG_SERVICE_ADDRESS_ARRAY(&g_fs_client_vars.
                    client_ctx, server_group->server_array.servers[i]);
            connections.ptr[count++] = &addr_parray->addrs[0]->conn;
        }
    } else {
        if (optind >= argc) {
            usage(argv);
            return 1;
        }
        host = argv[optind];

        if (strcmp(host, "all") == 0) {
            FCServerInfoArray *server_array;

            server_array = &g_fs_client_vars.client_ctx.cluster_cfg.
                ptr->server_cfg.sorted_server_arrays.by_id;
            if (server_array->count <= MAX_CONNECTIONS) {
                connections.ptr = connections.holder;
            } else {
                if ((connections.ptr=fc_malloc(sizeof(ConnectionInfo *) *
                                server_array->count)) == NULL)
                {
                    return ENOMEM;
                }
            }
            for (i=0; i<server_array->count; i++) {
                addr_parray = &FS_CFG_SERVICE_ADDRESS_ARRAY(&g_fs_client_vars.
                        client_ctx, server_array->servers + i);
                connections.ptr[count++] = &addr_parray->addrs[0]->conn;
            }
        } else {
            FCServerGroupInfo *server_group;
            if ((result=conn_pool_parse_server_info(host, &conn,
                            FS_SERVER_DEFAULT_SERVICE_PORT)) != 0)
            {
                return result;
            }

            server_group = fc_server_get_group_by_index(
                    &FS_CLUSTER_SERVER_CFG(&g_fs_client_vars.client_ctx),
                    FS_CFG_SERVICE_INDEX(&g_fs_client_vars.client_ctx));
            conn.comm_type = server_group->comm_type;
            connections.ptr[count++] = &conn;
        }
    }

    for (i=0; i<count; i++) {
        if ((result=fs_client_proto_service_stat(&g_fs_client_vars.client_ctx,
                        connections.ptr[i], data_group_id, &stat)) == 0)
        {
            output(connections.ptr[i], &stat);
        }
    }

    return result;
}
