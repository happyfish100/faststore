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

static bool include_block_space = false;

static void usage(char *argv[])
{
    fprintf(stderr, "Usage: %s [options] [host[:port]|all]\n"
            "    options:\n"
            "\t-c <client config filename>: default %s\n"
            "\t-s <server id>\n"
            "\t-G <server group id>\n"
            "\t-g <data group id>\n"
            "\t-I: used space include spaces occupied by "
            "blocks when storage engine enabled\n"
            "\t-h: help for usage\n\n"
            "    eg: %s -c %s all\n\n",
            argv[0], FS_CLIENT_DEFAULT_CONFIG_FILENAME,
            argv[0], FS_CLIENT_DEFAULT_CONFIG_FILENAME);
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

static void format_space_buff(const FSClientSpaceInfo *space,
        char *space_buff)
{
    SFSpaceStat ss;
    struct {
        char total[32];
        char used[32];
        char avail[32];
    } space_buffs, trunk_buffs;
    double space_used_ratio;
    double trunk_used_ratio;
    string_t padding_strings[6];
    char tmp_buff[32];
    int unit_value;
    int max_len;
    int padding_len;
    int i;
    char *unit_caption;

    if (space->trunk.used > 0 && space->trunk.used < 1024 * 1024 * 1024) {
        unit_value = 1024 * 1024;
        unit_caption = "MB";
    } else {
        unit_value = 1024 * 1024 * 1024;
        unit_caption = "GB";
    }
    ss.total = space->trunk.total + space->disk_avail;
    ss.used = space->trunk.used + space->block_used_space;
    if (ss.total <= 0) {
        ss.total = 0;
        space_used_ratio = 0.00;
    } else {
        space_used_ratio = (double)ss.used / (double)ss.total;
    }
    ss.avail = ss.total - ss.used;
    if (ss.avail < 0) {
        ss.avail = 0;
    }
    long_to_comma_str(ss.total / unit_value, space_buffs.total);
    long_to_comma_str(ss.used / unit_value, space_buffs.used);
    long_to_comma_str(ss.avail / unit_value, space_buffs.avail);

    long_to_comma_str(space->trunk.total / unit_value, trunk_buffs.total);
    long_to_comma_str(space->trunk.used / unit_value, trunk_buffs.used);
    long_to_comma_str((space->trunk.total - space->trunk.used) /
            unit_value, trunk_buffs.avail);
    if (space->trunk.total <= 0) {
        trunk_used_ratio = 0.00;
    } else {
        trunk_used_ratio = (double)space->trunk.used /
            (double)space->trunk.total;
    }

    FC_SET_STRING(padding_strings[0], space_buffs.total);
    FC_SET_STRING(padding_strings[1], space_buffs.used);
    FC_SET_STRING(padding_strings[2], space_buffs.avail);
    FC_SET_STRING(padding_strings[3], trunk_buffs.total);
    FC_SET_STRING(padding_strings[4], trunk_buffs.used);
    FC_SET_STRING(padding_strings[5], trunk_buffs.avail);
    max_len = padding_strings[0].len;
    for (i=1; i<6; i++) {
        if (padding_strings[i].len > max_len) {
            max_len = padding_strings[i].len;
        }
    }
    for (i=0; i<6; i++) {
        padding_len = max_len - padding_strings[i].len;
        if (padding_len > 0) {
            memcpy(tmp_buff, padding_strings[i].str,
                    padding_strings[i].len + 1);
            memset(padding_strings[i].str, ' ', padding_len);
            memcpy(padding_strings[i].str + padding_len,
                    tmp_buff, padding_strings[i].len + 1);
        }
    }

    sprintf(space_buff, "\t\tspace summary: {total: %s %s, used: %s %s "
            "(%.2f%%), avail: %s %s},\n\t\t  trunk space: {total: %s %s, "
            "used: %s %s (%.2f%%), avail: %s %s}", space_buffs.total,
            unit_caption, space_buffs.used, unit_caption,
            space_used_ratio * 100.00, space_buffs.avail,
            unit_caption, trunk_buffs.total, unit_caption,
            trunk_buffs.used, unit_caption, trunk_used_ratio * 100.00,
            trunk_buffs.avail, unit_caption);
}

static void output(const ConnectionInfo *conn,
        const FSClientServiceStat *stat)
{
    double avg_slices;
    char server_group_ids[64];
    char storage_engine_buff[256];
    char space_buff[256];
    char up_time_buff[32];
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
        format_space_buff(&stat->storage_engine.space, space_buff);
        sprintf(storage_engine_buff + len, ", data version {current: "
                "%"PRId64", delay: %"PRId64"},\n%s", stat->storage_engine.
                current_version, stat->storage_engine.version_delay,
                space_buff);
    }

    formatDatetime(stat->up_time, "%Y-%m-%d %H:%M:%S",
            up_time_buff, sizeof(up_time_buff));
    format_space_buff(&stat->space, space_buff);

    printf( "\tserver_id: %d\n"
            "\thost: %s:%u\n"
            "\tversion: %.*s\n"
            "\tis_leader: %s\n"
            "\tauth_enabled: %s\n"
            "\tslice storage engine: {%s}\n"
            "\tup_time: %s\n"
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
            "\t\tavg slices/OB: %.2f}\n"
            "%s\n\n",
            stat->server_id, conn->ip_addr, conn->port,
            stat->version.len, stat->version.str,
            (stat->is_leader ?  "true" : "false"),
            (stat->auth_enabled ?  "true" : "false"),
            storage_engine_buff, up_time_buff,
            get_server_group_ids(stat->server_id, server_group_ids),
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
            avg_slices, space_buff);
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
    while ((ch=getopt(argc, argv, "hc:s:G:g:I")) != -1) {
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
            case 'I':
                include_block_space = true;
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
                        connections.ptr[i], data_group_id, include_block_space,
                        &stat)) == 0)
        {
            output(connections.ptr[i], &stat);
        }
    }

    return result;
}
