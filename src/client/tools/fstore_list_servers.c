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
#include "sf/sf_cluster_cfg.h"
#include "../client_types.h"

static void usage(char *argv[])
{
    fprintf(stderr, "\nUsage: %s <cluster_config_filename>\n", argv[0]);
}

int main(int argc, char *argv[])
{
    const bool calc_sign = false;
    const char *config_filename;
    FSClusterConfig cluster;
    FSServerGroup *server_group;
    FSServerGroup *sgend;
    FCServerInfo **server;
    FCServerInfo **end;
    FCAddressInfo **addrs;
    int i;
    int k;
    int result;

    if (argc < 2) {
        usage(argv);
        return EINVAL;
    }

    config_filename = argv[1];
    log_init();

    if ((result=fs_cluster_cfg_load(&cluster, config_filename,
                    calc_sign)) != 0)
    {
        return result;
    }

    printf("fstore_group_count=%d\n", FS_SERVER_GROUP_COUNT(cluster));

    sgend = cluster.server_groups.groups + cluster.server_groups.count;
    for (server_group=cluster.server_groups.groups, i=0;
            server_group<sgend; server_group++, i++)
    {
        printf("fstore_group_%d=(", i + 1);
        end = server_group->server_array.servers +
            server_group->server_array.count;
        for (server=server_group->server_array.servers, k=0;
                server<end; server++, k++)
        {
            addrs = (*server)->group_addrs[cluster.cluster_group_index].
                address_array.addrs;
            if (k > 0) {
                printf(" ");
            }
            printf("%s", (*addrs)->conn.ip_addr);
        }
        printf(")\n");
    }

    return 0;
}
