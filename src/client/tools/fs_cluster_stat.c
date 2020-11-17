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
    fprintf(stderr, "Usage: %s [-c config_filename=/etc/fstore/client.conf] "
            "[-g data_group_id=0]\n", argv[0]);
}

static void output(FSClientClusterStatEntry *stats, const int count)
{
    FSClientClusterStatEntry *stat;
    FSClientClusterStatEntry *end;
    int prev_data_group_id;

    if (count == 0) {
        return;
    }

    prev_data_group_id = 0;
    end = stats + count;
    for (stat=stats; stat<end; stat++) {
        if (stat->data_group_id != prev_data_group_id) {
            printf("\ndata_group_id: %d\n", stat->data_group_id);
            prev_data_group_id = stat->data_group_id;
        }
        printf( "\tserver_id: %d, host: %s:%u, "
                "status: %d (%s), "
                "is_preseted: %d, "
                "is_master: %d, "
                "data_version: %"PRId64"\n",
                stat->server_id,
                stat->ip_addr, stat->port,
                stat->status,
                fs_get_server_status_caption(stat->status),
                stat->is_preseted,
                stat->is_master,
                stat->data_version
              );
    }
    printf("\nserver count: %d\n\n", count);
}

int main(int argc, char *argv[])
{
#define CLUSTER_MAX_STAT_COUNT  256
	int ch;
    const char *config_filename = "/etc/fstore/client.conf";
    int data_group_id;
    int alloc_size;
    int count;
    int bytes;
    FSClientClusterStatEntry fixed_stats[CLUSTER_MAX_STAT_COUNT];
    FSClientClusterStatEntry *stats;
	int result;

    /*
    if (argc < 2) {
        usage(argv);
        return 1;
    }
    */

    data_group_id = 0;
    while ((ch=getopt(argc, argv, "hc:g:")) != -1) {
        switch (ch) {
            case 'h':
                usage(argv);
                return 0;
            case 'c':
                config_filename = optarg;
                break;
            case 'g':
                data_group_id = strtol(optarg, NULL, 10);
                break;
            default:
                usage(argv);
                return 1;
        }
    }

    log_init();
    //g_log_context.log_level = LOG_DEBUG;


    if ((result=fs_client_init(config_filename)) != 0) {
        return result;
    }

    alloc_size = FS_DATA_GROUP_COUNT(*g_fs_client_vars.
            client_ctx.cluster_cfg.ptr) * 5;
    if (alloc_size < CLUSTER_MAX_STAT_COUNT) {
        alloc_size = CLUSTER_MAX_STAT_COUNT;
        stats = fixed_stats;
    } else {
        bytes = sizeof(FSClientClusterStatEntry) * alloc_size;
        stats = (FSClientClusterStatEntry *)fc_malloc(bytes);
        if (stats == NULL) {
            return ENOMEM;
        }
    }

    if ((result=fs_cluster_stat(&g_fs_client_vars.client_ctx,
                    data_group_id, stats, alloc_size, &count)) != 0)
    {
        fprintf(stderr, "fs_cluster_stat fail, "
                "errno: %d, error info: %s\n", result, STRERROR(result));
        return result;
    }

    output(stats, count);
    return 0;
}
