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

//server_group_info.h

#ifndef _SERVER_GROUP_INFO_H_
#define _SERVER_GROUP_INFO_H_

#include <time.h>
#include <pthread.h>
#include "server_global.h"

#ifdef __cplusplus
extern "C" {
#endif

int server_group_info_init(const char *cluster_config_filename);
int server_group_info_destroy();

FSClusterServerInfo *fs_get_server_by_id(const int server_id);

FSClusterDataServerInfo *fs_get_data_server_ex(
        FSClusterDataGroupInfo *group, const int server_id);

int fs_downgrade_data_server_status(const int old_status, int *new_status);

int fs_get_server_pair_base_offset(const int server_id1, const int server_id2);

int server_group_info_setup_sync_to_file_task();

static inline FSClusterDataGroupInfo *fs_get_data_group(const int data_group_id)
{
    int index;

    index = data_group_id - CLUSTER_DATA_GROUP_ARRAY.base_id;
    if (index < 0 || index >= CLUSTER_DATA_GROUP_ARRAY.count) {
        logError("file: "__FILE__", line: %d, "
                "data_group_id: %d out of bounds: [%d, %d]",
                __LINE__, data_group_id, CLUSTER_DATA_GROUP_ARRAY.base_id,
                CLUSTER_DATA_GROUP_ARRAY.base_id +
                CLUSTER_DATA_GROUP_ARRAY.count - 1);
        return NULL;
    }

    if (CLUSTER_DATA_GROUP_ARRAY.groups[index].id != data_group_id) {
        logError("file: "__FILE__", line: %d, "
                "data_group_id: %d != groups[%d].id: %d",
                __LINE__, data_group_id, index,
                CLUSTER_DATA_GROUP_ARRAY.groups[index].id);
        return NULL;
    }

    return CLUSTER_DATA_GROUP_ARRAY.groups + index;
}

static inline bool fs_is_my_data_group(const int data_group_id)
{
    int index;

    index = data_group_id - CLUSTER_DATA_GROUP_ARRAY.base_id;
    if (index < 0 || index >= CLUSTER_DATA_GROUP_ARRAY.count) {
        return false;
    }

    if (CLUSTER_DATA_GROUP_ARRAY.groups[index].id != data_group_id) {
        return false;
    }

    return (CLUSTER_DATA_GROUP_ARRAY.groups[index].myself != NULL);
}

static inline FSClusterDataServerInfo *fs_get_data_server(
        const int data_group_id, const int server_id)
{
    FSClusterDataGroupInfo *group;

    if ((group=fs_get_data_group(data_group_id)) == NULL) {
        return NULL;
    }

    return fs_get_data_server_ex(group, server_id);
}

static inline FSClusterDataServerInfo *fs_get_my_data_server(
        const int data_group_id)
{
    FSClusterDataGroupInfo *group;
    if ((group=fs_get_data_group(data_group_id)) == NULL) {
        return NULL;
    }

    return group->myself;
}

static inline uint64_t fs_get_my_ds_data_version(const int data_group_id)
{
    FSClusterDataServerInfo *ds;

    if ((ds=fs_get_my_data_server(data_group_id)) == NULL) {
        return 0;
    }

    return __sync_add_and_fetch(&ds->data.current_version, 0);
}

#ifdef __cplusplus
}
#endif

#endif
