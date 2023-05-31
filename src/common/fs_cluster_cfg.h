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


#ifndef _FS_CLUSTER_CFG_H
#define _FS_CLUSTER_CFG_H

#include "fs_types.h"
#include "fastcommon/ini_file_reader.h"
#include "fastcommon/server_id_func.h"

typedef struct {
    int alloc;
    int count;
    int *ids;
} FSIdArray;

typedef struct {
    int server_group_id;
    FCServerInfoPtrArray server_array;
    FSIdArray data_group;
} FSServerGroup;

typedef struct {
    int data_group_id;
    FSServerGroup *server_group;
} FSDataServerMapping;

typedef struct {
    int server_id;
    FSIdArray data_group;
} FSServerDataMapping;

typedef struct {
    int count;
    FSServerGroup *groups;
} FSServerGroupArray;

typedef struct {
    int count;
    FSDataServerMapping *mappings;
} FSDataGroupArray;

typedef struct {
    int count;
    FSServerDataMapping *mappings;
} FSServerDataMappingArray;

typedef struct {
    FCServerConfig server_cfg;
    FSServerGroupArray server_groups;
    FSDataGroupArray data_groups;
    FSServerDataMappingArray server_data_mappings;
    FCServerInfoPtrArray used_server_array;
    struct {
        int size;
        int mask;  //(~(FS_FILE_BLOCK_SIZE - 1))
    } file_block;
    int unused_server_count;
    int cluster_group_index;
    int replica_group_index;
    int service_group_index;
    FSClusterMD5Digests md5_digests;
} FSClusterConfig;

#define FS_SERVER_GROUP_COUNT(cluster_cfg) \
    (cluster_cfg).server_groups.count

#define FS_DATA_GROUP_COUNT(cluster_cfg) \
    (cluster_cfg).data_groups.count

#ifdef __cplusplus
extern "C" {
#endif

    static inline void fs_cluster_cfg_set_file_block_size(
            FSClusterConfig *cfg, const int file_block_size)
    {
        cfg->file_block.size = file_block_size;
        cfg->file_block.mask = ~(file_block_size - 1);
    }

    int fs_cluster_cfg_load(FSClusterConfig *cluster_cfg,
            const char *cluster_filename, const bool calc_signs);

    int fs_cluster_cfg_load_from_ini_ex1(FSClusterConfig *cluster_cfg,
            IniFullContext *ini_ctx, char *cluster_full_filename,
            const int size);

    static inline int fs_cluster_cfg_load_from_ini_ex(FSClusterConfig *
            cluster_cfg, IniContext *ini_context, const char *cfg_filename,
            const char *section_name, char *cluster_full_filename,
            const int size)
    {
        IniFullContext ini_ctx;
        FAST_INI_SET_FULL_CTX_EX(ini_ctx, cfg_filename,
                section_name, ini_context);
        return fs_cluster_cfg_load_from_ini_ex1(cluster_cfg,
                &ini_ctx, cluster_full_filename, size);
    }

    static inline int fs_cluster_cfg_load_from_ini(FSClusterConfig *cluster_cfg,
            IniContext *ini_context, const char *cfg_filename)
    {
        const char *section_name = NULL;
        char cluster_full_filename[PATH_MAX];
        return fs_cluster_cfg_load_from_ini_ex(cluster_cfg, ini_context,
                cfg_filename, section_name, cluster_full_filename,
                sizeof(cluster_full_filename));
    }

    void fs_cluster_cfg_destroy(FSClusterConfig *cluster_cfg);

    static inline FSServerGroup *fs_cluster_cfg_get_server_group(
            FSClusterConfig *cluster_cfg, const int data_group_index)
    {
        if (data_group_index < 0 || data_group_index >=
                cluster_cfg->data_groups.count)
        {
            return NULL;
        }

        return cluster_cfg->data_groups.mappings[data_group_index].server_group;
    }

    int fs_cluster_cfg_get_my_group_servers(FSClusterConfig *cluster_cfg,
            const int server_id, FCServerInfo **servers,
            const int size, int *count);

    FSIdArray *fs_cluster_cfg_get_my_data_group_ids(FSClusterConfig *cluster_cfg,
            const int server_id);

    FSIdArray *fs_cluster_cfg_get_assoc_data_group_ids(
            FSClusterConfig *cluster_cfg, const int server_id);

    int fs_cluster_cfg_get_assoc_group_info(FSClusterConfig *cluster_cfg,
            const int server_id, FSIdArray **data_group_id_array,
            FCServerInfo **servers, const int size, int *count);

    int fs_cluster_cfg_get_min_server_group_id(FSClusterConfig
            *cluster_cfg, const int server_id);

    static inline int fs_cluster_cfg_get_min_data_group_id(
            const FSIdArray *data_group)
    {
        if (data_group->count > 0) {
            return data_group->ids[0];
        } else {
            return -1;
        }
    }

    static inline int fs_cluster_cfg_get_max_data_group_id(
            const FSIdArray *data_group)
    {
        if (data_group->count > 0) {
            return data_group->ids[data_group->count - 1];
        } else {
            return -1;
        }
    }

    int fs_cluster_cfg_get_my_server_groups(FSClusterConfig *cluster_cfg,
            const int server_id, FSServerGroup **server_groups, const int size);

    const FSServerGroup *fs_cluster_cfg_get_server_group_by_id(
            FSClusterConfig *cluster_cfg, const int server_group_id);

    const FCServerInfoPtrArray *fs_cluster_cfg_get_used_servers(
            FSClusterConfig *cluster_cfg);

    void fs_cluster_cfg_to_log(FSClusterConfig *cluster_cfg);

    int fs_cluster_cfg_to_string(FSClusterConfig *cluster_cfg,
            FastBuffer *buffer);

#ifdef __cplusplus
}
#endif

#endif
