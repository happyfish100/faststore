
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
    int cluster_group_index;
    int service_group_index;
} FSClusterConfig;

#define FS_SERVER_GROUP_COUNT(cluster_cfg) \
    (cluster_cfg).server_groups.count

#define FS_DATA_GROUP_COUNT(cluster_cfg) \
    (cluster_cfg).data_groups.count

#ifdef __cplusplus
extern "C" {
#endif

    int fs_cluster_cfg_load(FSClusterConfig *cluster_cfg,
            const char *cluster_filename);

    int fs_cluster_cfg_load_from_ini(FSClusterConfig *cluster_cfg,
            IniContext *ini_context, const char *cfg_filename);

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

    int fs_cluster_cfg_get_group_servers(FSClusterConfig *cluster_cfg,
            const int server_id, FCServerInfo **servers,
            const int size, int *count);

    FSIdArray *fs_cluster_cfg_get_server_group_ids(FSClusterConfig *cluster_cfg,
            const int server_id);

    int fs_cluster_cfg_get_server_max_group_id(FSClusterConfig *cluster_cfg,
            const int server_id);

    void fs_cluster_cfg_to_log(FSClusterConfig *cluster_cfg);

    int fc_cluster_cfg_to_string(FSClusterConfig *cluster_cfg,
            FastBuffer *buffer);

#ifdef __cplusplus
}
#endif

#endif
