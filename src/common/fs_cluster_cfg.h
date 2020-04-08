
#ifndef _FS_CLUSTER_CFG_H
#define _FS_CLUSTER_CFG_H

#include "fs_types.h"
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
} FSClusterConfig;

#ifdef __cplusplus
extern "C" {
#endif

    int fs_cluster_cfg_load(FSClusterConfig *cluster_cfg,
            const char *cluster_filename);
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

    void fs_cluster_cfg_to_log(FSClusterConfig *cluster_cfg);

#ifdef __cplusplus
}
#endif

#endif
