
#ifndef _FS_CLUSTER_CFG_H
#define _FS_CLUSTER_CFG_H

#include "fs_types.h"
#include "fastcommon/server_id_func.h"

typedef struct {
    int count;
    FCServerInfo **servers;
} FSServerGroup;

typedef struct {
    int alloc;
    int count;
    int *data_group_ids;
} FSDataGroup;

typedef struct {
    int data_group_id;
    FSServerGroup *server_group;
} FSDataServerMapping;

typedef struct {
    int server_id;
    FSDataGroup *data_group;
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

    int fs_cluster_config_load(FSClusterConfig *cluster_cfg,
            const char *cluster_filename);
    void fs_cluster_config_destroy(FSClusterConfig *cluster_cfg);

#ifdef __cplusplus
}
#endif

#endif
