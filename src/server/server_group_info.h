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

FSClusterDataServerInfo *fs_get_data_server(const int data_group_id,
        const int server_id);

int fs_downgrade_data_server_status(const int old_status, int *new_status);

int fs_get_server_pair_base_offset(const int server_id1, const int server_id2);

int server_group_info_setup_sync_to_file_task();

time_t fs_get_last_shutdown_time();

static inline FSClusterDataGroupInfo *fs_get_data_group(const int data_group_id)
{
    int index;

    index = data_group_id - CLUSTER_DATA_RGOUP_ARRAY.base_id;
    if (index < 0 || index >= CLUSTER_DATA_RGOUP_ARRAY.count) {
        logError("file: "__FILE__", line: %d, "
                "data_group_id: %d out of bounds: [%d, %d]",
                __LINE__, data_group_id, CLUSTER_DATA_RGOUP_ARRAY.base_id,
                CLUSTER_DATA_RGOUP_ARRAY.base_id +
                CLUSTER_DATA_RGOUP_ARRAY.count - 1);
        return NULL;
    }

    if (CLUSTER_DATA_RGOUP_ARRAY.groups[index].id != data_group_id) {
        logError("file: "__FILE__", line: %d, "
                "data_group_id: %d != groups[%d].id: %d",
                __LINE__, data_group_id, index,
                CLUSTER_DATA_RGOUP_ARRAY.groups[index].id);
        return NULL;
    }

    return CLUSTER_DATA_RGOUP_ARRAY.groups + index;
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

static inline void server_group_info_set_status(FSClusterServerInfo *cs,
        const int status)
{
    //TODO
    /*
    if (cs->status != status) {
        cs->status = status;
        __sync_add_and_fetch(&CLUSTER_SERVER_ARRAY.change_version, 1);
    }
    */
}

#ifdef __cplusplus
}
#endif

#endif
