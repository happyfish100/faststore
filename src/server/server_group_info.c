#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include "fastcommon/logger.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/local_ip_func.h"
#include "server_global.h"
#include "server_group_info.h"

#define DATA_GROUP_INFO_SUBDIR_NAME               "cluster"
#define DATA_GROUP_INFO_FILENAME_FORMAT           "data_group%05d.info"

#define SERVER_SECTION_PREFIX_STR                 "server-"
#define SERVER_GROUP_INFO_ITEM_STATUS             "status"

static int server_group_info_write_to_file(FSClusterDataGroupInfo *group);

static int init_cluster_server_ptr_array(FSClusterDataGroupInfo *group)
{
    FSServerGroup *server_group;
    FCServerInfo **pp;
    FCServerInfo **end;
    FSClusterServerPtr *sp;
    int bytes;

    if ((server_group=fs_cluster_cfg_get_server_group(&CLUSTER_CONFIG_CTX,
                    group->data_group_id - 1)) == NULL)
    {
        return ENOENT;
    }


    bytes = sizeof(FSClusterServerPtr) * server_group->server_array.count;
    group->server_ptr_array.servers = (FSClusterServerPtr *)malloc(bytes);
    if (group->server_ptr_array.servers == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(group->server_ptr_array.servers, 0, bytes);
    group->server_ptr_array.count = server_group->server_array.count;

    end = server_group->server_array.servers + server_group->server_array.count;
    for (pp=server_group->server_array.servers,
            sp=group->server_ptr_array.servers;
            pp < end; pp++, sp++)
    {
        sp->cs = fs_get_server_by_id((*pp)->id);
    }

    return 0;
}

static int init_cluster_data_group_array(const char *filename)
{
    FSIdArray *id_array;
    FSClusterDataGroupInfo *group;
    FSClusterDataGroupInfo *end;
    int result;
    int bytes;
    int count;
    int data_group_id;
    int i;

    if ((id_array=fs_cluster_cfg_get_server_group_ids(&CLUSTER_CONFIG_CTX,
            CLUSTER_MYSELF_PTR->server->id)) == NULL)
    {
        logError("file: "__FILE__", line: %d, "
                "cluster config file: %s, no data group",
                __LINE__, filename);
        return ENOENT;
    }

    if ((count=fs_cluster_cfg_get_server_max_group_id(&CLUSTER_CONFIG_CTX,
            CLUSTER_MYSELF_PTR->server->id)) <= 0)
    {
        logError("file: "__FILE__", line: %d, "
                "cluster config file: %s, no data group",
                __LINE__, filename);
        return ENOENT;
    }

    bytes = sizeof(FSClusterDataGroupInfo) * count;
    CLUSTER_DATA_RGOUP_ARRAY.groups = (FSClusterDataGroupInfo *)malloc(bytes);
    if (CLUSTER_DATA_RGOUP_ARRAY.groups == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(CLUSTER_DATA_RGOUP_ARRAY.groups, 0, bytes);

    end = CLUSTER_DATA_RGOUP_ARRAY.groups + count;
    for (group=CLUSTER_DATA_RGOUP_ARRAY.groups; group<end; group++) {
        group->data_group_id = (group - CLUSTER_DATA_RGOUP_ARRAY.groups) + 1;
    }

    for (i=0; i<id_array->count; i++) {
        data_group_id = id_array->ids[i];
        if ((result=init_cluster_server_ptr_array(
                        CLUSTER_DATA_RGOUP_ARRAY.groups +
                        (data_group_id - 1))) != 0)
        {
            return result;
        }
    }

    CLUSTER_DATA_RGOUP_ARRAY.count = count;
    return 0;
}

static int init_cluster_server_array(const char *filename)
{
#define MAX_GROUP_SERVERS 64
    int bytes;
    int result;
    int count;
    FSClusterServerInfo *cs;
    FCServerInfo *servers[MAX_GROUP_SERVERS];
    FCServerInfo **server;
    FCServerInfo **end;

    if ((result=fs_cluster_cfg_get_group_servers(&CLUSTER_CONFIG_CTX,
                    CLUSTER_MYSELF_PTR->server->id, servers,
                    MAX_GROUP_SERVERS, &count)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "get group servers fail, errno: %d, error info: %s",
                __LINE__, result, STRERROR(result));
        return result;
    }

    bytes = sizeof(FSClusterServerInfo) * count;
    CLUSTER_SERVER_ARRAY.servers = (FSClusterServerInfo *)malloc(bytes);
    if (CLUSTER_SERVER_ARRAY.servers == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(CLUSTER_SERVER_ARRAY.servers, 0, bytes);

    end = servers + count;
    for (server=servers, cs=CLUSTER_SERVER_ARRAY.servers;
            server<end; server++, cs++)
    {
        cs->server = *server;
    }

    CLUSTER_SERVER_ARRAY.count = count;
    return 0;
}

static FCServerInfo *get_myself_in_cluster_cfg(const char *filename,
        int *err_no)
{
    const char *local_ip;
    struct {
        const char *ip_addr;
        int port;
    } found;
    FCServerInfo *server;
    FCServerInfo *myself;
    int ports[2];
    int count;
    int i;

    count = 0;
    ports[count++] = g_sf_context.inner_port;
    if (g_sf_context.outer_port != g_sf_context.inner_port) {
        ports[count++] = g_sf_context.outer_port;
    }

    myself = NULL;
    found.ip_addr = NULL;
    found.port = 0;
    local_ip = get_first_local_ip();
    while (local_ip != NULL) {
        for (i=0; i<count; i++) {
            server = fc_server_get_by_ip_port(&SERVER_CONFIG_CTX,
                    local_ip, ports[i]);
            if (server != NULL) {
                if (myself == NULL) {
                    myself = server;
                } else if (myself != server) {
                    logError("file: "__FILE__", line: %d, "
                            "cluster config file: %s, my ip and port "
                            "in more than one servers, %s:%d in "
                            "server id %d, and %s:%d in server id %d",
                            __LINE__, filename, found.ip_addr, found.port,
                            myself->id, local_ip, ports[i], server->id);
                    *err_no = EEXIST;
                    return NULL;
                }
            }

            found.ip_addr = local_ip;
            found.port = ports[i];
        }

        local_ip = get_next_local_ip(local_ip);
    }

    if (myself == NULL) {
        logError("file: "__FILE__", line: %d, "
                "cluster config file: %s, can't find myself "
                "by my local ip and listen port", __LINE__, filename);
        *err_no = ENOENT;
    }
    return myself;
}

static int find_myself_in_cluster_config(const char *filename)
{
    FCServerInfo *server;
    int result;

    if ((server=get_myself_in_cluster_cfg(filename, &result)) == NULL) {
        return result;
    }

    CLUSTER_MYSELF_PTR = fs_get_server_by_id(server->id);
    if (CLUSTER_MYSELF_PTR == NULL) {
        logError("file: "__FILE__", line: %d, "
                "cluster config file: %s, can't find myself "
                "by my server id: %d", __LINE__, filename, server->id);
        return ENOENT;
    }
    return 0;
}

FSClusterServerInfo *fs_get_server_by_id(const int server_id)
{
    FSClusterServerInfo *cs;
    FSClusterServerInfo *end;

    end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (cs=CLUSTER_SERVER_ARRAY.servers; cs<end; cs++) {
        if (cs->server->id == server_id) {
            return cs;
        }
    }

    return NULL;
}

static void get_server_group_filename(FSClusterDataGroupInfo *group,
        char *full_filename, const int size)
{
    snprintf(full_filename, size, "%s/%s/"DATA_GROUP_INFO_FILENAME_FORMAT,
            DATA_PATH_STR, DATA_GROUP_INFO_SUBDIR_NAME, group->data_group_id);
}

static int load_servers_from_ini_ctx(IniContext *ini_context,
        FSClusterServerPtrArray *server_ptr_array)
{
    FSClusterServerPtr *sp;
    FSClusterServerPtr *end;
    char section_name[64];

    end = server_ptr_array->servers + server_ptr_array->count;
    for (sp=server_ptr_array->servers; sp<end; sp++) {
        sprintf(section_name, "%s%d",
                SERVER_SECTION_PREFIX_STR,
                sp->cs->server->id);
        sp->status = iniGetIntValue(section_name,
                SERVER_GROUP_INFO_ITEM_STATUS, ini_context,
                FS_SERVER_STATUS_INIT);

        if (sp->status == FS_SERVER_STATUS_SYNCING ||
                sp->status == FS_SERVER_STATUS_ACTIVE)
        {
            sp->status = FS_SERVER_STATUS_OFFLINE;
        }
    }

    return 0;
}

static int load_server_group_info_from_file(FSClusterDataGroupInfo *group)
{
    char full_filename[PATH_MAX];
    IniContext ini_context;
    int result;

    get_server_group_filename(group, full_filename, sizeof(full_filename));
    if (access(full_filename, F_OK) != 0) {
        if (errno == ENOENT) {
            return server_group_info_write_to_file(group);
        }
    }

    if ((result=iniLoadFromFile(full_filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load from file \"%s\" fail, error code: %d",
                __LINE__, full_filename, result);
        return result;
    }

    result = load_servers_from_ini_ctx(&ini_context, &group->server_ptr_array);
    iniFreeContext(&ini_context);

    return result;
}

static int load_server_groups()
{
    FSClusterDataGroupInfo *group;
    FSClusterDataGroupInfo *end;
    int result;

    end = CLUSTER_DATA_RGOUP_ARRAY.groups + CLUSTER_DATA_RGOUP_ARRAY.count;
    for (group=CLUSTER_DATA_RGOUP_ARRAY.groups; group<end; group++) {
        if ((result=load_server_group_info_from_file(group)) != 0) {
            return result;
        }
    }

    return 0;
}

int server_group_info_init(const char *cluster_config_filename)
{
    char filepath[PATH_MAX];
    int result;
    bool create;

    snprintf(filepath, sizeof(filepath), "%s/%s",
            DATA_PATH_STR, DATA_GROUP_INFO_SUBDIR_NAME);
    if ((result=fc_check_mkdir_ex(filepath, 0775, &create)) != 0) {
        return result;
    }
    if (create) {
        SF_CHOWN_RETURN_ON_ERROR(filepath, geteuid(), getegid());
    }

    if ((result=find_myself_in_cluster_config(cluster_config_filename)) != 0) {
        return result;
    }

    if ((result=init_cluster_server_array(cluster_config_filename)) != 0) {
        return result;
    }

    if ((result=init_cluster_data_group_array(cluster_config_filename)) != 0) {
        return result;
    }

    if ((result=load_server_groups()) != 0) {
        return result;
    }

    return 0;
}

static int server_group_info_write_to_file(FSClusterDataGroupInfo *group)
{
    char full_filename[PATH_MAX];
    char buff[8 * 1024];
    char *p;
    FSClusterServerPtr *sp;
    FSClusterServerPtr *end;
    int result;
    int len;

    get_server_group_filename(group, full_filename, sizeof(full_filename));
    p = buff;
    end = group->server_ptr_array.servers + group->server_ptr_array.count;
    for (sp=group->server_ptr_array.servers; sp<end; sp++) {
        p += sprintf(p,
                "[%s%d]\n"
                "%s=%d\n\n",
                SERVER_SECTION_PREFIX_STR, sp->cs->server->id,
                SERVER_GROUP_INFO_ITEM_STATUS, sp->status
                );
    }

    len = p - buff;
    if ((result=safeWriteToFile(full_filename, buff, len)) != 0) {
        logError("file: "__FILE__", line: %d, "
            "write to file \"%s\" fail, "
            "errno: %d, error info: %s",
            __LINE__, full_filename,
            result, STRERROR(result));
    }

    return result;
}

static int server_group_info_sync_to_file(void *args)
{
    FSClusterDataGroupInfo *group;
    FSClusterDataGroupInfo *end;
    int result;

    result = 0;
    end = CLUSTER_DATA_RGOUP_ARRAY.groups + CLUSTER_DATA_RGOUP_ARRAY.count;
    for (group=CLUSTER_DATA_RGOUP_ARRAY.groups; group<end; group++) {
        if (group->last_synced_version == group->change_version) {
            continue;
        }

        group->last_synced_version = group->change_version;
        if ((result=server_group_info_write_to_file(group)) != 0) {
            break;
        }
    }

    return result;
}

int server_group_info_setup_sync_to_file_task()
{
    ScheduleEntry schedule_entry;
    ScheduleArray schedule_array;

    INIT_SCHEDULE_ENTRY(schedule_entry, sched_generate_next_id(),
            0, 0, 0, 1, server_group_info_sync_to_file, NULL);

    schedule_array.count = 1;
    schedule_array.entries = &schedule_entry;
    return sched_add_entries(&schedule_array);
}
