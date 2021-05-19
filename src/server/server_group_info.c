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
#include "cluster_topology.h"
#include "server_group_info.h"

typedef struct {
    int id1;
    int id2;
    int offset;
} ServerPairBaseIndexEntry;

typedef struct {
    int count;
    ServerPairBaseIndexEntry *entries;
} ServerPairBaseIndexArray;

#define DATA_GROUP_INFO_FILENAME           "data_group.info"

#define DATA_GROUP_SECTION_PREFIX_STR      "data-group-"
#define SERVER_GROUP_INFO_ITEM_VERSION     "version"
#define SERVER_GROUP_INFO_ITEM_IS_LEADER   "is_leader"
#define SERVER_GROUP_INFO_ITEM_SERVER      "server"

static ServerPairBaseIndexArray server_pair_index_array = {0, NULL};
static time_t last_shutdown_time = 0;
static uint64_t last_synced_version = 0;
static int last_refresh_file_time = 0;

static int server_group_info_write_to_file(const uint64_t current_version);

static int check_alloc_ds_ptr_array(FSClusterDataServerPtrArray *array)
{
    FSClusterDataServerInfo **servers;
    int new_alloc;
    int bytes;

    if (array->alloc > array->count) {
        return 0;
    }

    new_alloc = (array->alloc > 0) ? 2 * array->alloc : 16;
    bytes = sizeof(FSClusterDataServerInfo *) * new_alloc;
    servers = (FSClusterDataServerInfo **)fc_malloc(bytes);
    if (servers == NULL) {
        return ENOMEM;
    }

    if (array->servers != NULL) {
        if (array->count > 0) {
            memcpy(servers, array->servers, array->count *
                    sizeof(FSClusterDataServerInfo *));
        }
        free(array->servers);
    }

    array->alloc = new_alloc;
    array->servers = servers;
    return 0;
}

static int add_to_ds_ptr_array(FSClusterDataServerPtrArray *ds_ptr_array,
        FSClusterDataServerInfo *ds)
{
    int result;
    if ((result=check_alloc_ds_ptr_array(ds_ptr_array)) != 0) {
        return result;
    }

    ds_ptr_array->servers[ds_ptr_array->count++] = ds;
    return 0;
}

static int init_cluster_data_server_array(FSClusterDataGroupInfo *group)
{
    FSServerGroup *server_group;
    FCServerInfo **pp;
    FCServerInfo **end;
    FSClusterDataServerInfo *ds;
    int result;
    int bytes;
    int server_index;
    int master_index;

    if ((server_group=fs_cluster_cfg_get_server_group(&CLUSTER_CONFIG_CTX,
                    group->id - 1)) == NULL)
    {
        return ENOENT;
    }

    bytes = sizeof(FSClusterDataServerInfo) * server_group->server_array.count;
    group->data_server_array.servers = (FSClusterDataServerInfo *)fc_malloc(bytes);
    if (group->data_server_array.servers == NULL) {
        return ENOMEM;
    }
    memset(group->data_server_array.servers, 0, bytes);
    group->data_server_array.count = server_group->server_array.count;

    master_index = group->hash_code % server_group->server_array.count;
    end = server_group->server_array.servers + server_group->server_array.count;
    for (pp=server_group->server_array.servers,
            ds=group->data_server_array.servers;
            pp < end; pp++, ds++)
    {
        server_index = ds - group->data_server_array.servers;
        ds->dg = group;
        if ((ds->cs=fs_get_server_by_id((*pp)->id)) == NULL) {
            logError("file: "__FILE__", line: %d, "
                    "server id: %d not exist", __LINE__, (*pp)->id);
            return ENOENT;
        }
        ds->is_preseted = (server_index == master_index);

        if ((result=init_pthread_lock_cond_pair(&ds->replica.notify)) != 0) {
            return result;
        }
        if ((result=add_to_ds_ptr_array(&ds->cs->ds_ptr_array, ds)) != 0) {
            return result;
        }
    }

    return 0;
}

static int init_ds_ptr_array(FSClusterDataGroupInfo *group)
{
    FSClusterDataServerInfo *ds;
    FSClusterDataServerInfo *end;
    int bytes;

    group->ds_ptr_array.alloc = group->data_server_array.count;
    bytes = sizeof(FSClusterDataServerInfo *) *
        group->ds_ptr_array.alloc;
    group->ds_ptr_array.servers = (FSClusterDataServerInfo **)
        fc_malloc(bytes);
    if (group->ds_ptr_array.servers == NULL) {
        return ENOMEM;
    }

    group->ds_ptr_array.count = 0;
    end = group->data_server_array.servers + group->data_server_array.count;
    for (ds=group->data_server_array.servers; ds<end; ds++) {
        group->ds_ptr_array.servers[group->ds_ptr_array.count++] = ds;
    }

    return 0;
}

static int init_slave_ds_array(FSClusterDataGroupInfo *group)
{
    FSClusterDataServerInfo *ds;
    FSClusterDataServerInfo *end;
    int bytes;

    group->slave_ds_array.alloc = group->data_server_array.count - 1;
    if (group->slave_ds_array.alloc > 0) {
        bytes = sizeof(FSClusterDataServerInfo *) *
            group->slave_ds_array.alloc;
        group->slave_ds_array.servers = (FSClusterDataServerInfo **)
            fc_malloc(bytes);
        if (group->slave_ds_array.servers == NULL) {
            return ENOMEM;
        }
        memset(group->slave_ds_array.servers, 0, bytes);
    }

    /*
    logInfo("file: "__FILE__", line: %d, "
            "data group id: %d, data server count: %d",
            __LINE__, group->id, group->data_server_array.count);
            */

    group->slave_ds_array.count = 0;
    end = group->data_server_array.servers + group->data_server_array.count;
    for (ds=group->data_server_array.servers; ds<end; ds++) {
        if (ds->cs != CLUSTER_MYSELF_PTR) {
            group->slave_ds_array.servers[group->slave_ds_array.count++] = ds;
        }
    }

    return 0;
}

static int init_cluster_data_group_array(const char *filename,
        const int server_id, FSIdArray *assoc_gid_array)
{
    FSIdArray *id_array;
    FSClusterDataGroupInfo *group;
    int result;
    int bytes;
    int count;
    int min_id;
    int max_id;
    int data_group_id;
    int data_group_index;
    int i;

    if ((min_id=fs_cluster_cfg_get_min_data_group_id(assoc_gid_array)) <= 0) {
        logError("file: "__FILE__", line: %d, "
                "cluster config file: %s, no data group",
                __LINE__, filename);
        return ENOENT;
    }
    if ((max_id=fs_cluster_cfg_get_max_data_group_id(assoc_gid_array)) <= 0) {
        logError("file: "__FILE__", line: %d, "
                "cluster config file: %s, no data group",
                __LINE__, filename);
        return ENOENT;
    }

    count = (max_id - min_id) + 1;
    bytes = sizeof(FSClusterDataGroupInfo) * count;
    CLUSTER_DATA_RGOUP_ARRAY.groups = (FSClusterDataGroupInfo *)fc_malloc(bytes);
    if (CLUSTER_DATA_RGOUP_ARRAY.groups == NULL) {
        return ENOMEM;
    }
    memset(CLUSTER_DATA_RGOUP_ARRAY.groups, 0, bytes);

    for (i=0; i<assoc_gid_array->count; i++) {
        data_group_id = assoc_gid_array->ids[i];
        data_group_index = data_group_id - min_id;
        group = CLUSTER_DATA_RGOUP_ARRAY.groups + data_group_index;
        group->id = data_group_id;
        group->index = data_group_index;
        group->hash_code = fs_cluster_cfg_get_dg_hash_code(
                &CLUSTER_CONFIG_CTX, data_group_id - 1);
        if ((result=init_cluster_data_server_array(group)) != 0) {
            return result;
        }

        if ((result=init_ds_ptr_array(group)) != 0) {
            return result;
        }
    }
    CLUSTER_DATA_RGOUP_ARRAY.count = count;
    CLUSTER_DATA_RGOUP_ARRAY.base_id = min_id;

    if ((id_array=fs_cluster_cfg_get_my_data_group_ids(&CLUSTER_CONFIG_CTX,
            server_id)) == NULL)
    {
        logError("file: "__FILE__", line: %d, "
                "cluster config file: %s, no data group",
                __LINE__, filename);
        return ENOENT;
    }

    for (i=0; i<id_array->count; i++) {
        data_group_id = id_array->ids[i];
        group = CLUSTER_DATA_RGOUP_ARRAY.groups + (data_group_id - min_id);
        if ((group->myself=fs_get_data_server(data_group_id,
                        CLUSTER_MYSELF_PTR->server->id)) == NULL)
        {
            return ENOENT;
        }

        if ((result=init_slave_ds_array(group)) != 0) {
            return result;
        }

        /*
           logInfo("file: "__FILE__", line: %d, func: %s, "
           "%d. data_group_id = %d", __LINE__, __FUNCTION__,
           i + 1, data_group_id);
           */
    }

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
                            "in more than one servers, %s:%u in "
                            "server id %d, and %s:%u in server id %d",
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

static int compare_server_ptr(const void *p1, const void *p2)
{
    return (*((FCServerInfo **)p1))->id - (*((FCServerInfo **)p2))->id;
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

static int set_server_partner_attribute(const int server_id)
{
    int result;
    int count;
    FSClusterServerInfo *cs;
    FCServerInfo *servers[FS_MAX_GROUP_SERVERS];
    FCServerInfo **server;
    FCServerInfo **end;

    if ((result=fs_cluster_cfg_get_my_group_servers(&CLUSTER_CONFIG_CTX,
                    server_id, servers, FS_MAX_GROUP_SERVERS, &count)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "get group servers fail, errno: %d, error info: %s",
                __LINE__, result, STRERROR(result));
        return result;
    }

    end = servers + count;
    for (server=servers, cs=CLUSTER_SERVER_ARRAY.servers;
            server<end; server++, cs++)
    {
        if ((cs=fs_get_server_by_id((*server)->id)) == NULL) {
            logError("file: "__FILE__", line: %d, "
                    "can't find server id: %d",
                    __LINE__, (*server)->id);
            return ENOENT;
        }

        /*
        logInfo("file: "__FILE__", line: %d, func: %s, "
                "%d. id = %d", __LINE__, __FUNCTION__,
                cs->server_index + 1, cs->server->id);
                */
    }

    return 0;
}

static int init_cluster_server_array(const char *filename)
{
    int bytes;
    int result;
    int count;
    FCServerInfo *svr;
    FSClusterServerInfo *cs;
    FSIdArray *assoc_gid_array;
    FCServerInfo *servers[FS_MAX_GROUP_SERVERS];
    FCServerInfo **server;
    FCServerInfo **end;

    if ((svr=get_myself_in_cluster_cfg(filename, &result)) == NULL) {
        return result;
    }

    if ((result=fs_cluster_cfg_get_assoc_group_info(&CLUSTER_CONFIG_CTX,
            svr->id, &assoc_gid_array, servers, FS_MAX_GROUP_SERVERS,
            &count)) != 0)
    {
    }
    qsort(servers, count, sizeof(FCServerInfo *), compare_server_ptr);

    bytes = sizeof(FSClusterServerInfo) * count;
    CLUSTER_SERVER_ARRAY.servers = (FSClusterServerInfo *)fc_malloc(bytes);
    if (CLUSTER_SERVER_ARRAY.servers == NULL) {
        return ENOMEM;
    }
    memset(CLUSTER_SERVER_ARRAY.servers, 0, bytes);

    end = servers + count;
    for (server=servers, cs=CLUSTER_SERVER_ARRAY.servers;
            server<end; server++, cs++)
    {
        cs->server_index = server - servers;
        cs->server = *server;

        //logInfo("%d. id = %d", cs->server_index + 1, (*server)->id);
    }
    CLUSTER_SERVER_ARRAY.count = count;

    if ((result=find_myself_in_cluster_config(filename)) != 0) {
        return result;
    }

    if ((result=set_server_partner_attribute(svr->id)) != 0) {
        return result;
    }

    if ((result=init_cluster_data_group_array(filename, svr->id,
                    assoc_gid_array)) != 0)
    {
        return result;
    }

    /*
    logInfo("cluster server count: %d", count);
    {
        FSClusterServerInfo *cs_end;

        cs_end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
        for (cs=CLUSTER_SERVER_ARRAY.servers; cs<cs_end; cs++) {
            logInfo("server_id: %d, data group count: %d",
                    cs->server->id, cs->ds_ptr_array.count);
        }
    }
    */

    return 0;
}

static int init_cluster_notify_contexts()
{
    FSClusterServerInfo *cs;
    FSClusterServerInfo *end;
    int result;

    end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (cs=CLUSTER_SERVER_ARRAY.servers; cs<end; cs++) {
        cs->notify_ctx.server_id = cs->server->id;
        if ((result=cluster_topology_init_notify_ctx(&cs->notify_ctx)) != 0) {
            return result;
        }
    }

    return 0;
}

static int compare_server_pair_entry(const void *p1, const void *p2)
{
    int sub;

    sub = ((ServerPairBaseIndexEntry *)p1)->id1 -
        ((ServerPairBaseIndexEntry *)p2)->id1;
    if (sub != 0) {
        return sub;
    }

    return ((ServerPairBaseIndexEntry *)p1)->id2 -
        ((ServerPairBaseIndexEntry *)p2)->id2;
}

static int init_server_pair_index_array()
{
    ServerPairBaseIndexEntry *entry;
    FSClusterServerInfo *cs1;
    FSClusterServerInfo *cs2;
    FSClusterServerInfo *end;
    int count;
    int bytes;

    if (CLUSTER_SERVER_ARRAY.count <= 1) {
        return 0;
    }

    count = CLUSTER_SERVER_ARRAY.count * (CLUSTER_SERVER_ARRAY.count - 1) / 2;
    bytes = sizeof(ServerPairBaseIndexEntry) * count;
    server_pair_index_array.entries = (ServerPairBaseIndexEntry *)fc_malloc(bytes);
    if (server_pair_index_array.entries == NULL) {
        return ENOMEM;
    }

    entry = server_pair_index_array.entries;
    end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (cs1=CLUSTER_SERVER_ARRAY.servers; cs1<end; cs1++) {
        for (cs2=cs1+1; cs2<end; cs2++) {
            entry->id1 = cs1->server->id;
            entry->id2 = cs2->server->id;
            entry->offset = (entry - server_pair_index_array.entries) *
                REPLICA_CHANNELS_BETWEEN_TWO_SERVERS;
            entry++;
        }
    }

    /*
    logInfo("server count: %d, server_pair_index_array.count: %d, "
            "replica_channels_between_two_servers: %d",
            CLUSTER_SERVER_ARRAY.count, count, REPLICA_CHANNELS_BETWEEN_TWO_SERVERS);
            */

    server_pair_index_array.count = count;
    return 0;
}

int fs_get_server_pair_base_offset(const int server_id1, const int server_id2)
{
    ServerPairBaseIndexEntry target;
    ServerPairBaseIndexEntry *found;

    target.id1 = FC_MIN(server_id1, server_id2);
    target.id2 = FC_MAX(server_id1, server_id2);
    if ((found=(ServerPairBaseIndexEntry *)bsearch(&target,
                    server_pair_index_array.entries,
                    server_pair_index_array.count,
                    sizeof(ServerPairBaseIndexEntry),
                    compare_server_pair_entry)) != NULL)
    {
        return found->offset;
    }
    return -1;
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

FSClusterDataServerInfo *fs_get_data_server_ex(
        FSClusterDataGroupInfo *group, const int server_id)
{
    FSClusterDataServerArray *ds_array;
    FSClusterDataServerInfo *ds;
    FSClusterDataServerInfo *end;

    ds_array = &group->data_server_array;
    end = ds_array->servers + ds_array->count;
    for (ds=ds_array->servers; ds<end; ds++) {
        if (ds->cs->server->id == server_id) {
            return ds;
        }
    }

    logError("file: "__FILE__", line: %d, "
            "data_group_id: %d, server_id: %d not exist",
            __LINE__, group->id, server_id);
    return NULL;
}

static inline void get_server_group_filename(
        char *full_filename, const int size)
{
    snprintf(full_filename, size, "%s/%s",
            DATA_PATH_STR, DATA_GROUP_INFO_FILENAME);
}

int fs_downgrade_data_server_status(const int old_status, int *new_status)
{
    int result;

    switch (old_status) {
        case FS_DS_STATUS_INIT:
        case FS_DS_STATUS_OFFLINE:
            result = 0;
            *new_status = old_status;
            break;
        case FS_DS_STATUS_REBUILDING:
            result = 0;
            *new_status = FS_DS_STATUS_INIT;
            break;
        case FS_DS_STATUS_RECOVERING:
        case FS_DS_STATUS_ONLINE:
        case FS_DS_STATUS_ACTIVE:
            result = 0;
            *new_status = FS_DS_STATUS_OFFLINE;
            break;
        default:
            logError("file: "__FILE__", line: %d, "
                    "invalid status: %d, set to %d (INIT)",
                    __LINE__, old_status, FS_DS_STATUS_INIT);
            result = EINVAL;
            *new_status = FS_DS_STATUS_INIT;
            break;
    }

    return result;
}

static int load_group_servers_from_ini(const char *group_filename,
        IniContext *ini_context, FSClusterDataGroupInfo *group)
{
#define MAX_FIELD_COUNT 4
    FSClusterDataServerInfo *ds;
    FSClusterDataServerInfo *ds_end;
    IniItem *items;
    IniItem *it_end;
    IniItem *it;
    string_t fields[MAX_FIELD_COUNT];
    string_t value;
    int item_count;
    int field_count;
    int server_id;
    int status;
    int64_t data_version;
    char section_name[64];

    sprintf(section_name, "%s%d", DATA_GROUP_SECTION_PREFIX_STR,
            group->id);
    if ((items=iniGetValuesEx(section_name, SERVER_GROUP_INFO_ITEM_SERVER,
            ini_context, &item_count)) == NULL)
    {
        return 0;
    }

    ds_end = group->data_server_array.servers + group->data_server_array.count;
    it_end = items + item_count;
    for (it=items; it<it_end; it++) {
        FC_SET_STRING(value, it->value);
        field_count = split_string_ex(&value, ',', fields,
                MAX_FIELD_COUNT, false);
        if (field_count != 3) {
            logError("file: "__FILE__", line: %d, "
                    "group filename: %s, section: %s, item: %s, "
                    "invalid value: %s, field count: %d != 3",
                    __LINE__, group_filename, section_name,
                    SERVER_GROUP_INFO_ITEM_SERVER,
                    it->value, field_count);
            return EINVAL;
        }

        server_id = strtol(fields[0].str, NULL, 10);
        status = strtol(fields[1].str, NULL, 10);
        data_version = strtoll(fields[2].str, NULL, 10);

        fs_downgrade_data_server_status(status, &status);
        for (ds=group->data_server_array.servers; ds<ds_end; ds++) {
            if (ds->cs->server->id == server_id) {
                ds->status = status;
                ds->data.version = data_version;
                break;
            }
        }
    }

    return 0;
}

#define server_group_info_set_file_mtime() \
    server_group_info_set_file_mtime_ex(g_current_time)

static int server_group_info_set_file_mtime_ex(const time_t t)
{
    char full_filename[PATH_MAX];
    struct timeval times[2];

    times[0].tv_sec = t;
    times[0].tv_usec = 0;
    times[1].tv_sec = t;
    times[1].tv_usec = 0;

    get_server_group_filename(full_filename, sizeof(full_filename));
    if (utimes(full_filename, times) < 0) {
        logError("file: "__FILE__", line: %d, "
                "utimes file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, full_filename, errno, STRERROR(errno));
        return errno != 0 ? errno : EPERM;
    }
    return 0;
}

static int get_server_group_info_file_mtime(time_t *mtime)
{
    char full_filename[PATH_MAX];
    struct stat buf;

    get_server_group_filename(full_filename, sizeof(full_filename));
    if (stat(full_filename, &buf) < 0) {
        logError("file: "__FILE__", line: %d, "
                "stat file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, full_filename, errno, STRERROR(errno));
        return errno != 0 ? errno : EPERM;
    }

    *mtime = buf.st_mtime;
    return 0;
}

static int load_server_groups()
{
    FSClusterDataGroupInfo *group;
    FSClusterDataGroupInfo *end;
    char full_filename[PATH_MAX];
    IniContext ini_context;
    int result;

    get_server_group_filename(full_filename, sizeof(full_filename));
    if (access(full_filename, F_OK) != 0) {
        if (errno == ENOENT) {
            if ((result=server_group_info_write_to_file(
                            CLUSTER_CURRENT_VERSION)) != 0)
            {
                return result;
            }

            return server_group_info_set_file_mtime_ex(
                    g_current_time - 86400);
        }
    }

    if ((result=get_server_group_info_file_mtime(&last_shutdown_time)) != 0) {
        return result;
    }

    if ((result=iniLoadFromFile(full_filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load from file \"%s\" fail, error code: %d",
                __LINE__, full_filename, result);
        return result;
    }

    CLUSTER_MYSELF_PTR->is_leader = iniGetBoolValue(NULL,
            SERVER_GROUP_INFO_ITEM_IS_LEADER, &ini_context, false);
    CLUSTER_CURRENT_VERSION = iniGetInt64Value(NULL,
            SERVER_GROUP_INFO_ITEM_VERSION, &ini_context, 0);

    end = CLUSTER_DATA_RGOUP_ARRAY.groups + CLUSTER_DATA_RGOUP_ARRAY.count;
    for (group=CLUSTER_DATA_RGOUP_ARRAY.groups; group<end; group++) {
        if ((result=load_group_servers_from_ini(full_filename,
                        &ini_context, group)) != 0)
        {
            break;
        }
    }

    iniFreeContext(&ini_context);
    return result;
}

static FastBuffer file_buffer;

int server_group_info_init(const char *cluster_config_filename)
{
    int result;
    time_t t;
    struct tm tm_current;

    if ((result=fast_buffer_init_ex(&file_buffer, 2048)) != 0) {
        return result;
    }

    if ((result=init_cluster_server_array(cluster_config_filename)) != 0) {
        return result;
    }

    if ((result=init_server_pair_index_array()) != 0) {
        return result;
    }

    if ((result=load_server_groups()) != 0) {
        return result;
    }

    t = g_current_time + 89;
    localtime_r(&t, &tm_current);
    tm_current.tm_sec = 0;
    last_refresh_file_time = mktime(&tm_current);
    last_synced_version = CLUSTER_CURRENT_VERSION;

    return init_cluster_notify_contexts();
}

static int server_group_info_to_file_buffer(FSClusterDataGroupInfo *group)
{
    FSClusterDataServerInfo *ds;
    FSClusterDataServerInfo *end;
    int result;

    if ((result=fast_buffer_append(&file_buffer, "[%s%d]\n",
                    DATA_GROUP_SECTION_PREFIX_STR,
                    group->id)) != 0)
    {
        return result;
    }

    end = group->data_server_array.servers + group->data_server_array.count;
    for (ds=group->data_server_array.servers; ds<end; ds++) {
        if ((result=fast_buffer_append(&file_buffer, "%s=%d,%d,%"PRId64"\n",
                        SERVER_GROUP_INFO_ITEM_SERVER, ds->cs->server->id,
                        __sync_fetch_and_add(&ds->status, 0),
                        ds->data.version)) != 0)
        {
            return result;
        }
    }

    return 0;
}

static int server_group_info_write_to_file(const uint64_t current_version)
{
    FSClusterDataGroupInfo *group;
    FSClusterDataGroupInfo *end;
    char full_filename[PATH_MAX];
    int result;

    fast_buffer_reset(&file_buffer);
    fast_buffer_append(&file_buffer,
            "%s=%d\n"
            "%s=%"PRId64"\n",
            SERVER_GROUP_INFO_ITEM_IS_LEADER,
            CLUSTER_MYSELF_PTR->is_leader,
            SERVER_GROUP_INFO_ITEM_VERSION, current_version);

    end = CLUSTER_DATA_RGOUP_ARRAY.groups + CLUSTER_DATA_RGOUP_ARRAY.count;
    for (group=CLUSTER_DATA_RGOUP_ARRAY.groups; group<end; group++) {
        if ((result=server_group_info_to_file_buffer(group)) != 0) {
            return result;
        }
    }

    get_server_group_filename(full_filename, sizeof(full_filename));
    return safeWriteToFile(full_filename, file_buffer.data,
                    file_buffer.length);
}

time_t fs_get_last_shutdown_time()
{
    return last_shutdown_time;
}

static int server_group_info_sync_to_file(void *args)
{
    uint64_t current_version;
    int result;

    if (CLUSTER_LEADER_ATOM_PTR == NULL) {
        return 0;
    }

    current_version = __sync_add_and_fetch(&CLUSTER_CURRENT_VERSION, 0);
    if (last_synced_version == current_version) {
        if (g_current_time - last_refresh_file_time > 60) {
            last_refresh_file_time = g_current_time;
            return server_group_info_set_file_mtime();
        }
        return 0;
    }

    if ((result=server_group_info_write_to_file(current_version)) == 0) {
        last_synced_version = current_version;
    }
    last_refresh_file_time = g_current_time;
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
