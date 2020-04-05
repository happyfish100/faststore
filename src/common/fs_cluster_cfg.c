#include <limits.h>
#include "fastcommon/ini_file_reader.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fs_cluster_cfg.h"


static int load_server_groups(FSClusterConfig *cluster_cfg,
        const char *cluster_filename, IniContext *ini_context)
{
    int server_group_count;
    int bytes;

    server_group_count = iniGetIntValue(NULL, "server_group_count",
            ini_context, 0);
    if (server_group_count <= 0) {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, item \"server_group_count\" "
                "not exist or is invalid", __LINE__, cluster_filename);
        return EINVAL;
    }

    bytes = sizeof(FSServerGroup) * server_group_count;
    cluster_cfg->server_groups.groups = (FSServerGroup *)malloc(bytes);
    if (cluster_cfg->server_groups.groups == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(cluster_cfg->server_groups.groups, 0, bytes);
    cluster_cfg->server_groups.count = server_group_count;
    return 0;
}

static int load_data_groups(FSClusterConfig *cluster_cfg,
        const char *cluster_filename, IniContext *ini_context)
{
    int data_group_count;
    int bytes;

    data_group_count = iniGetIntValue(NULL, "data_group_count",
            ini_context, 0);
    if (data_group_count <= 0) {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, item \"data_group_count\" "
                "not exist or is invalid", __LINE__, cluster_filename);
        return EINVAL;
    }

    bytes = sizeof(FSDataServerMapping) * data_group_count;
    cluster_cfg->data_groups.mappings = (FSDataServerMapping *)malloc(bytes);
    if (cluster_cfg->data_groups.mappings == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(cluster_cfg->data_groups.mappings, 0, bytes);
    cluster_cfg->data_groups.count = data_group_count;
    return 0;
}

static int load_groups(FSClusterConfig *cluster_cfg,
        const char *cluster_filename, IniContext *ini_context)
{
    int result;

    if ((result=load_server_groups(cluster_cfg, cluster_filename,
                    ini_context)) != 0)
    {
        return result;
    }

    if ((result=load_data_groups(cluster_cfg, cluster_filename,
                    ini_context)) != 0)
    {
        return result;
    }

    return 0;
}

int fs_cluster_config_load(FSClusterConfig *cluster_cfg,
        const char *cluster_filename)
{
    IniContext ini_context;
    char full_filename[PATH_MAX];
    char *server_config_filename;
    const int min_hosts_each_group = 1;
    const bool share_between_groups = true;
    int result;

    if ((result=iniLoadFromFile(cluster_filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load conf file \"%s\" fail, ret code: %d",
                __LINE__, cluster_filename, result);
        return result;
    }

    server_config_filename = iniGetStrValue(NULL,
            "server_config_filename", &ini_context);
    if (server_config_filename == NULL || *server_config_filename == '\0') {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, item \"server_config_filename\" "
                "not exist or empty", __LINE__, cluster_filename);
        return ENOENT;
    }

    resolve_path(cluster_filename, server_config_filename,
            full_filename, sizeof(full_filename));
    if ((result=fc_server_load_from_file_ex(&cluster_cfg->server_cfg,
                    full_filename, FS_SERVER_DEFAULT_CLUSTER_PORT,
                    min_hosts_each_group, share_between_groups)) != 0)
    {
        return result;
    }

    result = load_groups(cluster_cfg, cluster_filename, &ini_context);
    iniFreeContext(&ini_context);
    return result;
}

void fs_cluster_config_destroy(FSClusterConfig *cluster_cfg)
{
}
