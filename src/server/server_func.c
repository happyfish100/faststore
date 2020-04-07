
#include <sys/stat.h>
#include <limits.h>
#include "fastcommon/ini_file_reader.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/md5.h"
#include "fastcommon/local_ip_func.h"
#include "sf/sf_global.h"
#include "sf/sf_service.h"
#include "common/fs_proto.h"
#include "server_global.h"
#include "server_group_info.h"
#include "server_func.h"

static int get_bytes_item_config(IniContext *ini_context,
        const char *filename, const char *item_name,
        const int64_t default_value, int64_t *bytes)
{
    int result;
    char *value;

    value = iniGetStrValue(NULL, item_name, ini_context);
    if (value == NULL) {
        *bytes = default_value;
        return 0;
    }
    if ((result=parse_bytes(value, 1, bytes)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, item: %s, value: %s is invalid",
                __LINE__, filename, item_name, value);
    }
    return result;
}

static void log_cluster_server_config()
{
    FastBuffer buffer;

    if (fast_buffer_init_ex(&buffer, 1024) != 0) {
        return;
    }
    fc_server_to_config_string(&SERVER_CONFIG_CTX, &buffer);
    log_it1(LOG_INFO, buffer.data, buffer.length);
    fast_buffer_destroy(&buffer);

    fc_server_to_log(&SERVER_CONFIG_CTX);
}

static int calc_cluster_config_sign()
{
    FastBuffer buffer;
    int result;

    if ((result=fast_buffer_init_ex(&buffer, 1024)) != 0) {
        return result;
    }
    fc_server_to_config_string(&SERVER_CONFIG_CTX, &buffer);
    my_md5_buffer(buffer.data, buffer.length, CLUSTER_CONFIG_SIGN_BUF);

    {
    char hex_buff[2 * CLUSTER_CONFIG_SIGN_LEN + 1];
    logInfo("cluster config length: %d, sign: %s", buffer.length,
            bin2hex((const char *)CLUSTER_CONFIG_SIGN_BUF,
                CLUSTER_CONFIG_SIGN_LEN, hex_buff));
    }
    fast_buffer_destroy(&buffer);
    return 0;
}

static int find_group_indexes_in_cluster_config(const char *filename)
{
    CLUSTER_GROUP_INDEX = fc_server_get_group_index(&SERVER_CONFIG_CTX,
            "cluster");
    if (CLUSTER_GROUP_INDEX < 0) {
        logError("file: "__FILE__", line: %d, "
                "cluster config file: %s, cluster group not configurated",
                __LINE__, filename);
        return ENOENT;
    }

    SERVICE_GROUP_INDEX = fc_server_get_group_index(&SERVER_CONFIG_CTX,
            "service");
    if (SERVICE_GROUP_INDEX < 0) {
        logError("file: "__FILE__", line: %d, "
                "cluster config file: %s, service group not configurated",
                __LINE__, filename);
        return ENOENT;
    }

    return 0;
}

static int load_cluster_config(IniContext *ini_context, const char *filename)
{
    int result;
    char *cluster_config_filename;
    char full_cluster_filename[PATH_MAX];

    cluster_config_filename = iniGetStrValue(NULL,
            "cluster_config_filename", ini_context);
    if (cluster_config_filename == NULL || *cluster_config_filename == '\0') {
        logError("file: "__FILE__", line: %d, "
                "item \"cluster_config_filename\" not exist or empty",
                __LINE__);
        return ENOENT;
    }

    resolve_path(filename, cluster_config_filename,
            full_cluster_filename, sizeof(full_cluster_filename));
    if ((result=fs_cluster_config_load(&CLUSTER_CONFIG_CTX,
            full_cluster_filename)) != 0)
    {
        return result;
    }

    fs_cluster_config_to_log(&CLUSTER_CONFIG_CTX);

    if ((result=server_group_info_init(full_cluster_filename)) != 0) {
        return result;
    }

    if ((result=find_group_indexes_in_cluster_config(
                    full_cluster_filename)) != 0)
    {
        return result;
    }
    if ((result=calc_cluster_config_sign()) != 0) {
        return result;
    }

    return 0;
}

static int load_data_path_config(IniContext *ini_context, const char *filename)
{
    char *data_path;

    data_path = iniGetStrValue(NULL, "data_path", ini_context);
    if (data_path == NULL) {
        data_path = "data";
    } else if (*data_path == '\0') {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, empty data_path! "
                "please set data_path correctly.",
                __LINE__, filename);
        return EINVAL;
    }

    if (*data_path == '/') {
        DATA_PATH_LEN = strlen(data_path);
        DATA_PATH_STR = strdup(data_path);
        if (DATA_PATH_STR == NULL) {
            logError("file: "__FILE__", line: %d, "
                    "malloc %d bytes fail", __LINE__, DATA_PATH_LEN + 1);
            return ENOMEM;
        }
    } else {
        DATA_PATH_LEN = strlen(SF_G_BASE_PATH) + strlen(data_path) + 1;
        DATA_PATH_STR = (char *)malloc(DATA_PATH_LEN + 1);
        if (DATA_PATH_STR == NULL) {
            logError("file: "__FILE__", line: %d, "
                    "malloc %d bytes fail", __LINE__, DATA_PATH_LEN + 1);
            return ENOMEM;
        }
        DATA_PATH_LEN = sprintf(DATA_PATH_STR, "%s/%s",
                SF_G_BASE_PATH, data_path);
    }
    chopPath(DATA_PATH_STR);

    if (access(DATA_PATH_STR, F_OK) != 0) {
        if (errno != ENOENT) {
            logError("file: "__FILE__", line: %d, "
                    "access %s fail, errno: %d, error info: %s",
                    __LINE__, DATA_PATH_STR, errno, STRERROR(errno));
            return errno != 0 ? errno : EPERM;
        }

        if (mkdir(DATA_PATH_STR, 0775) != 0) {
            logError("file: "__FILE__", line: %d, "
                    "mkdir %s fail, errno: %d, error info: %s",
                    __LINE__, DATA_PATH_STR, errno, STRERROR(errno));
            return errno != 0 ? errno : EPERM;
        }
        
        SF_CHOWN_RETURN_ON_ERROR(DATA_PATH_STR, geteuid(), getegid());
    }

    return 0;
}

static void server_log_configs()
{
    char sz_server_config[512];
    char sz_global_config[512];
    char sz_service_config[128];
    char sz_cluster_config[128];

    sf_global_config_to_string(sz_global_config, sizeof(sz_global_config));
    sf_context_config_to_string(&g_sf_context,
            sz_service_config, sizeof(sz_service_config));
    sf_context_config_to_string(&CLUSTER_SF_CTX,
            sz_cluster_config, sizeof(sz_cluster_config));

    snprintf(sz_server_config, sizeof(sz_server_config),
            "my server id = %d, data_path = %s, "
            "replica_channels_between_two_servers = %d, "
            "binlog_buffer_size = %d KB, "
            "cluster server count = %d",
            CLUSTER_MY_SERVER_ID,
            DATA_PATH_STR, REPLICA_CHANNELS_BETWEEN_TWO_SERVERS,
            BINLOG_BUFFER_SIZE / 1024,
            FC_SID_SERVER_COUNT(SERVER_CONFIG_CTX));

    logInfo("%s, service: {%s}, cluster: {%s}, %s",
            sz_global_config, sz_service_config,
            sz_cluster_config, sz_server_config);
    log_local_host_ip_addrs();
    log_cluster_server_config();
}

static int load_binlog_buffer_size(IniContext *ini_context,
        const char *filename)
{
    int64_t bytes;
    int result;

    if ((result=get_bytes_item_config(ini_context, filename,
                    "binlog_buffer_size", FS_DEFAULT_BINLOG_BUFFER_SIZE,
                    &bytes)) != 0)
    {
        return result;
    }
    if (bytes < 4096) {
        logWarning("file: "__FILE__", line: %d, "
                "config file: %s , binlog_buffer_size: %d is too small, "
                "set it to default: %d", __LINE__, filename,
                BINLOG_BUFFER_SIZE, FS_DEFAULT_BINLOG_BUFFER_SIZE);
        BINLOG_BUFFER_SIZE = FS_DEFAULT_BINLOG_BUFFER_SIZE;
    } else {
        BINLOG_BUFFER_SIZE = bytes;
    }

    return 0;
}


int server_load_config(const char *filename)
{
    IniContext ini_context;
    int result;

    if ((result=iniLoadFromFile(filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load conf file \"%s\" fail, ret code: %d",
                __LINE__, filename, result);
        return result;
    }

    if ((result=sf_load_config_ex("fs_serverd", filename, &ini_context,
                    "service", FS_SERVER_DEFAULT_SERVICE_PORT,
                    FS_SERVER_DEFAULT_SERVICE_PORT)) != 0)
    {
        return result;
    }

    if ((result=sf_load_context_from_config(&CLUSTER_SF_CTX,
                    filename, &ini_context, "cluster",
                    FS_SERVER_DEFAULT_CLUSTER_PORT,
                    FS_SERVER_DEFAULT_CLUSTER_PORT)) != 0)
    {
        return result;
    }

    if ((result=load_data_path_config(&ini_context, filename)) != 0) {
        return result;
    }

    REPLICA_CHANNELS_BETWEEN_TWO_SERVERS = iniGetIntValue(NULL,
            "replica_channels_between_two_servers",
            &ini_context, FS_DEFAULT_REPLICA_CHANNELS_BETWEEN_TWO_SERVERS);
    if (REPLICA_CHANNELS_BETWEEN_TWO_SERVERS <= 0) {
        REPLICA_CHANNELS_BETWEEN_TWO_SERVERS =
            FS_DEFAULT_REPLICA_CHANNELS_BETWEEN_TWO_SERVERS;
    }

    if ((result=load_binlog_buffer_size(&ini_context, filename)) != 0) {
        return result;
    }

    if ((result=load_cluster_config(&ini_context, filename)) != 0) {
        return result;
    }

    iniFreeContext(&ini_context);

    load_local_host_ip_addrs();
    server_log_configs();

    return 0;
}
