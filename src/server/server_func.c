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


#include <sys/stat.h>
#include <limits.h>
#include <math.h>
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
#include "server_binlog.h"
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

    //fc_server_to_log(&SERVER_CONFIG_CTX);
}

static int calc_cluster_config_sign()
{
    FastBuffer buffer;
    int result;

    if ((result=fast_buffer_init_ex(&buffer, 4096)) != 0) {
        return result;
    }
    fc_server_to_config_string(&SERVER_CONFIG_CTX, &buffer);
    my_md5_buffer(buffer.data, buffer.length, SERVERS_CONFIG_SIGN_BUF);

    fast_buffer_reset(&buffer);
    fc_cluster_cfg_to_string(&CLUSTER_CONFIG_CTX, &buffer);
    my_md5_buffer(buffer.data, buffer.length, CLUSTER_CONFIG_SIGN_BUF);

    //logInfo("cluster config:\n%.*s", buffer.length, buffer.data);

    fast_buffer_destroy(&buffer);
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
                "config file: %s, item \"cluster_config_filename\" "
                "not exist or empty", __LINE__, filename);
        return ENOENT;
    }

    resolve_path(filename, cluster_config_filename,
            full_cluster_filename, sizeof(full_cluster_filename));
    if ((result=fs_cluster_cfg_load(&CLUSTER_CONFIG_CTX,
            full_cluster_filename)) != 0)
    {
        return result;
    }

    fs_cluster_cfg_to_log(&CLUSTER_CONFIG_CTX);

    if ((result=server_group_info_init(full_cluster_filename)) != 0) {
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
        DATA_PATH_STR = fc_strdup1(data_path, DATA_PATH_LEN);
        if (DATA_PATH_STR == NULL) {
            return ENOMEM;
        }
    } else {
        DATA_PATH_LEN = strlen(SF_G_BASE_PATH) + strlen(data_path) + 1;
        DATA_PATH_STR = (char *)fc_malloc(DATA_PATH_LEN + 1);
        if (DATA_PATH_STR == NULL) {
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
    char sz_slowlog_config[256];
    char sz_service_config[128];
    char sz_cluster_config[128];
    char sz_replica_config[128];

    sf_global_config_to_string(sz_global_config, sizeof(sz_global_config));

    sf_slow_log_config_to_string(&SLOW_LOG_CFG, "slow_log",
            sz_slowlog_config, sizeof(sz_slowlog_config));

    sf_context_config_to_string(&g_sf_context,
            sz_service_config, sizeof(sz_service_config));
    sf_context_config_to_string(&CLUSTER_SF_CTX,
            sz_cluster_config, sizeof(sz_cluster_config));
    sf_context_config_to_string(&REPLICA_SF_CTX,
            sz_replica_config, sizeof(sz_replica_config));

    snprintf(sz_server_config, sizeof(sz_server_config),
            "my server id = %d, data_path = %s, data_threads = %d, "
            "replica_channels_between_two_servers = %d, "
            "recovery_threads_per_data_group = %d, "
            "recovery_max_queue_depth = %d, "
            "binlog_buffer_size = %d KB, "
            "local_binlog_check_last_seconds = %d s, "
            "slave_binlog_check_last_rows = %d, "
            "cluster server count = %d, "
            "idempotency_max_channel_count: %d",
            CLUSTER_MY_SERVER_ID, DATA_PATH_STR, DATA_THREAD_COUNT,
            REPLICA_CHANNELS_BETWEEN_TWO_SERVERS,
            RECOVERY_THREADS_PER_DATA_GROUP,
            RECOVERY_MAX_QUEUE_DEPTH,
            BINLOG_BUFFER_SIZE / 1024,
            LOCAL_BINLOG_CHECK_LAST_SECONDS,
            SLAVE_BINLOG_CHECK_LAST_ROWS,
            FC_SID_SERVER_COUNT(SERVER_CONFIG_CTX),
            SF_IDEMPOTENCY_MAX_CHANNEL_COUNT);

    logInfo("faststore V%d.%d.%d, %s, %s, service: {%s}, cluster: {%s}, "
            "replica: {%s}, %s", g_fs_global_vars.version.major,
            g_fs_global_vars.version.minor, g_fs_global_vars.version.patch,
            sz_global_config, sz_slowlog_config, sz_service_config,
            sz_cluster_config, sz_replica_config, sz_server_config);
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

static int load_storage_cfg(IniContext *ini_context, const char *filename)
{
    char *storage_config_filename;
    char full_filename[PATH_MAX];

    storage_config_filename = iniGetStrValue(NULL,
            "storage_config_filename", ini_context);
    if (storage_config_filename == NULL || *storage_config_filename == '\0') {
        logError("file: "__FILE__", line: %d, "
                "item \"storage_config_filename\" not exist or empty",
                __LINE__);
        return ENOENT;
    }

    resolve_path(filename, storage_config_filename,
            full_filename, sizeof(full_filename));
    return storage_config_load(&STORAGE_CFG, full_filename);
}

int server_load_config(const char *filename)
{
    IniContext ini_context;
    IniFullContext full_ini_ctx;
    int result;

    if ((result=iniLoadFromFile(filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load conf file \"%s\" fail, ret code: %d",
                __LINE__, filename, result);
        return result;
    }

    if ((result=sf_load_config("fs_serverd", filename, &ini_context,
                    "service", FS_SERVER_DEFAULT_SERVICE_PORT,
                    FS_SERVER_DEFAULT_SERVICE_PORT,
                    FS_TASK_BUFFER_FRONT_PADDING_SIZE)) != 0)
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

    if ((result=sf_load_context_from_config(&REPLICA_SF_CTX,
                    filename, &ini_context, "replica",
                    FS_SERVER_DEFAULT_REPLICA_PORT,
                    FS_SERVER_DEFAULT_REPLICA_PORT)) != 0)
    {
        return result;
    }

    if ((result=load_data_path_config(&ini_context, filename)) != 0) {
        return result;
    }

    FAST_INI_SET_FULL_CTX_EX(full_ini_ctx, filename, NULL, &ini_context);

    DATA_THREAD_COUNT = iniGetIntCorrectValue(&full_ini_ctx,
            "data_threads", FS_DEFAULT_DATA_THREAD_COUNT,
            FS_MIN_DATA_THREAD_COUNT, FS_MAX_DATA_THREAD_COUNT);

    REPLICA_CHANNELS_BETWEEN_TWO_SERVERS = iniGetIntCorrectValue(
            &full_ini_ctx, "replica_channels_between_two_servers",
            FS_DEFAULT_REPLICA_CHANNELS_BETWEEN_TWO_SERVERS,
            FS_MIN_REPLICA_CHANNELS_BETWEEN_TWO_SERVERS,
            FS_MAX_REPLICA_CHANNELS_BETWEEN_TWO_SERVERS);

    RECOVERY_THREADS_PER_DATA_GROUP = iniGetIntCorrectValue(&full_ini_ctx,
            "recovery_threads_per_data_group",
            FS_DEFAULT_RECOVERY_THREADS_PER_DATA_GROUP,
            FS_MIN_RECOVERY_THREADS_PER_DATA_GROUP,
            FS_MAX_RECOVERY_THREADS_PER_DATA_GROUP);

    RECOVERY_MAX_QUEUE_DEPTH = iniGetIntCorrectValue(&full_ini_ctx,
            "recovery_max_queue_depth", FS_DEFAULT_RECOVERY_MAX_QUEUE_DEPTH,
            FS_MIN_RECOVERY_MAX_QUEUE_DEPTH, FS_MAX_RECOVERY_MAX_QUEUE_DEPTH);

    LOCAL_BINLOG_CHECK_LAST_SECONDS = iniGetIntValue(NULL,
            "local_binlog_check_last_seconds", &ini_context,
            FS_DEFAULT_LOCAL_BINLOG_CHECK_LAST_SECONDS);

    SLAVE_BINLOG_CHECK_LAST_ROWS = iniGetIntCorrectValue(
            &full_ini_ctx, "slave_binlog_check_last_rows",
            FS_DEFAULT_SLAVE_BINLOG_CHECK_LAST_ROWS,
            FS_MIN_SLAVE_BINLOG_CHECK_LAST_ROWS,
            FS_MAX_SLAVE_BINLOG_CHECK_LAST_ROWS);

    if ((result=load_binlog_buffer_size(&ini_context, filename)) != 0) {
        return result;
    }

    if ((result=load_cluster_config(&ini_context, filename)) != 0) {
        return result;
    }

    if ((result=load_storage_cfg(&ini_context, filename)) != 0) {
        return result;
    }

    if ((result=sf_load_slow_log_config(filename, &ini_context,
                    &SLOW_LOG_CTX, &SLOW_LOG_CFG)) != 0)
    {
        return result;
    }

    iniFreeContext(&ini_context);

    g_server_global_vars.replica.active_test_interval = (int)
        ceil(SF_G_NETWORK_TIMEOUT / 2.00);
    g_sf_binlog_data_path = DATA_PATH_STR;

    load_local_host_ip_addrs();
    server_log_configs();
    storage_config_to_log(&STORAGE_CFG);

    return 0;
}
