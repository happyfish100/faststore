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
#include "fastcommon/local_ip_func.h"
#include "fastcommon/system_info.h"
#include "sf/sf_global.h"
#include "sf/sf_service.h"
#include "fastcfs/auth/fcfs_auth_for_server.h"
#include "fastcfs/vote/fcfs_vote_client.h"
#include "common/fs_proto.h"
#include "client/fs_client.h"
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

static int load_master_election_config(const char *filename)
{
    const char *section_name = "master-election";
    int result;
    IniContext ini_context;
    char *policy;
    char *remain;
    char *endptr;

    if ((result=iniLoadFromFile(filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load conf file \"%s\" fail, ret code: %d",
                __LINE__, filename, result);
        return result;
    }

    RESUME_MASTER_ROLE = iniGetBoolValue(section_name,
            "resume_master_role", &ini_context, true);
    MASTER_ELECTION_TIMEOUTS = FS_DEFAULT_MASTER_ELECTION_TIMEOUTS;
    MASTER_ELECTION_FAILOVER = iniGetBoolValue(section_name,
            "failover", &ini_context, true);
    policy = iniGetStrValue(section_name, "policy", &ini_context);
    if (policy == NULL || *policy == '\0' ||
            strcasecmp(policy, FS_MASTER_ELECTION_POLICY_STRICT_STR) == 0)
    {
        MASTER_ELECTION_POLICY = FS_MASTER_ELECTION_POLICY_STRICT_INT;
        return 0;
    }

    if (strncasecmp(policy, FS_MASTER_ELECTION_POLICY_TIMEOUT_STR,
                FS_MASTER_ELECTION_POLICY_TIMEOUT_LEN) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, section: %s, item: policy is invalid",
                __LINE__, filename, section_name);
        return EINVAL;
    }

    MASTER_ELECTION_POLICY = FS_MASTER_ELECTION_POLICY_TIMEOUT_INT;
    remain = policy + FS_MASTER_ELECTION_POLICY_TIMEOUT_LEN;
    if (*remain == '\0') {
        return 0;
    }

    trim_left(remain);
    if (*remain != ':') {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, section: %s, item: policy is invalid, "
                "expect colon (:) after %s", __LINE__, filename,
                section_name, FS_MASTER_ELECTION_POLICY_TIMEOUT_STR);
        return EINVAL;
    }
    remain++;  //skip colon
    trim_left(remain);
    if (*remain == '\0') {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, section: %s, item: policy, "
                "value: %s, expect timeout number after colon",
                __LINE__, filename, section_name, policy);
        return EINVAL;
    }

    MASTER_ELECTION_TIMEOUTS = strtol(remain, &endptr, 10);
    if ((endptr != NULL && *endptr != '\0') ||
            (MASTER_ELECTION_TIMEOUTS <= 0))
    {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, section: %s, item: policy, "
                "invalid timeout number: %s, length: %d",
                __LINE__, filename, section_name, remain,
                (int)strlen(remain));
        return EINVAL;
    }
    iniFreeContext(&ini_context);

    return 0;
}

static int load_leader_election_config(IniFullContext *ini_ctx)
{
    int result;

    LEADER_ELECTION_LOST_TIMEOUT = iniGetIntCorrectValue(
            ini_ctx, "leader_lost_timeout", 3, 1, 300);
    LEADER_ELECTION_MAX_WAIT_TIME = iniGetIntCorrectValue(
            ini_ctx, "max_wait_time", 30, 1, 3600);
    LEADER_ELECTION_MAX_SHUTDOWN_DURATION = iniGetIntCorrectValue(
            ini_ctx, "max_shutdown_duration", 300, 60, 86400);
    if ((result=sf_load_election_quorum_config(&LEADER_ELECTION_QUORUM,
                    ini_ctx)) == 0)
    {
        result = fcfs_vote_client_init_for_server(
                ini_ctx, &VOTE_NODE_ENABLED);
    }

    return result;
}

static inline int load_replication_quorum_config(IniFullContext *ini_ctx)
{
    ini_ctx->section_name = "data-replication";
    REPLICA_QUORUM_DEACTIVE_ON_FAILURES  = iniGetIntCorrectValue(
            ini_ctx, "deactive_on_failures", 3, 1, 100);
    return sf_load_replication_quorum_config(&REPLICATION_QUORUM, ini_ctx);
}

static int load_cluster_sub_config(const char *cluster_filename)
{
    IniContext ini_context;
    IniFullContext ini_ctx;
    int result;

    if ((result=iniLoadFromFile(cluster_filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load conf file \"%s\" fail, ret code: %d",
                __LINE__, cluster_filename, result);
        return result;
    }

    FAST_INI_SET_FULL_CTX_EX(ini_ctx, cluster_filename,
            "leader-election", &ini_context);
    if ((result=load_leader_election_config(&ini_ctx)) != 0) {
        return result;
    }

    result = load_replication_quorum_config(&ini_ctx);
    iniFreeContext(&ini_context);
    return result;
}

static void calc_my_data_groups_quorum_vars()
{
    FSIdArray *id_array;
    FSClusterDataGroupInfo *data_group;
    int group_id;
    int group_index;
    int i;

    id_array = fs_cluster_cfg_get_my_data_group_ids(&CLUSTER_CONFIG_CTX,
            CLUSTER_MYSELF_PTR->server->id);
    for (i=0; i<id_array->count; i++) {
        group_id = id_array->ids[i];
        group_index = group_id - CLUSTER_DATA_RGOUP_ARRAY.base_id;
        data_group = CLUSTER_DATA_RGOUP_ARRAY.groups + group_index;

        data_group->replica_quorum.need_majority = SF_REPLICATION_QUORUM_NEED_MAJORITY(
                REPLICATION_QUORUM, data_group->data_server_array.count);
        if (data_group->replica_quorum.need_majority && !REPLICA_QUORUM_NEED_MAJORITY) {
            REPLICA_QUORUM_NEED_MAJORITY = true;
        }

        data_group->replica_quorum.need_detect = SF_REPLICATION_QUORUM_NEED_DETECT(
                REPLICATION_QUORUM, data_group->data_server_array.count);
        if (data_group->replica_quorum.need_detect && !REPLICA_QUORUM_NEED_DETECT) {
            REPLICA_QUORUM_NEED_DETECT = true;
        }
    }
}

static int load_cluster_config(IniContext *ini_context, const char *filename,
        char *full_cluster_filename, const int size)
{
    int result;
    char *cluster_config_filename;

    cluster_config_filename = iniGetStrValue(NULL,
            "cluster_config_filename", ini_context);
    if (cluster_config_filename == NULL || *cluster_config_filename == '\0') {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, item \"cluster_config_filename\" "
                "not exist or empty", __LINE__, filename);
        return ENOENT;
    }

    resolve_path(filename, cluster_config_filename,
            full_cluster_filename, size);
    if ((result=fs_cluster_cfg_load_from_ini(&CLUSTER_CONFIG_CTX,
            ini_context, filename)) != 0)
    {
        return result;
    }

    if ((result=load_cluster_sub_config(full_cluster_filename)) != 0) {
        return result;
    }

    if ((result=load_master_election_config(full_cluster_filename)) != 0) {
        return result;
    }
    fs_cluster_cfg_to_log(&CLUSTER_CONFIG_CTX);

    if ((result=server_group_info_init(full_cluster_filename)) != 0) {
        return result;
    }

    calc_my_data_groups_quorum_vars();
    return 0;
}

static int load_slice_binlog_config(IniContext *ini_context,
        const char *filename)
{
    const char *section_name = "slice-binlog";
    int result;
    IniFullContext ini_ctx;

    FAST_INI_SET_FULL_CTX_EX(ini_ctx, filename, section_name, ini_context);
    SLICE_DEDUP_ENABLED = iniGetBoolValue(section_name,
            "dedup_enabled", ini_context, true);
    if ((result=iniGetPercentValue(&ini_ctx, "target_dedup_ratio",
                    &SLICE_DEDUP_RATIO, 0.10)) != 0)
    {
        return result;
    }

    if ((result=get_time_item_from_conf_ex(&ini_ctx, "dedup_time",
                    &SLICE_DEDUP_TIME, 2, 0, false)) != 0)
    {
        return result;
    }

    return 0;
}

static int load_replica_binlog_config(IniContext *ini_context,
        const char *filename)
{
    const char *section_name = "replica-binlog";
    int result;
    IniFullContext ini_ctx;

    FAST_INI_SET_FULL_CTX_EX(ini_ctx, filename, section_name, ini_context);
    REPLICA_KEEP_DAYS = iniGetIntValue(section_name,
            "keep_days", ini_context, 30);

    if ((result=get_time_item_from_conf_ex(&ini_ctx, "delete_time",
                    &REPLICA_DELETE_TIME, 5, 0, false)) != 0)
    {
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
        DATA_PATH_LEN = strlen(SF_G_BASE_PATH_STR) + strlen(data_path) + 1;
        DATA_PATH_STR = (char *)fc_malloc(DATA_PATH_LEN + 1);
        if (DATA_PATH_STR == NULL) {
            return ENOMEM;
        }
        DATA_PATH_LEN = sprintf(DATA_PATH_STR, "%s/%s",
                SF_G_BASE_PATH_STR, data_path);
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
        
        SF_CHOWN_TO_RUNBY_RETURN_ON_ERROR(DATA_PATH_STR);
    }

    return 0;
}

static int load_net_buffer_memory_limit(IniContext *ini_context,
        const char *filename)
{
    int result;
    IniFullContext ini_ctx;
    int64_t total_memory;

    FAST_INI_SET_FULL_CTX_EX(ini_ctx, filename, NULL, ini_context);
    if ((result=iniGetPercentCorrectValue(&ini_ctx,
                    "net_buffer_memory_limit",
                    &NET_BUFFER_MEMORY_LIMIT.ratio,
                    0.20, 0.01, 0.80)) != 0)
    {
        return result;
    }

    if ((result=get_sys_total_mem_size(&total_memory)) != 0) {
        return result;
    }
    NET_BUFFER_MEMORY_LIMIT.value = total_memory *
        NET_BUFFER_MEMORY_LIMIT.ratio;
    if (NET_BUFFER_MEMORY_LIMIT.value < 64 * 1024 * 1024) {
        NET_BUFFER_MEMORY_LIMIT.value = 64 * 1024 * 1024;
        NET_BUFFER_MEMORY_LIMIT.ratio = (double)NET_BUFFER_MEMORY_LIMIT.
            value / (double)total_memory;
    }
    return 0;
}

static void master_election_config_to_string(char *buff, const int size)
{
    int len;

    len = snprintf(buff, size, "master-election {resume_master_role: %d, "
            "failover=%s", RESUME_MASTER_ROLE, (MASTER_ELECTION_FAILOVER ?
                "true" : "false"));
    if (MASTER_ELECTION_FAILOVER) {
        if (MASTER_ELECTION_POLICY == FS_MASTER_ELECTION_POLICY_STRICT_INT) {
            len += snprintf(buff + len, size - len, ", policy=%s",
                    FS_MASTER_ELECTION_POLICY_STRICT_STR);
        } else {
            len += snprintf(buff + len, size - len, ", policy=%s:%d",
                    FS_MASTER_ELECTION_POLICY_TIMEOUT_STR,
                    MASTER_ELECTION_TIMEOUTS);
        }
    }
    len += snprintf(buff + len, size - len, "}");
}

static void slice_binlog_config_to_string(char *buff, const int size)
{
    int len;

    len = snprintf(buff, size, "slice-binlog {dedup_enabled: %d",
            SLICE_DEDUP_ENABLED);
    if (SLICE_DEDUP_ENABLED) {
        len += snprintf(buff + len, size - len, ", target_dedup_ratio=%.2f%%, "
                "dedup_time=%02d:%02d}", SLICE_DEDUP_RATIO * 100.00,
                SLICE_DEDUP_TIME.hour, SLICE_DEDUP_TIME.minute);
    } else {
        len += snprintf(buff + len, size - len, "}");
    }
}

static void replica_binlog_config_to_string(char *buff, const int size)
{
    int len;

    len = snprintf(buff, size, "replica-binlog {"
            "keep_days: %d", REPLICA_KEEP_DAYS);
    if (REPLICA_KEEP_DAYS > 0) {
        len += snprintf(buff + len, size - len, ", delete_time=%02d:%02d}",
                REPLICA_DELETE_TIME.hour, REPLICA_DELETE_TIME.minute);
    } else {
        len += snprintf(buff + len, size - len, "}");
    }
}

static void server_log_configs()
{
    char sz_server_config[1024];
    char sz_global_config[512];
    char sz_slowlog_config[256];
    char sz_service_config[128];
    char sz_cluster_config[128];
    char sz_replica_config[128];
    char sz_melection_config[128];
    char sz_replica_binlog_config[128];
    char sz_slice_binlog_config[128];
    char sz_auth_config[1024];

    sf_global_config_to_string(sz_global_config, sizeof(sz_global_config));

    sf_slow_log_config_to_string(&SLOW_LOG_CFG, "slow-log",
            sz_slowlog_config, sizeof(sz_slowlog_config));

    sf_context_config_to_string(&g_sf_context,
            sz_service_config, sizeof(sz_service_config));
    sf_context_config_to_string(&CLUSTER_SF_CTX,
            sz_cluster_config, sizeof(sz_cluster_config));
    sf_context_config_to_string(&REPLICA_SF_CTX,
            sz_replica_config, sizeof(sz_replica_config));

    fcfs_auth_for_server_cfg_to_string(&AUTH_CTX,
            sz_auth_config, sizeof(sz_auth_config));

    master_election_config_to_string(sz_melection_config,
            sizeof(sz_melection_config));

    slice_binlog_config_to_string(sz_slice_binlog_config,
            sizeof(sz_slice_binlog_config));
    replica_binlog_config_to_string(sz_replica_binlog_config,
            sizeof(sz_replica_binlog_config));

    snprintf(sz_server_config, sizeof(sz_server_config),
            "my server id = %d, cluster server group id = %d, "
            "data_path = %s, data_threads = %d, "
            "replica_channels_between_two_servers = %d, "
            "recovery_threads_per_data_group = %d, "
            "recovery_max_queue_depth = %d, "
            "rebuild_threads = %d, "
            "binlog_buffer_size = %d KB, "
            "net_buffer_memory_limit = %.2f%%, "
            "local_binlog_check_last_seconds = %d s, "
            "slave_binlog_check_last_rows = %d, "
            "cluster server count = %d, "
            "idempotency_max_channel_count: %d, "
            "leader-election {quorum: %s, "
            "vote_node_enabled: %d, "
            "leader_lost_timeout: %ds, "
            "max_wait_time: %ds, "
            "max_shutdown_duration: %ds}",
            CLUSTER_MY_SERVER_ID, CLUSTER_SERVER_GROUP_ID,
            DATA_PATH_STR, DATA_THREAD_COUNT,
            REPLICA_CHANNELS_BETWEEN_TWO_SERVERS,
            RECOVERY_THREADS_PER_DATA_GROUP,
            RECOVERY_MAX_QUEUE_DEPTH,
            DATA_REBUILD_THREADS,
            BINLOG_BUFFER_SIZE / 1024,
            NET_BUFFER_MEMORY_LIMIT.ratio * 100,
            LOCAL_BINLOG_CHECK_LAST_SECONDS,
            SLAVE_BINLOG_CHECK_LAST_ROWS,
            FC_SID_SERVER_COUNT(SERVER_CONFIG_CTX),
            SF_IDEMPOTENCY_MAX_CHANNEL_COUNT,
            sf_get_election_quorum_caption(LEADER_ELECTION_QUORUM),
            VOTE_NODE_ENABLED,
            LEADER_ELECTION_LOST_TIMEOUT,
            LEADER_ELECTION_MAX_WAIT_TIME,
            LEADER_ELECTION_MAX_SHUTDOWN_DURATION);

    logInfo("faststore V%d.%d.%d, %s, %s, service: {%s}, cluster: {%s}, "
            "replica: {%s}, %s, %s, data-replication {quorum: %s, "
            "deactive_on_failures: %d, quorum_need_majority: %d, "
            "quorum_need_detect: %d}, %s, %s, %s",
            g_fs_global_vars.version.major, g_fs_global_vars.version.minor,
            g_fs_global_vars.version.patch, sz_global_config,
            sz_slowlog_config, sz_service_config, sz_cluster_config,
            sz_replica_config, sz_server_config, sz_melection_config,
            sf_get_replication_quorum_caption(REPLICATION_QUORUM),
            REPLICA_QUORUM_DEACTIVE_ON_FAILURES,
            REPLICA_QUORUM_NEED_MAJORITY, REPLICA_QUORUM_NEED_DETECT,
            sz_slice_binlog_config, sz_replica_binlog_config, sz_auth_config);
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

static int init_net_retry_config(const char *config_filename)
{
    IniFullContext ini_ctx;
    IniContext ini_context;
    int result;

    if ((result=iniLoadFromFile(config_filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load conf file \"%s\" fail, ret code: %d",
                __LINE__, config_filename, result);
        return result;
    }

    FAST_INI_SET_FULL_CTX_EX(ini_ctx, config_filename,
            "data_recovery", &ini_context);
    result = sf_load_net_retry_config(&g_fs_client_vars.
            client_ctx.common_cfg.net_retry_cfg, &ini_ctx);

    iniFreeContext(&ini_context);
    return result;
}

static int server_init_client(const char *config_filename)
{
    int result;
    const bool bg_thread_enabled = false;

    /* set read rule for store path rebuild */
    g_fs_client_vars.client_ctx.common_cfg.read_rule =
        sf_data_read_rule_any_available;

    g_fs_client_vars.client_ctx.common_cfg.connect_timeout =
        SF_G_CONNECT_TIMEOUT;
    g_fs_client_vars.client_ctx.common_cfg.network_timeout =
        SF_G_NETWORK_TIMEOUT;
    snprintf(g_fs_client_vars.base_path,
            sizeof(g_fs_client_vars.base_path),
            "%s", SF_G_BASE_PATH_STR);
    g_fs_client_vars.client_ctx.cluster_cfg.ptr = &CLUSTER_CONFIG_CTX;
    g_fs_client_vars.client_ctx.cluster_cfg.group_index = g_fs_client_vars.
        client_ctx.cluster_cfg.ptr->replica_group_index;
    if ((result=init_net_retry_config(config_filename)) != 0) {
        return result;
    }

    if ((result=fs_simple_connection_manager_init(&g_fs_client_vars.
                    client_ctx, &g_fs_client_vars.client_ctx.cm,
                    bg_thread_enabled)) != 0)
    {
        return result;
    }
    g_fs_client_vars.client_ctx.auth = AUTH_CTX;
    g_fs_client_vars.client_ctx.inited = true;
    g_fs_client_vars.client_ctx.is_simple_conn_mananger = true;

    return 0;
}

int server_load_config(const char *filename)
{
    IniContext ini_context;
    IniFullContext full_ini_ctx;
    char full_cluster_filename[PATH_MAX];
    char *rebuild_threads;
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

    rebuild_threads = iniGetStrValue(full_ini_ctx.section_name,
            "rebuild_threads", full_ini_ctx.context);
    if (rebuild_threads == NULL || strcmp(rebuild_threads,
                "RECOVERY_THREADS") == 0)
    {
        DATA_REBUILD_THREADS = RECOVERY_THREADS_PER_DATA_GROUP *
            RECOVERY_MAX_QUEUE_DEPTH;
        if (DATA_REBUILD_THREADS > FS_MAX_DATA_REBUILD_THREADS) {
            DATA_REBUILD_THREADS = FS_MAX_DATA_REBUILD_THREADS;
        }
    } else {
        DATA_REBUILD_THREADS = iniGetIntCorrectValue(&full_ini_ctx,
                "rebuild_threads", FS_DEFAULT_DATA_REBUILD_THREADS,
                FS_MIN_DATA_REBUILD_THREADS, FS_MAX_DATA_REBUILD_THREADS);
    }

    LOCAL_BINLOG_CHECK_LAST_SECONDS = iniGetIntValue(NULL,
            "local_binlog_check_last_seconds", &ini_context,
            FS_DEFAULT_LOCAL_BINLOG_CHECK_LAST_SECONDS);

    SLAVE_BINLOG_CHECK_LAST_ROWS = iniGetIntCorrectValue(
            &full_ini_ctx, "slave_binlog_check_last_rows",
            FS_DEFAULT_SLAVE_BINLOG_CHECK_LAST_ROWS,
            FS_MIN_SLAVE_BINLOG_CHECK_LAST_ROWS,
            FS_MAX_SLAVE_BINLOG_CHECK_LAST_ROWS);

    if ((result=load_net_buffer_memory_limit(&ini_context, filename)) != 0) {
        return result;
    }

    if ((result=load_binlog_buffer_size(&ini_context, filename)) != 0) {
        return result;
    }

    if ((result=load_cluster_config(&ini_context, filename,
                    full_cluster_filename, sizeof(
                        full_cluster_filename))) != 0)
    {
        return result;
    }

    CLUSTER_SERVER_GROUP_ID = fs_cluster_cfg_get_min_server_group_id(
            &CLUSTER_CONFIG_CTX, CLUSTER_MY_SERVER_ID);
    if (CLUSTER_SERVER_GROUP_ID < 0) {
        result = -1 * CLUSTER_SERVER_GROUP_ID;
        logError("file: "__FILE__", line: %d, "
                "get min server group id fail, "
                "errno: %d, error info: %s",
                __LINE__, result, STRERROR(result));
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

    if ((result=load_slice_binlog_config(&ini_context, filename)) != 0) {
        return result;
    }

    if ((result=load_replica_binlog_config(&ini_context, filename)) != 0) {
        return result;
    }

    fcfs_auth_client_init_full_ctx(&AUTH_CTX);
    if ((result=fcfs_auth_for_server_init(&AUTH_CTX, &full_ini_ctx,
                    full_cluster_filename)) != 0)
    {
        return result;
    }

    iniFreeContext(&ini_context);
    
    if ((SYSTEM_CPU_COUNT=get_sys_cpu_count()) <= 0) {
        logCrit("file: "__FILE__", line: %d, "
                "get CPU count fail", __LINE__);
        return EINVAL;
    }

    g_server_global_vars.replica.active_test_interval = (int)
        ceil(SF_G_NETWORK_TIMEOUT / 2.00);
    if (g_server_global_vars.replica.active_test_interval == 0) {
        g_server_global_vars.replica.active_test_interval = 1;
    }

    load_local_host_ip_addrs();
    server_log_configs();
    storage_config_to_log(&STORAGE_CFG);

    return server_init_client(filename);
}
