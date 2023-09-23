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
#include <dlfcn.h>
#include "fastcommon/ini_file_reader.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/local_ip_func.h"
#include "fastcommon/system_info.h"
#include "sf/sf_global.h"
#include "sf/sf_service.h"
#include "sf/sf_func.h"
#include "fastcfs/auth/fcfs_auth_for_server.h"
#include "fastcfs/vote/fcfs_vote_client.h"
#include "common/fs_proto.h"
#include "client/fs_client.h"
#include "server_global.h"
#include "server_binlog.h"
#include "server_group_info.h"
#include "server_func.h"

#define EVENT_DEALER_THREAD_DEFAULT_COUNT  4
#define EVENT_DEALER_THREAD_MIN_COUNT      1
#define EVENT_DEALER_THREAD_MAX_COUNT     64

#define FS_SYSTEM_FLAG_FILENAME    ".fstore.dat"

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

static int load_master_election_config(IniFullContext *ini_ctx)
{
    char *policy;
    char *remain;
    char *endptr;

    RESUME_MASTER_ROLE = iniGetBoolValue(ini_ctx->section_name,
            "resume_master_role", ini_ctx->context, true);
    MASTER_ELECTION_TIMEOUTS = FS_DEFAULT_MASTER_ELECTION_TIMEOUTS;
    MASTER_ELECTION_FAILOVER = iniGetBoolValue(ini_ctx->section_name,
            "failover", ini_ctx->context, true);
    policy = iniGetStrValue(ini_ctx->section_name, "policy", ini_ctx->context);
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
                __LINE__, ini_ctx->filename, ini_ctx->section_name);
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
                "expect colon (:) after %s", __LINE__, ini_ctx->filename,
                ini_ctx->section_name, FS_MASTER_ELECTION_POLICY_TIMEOUT_STR);
        return EINVAL;
    }
    remain++;  //skip colon
    trim_left(remain);
    if (*remain == '\0') {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, section: %s, item: policy, "
                "value: %s, expect timeout number after colon",
                __LINE__, ini_ctx->filename, ini_ctx->section_name, policy);
        return EINVAL;
    }

    MASTER_ELECTION_TIMEOUTS = strtol(remain, &endptr, 10);
    if ((endptr != NULL && *endptr != '\0') ||
            (MASTER_ELECTION_TIMEOUTS <= 0))
    {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, section: %s, item: policy, "
                "invalid timeout number: %s, length: %d",
                __LINE__, ini_ctx->filename, ini_ctx->section_name,
                remain, (int)strlen(remain));
        return EINVAL;
    }

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

    ini_ctx.section_name = "master-election";
    if ((result=load_master_election_config(&ini_ctx)) != 0) {
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

int fs_write_to_sys_file()
{
    int len;
    char filename[PATH_MAX];
    char buff[256];

    snprintf(filename, sizeof(filename), "%s/%s",
            DATA_PATH_STR, FS_SYSTEM_FLAG_FILENAME);
    len = sprintf(buff, "file_block_size=%d\n"
            "slice_remove_files=%u\n",
            FILE_BLOCK_SIZE, SLICE_REMOVE_FILES);
    return safeWriteToFile(filename, buff, len);
}

static int load_from_sys_file()
{
    char filename[PATH_MAX];
    IniContext ini_context;
    int file_block_size;
    int result;

    snprintf(filename, sizeof(filename), "%s/%s",
            DATA_PATH_STR, FS_SYSTEM_FLAG_FILENAME);
    if (access(filename, F_OK) != 0) {
        result = errno != 0 ? errno : EPERM;
        if (result == ENOENT) {
            return fs_write_to_sys_file();
        }

        logError("file: "__FILE__", line: %d, "
                "access file %s fail, errno: %d, error info: %s",
                __LINE__, filename, result, STRERROR(result));
        return result;
    }

    if ((result=iniLoadFromFile(filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load conf file \"%s\" fail, ret code: %d",
                __LINE__, filename, result);
        return result;
    }

    file_block_size = iniGetIntValue(NULL,
            "file_block_size", &ini_context, 0);
    if (file_block_size != FILE_BLOCK_SIZE) {
        logError("file: "__FILE__", line: %d, "
                "file_block_size in cluster.conf changed, old: %d KB, "
                "new: %d KB, you must restore file_block_size to %d KB",
                __LINE__, file_block_size / 1024, FILE_BLOCK_SIZE / 1024,
                file_block_size / 1024);
        return EINVAL;
    }

    SLICE_REMOVE_FILES = iniGetIntValue(NULL,
            "slice_remove_files", &ini_context, 0);
    if (SLICE_REMOVE_FILES > 0 && !STORAGE_ENABLED) {
        logError("file: "__FILE__", line: %d, can't disable storage "
                "engine because slice_remove_files: %d > 0!",
                __LINE__, SLICE_REMOVE_FILES);
        return EINVAL;
    }

    iniFreeContext(&ini_context);
    return 0;
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

    SLICE_KEEP_DAYS = iniGetIntValue(section_name,
            "keep_days", ini_context, 30);

    if ((result=get_time_item_from_conf_ex(&ini_ctx, "delete_time",
                    &SLICE_DELETE_TIME, 5, 0, false)) != 0)
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

static int load_net_buffer_memory_limit(IniContext *ini_context,
        const char *filename)
{
    int result;
    IniFullContext ini_ctx;

    FAST_INI_SET_FULL_CTX_EX(ini_ctx, filename, NULL, ini_context);
    if ((result=iniGetPercentCorrectValue(&ini_ctx,
                    "net_buffer_memory_limit",
                    &NET_BUFFER_MEMORY_LIMIT.ratio,
                    0.20, 0.01, 0.80)) != 0)
    {
        return result;
    }

    NET_BUFFER_MEMORY_LIMIT.value = SYSTEM_TOTAL_MEMORY *
        NET_BUFFER_MEMORY_LIMIT.ratio;
    if (NET_BUFFER_MEMORY_LIMIT.value < 64 * 1024 * 1024) {
        NET_BUFFER_MEMORY_LIMIT.value = 64 * 1024 * 1024;
        NET_BUFFER_MEMORY_LIMIT.ratio = (double)NET_BUFFER_MEMORY_LIMIT.
            value / (double)SYSTEM_TOTAL_MEMORY;
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
    if (STORAGE_ENABLED) {
        if (SLICE_KEEP_DAYS > 0) {
            snprintf(buff, size, "slice-binlog {keep_days: %d, "
                    "delete_time=%02d:%02d}", SLICE_KEEP_DAYS,
                    SLICE_DELETE_TIME.hour, SLICE_DELETE_TIME.minute);
        } else {
            snprintf(buff, size, "slice-binlog {keep_days: %d}",
                    SLICE_KEEP_DAYS);
        }
    } else if (SLICE_DEDUP_ENABLED) {
        snprintf(buff, size, "slice-binlog {dedup_enabled: %d, "
                "target_dedup_ratio=%.2f%%, dedup_time=%02d:%02d}",
                SLICE_DEDUP_ENABLED, SLICE_DEDUP_RATIO * 100.00,
                SLICE_DEDUP_TIME.hour, SLICE_DEDUP_TIME.minute);
    } else {
        snprintf(buff, size, "slice-binlog {dedup_enabled: %d}",
                SLICE_DEDUP_ENABLED);
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
    char sz_server_config[2048];
    char sz_global_config[512];
    char sz_slowlog_config[256];
    char sz_service_config[128];
    char sz_cluster_config[128];
    char sz_replica_config[128];
    char sz_melection_config[128];
    char sz_replica_binlog_config[128];
    char sz_slice_binlog_config[128];
    char sz_auth_config[1024];
    int len;

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

    len = snprintf(sz_server_config, sizeof(sz_server_config),
            "my server id = %d, cluster server group id = %d, "
            "file_block_size: %d KB, data_path = %s, data_threads = %d, "
            "replica_channels_between_two_servers = %d, "
            "recovery_concurrent = %d, "
            "recovery_threads_per_data_group = %d, "
            "recovery_max_queue_depth = %d, "
            "rebuild_threads = %d, "
            "binlog_buffer_size = %d KB, "
            "binlog_call_fsync = %d, "
            "net_buffer_memory_limit = %.2f%%, "
            "local_binlog_check_last_seconds = %d s, "
            "slave_binlog_check_last_rows = %d, "
            "cluster server count = %d, "
            "idempotency_max_channel_count: %d, "
            "write_to_cache: %d, "
            "cache_flush_max_delay: %d s, "
            "object_block_hashtable_capacity: %"PRId64", "
            "object_block_shared_lock_count: %d, "
            "trunk_index_dump_base_time: %02d:%02d, "
            "trunk_index_dump_interval: %d s, "
            "leader-election {quorum: %s, "
            "vote_node_enabled: %d, "
            "leader_lost_timeout: %ds, "
            "max_wait_time: %ds, "
            "max_shutdown_duration: %ds}, "
            "storage-engine { enabled: %d",
            CLUSTER_MY_SERVER_ID, CLUSTER_SERVER_GROUP_ID,
            FILE_BLOCK_SIZE / 1024,
            DATA_PATH_STR, DATA_THREAD_COUNT,
            REPLICA_CHANNELS_BETWEEN_TWO_SERVERS,
            RECOVERY_CONCURRENT,
            RECOVERY_THREADS_PER_DATA_GROUP,
            RECOVERY_MAX_QUEUE_DEPTH,
            DATA_REBUILD_THREADS,
            BINLOG_BUFFER_SIZE / 1024,
            BINLOG_CALL_FSYNC,
            NET_BUFFER_MEMORY_LIMIT.ratio * 100,
            LOCAL_BINLOG_CHECK_LAST_SECONDS,
            SLAVE_BINLOG_CHECK_LAST_ROWS,
            FC_SID_SERVER_COUNT(SERVER_CONFIG_CTX),
            SF_IDEMPOTENCY_MAX_CHANNEL_COUNT,
            WRITE_TO_CACHE, CACHE_FLUSH_MAX_DELAY,
            OB_HASHTABLE_CAPACITY, OB_SHARED_LOCK_COUNT,
            DATA_CFG.trunk_index_dump_base_time.hour,
            DATA_CFG.trunk_index_dump_base_time.minute,
            DATA_CFG.trunk_index_dump_interval,
            sf_get_election_quorum_caption(LEADER_ELECTION_QUORUM),
            VOTE_NODE_ENABLED,
            LEADER_ELECTION_LOST_TIMEOUT,
            LEADER_ELECTION_MAX_WAIT_TIME,
            LEADER_ELECTION_MAX_SHUTDOWN_DURATION,
            STORAGE_ENABLED);

    if (STORAGE_ENABLED) {
        len += snprintf(sz_server_config + len, sizeof(sz_server_config) - len,
                ", library: %s, data_path: %s, block_binlog_subdirs: %d"
                ", block_segment_hashtable_capacity: %d"
                ", block_segment_shared_lock_count: %d"
                ", event_dealer_thread_count: %d"
                ", batch_store_on_modifies: %d, batch_store_interval: %d s"
                ", trunk_index_dump_base_time: %02d:%02d"
                ", trunk_index_dump_interval: %d s"
                ", eliminate_interval: %d s, memory_limit: %.2f%%}",
                STORAGE_ENGINE_LIBRARY, STORAGE_PATH_STR,
                BLOCK_BINLOG_SUBDIRS, g_server_global_vars->
                slice_storage.cfg.block_segment.htable_capacity,
                g_server_global_vars->slice_storage.cfg.
                block_segment.shared_lock_count,
                EVENT_DEALER_THREAD_COUNT,
                BATCH_STORE_ON_MODIFIES, BATCH_STORE_INTERVAL,
                TRUNK_INDEX_DUMP_BASE_TIME.hour,
                TRUNK_INDEX_DUMP_BASE_TIME.minute,
                TRUNK_INDEX_DUMP_INTERVAL,
                BLOCK_ELIMINATE_INTERVAL,
                STORAGE_MEMORY_TOTAL_LIMIT * 100);
    } else {
        snprintf(sz_server_config + len, sizeof(sz_server_config) - len, "}");
    }

    logInfo("faststore V%d.%d.%d, %s, %s, service: {%s}, cluster: {%s}, "
            "replica: {%s}", g_fs_global_vars.version.major,
            g_fs_global_vars.version.minor, g_fs_global_vars.version.patch,
            sz_global_config, sz_slowlog_config, sz_service_config,
            sz_cluster_config, sz_replica_config);
    logInfo("%s, %s, data-replication {quorum: %s, "
            "deactive_on_failures: %d, quorum_need_majority: %d, "
            "quorum_need_detect: %d}, %s, %s, %s",
            sz_server_config, sz_melection_config,
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
    const bool bg_thread_enabled = false;
    int result;

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
    fs_set_file_block_size(&g_fs_client_vars.client_ctx, FILE_BLOCK_SIZE);
    if ((result=init_net_retry_config(config_filename)) != 0) {
        return result;
    }

    if (CLUSTER_SERVER_GROUP->comm_type != fc_comm_type_sock ||
            REPLICA_SERVER_GROUP->comm_type != fc_comm_type_sock)
    {
        if ((result=conn_pool_global_init_for_rdma()) != 0) {
            return result;
        }
    }

    if ((result=fs_simple_connection_manager_init(&g_fs_client_vars.
                    client_ctx, &g_fs_client_vars.client_ctx.cm,
                    bg_thread_enabled)) != 0)
    {
        return result;
    }
    sf_connection_manager_set_exclude_server_id(&g_fs_client_vars.
            client_ctx.cm, CLUSTER_MYSELF_PTR->server->id);
    g_fs_client_vars.client_ctx.auth = AUTH_CTX;
    g_fs_client_vars.client_ctx.inited = true;
    g_fs_client_vars.client_ctx.is_simple_conn_mananger = true;

    return 0;
}

#define LOAD_API(var, fname) \
    do { \
        var = (fname##_func)dlsym(dlhandle, #fname); \
        if (var == NULL) {  \
            logError("file: "__FILE__", line: %d, "  \
                    "dlsym api %s fail, error info: %s", \
                    __LINE__, #fname, dlerror()); \
            return ENOENT; \
        } \
    } while (0)


static int load_storage_engine_apis()
{
    void *dlhandle;

    dlhandle = dlopen(STORAGE_ENGINE_LIBRARY, RTLD_LAZY);
    if (dlhandle == NULL) {
        logError("file: "__FILE__", line: %d, "
                "dlopen %s fail, error info: %s", __LINE__,
                STORAGE_ENGINE_LIBRARY, dlerror());
        return EFAULT;
    }

    LOAD_API(STORAGE_ENGINE_INIT_API, fs_storage_engine_init);
    LOAD_API(STORAGE_ENGINE_START_API, fs_storage_engine_start);
    LOAD_API(STORAGE_ENGINE_TERMINATE_API, fs_storage_engine_terminate);
    LOAD_API(STORAGE_ENGINE_STORE_API, fs_storage_engine_store);
    LOAD_API(STORAGE_ENGINE_REDO_API, fs_storage_engine_redo);
    LOAD_API(STORAGE_ENGINE_FETCH_API, fs_storage_engine_fetch);
    LOAD_API(STORAGE_ENGINE_WALK_API, fs_storage_engine_walk);

    return 0;
}

static int load_storage_engine_parames(IniFullContext *ini_ctx)
{
    int result;
    char *library;

    ini_ctx->section_name = "storage-engine";
    STORAGE_ENABLED = iniGetBoolValue(ini_ctx->section_name,
            "enabled", ini_ctx->context, false);
    if (!STORAGE_ENABLED) {
        return 0;
    }

    library = iniGetStrValue(ini_ctx->section_name,
            "library", ini_ctx->context);
    if (library == NULL) {
        library = "libfsstorage.so";
    } else if (*library == '\0') {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, section: %s, empty library!",
                __LINE__, ini_ctx->filename, ini_ctx->section_name);
        return EINVAL;
    }
    if ((STORAGE_ENGINE_LIBRARY=fc_strdup(library)) == NULL) {
        return ENOMEM;
    }
    if ((result=load_storage_engine_apis()) != 0) {
        return result;
    }

    if ((result=sf_load_data_path_config_ex(ini_ctx, "data_path",
                    "db", &STORAGE_PATH)) != 0)
    {
        return result;
    }

    if (fc_string_equals(&STORAGE_PATH, &DATA_PATH)) {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, section: %s, storage path MUST be "
                "different from the global data path", __LINE__,
                ini_ctx->filename, ini_ctx->section_name);
        return EINVAL;
    }

    BLOCK_BINLOG_SUBDIRS = iniGetIntCorrectValue(ini_ctx,
            "block_binlog_subdirs", FS_BLOCK_BINLOG_DEFAULT_SUBDIRS,
            FS_BLOCK_BINLOG_MIN_SUBDIRS, FS_BLOCK_BINLOG_MAX_SUBDIRS);

    BATCH_STORE_ON_MODIFIES = iniGetIntValue(ini_ctx->section_name,
            "batch_store_on_modifies", ini_ctx->context,
            FS_DEFAULT_BATCH_STORE_ON_MODIFIES);

    BATCH_STORE_INTERVAL = iniGetIntValue(ini_ctx->section_name,
            "batch_store_interval", ini_ctx->context,
            FS_DEFAULT_BATCH_STORE_INTERVAL);

    TRUNK_INDEX_DUMP_INTERVAL = iniGetIntValue(ini_ctx->section_name,
            "trunk_index_dump_interval", ini_ctx->context,
            FS_DEFAULT_TRUNK_INDEX_DUMP_INTERVAL);

    if ((result=get_time_item_from_conf_ex(ini_ctx,
                    "trunk_index_dump_base_time",
                    &TRUNK_INDEX_DUMP_BASE_TIME,
                    0, 0, false)) != 0)
    {
        return result;
    }

    BLOCK_ELIMINATE_INTERVAL = iniGetIntValue(ini_ctx->section_name,
            "eliminate_interval", ini_ctx->context,
            FS_DEFAULT_ELIMINATE_INTERVAL);
    if ((result=iniGetPercentValue(ini_ctx, "memory_limit",
                    &STORAGE_MEMORY_TOTAL_LIMIT, 0.60)) != 0)
    {
        return result;
    }

    if (STORAGE_MEMORY_TOTAL_LIMIT < 0.01) {
        logWarning("file: "__FILE__", line: %d, "
                "memory_limit: %%%.2f is too small, set to 1%%",
                __LINE__, STORAGE_MEMORY_TOTAL_LIMIT);
        STORAGE_MEMORY_TOTAL_LIMIT = 0.01;
    }
    if (STORAGE_MEMORY_TOTAL_LIMIT > 0.99) {
        logWarning("file: "__FILE__", line: %d, "
                "memory_limit: %%%.2f is too large, set to 99%%",
                __LINE__, STORAGE_MEMORY_TOTAL_LIMIT);
        STORAGE_MEMORY_TOTAL_LIMIT = 0.99;
    }

    g_server_global_vars->slice_storage.cfg.block_segment.htable_capacity =
        iniGetIntCorrectValue(ini_ctx, "block_segment_hashtable_capacity",
                1361, 163, 1403641);
    g_server_global_vars->slice_storage.cfg.block_segment.shared_lock_count =
        iniGetIntCorrectValue(ini_ctx, "block_segment_shared_lock_count",
                163, 1, 1361);

    EVENT_DEALER_THREAD_COUNT = iniGetIntCorrectValue(ini_ctx,
            "event_dealer_thread_count", EVENT_DEALER_THREAD_DEFAULT_COUNT,
            EVENT_DEALER_THREAD_MIN_COUNT, EVENT_DEALER_THREAD_MAX_COUNT);

    return 0;
}

static int load_storage_cfg(IniContext *ini_context, const char *filename)
{
    int result;
    int shared_lock_count;
    char *storage_config_filename;
    char full_filename[PATH_MAX];
    IniFullContext ini_ctx;

    FAST_INI_SET_FULL_CTX_EX(ini_ctx, filename, NULL, ini_context);
    DATA_CFG.path = DATA_PATH;
    DATA_CFG.binlog_buffer_size = BINLOG_BUFFER_SIZE;
    DATA_CFG.binlog_subdirs = 0;
    DATA_CFG.trunk_index_dump_interval = iniGetIntValue(NULL,
            "trunk_index_dump_interval", ini_ctx.context,
            FS_DEFAULT_TRUNK_INDEX_DUMP_INTERVAL);
    if ((result=get_time_item_from_conf_ex(&ini_ctx,
                    "trunk_index_dump_base_time",
                    &DATA_CFG.trunk_index_dump_base_time,
                    0, 0, false)) != 0)
    {
        return result;
    }

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
    STORAGE_FILENAME = fc_strdup(full_filename);
    if (STORAGE_FILENAME == NULL) {
        return ENOMEM;
    }

    WRITE_TO_CACHE = iniGetBoolValue(NULL,
            "write_to_cache", ini_context, true);
    if (WRITE_TO_CACHE) {
        CACHE_FLUSH_MAX_DELAY = iniGetIntValue(NULL,
                "cache_flush_max_delay", ini_context, 3);
        if (CACHE_FLUSH_MAX_DELAY <= 0) {
            logWarning("file: "__FILE__", line: %d, "
                    "config file: %s, item \"cache_flush_max_delay\": "
                    "%d is invalid, set to default: %d", __LINE__,
                    filename, CACHE_FLUSH_MAX_DELAY, 3);
            CACHE_FLUSH_MAX_DELAY = 3;
        }
    }

    OB_HASHTABLE_CAPACITY = iniGetInt64Value(NULL,
            "object_block_hashtable_capacity",
            ini_context, 11229331);
    if (OB_HASHTABLE_CAPACITY <= 0) {
        logWarning("file: "__FILE__", line: %d, "
                "config file: %s, item \"object_block_hashtable_capacity\": "
                "%"PRId64" is invalid, set to default: %d", __LINE__,
                filename, OB_HASHTABLE_CAPACITY, 11229331);
        OB_HASHTABLE_CAPACITY = 11229331;
    }

    shared_lock_count = iniGetIntValue(NULL,
            "object_block_shared_lock_count",
            ini_context, 163);
    if (shared_lock_count <= 0) {
        logWarning("file: "__FILE__", line: %d, config file: %s, "
                "item \"object_block_shared_lock_count\": %d "
                "is invalid, set to default: %d", __LINE__,
                filename, shared_lock_count, 163);
        OB_SHARED_LOCK_COUNT = 163;
    } else if (!fc_is_prime(shared_lock_count)) {
        OB_SHARED_LOCK_COUNT = fc_ceil_prime(shared_lock_count);
        logInfo("file: "__FILE__", line: %d, config file: %s, "
                "item \"object_block_shared_lock_count\": %d "
                "is not a prime number, set to %d", __LINE__,
                filename, shared_lock_count, OB_SHARED_LOCK_COUNT);
    } else {
        OB_SHARED_LOCK_COUNT = shared_lock_count;
    }

    return 0;
}

int server_load_config(const char *filename)
{
    IniContext ini_context;
    IniFullContext full_ini_ctx;
    char full_cluster_filename[PATH_MAX];
    DADataConfig data_cfg;
    char *rebuild_threads;
    FCServerGroupInfo *server_group;
    SFNetworkHandler *rdma_handler;
    int result;

    if ((result=iniLoadFromFile(filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load conf file \"%s\" fail, ret code: %d",
                __LINE__, filename, result);
        return result;
    }

    FAST_INI_SET_FULL_CTX_EX(full_ini_ctx, filename, NULL, &ini_context);
    if ((result=sf_load_data_path_config(&full_ini_ctx, &DATA_PATH)) != 0) {
        return result;
    }

    if ((result=load_cluster_config(&ini_context, filename,
                    full_cluster_filename, sizeof(
                        full_cluster_filename))) != 0)
    {
        return result;
    }

    server_group = fc_server_get_group_by_index(
            &SERVER_CONFIG_CTX, SERVICE_GROUP_INDEX);
    if ((result=sf_load_config("fs_serverd", server_group->comm_type,
                    filename, &ini_context, "service",
                    FS_SERVER_DEFAULT_SERVICE_PORT,
                    FS_SERVER_DEFAULT_SERVICE_PORT,
                    SERVER_CONFIG_CTX.buffer_size,
                    FS_TASK_BUFFER_FRONT_PADDING_SIZE)) != 0)
    {
        return result;
    }
    sf_service_set_smart_polling(&server_group->smart_polling);

    CLUSTER_SERVER_GROUP = fc_server_get_group_by_index(
            &SERVER_CONFIG_CTX, CLUSTER_GROUP_INDEX);
    if ((result=sf_load_context_from_config(&CLUSTER_SF_CTX,
                    CLUSTER_SERVER_GROUP->comm_type, filename,
                    &ini_context, "cluster",
                    FS_SERVER_DEFAULT_CLUSTER_PORT,
                    FS_SERVER_DEFAULT_CLUSTER_PORT)) != 0)
    {
        return result;
    }
    sf_service_set_smart_polling_ex(&CLUSTER_SF_CTX,
            &CLUSTER_SERVER_GROUP->smart_polling);

    REPLICA_SERVER_GROUP = fc_server_get_group_by_index(
            &SERVER_CONFIG_CTX, REPLICA_GROUP_INDEX);
    if ((result=sf_load_context_from_config(&REPLICA_SF_CTX,
                    REPLICA_SERVER_GROUP->comm_type, filename,
                    &ini_context, "replica",
                    FS_SERVER_DEFAULT_REPLICA_PORT,
                    FS_SERVER_DEFAULT_REPLICA_PORT)) != 0)
    {
        return result;
    }
    sf_service_set_smart_polling_ex(&REPLICA_SF_CTX,
            &REPLICA_SERVER_GROUP->smart_polling);

    //fs_cluster_cfg_to_log(&CLUSTER_CONFIG_CTX);
    if ((result=server_group_info_init(full_cluster_filename)) != 0) {
        return result;
    }
    calc_my_data_groups_quorum_vars();

    REPLICA_NET_HANDLER = sf_get_first_network_handler_ex(&REPLICA_SF_CTX);
    if ((rdma_handler=sf_get_rdma_network_handler3(&g_sf_context,
                    &CLUSTER_SF_CTX, &REPLICA_SF_CTX)) != NULL)
    {
        if ((result=sf_alloc_rdma_pd(&g_sf_context,
                        &SERVICE_GROUP_ADDRESS_ARRAY(
                            CLUSTER_MYSELF_PTR->server))) != 0)
        {
            return result;
        }
        if ((result=sf_alloc_rdma_pd(&CLUSTER_SF_CTX,
                        &CLUSTER_GROUP_ADDRESS_ARRAY(
                            CLUSTER_MYSELF_PTR->server))) != 0)
        {
            return result;
        }
        if ((result=sf_alloc_rdma_pd(&REPLICA_SF_CTX,
                        &REPLICA_GROUP_ADDRESS_ARRAY(
                            CLUSTER_MYSELF_PTR->server))) != 0)
        {
            return result;
        }

        TASK_PADDING_SIZE = rdma_handler->get_connection_size();
        RDMA_INIT_CONNECTION = rdma_handler->init_connection;
        RDMA_PD = rdma_handler->pd;
    }

    DATA_THREAD_COUNT = iniGetIntCorrectValue(&full_ini_ctx,
            "data_threads", FS_DEFAULT_DATA_THREAD_COUNT,
            FS_MIN_DATA_THREAD_COUNT, FS_MAX_DATA_THREAD_COUNT);

    REPLICA_CHANNELS_BETWEEN_TWO_SERVERS = iniGetIntCorrectValue(
            &full_ini_ctx, "replica_channels_between_two_servers",
            FS_DEFAULT_REPLICA_CHANNELS_BETWEEN_TWO_SERVERS,
            FS_MIN_REPLICA_CHANNELS_BETWEEN_TWO_SERVERS,
            FS_MAX_REPLICA_CHANNELS_BETWEEN_TWO_SERVERS);

    RECOVERY_CONCURRENT = iniGetIntCorrectValue(&full_ini_ctx,
            "recovery_concurrent", FS_DEFAULT_RECOVERY_CONCURRENT,
            FS_MIN_RECOVERY_CONCURRENT, FS_MAX_RECOVERY_CONCURRENT);

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

    BINLOG_CALL_FSYNC = iniGetBoolValue(NULL,
            "binlog_call_fsync", &ini_context, true);

    LOCAL_BINLOG_CHECK_LAST_SECONDS = iniGetIntValue(NULL,
            "local_binlog_check_last_seconds", &ini_context,
            FS_DEFAULT_LOCAL_BINLOG_CHECK_LAST_SECONDS);

    SLAVE_BINLOG_CHECK_LAST_ROWS = iniGetIntCorrectValue(
            &full_ini_ctx, "slave_binlog_check_last_rows",
            FS_DEFAULT_SLAVE_BINLOG_CHECK_LAST_ROWS,
            FS_MIN_SLAVE_BINLOG_CHECK_LAST_ROWS,
            FS_MAX_SLAVE_BINLOG_CHECK_LAST_ROWS);

    if ((result=get_sys_total_mem_size(&SYSTEM_TOTAL_MEMORY)) != 0) {
        return result;
    }

    if ((result=load_net_buffer_memory_limit(&ini_context, filename)) != 0) {
        return result;
    }

    if ((result=load_binlog_buffer_size(&ini_context, filename)) != 0) {
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

    if ((result=load_storage_engine_parames(&full_ini_ctx)) != 0) {
        return result;
    }

    if ((result=load_from_sys_file()) != 0) {
        return result;
    }

    if (STORAGE_ENABLED) {
        if (SLICE_DEDUP_ENABLED) {
            SLICE_DEDUP_ENABLED = false;
        }
    } else {
        if (SLICE_KEEP_DAYS > 0) {
            SLICE_KEEP_DAYS = 0;
        }
    }
    
    if ((SYSTEM_CPU_COUNT=get_sys_cpu_count()) <= 0) {
        logCrit("file: "__FILE__", line: %d, "
                "get CPU count fail", __LINE__);
        return EINVAL;
    }

    if (BLOCK_ELIMINATE_INTERVAL > 0) {
        g_server_global_vars->slice_storage.cfg.memory_limit = (int64_t)
            (SYSTEM_TOTAL_MEMORY * STORAGE_MEMORY_TOTAL_LIMIT *
             MEMORY_LIMIT_LEVEL1_RATIO);
        if (g_server_global_vars->slice_storage.cfg.
                memory_limit < 64 * 1024 * 1024)
        {
            g_server_global_vars->slice_storage.cfg.
                memory_limit = 64 * 1024 * 1024;
        }
    } else {
        g_server_global_vars->slice_storage.cfg.memory_limit = 0;  //no limit
    }

    data_cfg.path = STORAGE_PATH;
    data_cfg.binlog_buffer_size = BINLOG_BUFFER_SIZE;
    data_cfg.binlog_subdirs = BLOCK_BINLOG_SUBDIRS;
    data_cfg.trunk_index_dump_interval = TRUNK_INDEX_DUMP_INTERVAL;
    data_cfg.trunk_index_dump_base_time = TRUNK_INDEX_DUMP_BASE_TIME;
    if (STORAGE_ENABLED) {
        if ((result=STORAGE_ENGINE_INIT_API(&full_ini_ctx, CLUSTER_MY_SERVER_ID,
                        FILE_BLOCK_SIZE, &g_server_global_vars->slice_storage.cfg,
                        &data_cfg)) != 0)
        {
            return result;
        }
    }

    g_server_global_vars->replica.active_test_interval = (int)
        ceil(SF_G_NETWORK_TIMEOUT / 2.00);
    if (g_server_global_vars->replica.active_test_interval == 0) {
        g_server_global_vars->replica.active_test_interval = 1;
    }

    iniFreeContext(&ini_context);
    load_local_host_ip_addrs();
    server_log_configs();

    if ((result=server_init_client(filename)) != 0) {
        return result;
    }

    if ((result=conn_pool_set_rdma_extra_params(&REPLICA_CONN_EXTRA_PARAMS,
                    &SERVER_CONFIG_CTX, REPLICA_GROUP_INDEX)) != 0)
    {
        return result;
    }

    return 0;
}

void fs_server_restart(const char *reason)
{
    int result;
    pid_t pid;

    pid = fork();
    if (pid < 0) {
        result = (errno != 0 ? errno : EBUSY);
        logError("file: "__FILE__", line: %d, "
                "call fork fail, errno: %d, error info: %s",
                __LINE__, result, STRERROR(result));
        sf_terminate_myself();
        return;
    } else if (pid > 0) {  //parent process
        logInfo("file: "__FILE__", line: %d, "
                "restart because %s", __LINE__, reason);
        return;
    }

    //child process, close server sockets
    sf_socket_close_ex(&CLUSTER_SF_CTX);
    sf_socket_close_ex(&REPLICA_SF_CTX);
    sf_socket_close();
    if (execlp(CMDLINE_PROGRAM_FILENAME, CMDLINE_PROGRAM_FILENAME,
                CMDLINE_CONFIG_FILENAME, "restart", NULL) < 0)
    {
        result = errno != 0 ? errno : EBUSY;
        logError("file: "__FILE__", line: %d, "
                "exec \"%s %s restart\" fail, errno: %d, error info: %s",
                __LINE__, CMDLINE_PROGRAM_FILENAME, CMDLINE_CONFIG_FILENAME,
                result, STRERROR(result));

        log_sync_func(&g_log_context);
        kill(getppid(), SIGQUIT);
        exit(result);
    }
}
