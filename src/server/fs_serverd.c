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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <pthread.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/process_ctrl.h"
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_func.h"
#include "sf/sf_global.h"
#include "sf/sf_nio.h"
#include "sf/sf_service.h"
#include "sf/sf_util.h"
#include "fastcfs/auth/fcfs_auth_for_server.h"
#include "common/fs_proto.h"
#include "common/fs_types.h"
#include "client/fs_client.h"
#include "server_types.h"
#include "server_global.h"
#include "server_func.h"
#include "common_handler.h"
#include "service_handler.h"
#include "cluster_handler.h"
#include "replica_handler.h"
#include "cluster_relationship.h"
#include "cluster_topology.h"
#include "data_thread.h"
#include "server_storage.h"
#include "server_binlog.h"
#include "server_replication.h"
#include "server_recovery.h"
#include "storage/slice_op.h"
#include "rebuild/store_path_rebuild.h"
#include "dio/trunk_write_thread.h"
#include "dio/trunk_read_thread.h"
#include "shared_thread_pool.h"

static int setup_server_env(const char *config_filename);
static int setup_mblock_stat_task();

static bool daemon_mode = true;
static const char *config_filename;
static char g_pid_filename[MAX_PATH_SIZE];

static int init_nio_task(struct fast_task_info *task)
{
    FSSliceSNPairArray *slice_sn_parray;

    task->connect_timeout = SF_G_CONNECT_TIMEOUT;
    task->network_timeout = SF_G_NETWORK_TIMEOUT;
    slice_sn_parray = &((FSServerTaskArg *)task->arg)->
        context.slice_op_ctx.update.sarray;
    return fs_slice_array_init(slice_sn_parray);
}

static char *alloc_recv_buffer(struct fast_task_info *task,
        const int buff_size, bool *new_alloc)
{
    unsigned char cmd;
    SFSharedMBuffer *mbuffer;

    cmd = ((FSProtoHeader *)task->data)->cmd;
    if (cmd == FS_SERVICE_PROTO_SLICE_WRITE_REQ ||
            cmd == FS_REPLICA_PROTO_RPC_REQ)
    {
        *new_alloc = true;
        mbuffer = sf_shared_mbuffer_alloc(&SHARED_MBUFFER_CTX, buff_size);
        if (mbuffer == NULL) {
            return NULL;
        }
        return mbuffer->buff;
    } else {
        *new_alloc = false;
        return NULL;
    }
}

static int parse_cmd_options(int argc, char *argv[])
{
    int ch;
    const struct option longopts[] = {
        {FS_FORCE_ELECTION_LONG_OPTION_STR, no_argument, NULL, 'f'},
        {FS_DATA_REBUILD_LONG_OPTION_STR, required_argument, NULL, 'R'},
        {FS_MIGRATE_CLEAN_LONG_OPTION_STR, no_argument, NULL, 'C'},
        SF_COMMON_LONG_OPTIONS,
        {NULL, 0, NULL, 0}
    };

    DATA_REBUILD_PATH_STR = NULL;
    while ((ch = getopt_long(argc, argv, SF_COMMON_OPT_STRING"fCR:",
                    longopts, NULL)) != -1)
    {
        switch (ch) {
            case 'f':
                FORCE_LEADER_ELECTION = true;
                break;
            case 'R':
                DATA_REBUILD_PATH_STR = optarg;
                break;
            case 'C':
                MIGRATE_CLEAN_ENABLED = true;
                break;
            case '?':
                return EINVAL;
            default:
                break;
        }
    }

    if (DATA_REBUILD_PATH_STR != NULL && MIGRATE_CLEAN_ENABLED) {
        fprintf(stderr, "Error: option --%s and --%s "
                "can't appear at the same time!\n\n",
                FS_DATA_REBUILD_LONG_OPTION_STR,
                FS_MIGRATE_CLEAN_LONG_OPTION_STR);
        return EINVAL;
    }

    return 0;
}

static int process_cmdline(int argc, char *argv[], bool *continue_flag)
{
    char *action;
    const SFCMDOption other_options[] = {
        {{FS_FORCE_ELECTION_LONG_OPTION_STR,
             FS_FORCE_ELECTION_LONG_OPTION_LEN}, 'f', false,
        "-f | --"FS_FORCE_ELECTION_LONG_OPTION_STR
            ": force leader election"},

        {{FS_DATA_REBUILD_LONG_OPTION_STR,
             FS_DATA_REBUILD_LONG_OPTION_LEN}, 'R', true,
        "-R | --"FS_DATA_REBUILD_LONG_OPTION_STR
            " <store_path>: data rebuilding for the store path"},

        {{FS_MIGRATE_CLEAN_LONG_OPTION_STR,
             FS_MIGRATE_CLEAN_LONG_OPTION_LEN}, 'C', false,
        "-C | --"FS_MIGRATE_CLEAN_LONG_OPTION_STR
            ": enable migrate clean"},

        {{NULL, 0}, 0, false, NULL}
    };
    bool stop;
    int result;

    *continue_flag = false;
    if (argc < 2) {
        sf_usage_ex(argv[0], other_options);
        return 1;
    }

    config_filename = sf_parse_daemon_mode_and_action_ex(
            argc, argv, &g_fs_global_vars.version,
            &daemon_mode, &action, "start", other_options);
    if (config_filename == NULL) {
        return 0;
    }

    log_init2();
    //log_set_time_precision(&g_log_context, LOG_TIME_PRECISION_USECOND);

    result = sf_get_base_path_from_conf_file(config_filename);
    if (result != 0) {
        log_destroy();
        return result;
    }

    snprintf(g_pid_filename, sizeof(g_pid_filename), 
             "%s/serverd.pid", SF_G_BASE_PATH_STR);

    stop = false;
    result = process_action(g_pid_filename, action, &stop);
    if (result != 0) {
        if (result == EINVAL) {
            sf_usage(argv[0]);
        }
        log_destroy();
        return result;
    }

    if (stop) {
        log_destroy();
        return 0;
    }

    if ((result=parse_cmd_options(argc, argv)) == 0) {
        *continue_flag = true;
    }

    return result;
}

int main(int argc, char *argv[])
{
    pthread_t schedule_tid;
    int wait_count;
    int result;

    g_server_global_vars = malloc(sizeof(FSServerGlobalVars));
    if (g_server_global_vars == NULL) {
        fprintf(stderr, "malloc %d bytes fail!\n",
                (int)sizeof(FSServerGlobalVars));
        return ENOMEM;
    }
    memset(g_server_global_vars, 0, sizeof(FSServerGlobalVars));

    result = process_cmdline(argc, argv, (bool *)&SF_G_CONTINUE_FLAG);
    if (!SF_G_CONTINUE_FLAG) {
        return result;
    }

    sf_enable_exit_on_oom();
    srand(time(NULL));
    //fast_mblock_manager_init();

    //sched_set_delay_params(300, 1024);
    do {
        if ((result=setup_server_env(config_filename)) != 0) {
            break;
        }

        if ((result=sf_startup_schedule(&schedule_tid)) != 0) {
            break;
        }

        if ((result=sf_add_slow_log_schedule(&g_server_global_vars->
                        slow_log)) != 0)
        {
            break;
        }

        if ((result=server_group_info_setup_sync_to_file_task()) != 0) {
            break;
        }

        //sched_print_all_entries();

        if ((result=sf_socket_server_ex(&CLUSTER_SF_CTX)) != 0) {
            break;
        }
        if ((result=sf_socket_server_ex(&REPLICA_SF_CTX)) != 0) {
            break;
        }
        if ((result=sf_socket_server()) != 0) {
            break;
        }

        if ((result=write_to_pid_file(g_pid_filename)) != 0) {
            break;
        }

        if ((result=shared_thread_pool_init()) != 0) {
            break;
        }

        if ((result=service_handler_init()) != 0) {
            break;
        }

        if ((result=cluster_handler_init()) != 0) {
            break;
        }

        if ((result=replica_handler_init()) != 0) {
            break;
        }

        if ((result=trunk_write_thread_init()) != 0) {
            break;
        }

        if ((result=trunk_read_thread_init()) != 0) {
            break;
        }

        if ((result=data_thread_init()) != 0) {
            break;
        }

        if ((result=cluster_relationship_init()) != 0) {
            break;
        }

        if ((result=cluster_topology_init()) != 0) {
            break;
        }

        result = sf_service_init_ex2(&CLUSTER_SF_CTX, "cluster",
                cluster_alloc_thread_extra_data, NULL, NULL,
                sf_proto_set_body_length, NULL, NULL, cluster_deal_task_partly,
                cluster_task_finish_cleanup, cluster_recv_timeout_callback,
                1000, sizeof(FSProtoHeader), sizeof(FSServerTaskArg),
                init_nio_task, NULL);
        if (result != 0) {
            break;
        }
        sf_enable_thread_notify_ex(&CLUSTER_SF_CTX, true);
        sf_set_remove_from_ready_list_ex(&CLUSTER_SF_CTX, false);
        sf_accept_loop_ex(&CLUSTER_SF_CTX, false);

        if ((result=server_storage_init()) != 0) {
            break;
        }

        if ((result=server_binlog_init()) != 0) {
            break;
        }

        if ((result=server_replication_init()) != 0) {
            break;
        }

        if ((result=storage_allocator_prealloc_trunk_freelists()) != 0) {
            break;
        }

        if ((result=server_recovery_init(config_filename)) != 0) {
            break;
        }

        if ((result=store_path_rebuild_redo_step2()) != 0) {
            break;
        }

        if ((result=trunk_prealloc_init()) != 0) {
            break;
        }

        if ((result=fcfs_auth_for_server_start(&AUTH_CTX)) != 0) {
            break;
        }

        /* set read rule for data group recovery */
        g_fs_client_vars.client_ctx.common_cfg.read_rule =
            sf_data_read_rule_master_only;

        common_handler_init();
        //sched_print_all_entries();

        sf_service_set_thread_loop_callback_ex(&CLUSTER_SF_CTX,
                cluster_thread_loop_callback);
        sf_set_deal_task_func_ex(&CLUSTER_SF_CTX, cluster_deal_task_fully);

        result = sf_service_init_ex2(&REPLICA_SF_CTX, "replica",
                replica_alloc_thread_extra_data,
                replica_thread_loop_callback, NULL,
                sf_proto_set_body_length, alloc_recv_buffer, NULL,
                replica_deal_task, replica_task_finish_cleanup,
                replica_recv_timeout_callback, 1000,
                sizeof(FSProtoHeader), sizeof(FSServerTaskArg),
                init_nio_task, fs_release_task_send_buffer);
        if (result != 0) {
            break;
        }
        sf_enable_thread_notify_ex(&REPLICA_SF_CTX, true);
        sf_set_remove_from_ready_list_ex(&REPLICA_SF_CTX, false);
        sf_accept_loop_ex(&REPLICA_SF_CTX, false);

        result = sf_service_init_ex2(&g_sf_context, "service",
                service_alloc_thread_extra_data, NULL, NULL,
                sf_proto_set_body_length, alloc_recv_buffer, NULL,
                service_deal_task, service_task_finish_cleanup,
                NULL, 1000, sizeof(FSProtoHeader),
                sizeof(FSServerTaskArg), init_nio_task,
                fs_release_task_send_buffer);
        if (result != 0) {
            break;
        }
        sf_enable_thread_notify_ex(&g_sf_context, true);
        sf_set_remove_from_ready_list_ex(&g_sf_context, false);

        if ((result=cluster_relationship_start()) != 0) {
            break;
        }

        if ((result=replication_common_start()) != 0) {
            break;
        }

        if ((result=replication_quorum_start()) != 0) {
            return result;
        }

        if ((result=replica_clean_add_schedule()) != 0) {
            break;
        }

        if ((result=slice_clean_add_schedule()) != 0) {
            break;
        }

        if ((result=slice_dedup_add_schedule()) != 0) {
            break;
        }
    } while (0);

    if (result != 0) {
        lcrit("program exit abnomally with errno: %d", result);
        log_destroy();
        return result;
    }

    setup_mblock_stat_task();
    //sched_print_all_entries();

    sf_accept_loop();
    if (g_schedule_flag) {
        pthread_kill(schedule_tid, SIGINT);
    }

    trunk_write_thread_terminate();
    server_replication_terminate();
    server_recovery_terminate();

    wait_count = 0;
    while ((SF_G_ALIVE_THREAD_COUNT != 0 || SF_ALIVE_THREAD_COUNT(
                    CLUSTER_SF_CTX) != 0) || g_schedule_flag)
    {
        fc_sleep_ms(10);
        if (++wait_count > 1000) {
            lwarning("waiting timeout, exit!");
            break;
        }
    }

    server_binlog_destroy();
    server_storage_destroy();
    sf_service_destroy();

    delete_pid_file(g_pid_filename);
    logInfo("file: "__FILE__", line: %d, "
            "program exit normally.\n", __LINE__);
    log_destroy();
    return 0;
}

static int mblock_stat_task_func(void *args)
{
    //fast_mblock_manager_stat_print_ex(false, FAST_MBLOCK_ORDER_BY_ELEMENT_SIZE);
    //fast_mblock_manager_stat_print_ex(true, FAST_MBLOCK_ORDER_BY_ALLOC_BYTES);
    return 0;
}

static int setup_mblock_stat_task()
{
    ScheduleEntry schedule_entry;
    ScheduleArray schedule_array;

    INIT_SCHEDULE_ENTRY(schedule_entry, sched_generate_next_id(),
            0, 0, 0, 300,  mblock_stat_task_func, NULL);

    //schedule_entry.new_thread = true;

    schedule_array.count = 1;
    schedule_array.entries = &schedule_entry;
    return sched_add_entries(&schedule_array);
}

static int setup_server_env(const char *config_filename)
{
    const int min_alloc_once = 2;
    int result;
    int max_pkg_size;

    sf_set_current_time();
    if ((result=server_load_config(config_filename)) != 0) {
        return result;
    }

    if (daemon_mode) {
        daemon_init(false);
    }
    umask(0);

    max_pkg_size = g_sf_global_vars.max_pkg_size -
        g_sf_global_vars.task_buffer_extra_size;
    if ((result=sf_shared_mbuffer_init(&SHARED_MBUFFER_CTX, "net-mbuffer",
                    g_sf_global_vars.task_buffer_extra_size,
                    1024, max_pkg_size, min_alloc_once,
                    NET_BUFFER_MEMORY_LIMIT.value)) != 0)
    {
        return result;
    }

    result = sf_setup_signal_handler();

    log_set_cache(true);
    return result;
}
