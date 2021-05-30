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
    return fs_init_slice_op_ctx(slice_sn_parray);
}

static int parse_cmd_options(int argc, char *argv[])
{
    int ch;
    const struct option longopts[] = {
        {FS_FORCE_ELECTION_LONG_OPTION_STR, no_argument, NULL, 'f'},
        SF_COMMON_LONG_OPTIONS,
        {NULL, 0, NULL, 0}
    };

    while ((ch = getopt_long(argc, argv, SF_COMMON_OPT_STRING"f",
                    longopts, NULL)) != -1)
    {
        switch (ch) {
            case 'f':
                FORCE_LEADER_ELECTION = true;
                break;
            case '?':
                return EINVAL;
            default:
                break;
        }
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
    log_set_time_precision(&g_log_context, LOG_TIME_PRECISION_USECOND);

    result = get_base_path_from_conf_file(config_filename,
            SF_G_BASE_PATH_STR, sizeof(SF_G_BASE_PATH_STR));
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

        if ((result=sf_add_slow_log_schedule(&g_server_global_vars.
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
                sf_proto_set_body_length, cluster_deal_task_partly,
                cluster_task_finish_cleanup, cluster_recv_timeout_callback,
                1000, sizeof(FSProtoHeader), sizeof(FSServerTaskArg),
                init_nio_task);
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
            return result;
        }

        if ((result=server_recovery_init(config_filename)) != 0) {
            break;
        }

        if ((result=trunk_prealloc_init()) != 0) {
            return result;
        }

        if ((result=fcfs_auth_for_server_start(&AUTH_CTX)) != 0) {
            break;
        }

        common_handler_init();
        //sched_print_all_entries();

        sf_service_set_thread_loop_callback_ex(&CLUSTER_SF_CTX,
                cluster_thread_loop_callback);
        sf_set_deal_task_func_ex(&CLUSTER_SF_CTX, cluster_deal_task_fully);

        result = sf_service_init_ex2(&REPLICA_SF_CTX, "replica",
                replica_alloc_thread_extra_data,
                replica_thread_loop_callback, NULL,
                sf_proto_set_body_length, replica_deal_task,
                replica_task_finish_cleanup, replica_recv_timeout_callback,
                1000, sizeof(FSProtoHeader), sizeof(FSServerTaskArg),
                init_nio_task);
        if (result != 0) {
            break;
        }
        sf_enable_thread_notify_ex(&REPLICA_SF_CTX, true);
        sf_set_remove_from_ready_list_ex(&REPLICA_SF_CTX, false);

        result = sf_service_init_ex2(&g_sf_context, "service",
                service_alloc_thread_extra_data, NULL, NULL,
                sf_proto_set_body_length, service_deal_task,
                service_task_finish_cleanup, NULL, 1000,
                sizeof(FSProtoHeader), sizeof(FSServerTaskArg),
                init_nio_task);
        if (result != 0) {
            break;
        }
        sf_enable_thread_notify_ex(&g_sf_context, true);
        sf_set_remove_from_ready_list_ex(&g_sf_context, false);

        if ((result=cluster_relationship_start()) != 0) {
            break;
        }

        result = replication_common_start();
    } while (0);

    if (result != 0) {
        lcrit("program exit abnomally");
        log_destroy();
        return result;
    }

    setup_mblock_stat_task();
    //sched_print_all_entries();

    sf_accept_loop_ex(&REPLICA_SF_CTX, false);
    sf_accept_loop();
    if (g_schedule_flag) {
        pthread_kill(schedule_tid, SIGINT);
    }

    trunk_write_thread_terminate();
    server_binlog_terminate();
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
    int result;

    sf_set_current_time();
    if ((result=server_load_config(config_filename)) != 0) {
        return result;
    }

    if (daemon_mode) {
        daemon_init(false);
    }
    umask(0);

    result = sf_setup_signal_handler();

    log_set_cache(true);
    return result;
}
