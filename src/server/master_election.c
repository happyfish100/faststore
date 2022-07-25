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
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <fcntl.h>
#include <pthread.h>
#include "fastcommon/logger.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/common_blocked_queue.h"
#include "fastcommon/fc_atomic.h"
#include "server_global.h"
#include "cluster_relationship.h"
#include "cluster_topology.h"
#include "shared_thread_pool.h"
#include "master_election.h"

FSMasterElectionContext g_master_election_ctx = {0, 0};

int master_election_init()
{
    int result;
    int alloc_elements_once;

    if (CLUSTER_DATA_RGOUP_ARRAY.count < 512) {
        alloc_elements_once = CLUSTER_DATA_RGOUP_ARRAY.count * 4;
    } else if (CLUSTER_DATA_RGOUP_ARRAY.count < 1024) {
        alloc_elements_once = CLUSTER_DATA_RGOUP_ARRAY.count * 2;
    } else {
        alloc_elements_once = CLUSTER_DATA_RGOUP_ARRAY.count;
    }
    if ((result=common_blocked_queue_init_ex(&g_master_election_ctx.
                    queue, alloc_elements_once)) != 0)
    {
        return result;
    }

    if ((result=common_blocked_queue_init_ex(&g_master_election_ctx.
                    delay_queue, alloc_elements_once)) != 0)
    {
        return result;
    }

    if ((result=init_pthread_lock(&g_master_election_ctx.lock)) != 0) {
        return result;
    }

    return 0;
}

void master_election_destroy()
{
}

static inline int master_election_push_to_delay_queue(
        FSClusterDataGroupInfo *group)
{
    if (__sync_bool_compare_and_swap(&group->election.
                in_delay_queue, 0, 1))
    {
        return common_blocked_queue_push(&g_master_election_ctx.
                delay_queue, group);
    }

    return 0;
}

bool master_election_set_master(FSClusterDataGroupInfo *group,
        FSClusterDataServerInfo *old_master,
        FSClusterDataServerInfo *new_master)
{
    bool success;
    FSClusterDataServerInfo *ds;
    FSClusterDataServerInfo *ds_end;

    master_election_lock();
    if (new_master != NULL && FC_ATOMIC_GET(new_master->
                cs->status) != FS_SERVER_STATUS_ACTIVE)
    {
        success = false;
    } else {
        success = __sync_bool_compare_and_swap(&group->master,
                old_master, new_master);
        if (success) {
            if (old_master != NULL) {
                __sync_bool_compare_and_swap(&old_master->is_master, 1, 0);
            }
            if (new_master != NULL) {
                __sync_bool_compare_and_swap(&new_master->is_master, 0, 1);
            }
        }
    }

    if (success) {
        ds_end = group->data_server_array.servers +
            group->data_server_array.count;
        for (ds=group->data_server_array.servers; ds<ds_end; ds++) {
            if (ds != new_master && FC_ATOMIC_GET(ds->status) ==
                    FS_DS_STATUS_ACTIVE)
            {
                __sync_bool_compare_and_swap(&ds->status,
                        FS_DS_STATUS_ACTIVE, FS_DS_STATUS_OFFLINE);
                cluster_topology_data_server_chg_notify(ds,
                        FS_EVENT_SOURCE_CS_LEADER,
                        FS_EVENT_TYPE_STATUS_CHANGE, true);
            }
        }
    }
    master_election_unlock();

    if (success) {
        cluster_relationship_on_master_change(group, old_master, new_master);
    }

    return success;
}

void master_election_unset_all_masters()
{
    FSClusterDataGroupInfo *group;
    FSClusterDataGroupInfo *end;
    FSClusterDataServerInfo *master;

    end = CLUSTER_DATA_RGOUP_ARRAY.groups + CLUSTER_DATA_RGOUP_ARRAY.count;
    for (group=CLUSTER_DATA_RGOUP_ARRAY.groups; group<end; group++) {
        master = (FSClusterDataServerInfo *)__sync_fetch_and_add(
                &group->master, 0);
        if (master != NULL) {
            if (master_election_set_master(group, master, NULL)) {
                cluster_topology_data_server_chg_notify(master,
                        FS_EVENT_SOURCE_CS_LEADER,
                        FS_EVENT_TYPE_MASTER_CHANGE, true);
            }
        }

        if (group->data_server_array.count == 1) {
            master_election_queue_push(group);
        } else {
            master_election_push_to_delay_queue(group);
        }
    }
}

static int compare_ds_by_data_version(const void *p1, const void *p2)
{
    FSClusterDataServerInfo **ds1;
    FSClusterDataServerInfo **ds2;
    int64_t sub;

    ds1 = (FSClusterDataServerInfo **)p1;
    ds2 = (FSClusterDataServerInfo **)p2;
    if ((sub=fc_compare_int64(FC_ATOMIC_GET((*ds1)->data.current_version),
                    FC_ATOMIC_GET((*ds2)->data.current_version))) != 0)
    {
        return sub;
    }

    sub = FC_ATOMIC_GET((*ds1)->cs->status) -
        FC_ATOMIC_GET((*ds2)->cs->status);
    if (sub != 0) {
        return sub;
    }

    sub = (int)(*ds1)->is_preseted - (int)(*ds2)->is_preseted;
    if (sub != 0) {
        return sub;
    }

    return FC_ATOMIC_GET((*ds1)->is_master) -
        FC_ATOMIC_GET((*ds2)->is_master);
}

static inline FSClusterDataServerInfo *get_preseted_master(
        FSClusterDataGroupInfo *group)
{
    FSClusterDataServerInfo *ds;
    FSClusterDataServerInfo *end;

    end = group->data_server_array.servers + group->data_server_array.count;
    for (ds=group->data_server_array.servers; ds<end; ds++) {
        if (ds->is_preseted) {
            return ds;
        }
    }

    return group->data_server_array.servers;
}

static FSClusterDataServerInfo *select_master(FSClusterDataGroupInfo *group,
        int *result)
{
#define ACTIVE_WAIT_TIMEOUT    5
#define ONLINE_WAIT_TIMEOUT   30
#define OFFLINE_WAIT_TIMEOUT   5

#define IS_SERVER_TIMEDOUT(group, cs, election_start_time, timeout)  \
    (g_current_time - (cs->status_changed_time > 0 ?     \
                       FC_MIN(cs->status_changed_time,   \
                           election_start_time) : \
                           election_start_time) >= timeout)

    FSClusterDataServerInfo *online_data_servers[FS_MAX_GROUP_SERVERS];
    FSClusterDataServerInfo *last;
    FSClusterDataServerInfo *ds;
    FSClusterDataServerInfo *end;
    int64_t max_data_version;
    int election_start_time;
    int active_count;
    int waiting_report_count;
    int waiting_online_count;
    int waiting_offline_count;
    int *waiting_count;
    int master_index;
    int status;
    int timeout;

    if (group->election.start_time_ms == 0) {
        group->election.start_time_ms = get_current_time_ms();
        group->election.retry_count = 1;

        if (CLUSTER_MYSELF_PTR == CLUSTER_LEADER_ATOM_PTR &&
                group->myself != NULL)
        {
            CLUSTER_MYSELF_PTR->last_ping_time = g_current_time + 1;
        }
    } else {
        group->election.retry_count++;
    }
    election_start_time = (int)(group->election.start_time_ms / 1000);

    if (!MASTER_ELECTION_FAILOVER) {
        ds = get_preseted_master(group);
        if (FC_ATOMIC_GET(ds->cs->status) == FS_SERVER_STATUS_ACTIVE) {
            *result = 0;
            return ds;
        } else {
            *result = EAGAIN;
            return NULL;
        }
    }

    active_count = 0;
    waiting_report_count = 0;
    waiting_online_count = 0;
    waiting_offline_count = 0;
    end = group->data_server_array.servers + group->data_server_array.count;
    for (ds=group->data_server_array.servers; ds<end; ds++) {
        status = FC_ATOMIC_GET(ds->cs->status);
        if (status == FS_SERVER_STATUS_ACTIVE) {
            if (ds->cs->last_ping_time >= election_start_time) {
                active_count++;
            } else if (g_current_time - ds->cs->last_ping_time <=
                    ONLINE_WAIT_TIMEOUT + 5)
            {
                waiting_report_count++;
            } else {
                int64_t time_used;
                char time_buff[32];

                time_used = get_current_time_ms() -
                    group->election.start_time_ms;
                long_to_comma_str(time_used, time_buff);
                logError("file: "__FILE__", line: %d, "
                        "data group id: %d, waiting server id: %d "
                        "timeout, time used: %s ms", __LINE__, group->id,
                        ds->cs->server->id, time_buff);
                group->election.start_time_ms = 0;
                group->election.retry_count = 0;
                *result = EAGAIN;
                return NULL;
            }
        } else {
            if (status == FS_SERVER_STATUS_ONLINE) {
                timeout = ONLINE_WAIT_TIMEOUT;
                waiting_count = &waiting_online_count;
            } else {
                timeout = OFFLINE_WAIT_TIMEOUT;
                waiting_count = &waiting_offline_count;
            }

            if (!IS_SERVER_TIMEDOUT(group, ds->cs,
                        election_start_time, timeout))
            {
                (*waiting_count)++;
            }
        }
    }

    if (active_count == 0 || (waiting_report_count +
                waiting_online_count + waiting_offline_count) > 0)
    {
        /*
        logInfo("file: "__FILE__", line: %d, "
                "data group id: %d, active_count: %d, "
                "waiting_report_count: %d, waiting_online_count: %d, "
                "waiting_offline_count: %d", __LINE__, group->id,
                active_count, waiting_report_count, waiting_online_count,
                waiting_offline_count);
                */
        *result = EAGAIN;
        return NULL;
    }

    if (group->ds_ptr_array.count > 1) {
        qsort(group->ds_ptr_array.servers,
                group->ds_ptr_array.count,
                sizeof(FSClusterDataServerInfo *),
                compare_ds_by_data_version);
    }

    last = group->ds_ptr_array.servers[group->ds_ptr_array.count - 1];
    status = FC_ATOMIC_GET(last->cs->status);
    if (status == FS_SERVER_STATUS_ACTIVE) {
        if (last->is_preseted || active_count == group->ds_ptr_array.count) {
            *result = 0;
            return last;
        }
        timeout = ACTIVE_WAIT_TIMEOUT * 3;
    } else if (status == FS_SERVER_STATUS_ONLINE) {
        timeout = ONLINE_WAIT_TIMEOUT * 3;
    } else {
        timeout = OFFLINE_WAIT_TIMEOUT * 3;
    }

    if (!IS_SERVER_TIMEDOUT(group, last->cs,
                election_start_time, timeout))
    {
        *result = EAGAIN;
        return NULL;
    }

    max_data_version = -1;
    for (ds=last; ds>=group->data_server_array.servers; ds--) {
        if (FC_ATOMIC_GET(ds->cs->status) == FS_SERVER_STATUS_ACTIVE) {
            max_data_version = FC_ATOMIC_GET(ds->data.current_version);
            break;
        }
    }

    if (max_data_version == -1) {
        *result = EAGAIN;
        logWarning("file: "__FILE__", line: %d, "
                "data group id: %d, no active server!",
                __LINE__, group->id);
        return NULL;
    }

    if (max_data_version < FC_ATOMIC_GET(last->data.current_version)) {
        if (MASTER_ELECTION_POLICY == FS_MASTER_ELECTION_POLICY_STRICT_INT) {
            *result = EAGAIN;
            return NULL;
        } else {
            if (!IS_SERVER_TIMEDOUT(group, last->cs, election_start_time,
                        MASTER_ELECTION_TIMEOUTS))
            {
                *result = EAGAIN;
                return NULL;
            }
        }
    }

    active_count = 0;
    for (ds=group->data_server_array.servers; ds<end; ds++) {
        if (FC_ATOMIC_GET(ds->cs->status) == FS_SERVER_STATUS_ACTIVE &&
                FC_ATOMIC_GET(ds->data.current_version) >= max_data_version)
        {
            online_data_servers[active_count++] = ds;
        }
    }

    if (active_count == 0) {
        *result = ENOENT;
        logWarning("file: "__FILE__", line: %d, "
                "data group id: %d, no active server!",
                __LINE__, group->id);
        return NULL;
    }

    if (active_count == group->data_server_array.count) {
        *result = 0;
        return last;
    }

    master_index = group->election.hash_code % active_count;
    /*
    logInfo("data_group_id: %d, active_count: %d, master_index: %d, hash_code: %d",
            group->id, active_count, master_index, group->election.hash_code);
            */

    ds = online_data_servers[master_index];
    if (FC_ATOMIC_GET(ds->cs->status) == FS_SERVER_STATUS_ACTIVE) {
        *result = 0;
        return ds;
    }

    *result = EAGAIN;
    return NULL;
}

static int master_election_select_master(FSClusterDataGroupInfo *group)
{
    FSClusterDataServerInfo *ds;
    FSClusterDataServerInfo *end;
    FSClusterDataServerInfo *master;
    int64_t master_dv;
    int result;

    if ((master=(FSClusterDataServerInfo *)FC_ATOMIC_GET(
                    group->master)) != NULL)
    {
        if (__sync_bool_compare_and_swap(&group->election.reselect, 1, 0)) {
            master_dv = FC_ATOMIC_GET(master->data.current_version);
            end = group->data_server_array.servers +
                group->data_server_array.count;
            for (ds=group->data_server_array.servers; ds<end; ds++) {
                if (ds != master && FC_ATOMIC_GET(ds->data.
                            current_version) > master_dv)
                {
                    break;
                }
            }

            if (ds == end) {
                return 0;
            }

            logInfo("file: "__FILE__", line: %d, "
                    "re-select master, unset old master: %d",
                    __LINE__, master->cs->server->id);
            master_election_set_master(group, master, NULL);
        } else {
            return 0;
        }
    }

    master = select_master(group, &result);
    if (master == NULL) {
        return result;
    }

    if (master_election_set_master(group, NULL, master)) {
        int64_t time_used;
        char time_buff[32];

        cluster_topology_data_server_chg_notify(master,
                FS_EVENT_SOURCE_CS_LEADER,
                FS_EVENT_TYPE_MASTER_CHANGE, true);

        time_used = get_current_time_ms() - group->election.start_time_ms;
        long_to_comma_str(time_used, time_buff);
        logInfo("file: "__FILE__", line: %d, "
                "data group id: %d, elected master id: %d, "
                "is_preseted: %d, retry count: %d, time used: %s ms",
                __LINE__, group->id, master->cs->server->id,
                master->is_preseted, group->election.retry_count, time_buff);
    } else if (FC_ATOMIC_GET(group->master) == NULL) {
        return EAGAIN;
    }

    group->election.start_time_ms = 0;
    group->election.retry_count = 0;
    return 0;
}

static void select_master_thread_run(void *arg, void *thread_data)
{
    const int timeout = 60;
    int result;
    FSClusterDataGroupInfo *group;

    logDebug("file: "__FILE__", line: %d, "
            "select_master_thread enter ...", __LINE__);

    while (CLUSTER_MYSELF_PTR == CLUSTER_LEADER_ATOM_PTR) {
        if ((group=(FSClusterDataGroupInfo *)common_blocked_queue_timedpop_sec(
                        &g_master_election_ctx.queue, timeout)) == NULL)
        {
            break;
        }

        __sync_bool_compare_and_swap(&group->election.in_queue, 1, 0);
        FC_ATOMIC_DEC(g_master_election_ctx.waiting_count);

        if (CLUSTER_MYSELF_PTR != CLUSTER_LEADER_ATOM_PTR) {
            break;
        }

        result = master_election_select_master(group);
        if (result == EAGAIN) {
            master_election_push_to_delay_queue(group);
        }
    }

    __sync_bool_compare_and_swap(&g_master_election_ctx.is_running, 1, 0);
    logDebug("file: "__FILE__", line: %d, "
            "select_master_thread exit.", __LINE__);
}

static inline int master_election_thread_start()
{
    if (__sync_bool_compare_and_swap(&g_master_election_ctx.
                is_running, 0, 1))
    {
        return shared_thread_pool_run(select_master_thread_run, NULL);
    } else {
        return 0;
    }
}

void master_election_deal_delay_queue()
{
    struct common_blocked_node *node;
    struct common_blocked_node *current;
    FSClusterDataGroupInfo *group;

    if ((node=common_blocked_queue_try_pop_all_nodes(
                    &g_master_election_ctx.delay_queue)) != NULL)
    {
        current = node;
        do {
            group = (FSClusterDataGroupInfo *)current->data;
            __sync_bool_compare_and_swap(&group->
                    election.in_delay_queue, 1, 0);
            if (FC_ATOMIC_GET(group->master) == NULL) {
                master_election_queue_push(group);
            }

            current = current->next;
        } while (current != NULL);

        common_blocked_queue_free_all_nodes(
                &g_master_election_ctx.
                delay_queue, node);
    }
}

int master_election_queue_push(FSClusterDataGroupInfo *group)
{
    int result;
    int i;
    int waiting_count;

    if (FC_ATOMIC_GET(g_master_election_ctx.is_running) == 0) {
        master_election_thread_start();
    }

    if (__sync_bool_compare_and_swap(&group->election.in_queue, 0, 1)) {
        waiting_count = FC_ATOMIC_INC(g_master_election_ctx.waiting_count);
        if ((result=common_blocked_queue_push(&g_master_election_ctx.
                        queue, group)) != 0)
        {
            FC_ATOMIC_DEC(g_master_election_ctx.waiting_count);
            return result;
        }

        /* check to start election thread for rare case */
        i = 0;
        while ((FC_ATOMIC_GET(g_master_election_ctx.waiting_count) ==
                    waiting_count) && i++ < 10)
        {
            if (FC_ATOMIC_GET(g_master_election_ctx.is_running) == 0) {
                master_election_thread_start();
            }
            fc_sleep_ms(10);
        }
    }

    return 0;
}
