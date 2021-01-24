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
#include "fastcommon/ioevent_loop.h"
#include "fastcommon/fc_atomic.h"
#include "sf/sf_global.h"
#include "sf/sf_nio.h"
#include "common/fs_proto.h"
#include "server_global.h"
#include "cluster_relationship.h"
#include "cluster_topology.h"

static int cluster_topology_select_master(FSClusterDataGroupInfo *group,
        const bool by_decision);

static FSClusterDataServerInfo *find_data_group_server(
        const int gindex, FSClusterServerInfo *cs)
{
    FSClusterDataServerArray *ds_array;
    FSClusterDataServerInfo *ds;
    FSClusterDataServerInfo *end;

    ds_array = &CLUSTER_DATA_RGOUP_ARRAY.groups[gindex].data_server_array;
    end = ds_array->servers + ds_array->count;
    for (ds=ds_array->servers; ds<end; ds++) {
        if (ds->cs == cs) {
            return ds;
        }
    }

    return NULL;
}

int cluster_topology_init_notify_ctx(FSClusterTopologyNotifyContext *notify_ctx)
{
    int result;
    int count;
    int bytes;
    int index;
    int gindex;
    FSClusterServerInfo *cs;
    FSClusterServerInfo *end;

    if ((result=fc_queue_init(&notify_ctx->queue, (long)
                    (&((FSDataServerChangeEvent *)NULL)->next))) != 0)
    {
        return result;
    }

    count = CLUSTER_DATA_RGOUP_ARRAY.count * CLUSTER_SERVER_ARRAY.count;
    bytes = sizeof(FSDataServerChangeEvent) * count;
    notify_ctx->events = (FSDataServerChangeEvent *)fc_malloc(bytes);
    if (notify_ctx->events == NULL) {
        return ENOMEM;
    }
    memset(notify_ctx->events, 0, bytes);

    /*
    logInfo("data group count: %d, server count: %d\n",
            CLUSTER_DATA_RGOUP_ARRAY.count, CLUSTER_SERVER_ARRAY.count);
     */

    end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (gindex=0; gindex<CLUSTER_DATA_RGOUP_ARRAY.count; gindex++) {
        for (cs=CLUSTER_SERVER_ARRAY.servers; cs<end; cs++) {
            index = gindex * CLUSTER_SERVER_ARRAY.count + cs->server_index;
            notify_ctx->events[index].ds =
                find_data_group_server(gindex, cs);
        }
    }

    return 0;
}

void cluster_topology_data_server_chg_notify(FSClusterDataServerInfo *ds,
        const int source, const int event_type, const bool notify_self)
{
    FSClusterServerInfo *cs;
    FSClusterServerInfo *end;
    FSDataServerChangeEvent *event;
    struct fast_task_info *task;
    bool notify;
    int in_queue;

    end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (cs=CLUSTER_SERVER_ARRAY.servers; cs<end; cs++) {
        if (cs->is_leader || (!notify_self && ds->cs == cs)) {
            continue;
        }

        if (FC_ATOMIC_GET(cs->status) != FS_SERVER_STATUS_ACTIVE) {
            logDebug("file: "__FILE__", line: %d, "
                    "data group id: %d, data server id: %d, "
                    "target server id: %d not online! "
                    "ds is_master: %d, status: %d, data version: %"PRId64", "
                    "event {source: %c, type: %d} ", __LINE__,
                    ds->dg->id, ds->cs->server->id,
                    cs->server->id, FC_ATOMIC_GET(ds->is_master),
                    FC_ATOMIC_GET(ds->status),
                    FC_ATOMIC_GET(CLUSTER_CURRENT_VERSION),
                    source, event_type);
            continue;
        }

        task = (struct fast_task_info *)cs->notify_ctx.task;
        if (task == NULL) {
            continue;
        }

        event = cs->notify_ctx.events + (ds->dg->index *
                CLUSTER_SERVER_ARRAY.count + ds->cs->server_index);

        in_queue = __sync_add_and_fetch(&event->in_queue, 0);
        if (__sync_bool_compare_and_swap(&event->in_queue, 0, 1)) { //fetch event
            /*
            logInfo("file: "__FILE__", line: %d, "
                    "data group id: %d, data server id: %d, is_master: %d, "
                    "status: %d, target server id: %d, push to in_queue: %d, "
                    "data version: %"PRId64", event {source: %c, type: %d}, "
                    "ds: %p", __LINE__, ds->dg->id, ds->cs->server->id,
                    FC_ATOMIC_GET(ds->is_master), FC_ATOMIC_GET(ds->status),
                    cs->server->id, in_queue,
                    FC_ATOMIC_GET(CLUSTER_CURRENT_VERSION),
                    source, event_type, event->ds);
                    */

            event->source = source;
            event->type = event_type;
            fc_queue_push_ex(&cs->notify_ctx.queue, event, &notify);
            if (notify) {
                ioevent_notify_thread(task->thread_data);
            }
        } else {
            logDebug("file: "__FILE__", line: %d, "
                    "data group id: %d, data server id: %d, is_master: %d, "
                    "status: %d, target server id: %d, alread in_queue: %d, "
                    "data version: %"PRId64", event {source: %c, type: %d}, "
                    "ds: %p", __LINE__, ds->dg->id, ds->cs->server->id,
                    FC_ATOMIC_GET(ds->is_master), FC_ATOMIC_GET(ds->status),
                    cs->server->id, in_queue,
                    FC_ATOMIC_GET(CLUSTER_CURRENT_VERSION),
                    source, event_type, event->ds);
        }
    }
}

void cluster_topology_sync_all_data_servers(FSClusterServerInfo *cs)
{
    FSClusterDataGroupInfo *group;
    FSClusterDataGroupInfo *gend;
    FSClusterDataServerInfo *ds;
    FSClusterDataServerInfo *ds_end;
    FSDataServerChangeEvent *event;

    gend = CLUSTER_DATA_RGOUP_ARRAY.groups + CLUSTER_DATA_RGOUP_ARRAY.count;
    for (group=CLUSTER_DATA_RGOUP_ARRAY.groups; group<gend; group++) {
        ds_end = group->data_server_array.servers + group->data_server_array.count;
        for (ds=group->data_server_array.servers; ds<ds_end; ds++) {
            event = cs->notify_ctx.events + (ds->dg->index *
                    CLUSTER_SERVER_ARRAY.count + ds->cs->server_index);
            if (__sync_bool_compare_and_swap(&event->in_queue, 0, 1)) { //fetch event
                event->source = FS_EVENT_SOURCE_CS_LEADER;
                event->type = FS_EVENT_TYPE_STATUS_CHANGE |
                    FS_EVENT_TYPE_DV_CHANGE | FS_EVENT_TYPE_MASTER_CHANGE;
                fc_queue_push(&cs->notify_ctx.queue, event);
            }
        }
    }
}

static int process_notify_events(FSClusterTopologyNotifyContext *ctx)
{
    FSDataServerChangeEvent *event;
    FSClusterServerInfo *cs;
    FSClusterDataServerInfo *ds;
    volatile int *in_queue;
    FSProtoHeader *header;
    FSProtoPushDataServerStatusHeader *req_header;
    FSProtoPushDataServerStatusBodyPart *bp_start;
    FSProtoPushDataServerStatusBodyPart *body_part;
    int body_len;
    //int event_source;
    //int event_type;

    if (!(ctx->task->offset == 0 && ctx->task->length == 0)) {
        return EBUSY;
    }

    cs = ((FSServerTaskArg *)ctx->task->arg)->context.shared.cluster.peer;
    if (FC_ATOMIC_GET(cs->status) != FS_SERVER_STATUS_ACTIVE) {
        logDebug("file: "__FILE__", line: %d, "
                "server id: %d is not active, try again later",
                __LINE__, cs->server->id);
        return EAGAIN;
    }

    event = (FSDataServerChangeEvent *)fc_queue_try_pop_all(&ctx->queue);
    if (event == NULL) {
        return 0;
    }

    header = (FSProtoHeader *)ctx->task->data;
    req_header = (FSProtoPushDataServerStatusHeader *)(header + 1);
    bp_start = (FSProtoPushDataServerStatusBodyPart *)(req_header + 1);
    body_part = bp_start;
    while (event != NULL) {
        //event_source = event->source;
        //event_type = event->type;
        in_queue = &event->in_queue;
        ds = event->ds;
        event = event->next;
        __sync_bool_compare_and_swap(in_queue, 1, 0);  //release event

        int2buff(ds->dg->id, body_part->data_group_id);
        int2buff(ds->cs->server->id, body_part->server_id);
        body_part->is_master = FC_ATOMIC_GET(ds->is_master);
        body_part->status = FC_ATOMIC_GET(ds->status);
        long2buff(FC_ATOMIC_GET(ds->data.version), body_part->data_version);

        /*
        logInfo("push to target server id: %d (ctx: %p), event "
                "source: %c, type: %d, {data group id: %d, "
                "data server id: %d, is_master: %d, "
                "status: %d, data_version: %"PRId64"}, "
                "cluster version: %"PRId64, ctx->server_id, ctx,
                event_source, event_type, ds->dg->id, ds->cs->server->id,
                body_part->is_master, body_part->status,
                ds->data.version, FC_ATOMIC_GET(CLUSTER_CURRENT_VERSION));
                */

        ++body_part;
    }

    long2buff(__sync_add_and_fetch(&CLUSTER_CURRENT_VERSION, 0),
            req_header->current_version);
    int2buff(body_part - bp_start, req_header->data_server_count);
    body_len = (char *)body_part - (char *)req_header;
    SF_PROTO_SET_HEADER(header, FS_CLUSTER_PROTO_PUSH_DATA_SERVER_STATUS,
            body_len);
    ctx->task->length = sizeof(FSProtoHeader) + body_len;
    return sf_send_add_event((struct fast_task_info *)ctx->task);
}

int cluster_topology_process_notify_events(FSClusterNotifyContextPtrArray *
        notify_ctx_ptr_array)
{
    FSClusterTopologyNotifyContext **ctx;
    FSClusterTopologyNotifyContext **end;

    end = notify_ctx_ptr_array->contexts + notify_ctx_ptr_array->count;
    for (ctx=notify_ctx_ptr_array->contexts; ctx<end; ctx++) {
        process_notify_events(*ctx);
    }

    return 0;
}

static bool downgrade_data_server_status(FSClusterDataServerInfo *ds,
        const bool remove_recovery_flag)
{
    int old_status;
    int new_status;

    old_status = __sync_fetch_and_add(&ds->status, 0);
    if (old_status == FS_DS_STATUS_ACTIVE) {
        new_status = FS_DS_STATUS_OFFLINE;
    } else if (remove_recovery_flag) {
        fs_downgrade_data_server_status(old_status, &new_status);
    } else {
        new_status = old_status;
    }

    return cluster_relationship_set_ds_status_ex(ds, old_status, new_status);
}

static void cluster_topology_offline_data_server(
        FSClusterDataServerInfo *ds, const bool unset_master)
{
    FSClusterDataServerInfo *cur;
    FSClusterDataServerInfo *end;
    bool notify;

    notify = downgrade_data_server_status(ds, unset_master);
    if (unset_master && FC_ATOMIC_GET(ds->is_master)) {
        __sync_bool_compare_and_swap(&ds->is_master, 1, 0);
        if (__sync_bool_compare_and_swap(&ds->dg->master, ds, NULL)) {
            cluster_relationship_on_master_change(ds, NULL);

            end = ds->dg->data_server_array.servers +
                ds->dg->data_server_array.count;
            for (cur=ds->dg->data_server_array.servers; cur<end; cur++) {
                if (cur != ds) {
                    if (downgrade_data_server_status(cur, false)) {
                        cluster_topology_data_server_chg_notify(cur,
                                FS_EVENT_SOURCE_CS_LEADER,
                                FS_EVENT_TYPE_STATUS_CHANGE, true);
                    }
                }
            }

            cluster_topology_select_master(ds->dg, false);
        }
        notify = true;
    }

    if (notify) {
        cluster_topology_data_server_chg_notify(ds,
                FS_EVENT_SOURCE_CS_LEADER,
                FS_EVENT_TYPE_STATUS_CHANGE, true);
    }
}

bool cluster_topology_activate_server(FSClusterServerInfo *cs)
{
    FSClusterDataServerInfo **ds;
    FSClusterDataServerInfo **end;

    if (CLUSTER_MYSELF_PTR != CLUSTER_LEADER_ATOM_PTR) {
        return false;
    }

    cluster_relationship_set_server_status(cs, FS_SERVER_STATUS_ACTIVE);
    cluster_relationship_remove_from_inactive_sarray(cs);

    end = cs->ds_ptr_array.servers + cs->ds_ptr_array.count;
    for (ds=cs->ds_ptr_array.servers; ds<end; ds++) {
        if (__sync_fetch_and_add(&(*ds)->dg->master, 0) == NULL) {
            cluster_topology_select_master((*ds)->dg, false);
        }
    }

    return true;
}

bool cluster_topology_deactivate_server(FSClusterServerInfo *cs)
{
    FSClusterDataServerInfo **ds;
    FSClusterDataServerInfo **end;

    if (cluster_relationship_swap_server_status(cs,
                FS_SERVER_STATUS_ACTIVE, FS_SERVER_STATUS_OFFLINE))
    {
        if (CLUSTER_MYSELF_PTR != CLUSTER_LEADER_ATOM_PTR) {
            return false;
        }

        end = cs->ds_ptr_array.servers + cs->ds_ptr_array.count;
        for (ds=cs->ds_ptr_array.servers; ds<end; ds++) {
            cluster_topology_offline_data_server(*ds, true);
        }
        cluster_relationship_add_to_inactive_sarray(cs);
    }

    return true;
}

void cluster_topology_offline_all_data_servers(FSClusterServerInfo *leader)
{
    FSClusterDataGroupInfo *group;
    FSClusterDataGroupInfo *gend;
    FSClusterDataServerInfo *ds;
    FSClusterDataServerInfo *send;

    gend = CLUSTER_DATA_RGOUP_ARRAY.groups + CLUSTER_DATA_RGOUP_ARRAY.count;
    for (group=CLUSTER_DATA_RGOUP_ARRAY.groups; group<gend; group++) {
        group->election.start_time_ms = 0;
        group->election.retry_count = 0;
        send = group->data_server_array.servers + group->data_server_array.count;
        for (ds=group->data_server_array.servers; ds<send; ds++) {
            cluster_topology_offline_data_server(ds, false);
        }
    }
}

int cluster_topology_offline_slave_data_servers(
        FSClusterServerInfo *peer, int *count)
{
    FSClusterDataServerInfo **pp;
    FSClusterDataServerInfo **end;
    FSClusterDataServerInfo *ds;
    FSClusterDataServerInfo *master;
    int old_status;
    bool changed;

    *count = 0;
    if (peer == CLUSTER_MYSELF_PTR) {
        logError("file: "__FILE__", line: %d, "
                "can't offline myself!", __LINE__);
        return EINVAL;
    }

    end = peer->ds_ptr_array.servers + peer->ds_ptr_array.count;
    for (pp=peer->ds_ptr_array.servers; pp<end; pp++) {
        master = (FSClusterDataServerInfo *)__sync_fetch_and_add(
                &(*pp)->dg->master, 0);
        if (master == NULL) {
            continue;
        }

        if (master->cs == CLUSTER_MYSELF_PTR) { //i am master
            ds = *pp;
        } else if (master->cs == peer) {  //peer is master
            ds = (*pp)->dg->myself;
            if (ds == NULL) {
                continue;
            }
        } else {
            continue;
        }

        old_status = __sync_fetch_and_add(&ds->status, 0);
        if (old_status == FS_DS_STATUS_ACTIVE) {
            if (master->cs == CLUSTER_MYSELF_PTR) { //report peer/slave status
                changed = cluster_relationship_report_ds_status(ds,
                        old_status, FS_DS_STATUS_OFFLINE,
                        FS_EVENT_SOURCE_MASTER_OFFLINE) == 0;
            } else {  //i am slave
                changed = cluster_relationship_swap_report_ds_status(ds,
                        old_status, FS_DS_STATUS_OFFLINE,
                        FS_EVENT_SOURCE_MASTER_OFFLINE);
            }

            if (changed) {
                ++(*count);
            }
        }
    }

    return 0;
}

static inline void clear_decision_action(FSClusterDataGroupInfo *group)
{
    int old_action;
    if ((old_action=__sync_fetch_and_add(&group->delay_decision.
                    action, 0)) != FS_CLUSTER_DELAY_DECISION_NO_OP)
    {
        __sync_bool_compare_and_swap(&group->delay_decision.action,
                old_action, FS_CLUSTER_DELAY_DECISION_NO_OP);
    }
}

void cluster_topology_set_check_master_flags()
{
    FSClusterDataGroupInfo *group;
    FSClusterDataGroupInfo *end;
    FSClusterDataServerInfo *master;
    int old_count;
    int new_count;

    old_count = __sync_add_and_fetch(&CLUSTER_DATA_RGOUP_ARRAY.
            delay_decision_count, 0);
    if (old_count != 0) {
        __sync_bool_compare_and_swap(&CLUSTER_DATA_RGOUP_ARRAY.
                delay_decision_count, old_count, 0);
    }

    new_count = 0;
    end = CLUSTER_DATA_RGOUP_ARRAY.groups + CLUSTER_DATA_RGOUP_ARRAY.count;
    for (group=CLUSTER_DATA_RGOUP_ARRAY.groups; group<end; group++) {
        clear_decision_action(group);
        master = (FSClusterDataServerInfo *)__sync_fetch_and_add(
                &group->master, 0);
        if (master != NULL) {
            if (__sync_bool_compare_and_swap(&group->master, master, NULL)) {
                __sync_bool_compare_and_swap(&master->is_master, 1, 0);
                cluster_relationship_on_master_change(master, NULL);

                cluster_topology_data_server_chg_notify(master,
                        FS_EVENT_SOURCE_CS_LEADER,
                        FS_EVENT_TYPE_MASTER_CHANGE, true);
            }
        }

        if (__sync_bool_compare_and_swap(&group->delay_decision.action,
                    FS_CLUSTER_DELAY_DECISION_NO_OP,
                    FS_CLUSTER_DELAY_DECISION_SELECT_MASTER))
        {
            group->delay_decision.expire_time = g_current_time + 1;
            ++new_count;
        }
    }

    /*
    logInfo("file: "__FILE__", line: %d, "
            "decision old_count: %d, new_count: %d",
            __LINE__, old_count, new_count);
            */

    if (new_count > 0) {
        __sync_add_and_fetch(&CLUSTER_DATA_RGOUP_ARRAY.
                delay_decision_count, new_count);
    }
}

static int compare_ds_by_data_version(const void *p1, const void *p2)
{
    FSClusterDataServerInfo **ds1;
    FSClusterDataServerInfo **ds2;
    int64_t sub;

    ds1 = (FSClusterDataServerInfo **)p1;
    ds2 = (FSClusterDataServerInfo **)p2;
    if ((sub=fc_compare_int64(FC_ATOMIC_GET((*ds1)->data.version),
                    FC_ATOMIC_GET((*ds2)->data.version))) != 0)
    {
        return sub;
    }

    sub = FC_ATOMIC_GET((*ds1)->cs->status) -
        FC_ATOMIC_GET((*ds2)->cs->status);
    if (sub != 0) {
        return sub;
    }

    sub = FC_ATOMIC_GET((*ds1)->is_master) -
        FC_ATOMIC_GET((*ds2)->is_master);
    if (sub != 0) {
        return sub;
    }

    return (int)(*ds1)->is_preseted - (int)(*ds2)->is_preseted;
}

static FSClusterDataServerInfo *select_master(FSClusterDataGroupInfo *group,
        int *result)
{
#define OFFLINE_WAIT_TIMEOUT   4
#define ONLINE_WAIT_TIMEOUT   30

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
        *result = 0;
        return last;
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
    for (ds=end-1; ds>=group->data_server_array.servers; ds--) {
        if (FC_ATOMIC_GET(ds->cs->status) == FS_SERVER_STATUS_ACTIVE) {
            max_data_version = FC_ATOMIC_GET(ds->data.version);
            break;
        }
    }

    if (max_data_version == -1) {
        *result = ENOENT;
        return NULL;
    }

    active_count = 0;
    for (ds=group->data_server_array.servers; ds<end; ds++) {
        if (FC_ATOMIC_GET(ds->cs->status) == FS_SERVER_STATUS_ACTIVE &&
                FC_ATOMIC_GET(ds->data.version) >= max_data_version)
        {
            online_data_servers[active_count++] = ds;
        }
    }

    if (active_count == 0) {
        *result = ENOENT;
        return NULL;
    }

    if (active_count == group->data_server_array.count) {
        *result = 0;
        return last;
    }

    master_index = group->hash_code % active_count;
    /*
    logInfo("data_group_id: %d, active_count: %d, master_index: %d, hash_code: %d",
            group->id, active_count, master_index, group->hash_code);
            */

    ds = online_data_servers[master_index];
    if (FC_ATOMIC_GET(ds->cs->status) == FS_SERVER_STATUS_ACTIVE) {
        *result = 0;
        return ds;
    }

    *result = EAGAIN;
    return NULL;
}

static int cluster_topology_select_master(FSClusterDataGroupInfo *group,
        const bool by_decision)
{
    FSClusterDataServerInfo *master;
    int old_action;
    int result;

    PTHREAD_MUTEX_LOCK(&group->lock);
    if (__sync_add_and_fetch(&group->master, 0) == NULL) {
        master = select_master(group, &result);
    } else {
        master = NULL;
        result = 0;
    }
    PTHREAD_MUTEX_UNLOCK(&group->lock);

    if (master == NULL) {
        if (by_decision) {
            group->delay_decision.expire_time = g_current_time + 1;
            return EAGAIN;
        }

        if ((old_action=FC_ATOMIC_GET(group->delay_decision.
                        action)) == FS_CLUSTER_DELAY_DECISION_NO_OP)
        {
            if (__sync_bool_compare_and_swap(&group->delay_decision.action,
                        old_action, FS_CLUSTER_DELAY_DECISION_SELECT_MASTER))
            {
                group->delay_decision.expire_time = g_current_time + 1;
                __sync_add_and_fetch(&CLUSTER_DATA_RGOUP_ARRAY.
                        delay_decision_count, 1);
            }
        }

        return result;
    }

    if (__sync_bool_compare_and_swap(&group->master, NULL, master)) {
        int64_t time_used;
        char time_buff[32];

        __sync_bool_compare_and_swap(&master->is_master, 0, 1);
        cluster_relationship_on_master_change(NULL, master);
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
    }
    group->election.start_time_ms = 0;
    group->election.retry_count = 0;

    return 0;
}

static int decision_select_master(FSClusterDataGroupInfo *group)
{
    if (__sync_add_and_fetch(&group->master, 0) != NULL) {
        return 0;
    }

    if (group->delay_decision.expire_time > g_current_time) {
        return EAGAIN;
    }

    return cluster_topology_select_master(group, true);
}

void cluster_topology_check_and_make_delay_decisions()
{
    FSClusterDataGroupInfo *group;
    FSClusterDataGroupInfo *end;
    int result;
    int action;
    int decision_count;
    int done_count;

    decision_count = __sync_add_and_fetch(&CLUSTER_DATA_RGOUP_ARRAY.
            delay_decision_count, 0);
    if (decision_count == 0) {
        return;
    }

    done_count = 0;
    end = CLUSTER_DATA_RGOUP_ARRAY.groups + CLUSTER_DATA_RGOUP_ARRAY.count;
    for (group=CLUSTER_DATA_RGOUP_ARRAY.groups; group<end; group++) {
        action = __sync_fetch_and_add(&group->delay_decision.action, 0);
        switch (action) {
            case FS_CLUSTER_DELAY_DECISION_NO_OP:
                continue;
            case FS_CLUSTER_DELAY_DECISION_SELECT_MASTER:
                result = decision_select_master(group);
                break;
            default:
                continue;
        }

        /*
        logInfo("decision_count: %d, decision old action: %d, "
                "new action: %d, result: %d", decision_count, action,
                __sync_sub_and_fetch(&group->delay_decision.action, 0),
                result);
                */

        if (result != EAGAIN) {
            if (__sync_bool_compare_and_swap(&group->delay_decision.action,
                        action, FS_CLUSTER_DELAY_DECISION_NO_OP))
            {
                ++done_count;
            }
        }
    }

    if (done_count > 0) {
        __sync_sub_and_fetch(&CLUSTER_DATA_RGOUP_ARRAY.
                delay_decision_count, done_count);
    }
}
