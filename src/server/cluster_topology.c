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
#include "master_election.h"
#include "cluster_relationship.h"
#include "cluster_topology.h"

static FSClusterDataServerInfo *find_data_group_server(
        const int gindex, FSClusterServerInfo *cs)
{
    FSClusterDataServerArray *ds_array;
    FSClusterDataServerInfo *ds;
    FSClusterDataServerInfo *end;

    ds_array = &CLUSTER_DATA_GROUP_ARRAY.groups[gindex].data_server_array;
    end = ds_array->servers + ds_array->count;
    for (ds=ds_array->servers; ds<end; ds++) {
        if (ds->cs == cs) {
            return ds;
        }
    }

    return NULL;
}

int cluster_topology_init()
{
    int header_size;

    header_size = sizeof(FSProtoHeader) + sizeof(
            FSProtoPushDataServerStatusHeader);
    CT_MAX_EVENTS_PER_PKG = (CLUSTER_SF_CTX.net_buffer_cfg.max_buff_size -
            header_size) / sizeof(FSProtoPushDataServerStatusBodyPart);
    CT_ACTIVE_TEST_INTERVAL = CLUSTER_NETWORK_TIMEOUT / 2;
    if (CT_ACTIVE_TEST_INTERVAL == 0) {
        CT_ACTIVE_TEST_INTERVAL = 1;
    }

    return 0;
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

    count = CLUSTER_DATA_GROUP_ARRAY.count * CLUSTER_SERVER_ARRAY.count;
    bytes = sizeof(FSDataServerChangeEvent) * count;
    notify_ctx->events = (FSDataServerChangeEvent *)fc_malloc(bytes);
    if (notify_ctx->events == NULL) {
        return ENOMEM;
    }
    memset(notify_ctx->events, 0, bytes);

    /*
    logInfo("data group count: %d, server count: %d\n",
            CLUSTER_DATA_GROUP_ARRAY.count, CLUSTER_SERVER_ARRAY.count);
            */

    end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (gindex=0; gindex<CLUSTER_DATA_GROUP_ARRAY.count; gindex++) {
        for (cs=CLUSTER_SERVER_ARRAY.servers; cs<end; cs++) {
            index = gindex * CLUSTER_SERVER_ARRAY.count + cs->server_index;
            notify_ctx->events[index].ds =
                find_data_group_server(gindex, cs);
        }
    }

    return 0;
}

static inline void push_event_to_queue(FSClusterServerInfo *cs,
        FSClusterDataServerInfo *ds, FSDataServerChangeEvent *event,
        bool *notify)
{
#ifdef FS_EVENT_DEBUG_FLAG
    event->sn = FC_ATOMIC_INC(EVENT_STATS_PRODUCE);
    logInfo("file: "__FILE__", line: %d, "
            "produce event count: %"PRId64", data group id: %d, "
            "data server id: %d, is_master: %d, status: %d, "
            "target server id: %d, push to in_queue: %d, "
            "data version: %"PRId64", event {source: %c, type: %d}, "
            "cs: %p, ds: %p", __LINE__, event->sn, ds->dg->id, ds->cs->
            server->id, FC_ATOMIC_GET(ds->is_master), FC_ATOMIC_GET(
                ds->status), cs->server->id, FC_ATOMIC_GET(event->
                    in_queue), FC_ATOMIC_GET(CLUSTER_CURRENT_VERSION),
            event->source, event->type, cs, event->ds);
#endif

    fc_queue_push_ex(&cs->notify_ctx.queue, event, notify);
}

void cluster_topology_data_server_chg_notify(FSClusterDataServerInfo *ds,
        const int source, const int event_type, const bool notify_self)
{
    FSClusterServerInfo *cs;
    FSClusterServerInfo *end;
    FSDataServerChangeEvent *event;
    struct fast_task_info *task;
    bool notify;

    end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (cs=CLUSTER_SERVER_ARRAY.servers; cs<end; cs++) {
        if ((cs == CLUSTER_MYSELF_PTR) ||
                (!notify_self && ds->cs == cs))
        {
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
        if (__sync_bool_compare_and_swap(&event->in_queue, 0, 1)) { //fetch event
            event->source = source;
            event->type = event_type;
            push_event_to_queue(cs, ds, event, &notify);
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
                    cs->server->id, FC_ATOMIC_GET(event->in_queue),
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
    bool notify;

    gend = CLUSTER_DATA_GROUP_ARRAY.groups + CLUSTER_DATA_GROUP_ARRAY.count;
    for (group=CLUSTER_DATA_GROUP_ARRAY.groups; group<gend; group++) {
        ds_end = group->data_server_array.servers +
            group->data_server_array.count;
        for (ds=group->data_server_array.servers; ds<ds_end; ds++) {
            event = cs->notify_ctx.events + (ds->dg->index *
                    CLUSTER_SERVER_ARRAY.count + ds->cs->server_index);
            if (__sync_bool_compare_and_swap(&event->in_queue, 0, 1)) { //fetch event
                event->source = FS_EVENT_SOURCE_CS_LEADER;
                event->type = FS_EVENT_TYPE_STATUS_CHANGE |
                    FS_EVENT_TYPE_DV_CHANGE | FS_EVENT_TYPE_MASTER_CHANGE;
                push_event_to_queue(cs, ds, event, &notify);
            }
        }
    }
}

static inline void send_active_test_package(struct fast_task_info *task)
{
    task->send.ptr->length = sizeof(FSProtoHeader);
    SF_PROTO_SET_HEADER((FSProtoHeader *)task->send.ptr->data,
            SF_PROTO_ACTIVE_TEST_REQ, 0);
    sf_send_add_event(task);
}

static int process_notify_events(FSClusterTopologyNotifyContext *ctx)
{
    struct fc_queue_info qinfo;
    struct fast_task_info *task;
    FSDataServerChangeEvent *event;
    FSClusterServerInfo *cs;
    FSClusterDataServerInfo *ds;
    volatile int *in_queue;
    FSProtoHeader *header;
    FSProtoPushDataServerStatusHeader *req_header;
    FSProtoPushDataServerStatusBodyPart *bp_start;
    FSProtoPushDataServerStatusBodyPart *body_part;
    int body_len;
#ifdef FS_EVENT_DEBUG_FLAG
    int event_source;
    int event_type;
    int64_t sn;
    int64_t consume_count;
#endif

    task = (struct fast_task_info *)ctx->task;
    if (task->canceled || TASK_PENDING_SEND_COUNT > 0) {
        return 0;
    }

    cs = ((FSServerTaskArg *)task->arg)->context.shared.cluster.peer;
    if (FC_ATOMIC_GET(cs->status) != FS_SERVER_STATUS_ACTIVE) {
        logDebug("file: "__FILE__", line: %d, "
                "server id: %d is not active, try again later",
                __LINE__, cs->server->id);
        return EAGAIN;
    }

    fc_queue_try_pop_to_queue(&ctx->queue, &qinfo);
    if (qinfo.head == NULL) {
        if (CLUSTER_PEER != NULL) {
            if (g_current_time - CLUSTER_PEER->last_net_comm_time >=
                    CT_ACTIVE_TEST_INTERVAL)
            {
                ++TASK_PENDING_SEND_COUNT;
                CLUSTER_PEER->last_net_comm_time = g_current_time;
                send_active_test_package(task);
            }
        }

        return 0;
    }

    ++TASK_PENDING_SEND_COUNT;
    if (CLUSTER_PEER != NULL) {
        CLUSTER_PEER->last_net_comm_time = g_current_time;
    }

    event = (FSDataServerChangeEvent *)qinfo.head;
    header = (FSProtoHeader *)task->send.ptr->data;
    req_header = (FSProtoPushDataServerStatusHeader *)(header + 1);
    bp_start = (FSProtoPushDataServerStatusBodyPart *)(req_header + 1);
    body_part = bp_start;
    do {
#ifdef FS_EVENT_DEBUG_FLAG
        event_source = event->source;
        event_type = event->type;
        sn = event->sn;
#endif
        in_queue = &event->in_queue;
        ds = event->ds;
        event = event->next;
        __sync_bool_compare_and_swap(in_queue, 1, 0);  //release event

        int2buff(ds->dg->id, body_part->data_group_id);
        int2buff(ds->cs->server->id, body_part->server_id);
        body_part->is_master = FC_ATOMIC_GET(ds->is_master);
        body_part->status = FC_ATOMIC_GET(ds->status);
        long2buff(FC_ATOMIC_GET(ds->data.current_version),
                body_part->data_versions.current);
        long2buff(FC_ATOMIC_GET(ds->data.confirmed_version),
                body_part->data_versions.confirmed);

#ifdef FS_EVENT_DEBUG_FLAG
        consume_count = FC_ATOMIC_INC(EVENT_STATS_CONSUME);
        logInfo("file: "__FILE__", line: %d, consume event count: %"PRId64", "
                "sn: %"PRId64", target server id: %d (ctx: %p), event source: "
                "%c, type: %d, {data group id: %d, data server id: %d, "
                "is_master: %d, status: %d, data_version: %"PRId64"}, "
                "cluster version: %"PRId64, __LINE__, consume_count,
                sn, ctx->server_id, ctx, event_source, event_type,
                ds->dg->id, ds->cs->server->id, body_part->is_master,
                body_part->status, ds->data.current_version,
                FC_ATOMIC_GET(CLUSTER_CURRENT_VERSION));
#endif

        ++body_part;
        if (body_part - bp_start == CT_MAX_EVENTS_PER_PKG) {
            break;
        }
    } while (event != NULL);

    if (event != NULL) {
        qinfo.head = event;
        fc_queue_push_queue_to_head_silence(&ctx->queue, &qinfo);
    }

    long2buff(FC_ATOMIC_GET(CLUSTER_CURRENT_VERSION),
            req_header->current_version);
    int2buff(body_part - bp_start, req_header->data_server_count);
    body_len = (char *)body_part - (char *)req_header;
    header->cmd = FS_CLUSTER_PROTO_PUSH_DS_STATUS_REQ;
    int2buff(body_len, header->body_len);
    task->send.ptr->length = sizeof(FSProtoHeader) + body_len;
    return sf_send_add_event(task);
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

    old_status = FC_ATOMIC_GET(ds->status);
    if (old_status == FS_DS_STATUS_ACTIVE) {
        new_status = FS_DS_STATUS_OFFLINE;
    } else if (remove_recovery_flag) {
        fs_downgrade_data_server_status(old_status, &new_status);
    } else {
        new_status = old_status;
    }

    return cluster_relationship_set_ds_status_ex(ds, old_status, new_status);
}

static void cluster_topology_offline_data_server_ex(FSClusterDataServerInfo *ds,
        const bool unset_master, bool *trigger_election)
{
    FSClusterDataServerInfo *cur;
    FSClusterDataServerInfo *end;
    bool notify;

    notify = downgrade_data_server_status(ds, unset_master);
    if (unset_master && FC_ATOMIC_GET(ds->is_master)) {
        if (master_election_set_master(ds->dg, ds, NULL)) {
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

            *trigger_election = true;
        } else {
            *trigger_election = false;
        }

        notify = true;
    } else {
        *trigger_election = false;
    }

    if (notify) {
        cluster_topology_data_server_chg_notify(ds,
                FS_EVENT_SOURCE_CS_LEADER,
                FS_EVENT_TYPE_STATUS_CHANGE, true);
    }
}

static inline void cluster_topology_offline_data_server(
        FSClusterDataServerInfo *ds)
{
    const bool unset_master = false;
    bool trigger_election;
    cluster_topology_offline_data_server_ex(ds,
            unset_master, &trigger_election);
}

bool cluster_topology_activate_server(FSClusterServerInfo *cs)
{
    FSClusterDataServerInfo **ds;
    FSClusterDataServerInfo **end;
    struct common_blocked_chain chain;
    int dg_count;

    if (CLUSTER_MYSELF_PTR != CLUSTER_LEADER_ATOM_PTR) {
        return false;
    }

    cluster_relationship_set_server_status(cs, FS_SERVER_STATUS_ACTIVE);
    cluster_relationship_remove_from_inactive_sarray(cs);

    chain.head = chain.tail = NULL;
    dg_count = 0;
    end = cs->ds_ptr_array.servers + cs->ds_ptr_array.count;
    for (ds=cs->ds_ptr_array.servers; ds<end; ds++) {
        if (FC_ATOMIC_GET((*ds)->dg->master) == NULL) {
            master_election_add_to_chain(&chain, &dg_count, (*ds)->dg);
        }
    }

    if (chain.head != NULL) {
        chain.tail->next = NULL;
        master_election_queue_batch_push(&chain, dg_count);
    }

    return true;
}

bool cluster_topology_deactivate_server(FSClusterServerInfo *cs)
{
    FSClusterDataServerInfo **ds;
    FSClusterDataServerInfo **end;
    struct common_blocked_chain chain;
    int dg_count;
    bool trigger_election;

    if (cluster_relationship_swap_server_status(cs,
                FS_SERVER_STATUS_ACTIVE, FS_SERVER_STATUS_OFFLINE))
    {
        if (CLUSTER_MYSELF_PTR != CLUSTER_LEADER_ATOM_PTR) {
            return false;
        }

        chain.head = chain.tail = NULL;
        dg_count = 0;
        end = cs->ds_ptr_array.servers + cs->ds_ptr_array.count;
        for (ds=cs->ds_ptr_array.servers; ds<end; ds++) {
            cluster_topology_offline_data_server_ex(*ds,
                    true, &trigger_election);
            if (trigger_election) {
                master_election_add_to_chain(&chain, &dg_count, (*ds)->dg);
            }
        }
        if (chain.head != NULL) {
            chain.tail->next = NULL;
            master_election_queue_batch_push(&chain, dg_count);
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

    gend = CLUSTER_DATA_GROUP_ARRAY.groups + CLUSTER_DATA_GROUP_ARRAY.count;
    for (group=CLUSTER_DATA_GROUP_ARRAY.groups; group<gend; group++) {
        group->election.start_time_ms = 0;
        group->election.retry_count = 0;
        send = group->data_server_array.servers +
            group->data_server_array.count;
        for (ds=group->data_server_array.servers; ds<send; ds++) {
            cluster_topology_offline_data_server(ds);
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
