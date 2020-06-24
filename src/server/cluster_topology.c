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
#include <assert.h>
#include <pthread.h>
#include "fastcommon/logger.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/ioevent_loop.h"
#include "sf/sf_global.h"
#include "sf/sf_nio.h"
#include "common/fs_proto.h"
#include "server_global.h"
#include "cluster_relationship.h"
#include "cluster_topology.h"

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
            //logInfo("data group index: %d, server id: %d", gindex, cs->server->id);
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
    int i;
    FSClusterServerInfo *cs;
    FSClusterServerInfo *end;

    if ((result=fc_queue_init(&notify_ctx->queue, (long)
                    (&((FSDataServerChangeEvent *)NULL)->next))) != 0)
    {
        return result;
    }

    count = CLUSTER_DATA_RGOUP_ARRAY.count * CLUSTER_SERVER_ARRAY.count;
    bytes = sizeof(FSDataServerChangeEvent) * count;
    notify_ctx->events = (FSDataServerChangeEvent *)malloc(bytes);
    if (notify_ctx->events == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(notify_ctx->events, 0, bytes);

    logInfo("data group count: %d, server count: %d\n",
            CLUSTER_DATA_RGOUP_ARRAY.count, CLUSTER_SERVER_ARRAY.count);

    end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (i=0; i<CLUSTER_DATA_RGOUP_ARRAY.count; i++) {
        for (cs=CLUSTER_SERVER_ARRAY.servers; cs<end; cs++) {
            index = i * CLUSTER_SERVER_ARRAY.count + cs->server_index;
            notify_ctx->events[index].data_server =
                find_data_group_server(i, cs);
        }
    }

    return 0;
}

void cluster_topology_data_server_chg_notify(FSClusterDataServerInfo *
        data_server, const bool notify_self)
{
    FSClusterServerInfo *cs;
    FSClusterServerInfo *end;
    FSDataServerChangeEvent *event;
    struct fast_task_info *task;
    bool notify;

    end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (cs=CLUSTER_SERVER_ARRAY.servers; cs<end; cs++) {
        if (cs->is_leader || (!notify_self && data_server->cs == cs) ||
                (__sync_fetch_and_add(&cs->active, 0) == 0))
        {
            continue;
        }
        task = (struct fast_task_info *)cs->notify_ctx.task;
        if (task == NULL) {
            continue;
        }

        event = cs->notify_ctx.events + (data_server->dg->index *
                CLUSTER_SERVER_ARRAY.count + data_server->cs->server_index);
        assert(event->data_server == data_server);

        if (__sync_bool_compare_and_swap(&event->in_queue, 0, 1)) { //fetch event
            fc_queue_push_ex(&cs->notify_ctx.queue, event, &notify);
            if (notify) {
                iovent_notify_thread(task->thread_data);
            }
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
            if (ds->cs == cs) {
                continue;
            }

            event = cs->notify_ctx.events + (ds->dg->index *
                    CLUSTER_SERVER_ARRAY.count + ds->cs->server_index);
            assert(event->data_server == ds);
            if (__sync_bool_compare_and_swap(&event->in_queue, 0, 1)) { //fetch event
                fc_queue_push(&cs->notify_ctx.queue, event);
            }
        }
    }
}

static int process_notify_events(FSClusterTopologyNotifyContext *ctx)
{
    FSDataServerChangeEvent *event;
    FSProtoHeader *header;
    FSProtoPushDataServerStatusHeader *req_header;
    FSProtoPushDataServerStatusBodyPart *bp_start;
    FSProtoPushDataServerStatusBodyPart *body_part;
    int body_len;

    if (!(ctx->task->offset == 0 && ctx->task->length == 0)) {
        return EBUSY;
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
        int2buff(event->data_server->dg->id, body_part->data_group_id);
        int2buff(event->data_server->cs->server->id, body_part->server_id);
        body_part->is_master = event->data_server->is_master;
        body_part->status = event->data_server->status;
        long2buff(event->data_server->data_version, body_part->data_version);

        __sync_bool_compare_and_swap(&event->in_queue, 1, 0);  //release event

        ++body_part;
        event = event->next;
    }

    long2buff(__sync_add_and_fetch(&CLUSTER_CURRENT_VERSION, 0),
            req_header->current_version);
    int2buff(body_part - bp_start, req_header->data_server_count);
    body_len = (char *)body_part - (char *)req_header;
    FS_PROTO_SET_HEADER(header, FS_CLUSTER_PROTO_PUSH_DATA_SERVER_STATUS,
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

static inline void cluster_topology_offline_data_server(
        FSClusterDataServerInfo *ds, const bool unset_master)
{
    bool notify;

    notify = false;
    if (unset_master && __sync_fetch_and_add(&ds->is_master, 0)) {
        __sync_bool_compare_and_swap(&ds->dg->master, ds, NULL);
        __sync_bool_compare_and_swap(&ds->is_master, true, false);
        notify = true;
    }
    if (ds->status == FS_SERVER_STATUS_SYNCING || 
            ds->status == FS_SERVER_STATUS_ACTIVE)
    {
        ds->status = FS_SERVER_STATUS_OFFLINE;
        notify = true;
    }

    if (notify) {
        cluster_topology_data_server_chg_notify(ds, false);
    }
}

void cluster_topology_activate_server(FSClusterServerInfo *cs)
{
    __sync_bool_compare_and_swap(&cs->active, 0, 1);
    cluster_relationship_remove_from_inactive_sarray(cs);
}

void cluster_topology_deactivate_server(FSClusterServerInfo *cs)
{
    FSClusterDataServerInfo **ds;
    FSClusterDataServerInfo **end;

    __sync_bool_compare_and_swap(&cs->active, 1, 0);

    end = cs->ds_ptr_array.servers + cs->ds_ptr_array.count;
    for (ds=cs->ds_ptr_array.servers; ds<end; ds++) {
        cluster_topology_offline_data_server(*ds, true);
    }
    cluster_relationship_add_to_inactive_sarray(cs);
}

void cluster_topology_offline_all_data_servers()
{
    FSClusterDataGroupInfo *group;
    FSClusterDataGroupInfo *gend;
    FSClusterDataServerInfo *ds;
    FSClusterDataServerInfo *send;

    gend = CLUSTER_DATA_RGOUP_ARRAY.groups + CLUSTER_DATA_RGOUP_ARRAY.count;
    for (group=CLUSTER_DATA_RGOUP_ARRAY.groups; group<gend; group++) {
        send = group->data_server_array.servers + group->data_server_array.count;
        for (ds=group->data_server_array.servers; ds<send; ds++) {
            if (ds->cs != CLUSTER_MYSELF_PTR) {
                cluster_topology_offline_data_server(ds, false);
            }
        }
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
        master = (FSClusterDataServerInfo *)group->master;
        if (master == NULL) {
            if (group->delay_decision.action !=
                    FS_CLUSTER_DELAY_DECISION_NO_OP)
            {
                group->delay_decision.action = 
                    FS_CLUSTER_DELAY_DECISION_NO_OP;
            }
            continue;
        }

        group->delay_decision.action = FS_CLUSTER_DELAY_DECISION_CHECK_MASTER;
        group->delay_decision.expire_time = g_current_time + 5;
        ++new_count;
    }

    logInfo("file: "__FILE__", line: %d, "
            "old_count: %d, new_count: %d",
            __LINE__, old_count, new_count);

    if (new_count > 0) {
        __sync_add_and_fetch(&CLUSTER_DATA_RGOUP_ARRAY.
                delay_decision_count, new_count);
    }
}

static int decision_check_master(FSClusterDataGroupInfo *group)
{
    FSClusterDataServerInfo *master;

    if (group->delay_decision.expire_time >= g_current_time) {
        return EAGAIN;
    }

    master = (FSClusterDataServerInfo *)__sync_fetch_and_add(&group->master, 0);
    if (master == NULL) {
        return 0;
    }

    if (__sync_fetch_and_add(&master->status, 0) != FS_SERVER_STATUS_ACTIVE) {
        if (__sync_bool_compare_and_swap(&group->master, master, NULL)) {
            __sync_bool_compare_and_swap(&master->is_master, true, false);
            cluster_topology_data_server_chg_notify(master, true);

            group->delay_decision.action = FS_CLUSTER_DELAY_DECISION_SELECT_MASTER;
            group->delay_decision.expire_time = g_current_time + 5;
            return EAGAIN;
        }
    }

    return 0;
}

static FSClusterDataServerInfo *select_master(FSClusterDataGroupInfo *group,
        const bool force, int *result)
{
    FSClusterDataServerInfo *ds;
    FSClusterDataServerInfo *end;
    FSClusterDataServerInfo *master;
    int active_count;
    int master_index;
    int index;

    active_count = 0;
    master = NULL;
    end = group->data_server_array.servers + group->data_server_array.count;
    for (ds=group->data_server_array.servers; ds<end; ds++) {
        if (__sync_fetch_and_add(&ds->status, 0) == FS_SERVER_STATUS_ACTIVE) {
            if (ds->is_preseted) {
                *result = 0;
                return ds;
            }
            ++active_count;
        }
    }

    if (active_count == 0) {
        *result = ENOENT;
        return NULL;
    }

    if (!force) {
        if (group->delay_decision.action == FS_CLUSTER_DELAY_DECISION_NO_OP) {
            group->delay_decision.action = FS_CLUSTER_DELAY_DECISION_SELECT_MASTER;
            group->delay_decision.expire_time = g_current_time + 5;
            __sync_add_and_fetch(&CLUSTER_DATA_RGOUP_ARRAY.
                    delay_decision_count, 1);
        }
        *result = EAGAIN;
        return NULL;
    }

    master_index = group->id % active_count;
    index = 0;
    for (ds=group->data_server_array.servers; ds<end; ds++) {
        if (__sync_fetch_and_add(&ds->status, 0) == FS_SERVER_STATUS_ACTIVE) {
            if (index == master_index) {
                *result = 0;
                return ds;
            }
            ++index;
        }
    }

    *result = ENOENT;
    return NULL;
}

int cluster_topology_select_master(FSClusterDataGroupInfo *group,
        const bool force)
{
    FSClusterDataServerInfo *master;
    int result;

    if ((master=select_master(group, force, &result)) == NULL) {
        return result;
    }

    if (__sync_bool_compare_and_swap(&group->master, NULL, master)) {
        __sync_bool_compare_and_swap(&master->is_master, false, true);
        cluster_topology_data_server_chg_notify(master, true);

        logInfo("file: "__FILE__", line: %d, "
                "data group id: %d, elected master id: %d, "
                "is_preseted: %d", __LINE__, group->id,
                master->cs->server->id, master->is_preseted);
    }

    return 0;
}

static int decision_select_master(FSClusterDataGroupInfo *group)
{
    if (__sync_add_and_fetch(&group->master, 0) != NULL) {
        return 0;
    }

    if (group->delay_decision.expire_time >= g_current_time) {
        return EAGAIN;
    }

    cluster_topology_select_master(group, true);
    return 0;
}

void cluster_topology_change_data_server_status(FSClusterDataServerInfo *ds,
        const int new_status)
{
    int old_status;

    old_status = __sync_fetch_and_add(&ds->status, 0);
    if (new_status == old_status) {
        return;
    }

    if (!__sync_bool_compare_and_swap(&ds->status, old_status, new_status)) {
        return;
    }

    if (new_status == FS_SERVER_STATUS_ACTIVE &&
            __sync_fetch_and_add(&ds->dg->master, 0) == NULL)
    {
        cluster_topology_select_master(ds->dg, false);
    }
}

void cluster_topology_check_and_make_delay_decisions()
{
    FSClusterDataGroupInfo *group;
    FSClusterDataGroupInfo *end;
    int result;
    int decision_count;
    int done_count;

    decision_count = __sync_add_and_fetch(&CLUSTER_DATA_RGOUP_ARRAY.
            delay_decision_count, 0);
    if (decision_count == 0) {
        return;
    }

    logInfo("file: "__FILE__", line: %d, "
            "decision_count: %d", __LINE__, decision_count);

    done_count = 0;
    end = CLUSTER_DATA_RGOUP_ARRAY.groups + CLUSTER_DATA_RGOUP_ARRAY.count;
    for (group=CLUSTER_DATA_RGOUP_ARRAY.groups; group<end; group++) {
        switch (group->delay_decision.action) {
            case FS_CLUSTER_DELAY_DECISION_NO_OP:
                continue;
            case FS_CLUSTER_DELAY_DECISION_CHECK_MASTER:
                result = decision_check_master(group);
                break;
            case FS_CLUSTER_DELAY_DECISION_SELECT_MASTER:
                result = decision_select_master(group);
                break;
            default:
                continue;
        }

        if (result == 0) {
            group->delay_decision.action = FS_CLUSTER_DELAY_DECISION_NO_OP;
            ++done_count;
        }
    }

    if (done_count > 0) {
        __sync_sub_and_fetch(&CLUSTER_DATA_RGOUP_ARRAY.
                delay_decision_count, done_count);
    }
}