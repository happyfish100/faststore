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

//cluster_relationship.h

#ifndef _CLUSTER_RELATIONSHIP_H_
#define _CLUSTER_RELATIONSHIP_H_

#include <time.h>
#include <pthread.h>
#include "fastcommon/fc_atomic.h"
#include "server_types.h"
#include "server_group_info.h"
#include "master_election.h"

#ifdef __cplusplus
extern "C" {
#endif

int cluster_relationship_init();
int cluster_relationship_destroy();

int cluster_relationship_start();

int cluster_relationship_pre_set_leader(FSClusterServerInfo *leader);

int cluster_relationship_commit_leader(FSClusterServerInfo *leader);

void cluster_relationship_trigger_reselect_leader();

void cluster_relationship_trigger_reselect_master(
        FSClusterDataGroupInfo *group);

int cluster_relationship_report_reselect_master_to_leader(
        FSClusterDataServerInfo *ds);

bool cluster_relationship_set_ds_status_ex(FSClusterDataServerInfo *ds,
        const int old_status, const int new_status);

static inline bool cluster_relationship_set_ds_status(
        FSClusterDataServerInfo *ds, const int new_status)
{
    int old_status;
    old_status = __sync_add_and_fetch(&ds->status, 0);
    return cluster_relationship_set_ds_status_ex(ds, old_status, new_status);
}

int cluster_relationship_set_ds_status_and_dv(FSClusterDataServerInfo *ds,
        const int status, const FSClusterDataVersionPair *data_versions);

int cluster_relationship_report_ds_status(FSClusterDataServerInfo *ds,
        const int old_status, const int new_status, const int source);

bool cluster_relationship_swap_report_ds_status(FSClusterDataServerInfo *ds,
        const int old_status, const int new_status, const int source);

void cluster_relationship_trigger_report_ds_status(FSClusterDataServerInfo *ds);

void cluster_relationship_on_master_change(FSClusterDataGroupInfo *group,
        FSClusterDataServerInfo *old_master,
        FSClusterDataServerInfo *new_master);

void cluster_relationship_add_to_inactive_sarray(FSClusterServerInfo *cs);

void cluster_relationship_remove_from_inactive_sarray(FSClusterServerInfo *cs);

static inline bool cluster_relationship_swap_server_status(
        FSClusterServerInfo *cs, const int old_status, const int new_status)
{
    bool success;

    master_election_lock();
    success = __sync_bool_compare_and_swap(&cs->status,
            old_status, new_status);
    master_election_unlock();

    if (success) {
        cs->status_changed_time = g_current_time;
    }
    return success;
}

static inline bool cluster_relationship_set_server_status(
        FSClusterServerInfo *cs, const int new_status)
{
    int old_status;

    while (1) {
        old_status = FC_ATOMIC_GET(cs->status);
        if (new_status == old_status) {
            return false;
        }

        if (cluster_relationship_swap_server_status(
                    cs, old_status, new_status))
        {
            return true;
        }
    }
}

static inline int cluster_relationship_get_max_buffer_size()
{
    int bytes;
    int buffer_size;

    bytes = sizeof(FSProtoHeader) + sizeof(FSProtoPingLeaderReqHeader) +
        sizeof(FSProtoPingLeaderReqBodyPart) * CLUSTER_DATA_GROUP_ARRAY.count;
    buffer_size = 4 * 1024;
    while (buffer_size < bytes) {
        buffer_size *= 2;
    }

    return buffer_size;
}

#ifdef __cplusplus
}
#endif

#endif
