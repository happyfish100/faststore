//cluster_relationship.h

#ifndef _CLUSTER_RELATIONSHIP_H_
#define _CLUSTER_RELATIONSHIP_H_

#include <time.h>
#include <pthread.h>
#include "server_types.h"
#include "server_group_info.h"

#ifdef __cplusplus
extern "C" {
#endif

extern FSClusterServerInfo *g_next_leader;

int cluster_relationship_init();
int cluster_relationship_destroy();

int cluster_relationship_pre_set_leader(FSClusterServerInfo *leader);

int cluster_relationship_commit_leader(FSClusterServerInfo *leader);

void cluster_relationship_trigger_reselect_leader();

bool cluster_relationship_set_ds_status_ex(FSClusterDataServerInfo *ds,
        const int old_status, const int new_status);

static inline bool cluster_relationship_set_ds_status(
        FSClusterDataServerInfo *ds, const int new_status)
{
    int old_status;
    old_status = __sync_add_and_fetch(&ds->status, 0);
    return cluster_relationship_set_ds_status_ex(ds, old_status, new_status);
}

bool cluster_relationship_set_ds_status_and_dv(FSClusterDataServerInfo *ds,
        const int status, const uint64_t data_version);

void cluster_relationship_report_ds_status(FSClusterDataServerInfo *ds);

bool cluster_relationship_swap_report_ds_status(FSClusterDataServerInfo *ds,
        const int old_status, const int new_status);

bool cluster_relationship_set_report_ds_status(FSClusterDataServerInfo *ds,
        const int new_status);

int cluster_relationship_on_master_change(FSClusterDataServerInfo *old_master,
        FSClusterDataServerInfo *new_master);

void cluster_relationship_add_to_inactive_sarray(FSClusterServerInfo *cs);

void cluster_relationship_remove_from_inactive_sarray(FSClusterServerInfo *cs);

#ifdef __cplusplus
}
#endif

#endif
