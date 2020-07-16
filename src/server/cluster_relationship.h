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

bool cluster_relationship_set_ds_status(FSClusterDataServerInfo *ds,
        const int new_status);

bool cluster_relationship_set_ds_status_and_dv(FSClusterDataServerInfo *ds,
        const int status, const uint64_t data_version);

void cluster_relationship_set_my_status(FSClusterDataServerInfo *ds,
        const int new_status, const bool notify_leader);

void cluster_relationship_add_to_inactive_sarray(FSClusterServerInfo *cs);

void cluster_relationship_remove_from_inactive_sarray(FSClusterServerInfo *cs);

#ifdef __cplusplus
}
#endif

#endif
