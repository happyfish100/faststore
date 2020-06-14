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

#ifdef __cplusplus
}
#endif

#endif
