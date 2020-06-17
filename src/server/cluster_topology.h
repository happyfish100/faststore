//cluster_topology.h

#ifndef _CLUSTER_TOPOLOGY_H_
#define _CLUSTER_TOPOLOGY_H_

#include <time.h>
#include <pthread.h>
#include "server_types.h"
#include "server_group_info.h"

#ifdef __cplusplus
extern "C" {
#endif

int cluster_topology_init_notify_ctx(FSClusterTopologyNotifyContext *notify_ctx);

static inline void cluster_topology_activate_server(FSClusterServerInfo *cs)
{
    __sync_bool_compare_and_swap(&cs->active, 0, 1);
}

static inline void cluster_topology_deactivate_server(FSClusterServerInfo *cs)
{
    __sync_bool_compare_and_swap(&cs->active, 1, 0);
}

int cluster_topology_data_server_chg_notify(FSClusterDataServerInfo *
        data_server, const bool notify_self);

#ifdef __cplusplus
}
#endif

#endif
