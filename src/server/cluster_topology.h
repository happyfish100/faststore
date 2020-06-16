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

int cluster_topology_data_server_chg_notify(const int data_group_id,
        const int server_index);

#ifdef __cplusplus
}
#endif

#endif
