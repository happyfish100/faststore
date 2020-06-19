//cluster_topology.h

#ifndef _CLUSTER_TOPOLOGY_H_
#define _CLUSTER_TOPOLOGY_H_

#include <time.h>
#include <errno.h>
#include "fastcommon/logger.h"
#include "server_types.h"
#include "server_group_info.h"

#ifdef __cplusplus
extern "C" {
#endif

int cluster_topology_init_notify_ctx(FSClusterTopologyNotifyContext *notify_ctx);

static inline int cluster_topology_add_notify_ctx(
        FSClusterNotifyContextPtrArray *notify_array,
        FSClusterTopologyNotifyContext *notify_ctx)
{
    if (notify_array->count >= notify_array->alloc) {
        logError("file: "__FILE__", line: %d, "
                "notify contexts exceeds max count: %d",
                __LINE__, notify_array->alloc);
        return EOVERFLOW;
    }

    notify_array->contexts[notify_array->count++] = notify_ctx;
    return 0;
}

static inline int cluster_topology_remove_notify_ctx(
        FSClusterNotifyContextPtrArray *notify_array,
        FSClusterTopologyNotifyContext *notify_ctx)
{
    int i;
    int m;

    for (i=0; i<notify_array->count; i++) {
        if (notify_array->contexts[i] == notify_ctx) {
            break;
        }
    }

    if (i == notify_array->count) {
        logWarning("file: "__FILE__", line: %d, "
                "notify context: %p not exist",
                __LINE__, notify_ctx);
        return ENOENT;
    }

    for (m=i+1; m<notify_array->count; m++) {
        notify_array->contexts[m - 1] = notify_array->contexts[m];
    }
    notify_array->count--;
    return 0;
}

static inline void cluster_topology_activate_server(FSClusterServerInfo *cs)
{
    __sync_bool_compare_and_swap(&cs->active, 0, 1);
}

static inline void cluster_topology_deactivate_server(FSClusterServerInfo *cs)
{
    __sync_bool_compare_and_swap(&cs->active, 1, 0);
}

void cluster_topology_data_server_chg_notify(FSClusterDataServerInfo *
        data_server, const bool notify_self);

void cluster_topology_sync_all_data_servers(FSClusterServerInfo *cs);

int cluster_topology_process_notify_events(FSClusterNotifyContextPtrArray *
        notify_ctx_ptr_array);

void cluster_topology_change_data_server_status(FSClusterDataServerInfo *
        data_server, const int new_status);

void cluster_topology_set_check_master_flags();

void cluster_topology_check_and_make_delay_decisions();

#ifdef __cplusplus
}
#endif

#endif
