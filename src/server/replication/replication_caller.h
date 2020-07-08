//replication_caller.h

#ifndef _REPLICATION_CALLER_H_
#define _REPLICATION_CALLER_H_

#include "replication_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int replication_caller_init();
void replication_caller_destroy();

void replication_caller_release_rpc_entry(ReplicationRPCEntry *rpc);

int replication_caller_push_to_slave_queues(struct fast_task_info *task);

#ifdef __cplusplus
}
#endif

#endif
