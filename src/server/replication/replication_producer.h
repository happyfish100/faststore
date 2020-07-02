//replication_producer.h

#ifndef _REPLICATION_PRODUCER_H_
#define _REPLICATION_PRODUCER_H_

#include "replication_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int replication_producer_init();
void replication_producer_destroy();
void replication_producer_terminate();

int replication_producer_start();

void replication_producer_release_rbuffer(ServerBinlogRecordBuffer *rbuffer);

int replication_producer_push_to_slave_queues(struct fast_task_info *task);

int fs_get_replication_count();
FSReplication *fs_get_idle_replication_by_peer(const int peer_id);

#ifdef __cplusplus
}
#endif

#endif
