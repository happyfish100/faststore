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

ServerBinlogRecordBuffer *replication_producer_alloc_rbuffer();

void replication_producer_free_rbuffer(ServerBinlogRecordBuffer *rbuffer);

int replication_producer_push_to_queues(const int data_group_id,
        const uint32_t hash_code, ServerBinlogRecordBuffer *rbuffer);

int fs_get_replication_count();
FSReplication *fs_get_idle_replication_by_peer(const int peer_id);

#ifdef __cplusplus
}
#endif

#endif
