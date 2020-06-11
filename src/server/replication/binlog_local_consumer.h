//binlog_local_consumer.h

#ifndef _BINLOG_LOCAL_CONSUMER_H_
#define _BINLOG_LOCAL_CONSUMER_H_

#include "replication_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int binlog_local_consumer_init();
void binlog_local_consumer_destroy();
void binlog_local_consumer_terminate();

int binlog_local_consumer_start();

int binlog_local_consumer_push_to_queues(const int data_group_index,
        ServerBinlogRecordBuffer *rbuffer);

int fs_get_replication_count();
FSReplication *fs_get_idle_replication_by_peer(const int peer_id);

#ifdef __cplusplus
}
#endif

#endif
