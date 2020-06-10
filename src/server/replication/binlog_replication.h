//binlog_replication.h

#ifndef _BINLOG_REPLICATION_H_
#define _BINLOG_REPLICATION_H_

#include "replication_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int binlog_replication_bind_thread(FSSlaveReplication *replication);
int binlog_replication_rebind_thread(FSSlaveReplication *replication);

int binlog_replication_process(FSServerContext *server_ctx);

void clean_connected_replications(FSServerContext *server_ctx);

int binlog_replications_check_response_data_version(
        FSSlaveReplication *replication,
        const int64_t data_version);

#ifdef __cplusplus
}
#endif

#endif
