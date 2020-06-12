//binlog_replication.h

#ifndef _BINLOG_REPLICATION_H_
#define _BINLOG_REPLICATION_H_

#include "replication_types.h"

#ifdef __cplusplus
extern "C" {
#endif

//replication server side
void binlog_replication_bind_task(FSReplication *replication,
        struct fast_task_info *task);

//replication client side
int binlog_replication_bind_thread(FSReplication *replication);

//replication server and client
int binlog_replication_unbind(FSReplication *replication);

int binlog_replication_process(FSServerContext *server_ctx);

void clean_connected_replications(FSServerContext *server_ctx);

int binlog_replications_check_response_data_version(
        FSReplication *replication,
        const int64_t data_version);

static inline void set_replication_stage(FSReplication *
        replication, const int stage)
{
    replication->stage = stage;
}

#ifdef __cplusplus
}
#endif

#endif
