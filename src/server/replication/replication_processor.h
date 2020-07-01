//replication_processor.h

#ifndef _REPLICATION_PROCESSOR_H_
#define _REPLICATION_PROCESSOR_H_

#include "replication_types.h"

#ifdef __cplusplus
extern "C" {
#endif

//replication server side
void replication_processor_bind_task(FSReplication *replication,
        struct fast_task_info *task);

//replication client side
int replication_processor_bind_thread(FSReplication *replication);

//replication server and client
int replication_processor_unbind(FSReplication *replication);

int replication_processor_process(FSServerContext *server_ctx);

void clean_connected_replications(FSServerContext *server_ctx);

int replication_processors_check_response_data_version(
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
