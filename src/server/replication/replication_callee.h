//replication_callee.h

#ifndef _REPLICATION_CALLEE_H_
#define _REPLICATION_CALLEE_H_

#include "replication_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int replication_callee_init();
void replication_callee_destroy();
void replication_callee_terminate();

int replication_callee_push_to_rpc_result_queue(FSReplication *replication,
        const uint64_t data_version, const int err_no);

#ifdef __cplusplus
}
#endif

#endif
