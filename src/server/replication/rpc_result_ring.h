//rpc_result_ring.h

#ifndef _RPC_RESULT_RING_H_
#define _RPC_RESULT_RING_H_

#include "../server_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int rpc_result_ring_check_init(FSReplicaRPCResultContext *ctx,
        const int alloc_size);

void rpc_result_ring_destroy(FSReplicaRPCResultContext *ctx);

int rpc_result_ring_add(FSReplicaRPCResultContext *ctx,
        const uint64_t data_version, struct fast_task_info *waiting_task,
        const int64_t task_version);

int rpc_result_ring_remove(FSReplicaRPCResultContext *ctx,
        const uint64_t data_version);

void rpc_result_ring_clear_all(FSReplicaRPCResultContext *ctx);

void rpc_result_ring_clear_timeouts(FSReplicaRPCResultContext *ctx);

#ifdef __cplusplus
}
#endif

#endif
