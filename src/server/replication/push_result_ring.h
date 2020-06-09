//push_result_ring.h

#ifndef _PUSH_RESULT_RING_H_
#define _PUSH_RESULT_RING_H_

#include "../server_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int push_result_ring_check_init(FSBinlogPushResultContext *ctx,
        const int alloc_size);

void push_result_ring_destroy(FSBinlogPushResultContext *ctx);

int push_result_ring_add(FSBinlogPushResultContext *ctx,
        const uint64_t data_version, struct fast_task_info *waiting_task,
        const int64_t task_version);

int push_result_ring_remove(FSBinlogPushResultContext *ctx,
        const uint64_t data_version);

void push_result_ring_clear_all(FSBinlogPushResultContext *ctx);

void push_result_ring_clear_timeouts(FSBinlogPushResultContext *ctx);

#ifdef __cplusplus
}
#endif

#endif
