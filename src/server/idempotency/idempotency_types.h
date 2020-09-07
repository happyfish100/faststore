
#ifndef _IDEMPOTENCY_SERVER_TYPES_H
#define _IDEMPOTENCY_SERVER_TYPES_H

#include "fastcommon/fast_mblock.h"
#include "fastcommon/fast_timer.h"

typedef struct idempotency_request {
    uint64_t req_id;
    volatile int ref_count;
    bool finished;
    struct {
        int result;
        int inc_alloc;
    } output;
    struct fast_mblock_man *allocator;  //for free
    struct idempotency_request *next;
} IdempotencyRequest;

typedef struct idempotency_request_htable {
    IdempotencyRequest **buckets;
    int count;
    pthread_mutex_t lock;
} IdempotencyRequestHTable;

typedef struct idempotency_channel {
    FastTimerEntry timer;  //must be the first
    uint32_t id;
    int key;      //for retrieve validation
    volatile int ref_count;
    volatile char is_valid;
    IdempotencyRequestHTable request_htable;
    struct idempotency_channel *next;
} IdempotencyChannel;

#ifdef __cplusplus
extern "C" {
#endif

#ifdef __cplusplus
}
#endif

#endif
