
#ifndef _IDEMPOTENCY_REQUEST_H
#define _IDEMPOTENCY_REQUEST_H

#include "../../common/fs_types.h"

typedef struct idempotency_request {
    uint64_t req_id;
    struct idempotency_request *next;
} IdempotencyRequest;

typedef struct idempotency_request_htable {
    IdempotencyRequest **buckets;
    int count;
} IdempotencyRequestHTable;

#ifdef __cplusplus
extern "C" {
#endif

    void idempotency_request_init(const uint32_t hint_capacity);

    int idempotency_request_htable_add(IdempotencyRequestHTable *htable,
            IdempotencyRequest *request);

    IdempotencyRequest *idempotency_request_htable_remove(
            IdempotencyRequestHTable *htable, const uint64_t req_id);

    IdempotencyRequest *idempotency_request_htable_clear(
            IdempotencyRequestHTable *htable);

#ifdef __cplusplus
}
#endif

#endif
