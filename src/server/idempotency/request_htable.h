
#ifndef _IDEMPOTENCY_REQUEST_HTABLE_H
#define _IDEMPOTENCY_REQUEST_HTABLE_H

#include "../../common/fs_types.h"
#include "idempotency_types.h"

#ifdef __cplusplus
extern "C" {
#endif

    void idempotency_request_init(const uint32_t hint_capacity);

    int idempotency_request_htable_add(IdempotencyRequestHTable
            *htable, IdempotencyRequest *request);

    int idempotency_request_htable_remove(IdempotencyRequestHTable *htable,
            const uint64_t req_id);

    void idempotency_request_htable_clear(IdempotencyRequestHTable *htable);

    static inline void idempotency_request_release(IdempotencyRequest *request)
    {
        if (__sync_sub_and_fetch(&request->ref_count, 1) == 0) {
            fast_mblock_free_object(request->allocator, request);
        }
    }

#ifdef __cplusplus
}
#endif

#endif
