
#ifndef _IDEMPOTENCY_CHANNEL_H
#define _IDEMPOTENCY_CHANNEL_H

#include "fastcommon/fast_timer.h"
#include "../../common/fs_types.h"
#include "request.h"

typedef void (*free_idempotency_requests_func)(IdempotencyRequest *head);

typedef struct idempotency_channel {
    FastTimerEntry timer;  //must be the first
    uint32_t id;
    IdempotencyRequestHTable request_htable;
    struct idempotency_channel *next;
} IdempotencyChannel;

#ifdef __cplusplus
extern "C" {
#endif

    int idempotency_channel_init(const uint32_t max_channel_id,
            const int request_hint_capacity,
            const uint32_t reserve_interval,
            free_idempotency_requests_func free_func);

    IdempotencyChannel *idempotency_channel_alloc(const uint32_t channel_id);

    void idempotency_channel_release(IdempotencyChannel *channel);

    void idempotency_channel_free(IdempotencyChannel *channel);


    static inline int idempotency_channel_add_request(IdempotencyChannel *
            channel, IdempotencyRequest *request)
    {
        return idempotency_request_htable_add(
                &channel->request_htable, request);
    }

    static inline IdempotencyRequest *idempotency_channel_remove_request(
            IdempotencyChannel *channel, const uint64_t req_id)
    {
        return idempotency_request_htable_remove(
                &channel->request_htable, req_id);
    }

#ifdef __cplusplus
}
#endif

#endif
