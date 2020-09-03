
#ifndef _IDEMPOTENCY_CHANNEL_HTABLE_H
#define _IDEMPOTENCY_CHANNEL_HTABLE_H

#include "../../common/fs_types.h"
#include "idempotency_types.h"

typedef struct channel_shared_locks {
    pthread_mutex_t *locks;
    uint32_t count;
} ChannelSharedLocks;

typedef struct idempotency_channel_htable {
    IdempotencyChannel **buckets;
    uint32_t capacity;
    uint32_t count;
} IdempotencyChannelHTable;

typedef struct channel_htable_context {
    ChannelSharedLocks shared_locks;
    IdempotencyChannelHTable htable;
} ChannelHTableContext;

#ifdef __cplusplus
extern "C" {
#endif

    int idempotency_channel_htable_init(ChannelHTableContext *ctx,
            const uint32_t shared_lock_count, const uint32_t hint_capacity);

    int idempotency_channel_htable_add(ChannelHTableContext *ctx,
            IdempotencyChannel *channel);

    IdempotencyChannel *idempotency_channel_htable_remove(
            ChannelHTableContext *ctx, const uint32_t channel_id);

    IdempotencyChannel *idempotency_channel_htable_find(
            ChannelHTableContext *ctx, const uint32_t channel_id);

#ifdef __cplusplus
}
#endif

#endif
