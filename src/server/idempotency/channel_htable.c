#include <limits.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fast_mblock.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "channel_htable.h"

int idempotency_channel_htable_init(ChannelHTableContext *ctx,
        const uint32_t shared_lock_count, const uint32_t hint_capacity)
{
    int result;
    int64_t bytes;
    pthread_mutex_t *lock;
    pthread_mutex_t *end;

    ctx->shared_locks.count = fc_ceil_prime(shared_lock_count);
    ctx->htable.capacity = fc_ceil_prime(hint_capacity);

    bytes = sizeof(pthread_mutex_t) * ctx->shared_locks.count;
    ctx->shared_locks.locks = (pthread_mutex_t *)fc_malloc(bytes);
    if (ctx->shared_locks.locks == NULL) {
        return ENOMEM;
    }
    end = ctx->shared_locks.locks + ctx->shared_locks.count;
    for (lock=ctx->shared_locks.locks; lock<end; lock++) {
        if ((result=init_pthread_lock(lock)) != 0) {
            return result;
        }
    }

    bytes = sizeof(IdempotencyChannel *) * ctx->htable.capacity;
    ctx->htable.buckets = (IdempotencyChannel **)fc_malloc(bytes);
    if (ctx->htable.buckets == NULL) {
        return ENOMEM;
    }
    memset(ctx->htable.buckets, 0, bytes);
    ctx->htable.count = 0;

    return 0;
}

int idempotency_channel_htable_add(ChannelHTableContext *ctx,
        IdempotencyChannel *channel)
{
    int result;
    pthread_mutex_t *lock;
    IdempotencyChannel **bucket;
    IdempotencyChannel *previous;
    IdempotencyChannel *current;

    lock = ctx->shared_locks.locks + channel->id % ctx->shared_locks.count;
    bucket = ctx->htable.buckets + channel->id % ctx->htable.capacity;
    previous = NULL;
    result = 0;

    PTHREAD_MUTEX_LOCK(lock);
    current = *bucket;
    while (current != NULL) {
        if (current->id == channel->id) {
            result = EEXIST;
            break;
        } else if (current->id > channel->id) {
            break;
        }

        previous = current;
        current = current->next;
    }

    if (result == 0) {
        if (previous == NULL) {
            channel->next = *bucket;
            *bucket = channel;
        } else {
            channel->next = previous->next;
            previous->next = channel;
        }
        ctx->htable.count++;
    }
    PTHREAD_MUTEX_UNLOCK(lock);

    return result;
}

IdempotencyChannel *idempotency_channel_htable_remove(
        ChannelHTableContext *ctx, const uint32_t channel_id)
{
    pthread_mutex_t *lock;
    IdempotencyChannel **bucket;
    IdempotencyChannel *previous;
    IdempotencyChannel *current;

    lock = ctx->shared_locks.locks + channel_id % ctx->shared_locks.count;
    bucket = ctx->htable.buckets + channel_id % ctx->htable.capacity;
    previous = NULL;

    PTHREAD_MUTEX_LOCK(lock);
    current = *bucket;
    while (current != NULL) {
        if (current->id == channel_id) {
            if (previous == NULL) {
                *bucket = current->next;
            } else {
                previous->next = current->next;
            }
            ctx->htable.count--;
            break;
        } else if (current->id > channel_id) {
            current = NULL;
            break;
        }

        previous = current;
        current = current->next;
    }
    PTHREAD_MUTEX_UNLOCK(lock);

    return current;
}

IdempotencyChannel *idempotency_channel_htable_find(
        ChannelHTableContext *ctx, const uint32_t channel_id)
{
    pthread_mutex_t *lock;
    IdempotencyChannel **bucket;
    IdempotencyChannel *current;

    lock = ctx->shared_locks.locks + channel_id % ctx->shared_locks.count;
    bucket = ctx->htable.buckets + channel_id % ctx->htable.capacity;

    PTHREAD_MUTEX_LOCK(lock);
    current = *bucket;
    while (current != NULL) {
        if (current->id == channel_id) {
            break;
        } else if (current->id > channel_id) {
            current = NULL;
            break;
        }

        current = current->next;
    }
    PTHREAD_MUTEX_UNLOCK(lock);

    return current;
}
