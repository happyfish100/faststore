//client_channel.h

#ifndef IDEMPOTENCY_CLIENT_CHANNEL_H
#define IDEMPOTENCY_CLIENT_CHANNEL_H

#include "fastcommon/pthread_func.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/fc_atomic.h"
#include "client_types.h"

#ifdef __cplusplus
extern "C" {
#endif

#define client_channel_init() client_channel_init_ex(0)

int client_channel_init_ex(const int hint_capacity);
void client_channel_destroy();

struct idempotency_client_channel *idempotency_client_channel_get(
        const char *server_ip, const short server_port,
        const int timeout, int *err_no);

static inline uint64_t idempotency_client_channel_next_seq_id(
        struct idempotency_client_channel *channel)
{
    return __sync_add_and_fetch(&channel->next_req_id, 1);
}

int idempotency_client_channel_push(struct idempotency_client_channel *channel,
        const uint64_t req_id);

int idempotency_client_channel_check_reconnect(
        IdempotencyClientChannel *channel);

static inline void idempotency_client_channel_set_id_key(
        IdempotencyClientChannel *channel, const uint32_t new_id,
        const uint32_t new_key)
{
    uint32_t old_id;
    uint32_t old_key;

    old_id = __sync_add_and_fetch(&channel->id, 0);
    old_key = __sync_add_and_fetch(&channel->key, 0);
    FC_ATOMIC_CAS(channel->id, old_id, new_id);
    FC_ATOMIC_CAS(channel->key, old_key, new_key);
}

#define idempotency_client_channel_check_wait(channel)  \
    idempotency_client_channel_check_wait_ex(channel, 1)

static inline int idempotency_client_channel_check_wait_ex(
        struct idempotency_client_channel *channel, const int timeout)
{
    struct timespec ts;

    if (__sync_add_and_fetch(&channel->established, 0)) {
        return 0;
    }

    PTHREAD_MUTEX_LOCK(&channel->lc_pair.lock);
    idempotency_client_channel_check_reconnect(channel);

    ts.tv_sec = get_current_time() + timeout;
    ts.tv_nsec = 0;
    pthread_cond_timedwait(&channel->lc_pair.cond,
            &channel->lc_pair.lock, &ts);
    PTHREAD_MUTEX_UNLOCK(&channel->lc_pair.lock);

    return __sync_add_and_fetch(&channel->established, 0) ? 0 : EAGAIN;
}

#ifdef __cplusplus
}
#endif

#endif
