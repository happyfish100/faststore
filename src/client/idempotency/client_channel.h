//client_channel.h

#ifndef IDEMPOTENCY_CLIENT_CHANNEL_H
#define IDEMPOTENCY_CLIENT_CHANNEL_H

#include "client_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int client_channel_init();
void client_channel_destroy();

struct idempotency_client_channel *idempotency_client_channel_get(
        const char *server_ip, const short server_port);

static inline uint64_t idempotency_client_channel_next_seq_id(
        struct idempotency_client_channel *channel)
{
    return __sync_add_and_fetch(&channel->next_req_id, 1);
}

int idempotency_client_channel_push(struct idempotency_client_channel *channel,
        const uint64_t req_id);

#ifdef __cplusplus
}
#endif

#endif
