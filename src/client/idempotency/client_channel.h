//client_channel.h

#ifndef IDEMPOTENCY_CLIENT_CHANNEL_H
#define IDEMPOTENCY_CLIENT_CHANNEL_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fastcommon/fast_task_queue.h"

#ifdef __cplusplus
extern "C" {
#endif

struct idempotency_client_channel;

int client_channel_init();
void client_channel_destroy();

struct idempotency_client_channel *idempotency_client_channel_get(
        const char *server_ip, const short server_port);

int idempotency_client_channel_push(struct idempotency_client_channel *channel,
        const uint64_t req_id);

#ifdef __cplusplus
}
#endif

#endif
