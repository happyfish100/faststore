//client_channel.c

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include "fastcommon/logger.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/fc_queue.h"
#include "sf/sf_util.h"
#include "sf/sf_func.h"
#include "sf/sf_nio.h"
#include "sf/sf_global.h"
#include "sf/sf_service.h"
#include "client_channel.h"

typedef struct idempotency_client_channel {
    uint32_t channel_id;
    volatile uint64_t next_req_id;
    struct fast_task_info *task;
    struct fc_queue queue;
} IdempotencyClientChannel;

int client_channel_init()
{
    return 0;
}

void client_channel_destroy()
{
}

struct idempotency_client_channel *idempotency_client_channel_get(
        const char *server_ip, const short server_port)
{
    return NULL;
}

int idempotency_client_channel_push(struct idempotency_client_channel *channel,
        const uint64_t req_id)
{
    return 0;
}
