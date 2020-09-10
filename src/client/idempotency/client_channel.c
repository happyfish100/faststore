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

typedef struct {
    IdempotencyClientChannel **buckets;
    uint32_t capacity;
    uint32_t count;
    pthread_mutex_t lock;
} ClientChannelHashtable;

typedef struct {
    struct fast_mblock_man channel_allocator;
    ClientChannelHashtable htable;
} ClientChannelContext;

static ClientChannelContext channel_context;

static int init_htable(ClientChannelHashtable *htable, const int hint_capacity)
{
    int result;
    int bytes;

    if ((result=init_pthread_lock(&htable->lock)) != 0) {
        return result;
    }

    if (hint_capacity <= 1024) {
        htable->capacity = 1361;
    } else {
        htable->capacity = fc_ceil_prime(hint_capacity);
    }
    bytes = sizeof(IdempotencyClientChannel *) * htable->capacity;
    htable->buckets = (IdempotencyClientChannel **)fc_malloc(bytes);
    if (htable->buckets == NULL) {
        return ENOMEM;
    }
    memset(htable->buckets, 0, bytes);
    htable->count = 0;

    return 0;
}

static int idempotency_channel_alloc_init(void *element, void *args)
{
    int result;
    IdempotencyClientChannel *channel;

    channel = (IdempotencyClientChannel *)element;
    if ((result=fast_mblock_init_ex1(&channel->receipt_allocator,
                    "idempotency_receipt", sizeof(IdempotencyClientReceipt),
                    1024, 0, NULL, NULL, true)) != 0)
    {
        return result;
    }

    return fc_queue_init(&channel->queue, (long)
            (&((IdempotencyClientReceipt *)NULL)->next));
}

int client_channel_init(const int hint_capacity)
{
    int result;
    if ((result=fast_mblock_init_ex1(&channel_context.channel_allocator,
                    "channel_info", sizeof(IdempotencyClientChannel),
                    64, 0, idempotency_channel_alloc_init, NULL, true)) != 0)
    {
        return result;
    }

    if ((result=init_htable(&channel_context.htable, hint_capacity)) != 0) {
        return result;
    }

    return 0;
}

void client_channel_destroy()
{
}

struct fast_task_info *alloc_channel_task(IdempotencyClientChannel *channel,
        const uint32_t hash_code, const char *server_ip, const short port)
{
    struct fast_task_info *task;

    task = free_queue_pop();
    if (task == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc task buff failed, you should "
                "increase the parameter: max_connections",
                __LINE__);
        return NULL;
    }

    snprintf(task->server_ip, sizeof(task->server_ip), "%s", server_ip);
    task->port = port;
    task->canceled = false;
    task->ctx = &g_sf_context;
    task->event.fd = -1;
    task->arg = channel;
    task->thread_data = g_sf_context.thread_data +
        hash_code % g_sf_context.work_threads;
    channel->in_ioevent = 1;
    if (sf_nio_notify(task, SF_NIO_STAGE_CONNECT) != 0) {
        channel->in_ioevent = 0;
        free_queue_push(task);
        return NULL;
    }
    channel->last_connect_time = get_current_time();

    return task;
}

int idempotency_client_channel_check_reconnect(
        IdempotencyClientChannel *channel)
{
    int result;
    time_t current_time;

    if (!__sync_bool_compare_and_swap(&channel->in_ioevent, 0, 1)) {
        return 0;
    }

    current_time = get_current_time();
    if (channel->last_connect_time >= current_time) {
        sleep(1);
        channel->last_connect_time = ++current_time;
    }

    if ((result=sf_nio_notify(channel->task, SF_NIO_STAGE_CONNECT)) == 0) {
        channel->last_connect_time = current_time;
    } else {
        __sync_bool_compare_and_swap(&channel->in_ioevent, 1, 0); //rollback
    }
    return result;
}

struct idempotency_client_channel *idempotency_client_channel_get(
        const char *server_ip, const short server_port)
{
    int r;
    int key_len;
    bool found;
    char key[64];
    uint32_t hash_code;
    IdempotencyClientChannel **bucket;
    IdempotencyClientChannel *previous;
    IdempotencyClientChannel *current;
    IdempotencyClientChannel *channel;

    key_len = snprintf(key, sizeof(key), "%s_%d", server_ip, server_port);
    hash_code = simple_hash(key, key_len);
    bucket = channel_context.htable.buckets +
        hash_code % channel_context.htable.capacity;
    previous = NULL;
    channel = NULL;
    found = false;

    PTHREAD_MUTEX_LOCK(&channel_context.htable.lock);
    do {
        current = *bucket;
        while (current != NULL) {
            r = conn_pool_compare_ip_and_port(current->task->server_ip,
                    current->task->port, server_ip, server_port);
            if (r == 0) {
                channel = current;
                break;
            } else if (r > 0) {
                break;
            }

            previous = current;
            current = current->next;
        }

        if (channel != NULL) {
            found = true;
            break;
        }

        channel = (IdempotencyClientChannel *)fast_mblock_alloc_object(
                &channel_context.channel_allocator);
        if (channel == NULL) {
            break;
        }

        channel->task = alloc_channel_task(channel,
                hash_code, server_ip, server_port);
        if (channel->task == NULL) {
            fast_mblock_free_object(&channel_context.
                    channel_allocator, channel);
            channel = NULL;
            break;
        }

        if (previous == NULL) {
            channel->next = *bucket;
            *bucket = channel;
        } else {
            channel->next = previous->next;
            previous->next = channel;
        }
        channel_context.htable.count++;
    } while (0);
    PTHREAD_MUTEX_UNLOCK(&channel_context.htable.lock);

    if (found) {
        idempotency_client_channel_check_reconnect(channel);
    }
    return channel;
}

int idempotency_client_channel_push(struct idempotency_client_channel *channel,
        const uint64_t req_id)
{
    IdempotencyClientReceipt *receipt;
    bool notify;

    receipt = (IdempotencyClientReceipt *)fast_mblock_alloc_object(
            &channel->receipt_allocator);
    if (receipt == NULL) {
        return ENOMEM;
    }

    receipt->req_id = req_id;
    fc_queue_push_ex(&channel->queue, receipt, &notify);
    if (notify) {
        if (__sync_add_and_fetch(&channel->in_ioevent, 0)) {
            sf_nio_notify(channel->task, SF_NIO_STAGE_CONTINUE);
        } else {
            return idempotency_client_channel_check_reconnect(channel);
        }
    }

    return 0;
}
