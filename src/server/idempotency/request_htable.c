#include <limits.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fast_mblock.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "request_htable.h"

typedef struct idempotency_request_context {
    uint32_t htable_capacity;
} IdempotencyRequestContext;

static IdempotencyRequestContext request_ctx;

void idempotency_request_init(const uint32_t htable_capacity)
{
    request_ctx.htable_capacity = htable_capacity;
}

int idempotency_request_htable_add(IdempotencyRequestHTable *htable,
        IdempotencyRequest *request)
{
    int result;
    IdempotencyRequest **bucket;
    IdempotencyRequest *previous;
    IdempotencyRequest *current;

    bucket = htable->buckets + request->req_id % request_ctx.htable_capacity;
    previous = NULL;
    result = 0;

    PTHREAD_MUTEX_LOCK(&htable->lock);
    current = *bucket;
    while (current != NULL) {
        if (current->req_id == request->req_id) {
            *request = *current;
            result = EEXIST;
            break;
        } else if (current->req_id > request->req_id) {
            break;
        }

        previous = current;
        current = current->next;
    }

    if (result == 0) {
        if (previous == NULL) {
            request->next = *bucket;
            *bucket = request;
        } else {
            request->next = previous->next;
            previous->next = request;
        }
        htable->count++;
    }
    PTHREAD_MUTEX_UNLOCK(&htable->lock);

    return result;
}

IdempotencyRequest *idempotency_request_htable_remove(
        IdempotencyRequestHTable *htable, const uint64_t req_id)
{
    IdempotencyRequest **bucket;
    IdempotencyRequest *previous;
    IdempotencyRequest *current;

    bucket = htable->buckets + req_id % request_ctx.htable_capacity;
    previous = NULL;

    PTHREAD_MUTEX_LOCK(&htable->lock);
    current = *bucket;
    while (current != NULL) {
        if (current->req_id == req_id) {
            if (previous == NULL) {
                *bucket = current->next;
            } else {
                previous->next = current->next;
            }
            htable->count--;
            break;
        } else if (current->req_id > req_id) {
            current = NULL;
            break;
        }

        previous = current;
        current = current->next;
    }
    PTHREAD_MUTEX_UNLOCK(&htable->lock);

    return current;
}

IdempotencyRequest *idempotency_request_htable_clear(
        IdempotencyRequestHTable *htable)
{
    IdempotencyRequest **bucket;
    IdempotencyRequest **end;
    IdempotencyRequest *head;
    IdempotencyRequest *previous;
    IdempotencyRequest *current;

    head = NULL;
    PTHREAD_MUTEX_LOCK(&htable->lock);
    do {
        if (htable->count == 0) {
            break;
        }

        previous = NULL;
        end = htable->buckets + request_ctx.htable_capacity;
        for (bucket=htable->buckets; bucket<end; bucket++) {
            if (*bucket == NULL) {
                continue;
            }

            current = *bucket;
            do {
                if (previous == NULL) {
                    head = current;
                } else {
                    previous->next = current;
                }
                previous = current;
                current = current->next;
            } while (current != NULL);

            *bucket = NULL;
        }

        if (previous != NULL) {
            previous->next = NULL;
        }

        htable->count = 0;
    } while (0);

    PTHREAD_MUTEX_UNLOCK(&htable->lock);
    return head;
}
