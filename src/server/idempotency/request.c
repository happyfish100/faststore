#include <limits.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fast_mblock.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "request.h"

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
    IdempotencyRequest **bucket;
    IdempotencyRequest *previous;
    IdempotencyRequest *current;

    bucket = htable->buckets + request->req_id % request_ctx.htable_capacity;
    previous = NULL;
    current = *bucket;
    while (current != NULL) {
        if (current->req_id == request->req_id) {
            *request = *current;
            return EEXIST;
        } else if (current->req_id > request->req_id) {
            break;
        }

        previous = current;
        current = current->next;
    }

    if (previous == NULL) {
        request->next = *bucket;
        *bucket = request;
    } else {
        request->next = previous->next;
        previous->next = request;
    }

    return 0;
}

IdempotencyRequest *idempotency_request_htable_remove(
        IdempotencyRequestHTable *htable, const uint64_t req_id)
{
    IdempotencyRequest **bucket;
    IdempotencyRequest *previous;
    IdempotencyRequest *current;

    bucket = htable->buckets + req_id % request_ctx.htable_capacity;
    previous = NULL;
    current = *bucket;
    while (current != NULL) {
        if (current->req_id == req_id) {
            if (previous == NULL) {
                *bucket = current->next;
            } else {
                previous->next = current->next;
            }
            return current;
        } else if (current->req_id > req_id) {
            break;
        }

        previous = current;
        current = current->next;
    }

    return NULL;
}
