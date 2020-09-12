
#ifndef _IDEMPOTENCY_CLIENT_TYPES_H
#define _IDEMPOTENCY_CLIENT_TYPES_H

#include "fastcommon/fast_task_queue.h"
#include "fastcommon/fast_mblock.h"
#include "fastcommon/fc_queue.h"

typedef struct idempotency_client_receipt {
    uint64_t req_id;
    struct idempotency_client_receipt *next;
} IdempotencyClientReceipt;

typedef struct idempotency_client_channel {
    uint32_t id;  //channel id
    int key;      //channel key
    volatile short in_ioevent;
    time_t last_connect_time;
    volatile uint64_t next_req_id;
    struct fast_mblock_man receipt_allocator;
    struct fast_task_info *task;
    struct fc_queue queue;
    struct fc_queue_info waiting_resp_qinfo;
    struct idempotency_client_channel *next;
} IdempotencyClientChannel;

#ifdef __cplusplus
extern "C" {
#endif

#ifdef __cplusplus
}
#endif

#endif