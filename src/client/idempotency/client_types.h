
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
    volatile uint32_t id;  //channel id, 0 for invalid
    volatile int key;      //channel key
    volatile char in_ioevent;
    volatile char in_heartbeat;
    volatile char established;
    time_t last_connect_time;
    time_t last_pkg_time;  //last communication time
    pthread_lock_cond_pair_t lc_pair;  //for channel valid check and notify
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
