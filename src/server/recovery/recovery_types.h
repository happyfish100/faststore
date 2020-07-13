//recovery_types.h

#ifndef _RECOVERY_TYPES_H_
#define _RECOVERY_TYPES_H_

#include <time.h>
#include <limits.h>
#include <pthread.h>
#include "fastcommon/fast_buffer.h"
#include "fastcommon/common_blocked_queue.h"
#include "../server_types.h"

typedef struct data_recovery_context {
    int64_t start_time;   //in ms
    int data_group_id;
} DataRecoveryContext;

#ifdef __cplusplus
extern "C" {
#endif

#ifdef __cplusplus
}
#endif

#endif
