//receipt_handler.h

#ifndef IDEMPOTENCY_RECEIPT_HANDLER_H
#define IDEMPOTENCY_RECEIPT_HANDLER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fastcommon/fast_task_queue.h"

#ifdef __cplusplus
extern "C" {
#endif

int receipt_handler_init();
int receipt_handler_destroy();

#ifdef __cplusplus
}
#endif

#endif
