//replica_handler.h

#ifndef FS_REPLICA_HANDLER_H
#define FS_REPLICA_HANDLER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fastcommon/fast_task_queue.h"
#include "server_types.h"

#ifdef __cplusplus

extern "C" {
#endif

int replica_handler_init();
int replica_handler_destroy();
int replica_deal_task(struct fast_task_info *task);
int replica_recv_timeout_callback(struct fast_task_info *task);
void replica_task_finish_cleanup(struct fast_task_info *task);
void *replica_alloc_thread_extra_data(const int thread_index);
int replica_thread_loop_callback(struct nio_thread_data *thread_data);

#ifdef __cplusplus
}
#endif

#endif
