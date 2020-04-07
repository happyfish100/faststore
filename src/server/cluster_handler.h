//cluster_handler.h

#ifndef FS_CLUSTER_HANDLER_H
#define FS_CLUSTER_HANDLER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fastcommon/fast_task_queue.h"
#include "server_types.h"

#ifdef __cplusplus

extern "C" {
#endif

int cluster_handler_init();
int cluster_handler_destroy();
int cluster_deal_task(struct fast_task_info *task);
void cluster_task_finish_cleanup(struct fast_task_info *task);
void *cluster_alloc_thread_extra_data(const int thread_index);
int cluster_thread_loop_callback(struct nio_thread_data *thread_data);

#ifdef __cplusplus
}
#endif

#endif
