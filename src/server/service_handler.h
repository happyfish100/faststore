//service_handler.h

#ifndef FS_SERVER_HANDLER_H
#define FS_SERVER_HANDLER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fastcommon/fast_task_queue.h"
#include "server_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int service_handler_init();
int service_handler_destroy();
int service_deal_task(struct fast_task_info *task);
void service_task_finish_cleanup(struct fast_task_info *task);
void *service_alloc_thread_extra_data(const int thread_index);
//int service_thread_loop(struct nio_thread_data *thread_data);

#ifdef __cplusplus
}
#endif

#endif
