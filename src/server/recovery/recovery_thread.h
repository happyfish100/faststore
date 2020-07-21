//recovery_thread.h

#ifndef _RECOVERY_THREAD_H_
#define _RECOVERY_THREAD_H_

#include "recovery_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int recovery_thread_init();

void recovery_thread_destroy();

int recovery_thread_push_to_queue(FSClusterDataServerInfo *ds);

#ifdef __cplusplus
}
#endif

#endif
