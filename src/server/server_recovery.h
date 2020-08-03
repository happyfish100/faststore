//server_recovery.h

#ifndef _SERVER_RECOVERY_H_
#define _SERVER_RECOVERY_H_

#include "recovery/recovery_thread.h"
#include "recovery/data_recovery.h"

#ifdef __cplusplus
extern "C" {
#endif

int server_recovery_init();
void server_recovery_destroy();
void server_recovery_terminate();

#ifdef __cplusplus
}
#endif

#endif
