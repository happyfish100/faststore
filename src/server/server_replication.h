//server_replication.h

#ifndef _SERVER_REPLICATION_H_
#define _SERVER_REPLICATION_H_

#include "replication/replication_processor.h"
#include "replication/replication_common.h"
#include "replication/replication_caller.h"
#include "replication/replication_callee.h"

#ifdef __cplusplus
extern "C" {
#endif

int server_replication_init();
void server_replication_destroy();
void server_replication_terminate();

#ifdef __cplusplus
}
#endif

#endif
