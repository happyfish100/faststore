//replication_common.h

#ifndef _REPLICATION_COMMON_H_
#define _REPLICATION_COMMON_H_

#include "replication_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int replication_common_init();
void replication_common_destroy();
void replication_common_terminate();

int replication_common_start();

int fs_get_replication_count();
FSReplication *fs_get_idle_replication_by_peer(const int peer_id);

#ifdef __cplusplus
}
#endif

#endif
