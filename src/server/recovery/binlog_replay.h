//binlog_replay.h

#ifndef _BINLOG_REPLAY_H_
#define _BINLOG_REPLAY_H_

#include "recovery_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int binlog_replay_init();
void binlog_replay_destroy();

int data_recovery_replay_binlog(DataRecoveryContext *ctx);

int data_recovery_unlink_replay_binlog(DataRecoveryContext *ctx);

#ifdef __cplusplus
}
#endif

#endif
