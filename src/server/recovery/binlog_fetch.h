//binlog_fetch.h

#ifndef _BINLOG_FETCH_H_
#define _BINLOG_FETCH_H_

#include "recovery_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int data_recovery_fetch_binlog(DataRecoveryContext *ctx, int64_t *binlog_size);

int data_recovery_unlink_fetched_binlog(DataRecoveryContext *ctx);

#ifdef __cplusplus
}
#endif

#endif
