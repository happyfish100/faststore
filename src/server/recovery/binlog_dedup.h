//binlog_dedup.h

#ifndef _BINLOG_DEDUP_H_
#define _BINLOG_DEDUP_H_

#include "recovery_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int data_recovery_dedup_binlog(DataRecoveryContext *ctx, int64_t *binlog_count);

#ifdef __cplusplus
}
#endif

#endif
