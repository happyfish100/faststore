//binlog_repair.h

#ifndef _BINLOG_REPAIR_H_
#define _BINLOG_REPAIR_H_

#include "binlog_types.h"
#include "binlog_check.h"

#ifdef __cplusplus
extern "C" {
#endif

int binlog_consistency_repair_replica(BinlogConsistencyContext *ctx);
int binlog_consistency_repair_slice(BinlogConsistencyContext *ctx);

#ifdef __cplusplus
}
#endif

#endif
