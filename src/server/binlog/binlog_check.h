//binlog_check.h

#ifndef _BINLOG_CHECK_H_
#define _BINLOG_CHECK_H_

#include "binlog_types.h"

#define BINLOG_CHECK_RESULT_REPLICA_DIRTY  1
#define BINLOG_CHECK_RESULT_SLICE_DIRTY    2

#ifdef __cplusplus
extern "C" {
#endif

int binlog_consistency_check(int *flags);

#ifdef __cplusplus
}
#endif

#endif
