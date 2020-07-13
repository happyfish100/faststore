//data_recovery.h

#ifndef _DATA_RECOVERY_H_
#define _DATA_RECOVERY_H_

#include "recovery_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int data_recovery_init(DataRecoveryContext *ctx, const int data_group_id);

void data_recovery_destroy(DataRecoveryContext *ctx);

#ifdef __cplusplus
}
#endif

#endif
