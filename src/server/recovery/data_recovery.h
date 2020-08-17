//data_recovery.h

#ifndef _DATA_RECOVERY_H_
#define _DATA_RECOVERY_H_

#include "recovery_types.h"
#include "../binlog/binlog_reader.h"

#define DATA_RECOVERY_THREADS_LIMIT  2

#define DATA_RECOVERY_CATCH_UP_DOING       0
#define DATA_RECOVERY_CATCH_UP_LAST_BATCH  1

#ifdef __cplusplus
extern "C" {
#endif

int data_recovery_init();
void data_recovery_destroy();

int data_recovery_start(FSClusterDataServerInfo *ds);

static inline void data_recovery_get_subdir_name(DataRecoveryContext *ctx,
        const char *subdir, char *subdir_name)
{
    sprintf(subdir_name, "%s/%d/%s", FS_RECOVERY_BINLOG_SUBDIR_NAME,
            ctx->ds->dg->id, subdir);
}

FSClusterDataServerInfo *data_recovery_get_master(
        DataRecoveryContext *ctx, int *err_no);

#ifdef __cplusplus
}
#endif

#endif
