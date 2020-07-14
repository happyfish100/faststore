#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <pthread.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "data_recovery.h"

static int init_recovery_path(DataRecoveryContext *ctx)
{
    char filepath[PATH_MAX];
    int result;
    int path_len;
    bool create;

    path_len = snprintf(filepath, sizeof(filepath), "%s/%s",
            DATA_PATH_STR, FS_RECOVERY_BINLOG_SUBDIR_NAME);
    if ((result=fc_check_mkdir_ex(filepath, 0775, &create)) != 0) {
        return result;
    }
    if (create) {
        SF_CHOWN_RETURN_ON_ERROR(filepath, geteuid(), getegid());
    }

    snprintf(filepath + path_len, sizeof(filepath) - path_len, "/%d",
            ctx->data_group_id);
    if ((result=fc_check_mkdir_ex(filepath, 0775, &create)) != 0) {
        return result;
    }
    if (create) {
        SF_CHOWN_RETURN_ON_ERROR(filepath, geteuid(), getegid());
    }

    return 0;
}

int data_recovery_init(DataRecoveryContext *ctx, const int data_group_id)
{
    ctx->start_time = get_current_time_ms();
    ctx->data_group_id = data_group_id;
    return init_recovery_path(ctx);
}

void data_recovery_destroy(DataRecoveryContext *ctx)
{
}
