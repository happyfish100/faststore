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
#include "../server_group_info.h"
#include "data_recovery.h"

static int init_recovery_sub_path(DataRecoveryContext *ctx, const char *subdir)
{
    char filepath[PATH_MAX];
    const char *subdir_names[3];
    char data_group_id[16];
    int result;
    int gid_len;
    int path_len;
    int i;
    bool create;

    gid_len = sprintf(data_group_id, "%d", ctx->data_group_id);
    subdir_names[0] = FS_RECOVERY_BINLOG_SUBDIR_NAME;
    subdir_names[1] = data_group_id;
    subdir_names[2] = subdir;

    path_len = snprintf(filepath, sizeof(filepath), "%s", DATA_PATH_STR);
    if (PATH_MAX - path_len < gid_len + strlen(FS_RECOVERY_BINLOG_SUBDIR_NAME)
            + strlen(subdir) + 3)
    {
        logError("file: "__FILE__", line: %d, "
                "the length of data path is too long, exceeds %d",
                __LINE__, PATH_MAX);
        return EOVERFLOW;
    }

    for (i=0; i<3; i++) {
        path_len += sprintf(filepath + path_len, "/%s", subdir_names[i]);

        logInfo("%d. filepath: %s", i + 1, filepath);
        if ((result=fc_check_mkdir_ex(filepath, 0775, &create)) != 0) {
            return result;
        }
        if (create) {
            SF_CHOWN_RETURN_ON_ERROR(filepath, geteuid(), getegid());
        }
    }

    return 0;
}

FSClusterDataServerInfo *data_recovery_get_master(
        DataRecoveryContext *ctx, int *err_no)
{
    FSClusterDataGroupInfo *group;
    FSClusterDataServerInfo *master;

    if ((group=fs_get_data_group(ctx->data_group_id)) == NULL) {
        *err_no = ENOENT;
        return NULL;
    }
    master = (FSClusterDataServerInfo *)
        __sync_fetch_and_add(&group->master, 0);
    if (master == NULL) {
        logError("file: "__FILE__", line: %d, "
                "data group id: %d, no master",
                __LINE__, ctx->data_group_id);
        *err_no = ENOENT;
        return NULL;
    }

    if (group->myself == NULL) {
        logError("file: "__FILE__", line: %d, "
                "data group id: %d NOT belongs to me",
                __LINE__, ctx->data_group_id);
        *err_no = ENOENT;
        return NULL;
    }

    if (group->myself == master) {
        logError("file: "__FILE__", line: %d, "
                "data group id: %d, i am already master, "
                "do NOT need recovery!", __LINE__, ctx->data_group_id);
        *err_no = EBUSY;
        return NULL;
    }

    *err_no = 0;
    return master;
}

int data_recovery_init(DataRecoveryContext *ctx, const int data_group_id)
{
    int result;
    FSClusterDataServerInfo *master;

    ctx->start_time = get_current_time_ms();
    ctx->data_group_id = data_group_id;

    if ((master=data_recovery_get_master(ctx, &result)) == NULL) {
        return result;
    }

    ctx->last_data_version = master->dg->myself->data_version;
    if ((result=init_recovery_sub_path(ctx,
                    RECOVERY_BINLOG_SUBDIR_NAME_FETCH)) != 0)
    {
        return result;
    }
    if ((result=init_recovery_sub_path(ctx,
                    RECOVERY_BINLOG_SUBDIR_NAME_REPLAY)) != 0)
    {
        return result;
    }

    return 0;
}

void data_recovery_destroy(DataRecoveryContext *ctx)
{
}
