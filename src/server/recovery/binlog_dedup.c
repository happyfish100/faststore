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
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "../../common/fs_proto.h"
#include "../server_global.h"
#include "../binlog/replica_binlog.h"
#include "data_recovery.h"
#include "binlog_dedup.h"

static int check_and_open_binlog_file(DataRecoveryContext *ctx)
{
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];
    char full_filename[PATH_MAX];
    struct stat stbuf;
    int result;
    int distance;
    uint64_t last_data_version;
    bool unlink_flag;

    data_recovery_get_subdir_name(ctx, RECOVERY_BINLOG_SUBDIR_NAME_FETCH,
            subdir_name);

    binlog_reader_get_filename(subdir_name, 0,
            full_filename, sizeof(full_filename));

    unlink_flag = false;
    do {
        if (stat(full_filename, &stbuf) != 0) {
            if (errno == ENOENT) {
                break;
            } else {
                logError("file: "__FILE__", line: %d, "
                        "stat file %s fail, errno: %d, error info: %s",
                        __LINE__, full_filename, errno, STRERROR(errno));
                return errno != 0 ? errno : EPERM;
            }
        }

        if (stbuf.st_size == 0) {
            break;
        }

        distance = g_current_time - stbuf.st_mtime;
        if (!(distance >= 0 && distance <= 3600)) {
            logWarning("file: "__FILE__", line: %d, "
                    "data_group_id: %d, binlog file: %s is too old, "
                    "should dedup the data binlog again", __LINE__,
                    ctx->data_group_id, full_filename);
            unlink_flag = true;
            break;
        }

        if ((result=replica_binlog_get_last_data_version(
                        full_filename, &last_data_version)) != 0)
        {
            logWarning("file: "__FILE__", line: %d, "
                    "data_group_id: %d, binlog file: %s, get the last "
                    "data version fail, should dedup the data binlog again",
                    __LINE__, ctx->data_group_id, full_filename);
            unlink_flag = true;
            break;
        }

        if (last_data_version <= ctx->last_data_version) {
            logWarning("file: "__FILE__", line: %d, "
                    "data_group_id: %d, binlog file: %s, the last data "
                    "version: %"PRId64" <= my current data version: %"PRId64
                    ", should dedup the data binlog again", __LINE__,
                    ctx->data_group_id, full_filename, last_data_version,
                    ctx->last_data_version);
            unlink_flag = true;
            break;
        }

        ctx->last_data_version = last_data_version;
    } while (0);

    if (unlink_flag) {
        if (unlink(full_filename) != 0) {
            logError("file: "__FILE__", line: %d, "
                    "unlink file %s fail, errno: %d, error info: %s",
                    __LINE__, full_filename, errno, STRERROR(errno));
            return errno != 0 ? errno : EPERM;
        }
    }

    if ((ctx->fd=open(full_filename, O_WRONLY | O_CREAT | O_APPEND,
                    0644)) < 0)
    {
        logError("file: "__FILE__", line: %d, "
                "open binlog file %s fail, errno: %d, error info: %s",
                __LINE__, full_filename, errno, STRERROR(errno));
        return errno != 0 ? errno : EACCES;
    }

    return 0;
}

static int do_dedup_binlog(DataRecoveryContext *ctx)
{
    return 0;
}

int data_recovery_dedup_binlog(DataRecoveryContext *ctx)
{
    int result;

    if ((result=check_and_open_binlog_file(ctx)) != 0) {
        return result;
    }

    return do_dedup_binlog(ctx);
}
