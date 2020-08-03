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
#include "../../common/fs_func.h"
#include "../server_global.h"
#include "../cluster_relationship.h"
#include "../server_binlog.h"
#include "../server_replication.h"
#include "data_recovery.h"
#include "binlog_replay.h"

typedef struct {
    BinlogReadThreadContext rdthread_ctx;
    BinlogReadThreadResult *r;
    ReplicaBinlogRecord record;  //TODO
} BinlogReplayContext;

static int deal_binlog_buffer(DataRecoveryContext *ctx)
{
    BinlogReplayContext *replay_ctx;
    char *p;
    char *line_end;
    char *end;
    BufferInfo *buffer;
    string_t line;
    char error_info[256];
    int result;
    int op_type;

    replay_ctx = (BinlogReplayContext *)ctx->arg;
    result = 0;
    *error_info = '\0';
    buffer = &replay_ctx->r->buffer;
    end = buffer->buff + buffer->length;
    p = buffer->buff;
    while (p < end) {
        line_end = (char *)memchr(p, '\n', end - p);
        if (line_end == NULL) {
            strcpy(error_info, "expect end line (\\n)");
            result = EINVAL;
            break;
        }

        line_end++;
        line.str = p;
        line.len = line_end - p;
        if ((result=replica_binlog_record_unpack(&line,
                        &replay_ctx->record, error_info)) != 0)
        {
            break;
        }

        op_type = replay_ctx->record.op_type;
        fs_calc_block_hashcode(&replay_ctx->record.bs_key.block);

        //TODO
        if (result != 0) {
            snprintf(error_info, sizeof(error_info),
                    "%s fail, errno: %d, error info: %s",
                    replica_binlog_get_op_type_caption(op_type),
                    result, STRERROR(result));
            break;
        }

        p = line_end;
    }

    if (result != 0) {
        ServerBinlogReader *reader;
        int64_t offset;
        int64_t line_count;

        reader = &replay_ctx->rdthread_ctx.reader;
        offset = reader->position.offset + (p - buffer->buff);
        fc_get_file_line_count_ex(reader->filename, offset, &line_count);

        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", %s",
                __LINE__, reader->filename,
                line_count + 1, error_info);
    }

    return result;
}

static int do_replay_binlog(DataRecoveryContext *ctx)
{
    BinlogReplayContext *replay_ctx;
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];
    int result;

    replay_ctx = (BinlogReplayContext *)ctx->arg;
    data_recovery_get_subdir_name(ctx, RECOVERY_BINLOG_SUBDIR_NAME_REPLAY,
            subdir_name);
    if ((result=binlog_read_thread_init(&replay_ctx->rdthread_ctx, subdir_name,
                    NULL, NULL, BINLOG_BUFFER_SIZE)) != 0)
    {
        return result;
    }

    logInfo("file: "__FILE__", line: %d, "
            "dedup %s data ...", __LINE__, subdir_name);

    result = 0;
    while (SF_G_CONTINUE_FLAG) {
        if ((replay_ctx->r=binlog_read_thread_fetch_result(
                        &replay_ctx->rdthread_ctx)) == NULL)
        {
            result = EINTR;
            break;
        }

        logInfo("errno: %d, buffer length: %d", replay_ctx->r->err_no,
                replay_ctx->r->buffer.length);
        if (replay_ctx->r->err_no == ENOENT) {
            break;
        } else if (replay_ctx->r->err_no != 0) {
            result = replay_ctx->r->err_no;
            break;
        }

        if ((result=deal_binlog_buffer(ctx)) != 0) {
            break;
        }

        binlog_read_thread_return_result_buffer(&replay_ctx->rdthread_ctx,
                replay_ctx->r);
    }

    binlog_read_thread_terminate(&replay_ctx->rdthread_ctx);
    return result;
}

int data_recovery_replay_binlog(DataRecoveryContext *ctx)
{
    int result;
    BinlogReplayContext replay_ctx;

    ctx->arg = &replay_ctx;
    memset(&replay_ctx, 0, sizeof(replay_ctx));

    if ((result=do_replay_binlog(ctx)) == 0) {
    }

    return result;
}
