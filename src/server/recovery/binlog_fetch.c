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
#include "../server_binlog.h"
#include "../server_replication.h"
#include "data_recovery.h"
#include "binlog_fetch.h"

typedef struct {
    int fd;
    SharedBuffer *buffer;  //for network
} BinlogFetchContext;


static inline void get_fetched_binlog_filename(DataRecoveryContext *ctx,
        char *full_filename, const int size)
{
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];

    data_recovery_get_subdir_name(ctx, RECOVERY_BINLOG_SUBDIR_NAME_FETCH,
            subdir_name);
    binlog_reader_get_filename(subdir_name, 0, full_filename, size);
}

static int check_and_open_binlog_file(DataRecoveryContext *ctx)
{
    BinlogFetchContext *fetch_ctx;
    char full_filename[PATH_MAX];
    struct stat stbuf;
    int result;
    int distance;
    uint64_t last_data_version;
    bool unlink_flag;

    fetch_ctx = (BinlogFetchContext *)ctx->arg;
    get_fetched_binlog_filename(ctx, full_filename, sizeof(full_filename));
    unlink_flag = false;
    ctx->fetch.last_data_version = ctx->master->dg->myself->data_version;
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
                    "should fetch the data binlog again", __LINE__,
                    ctx->data_group_id, full_filename);
            unlink_flag = true;
            break;
        }

        if ((result=replica_binlog_get_last_data_version(
                        full_filename, &last_data_version)) != 0)
        {
            logWarning("file: "__FILE__", line: %d, "
                    "data_group_id: %d, binlog file: %s, get the last "
                    "data version fail, should fetch the data binlog again",
                    __LINE__, ctx->data_group_id, full_filename);
            unlink_flag = true;
            break;
        }

        if (last_data_version <= ctx->master->dg->myself->data_version) {
            logWarning("file: "__FILE__", line: %d, "
                    "data_group_id: %d, binlog file: %s, the last data "
                    "version: %"PRId64" <= my current data version: %"PRId64
                    ", should fetch the data binlog again", __LINE__,
                    ctx->data_group_id, full_filename, last_data_version,
                    ctx->master->dg->myself->data_version);
            unlink_flag = true;
            break;
        }

        ctx->fetch.last_data_version = last_data_version;
    } while (0);

    if (unlink_flag) {
        if (unlink(full_filename) != 0) {
            logError("file: "__FILE__", line: %d, "
                    "unlink file %s fail, errno: %d, error info: %s",
                    __LINE__, full_filename, errno, STRERROR(errno));
            return errno != 0 ? errno : EPERM;
        }
        ctx->fetch.last_data_version = ctx->master->dg->myself->data_version;
    }

    if ((fetch_ctx->fd=open(full_filename, O_WRONLY | O_CREAT | O_APPEND,
                    0644)) < 0)
    {
        logError("file: "__FILE__", line: %d, "
                "open binlog file %s fail, errno: %d, error info: %s",
                __LINE__, full_filename, errno, STRERROR(errno));
        return errno != 0 ? errno : EACCES;
    }

    return 0;
}

static int fetch_binlog_to_local(ConnectionInfo *conn,
        DataRecoveryContext *ctx, const unsigned char req_cmd,
        const unsigned char resp_cmd, char *out_buff,
        const int out_bytes, bool *is_last)
{
    int result;
    int bheader_size;
    int binlog_length;
    BinlogFetchContext *fetch_ctx;
    FSProtoHeader *header;
    FSProtoReplicaFetchBinlogRespBodyHeader *common_bheader;
    FSResponseInfo response;

    if (req_cmd == FS_REPLICA_PROTO_FETCH_BINLOG_FIRST_REQ) {
        bheader_size = sizeof(FSProtoReplicaFetchBinlogFirstRespBodyHeader);
    } else {
        bheader_size = sizeof(FSProtoReplicaFetchBinlogNextRespBodyHeader);
    }

    fetch_ctx = (BinlogFetchContext *)ctx->arg;
    response.error.length = 0;
    header = (FSProtoHeader *)out_buff;
    FS_PROTO_SET_HEADER(header, req_cmd, out_bytes - sizeof(FSProtoHeader));
    if ((result=fs_send_and_check_response_header(conn, out_buff,
            out_bytes, &response, SF_G_NETWORK_TIMEOUT, resp_cmd)) != 0)
    {
        fs_log_network_error(&response, conn, result);
        return result;
    }

    if (response.header.body_len < bheader_size) {
        logError("file: "__FILE__", line: %d, "
                "response body length: %d is too short, "
                "the min body length is %d", __LINE__,
                response.header.body_len, bheader_size);
        return EINVAL;
    }
    if (response.header.body_len > fetch_ctx->buffer->capacity) {
        logError("file: "__FILE__", line: %d, "
                "response body length: %d is too large, "
                "the max body length is %d", __LINE__,
                response.header.body_len, fetch_ctx->buffer->capacity);
        return EOVERFLOW;
    }

    if ((result=tcprecvdata_nb(conn->sock, fetch_ctx->buffer->buff,
                    response.header.body_len, SF_G_NETWORK_TIMEOUT)) != 0)
    {
        response.error.length = snprintf(response.error.message,
                sizeof(response.error.message),
                "recv data fail, errno: %d, error info: %s",
                result, STRERROR(result));
        fs_log_network_error(&response, conn, result);
        return result;
    }

    common_bheader = (FSProtoReplicaFetchBinlogRespBodyHeader *)
        fetch_ctx->buffer->buff;
    binlog_length = buff2int(common_bheader->binlog_length);
    *is_last = common_bheader->is_last;
    if (response.header.body_len != bheader_size + binlog_length) {
        logError("file: "__FILE__", line: %d, "
                "response body length: %d != body header size: %d"
                " + binlog_length: %d ", __LINE__, response.header.body_len,
                bheader_size, binlog_length);
        return EINVAL;
    }

    if (req_cmd == FS_REPLICA_PROTO_FETCH_BINLOG_FIRST_REQ) {
        FSProtoReplicaFetchBinlogFirstRespBodyHeader *first_bheader;

        first_bheader = (FSProtoReplicaFetchBinlogFirstRespBodyHeader *)
            fetch_ctx->buffer->buff;
        ctx->fetch.until_version = buff2long(first_bheader->until_version);
        ctx->fetch.is_online = first_bheader->is_online;

        logInfo("data group id: %d, is_online: %d, until_version: %"PRId64,
                ctx->data_group_id, ctx->fetch.is_online,
                ctx->fetch.until_version);
    }

    if (binlog_length == 0) {
        return 0;
    }

    if (write(((BinlogFetchContext *)ctx->arg)->fd,
                fetch_ctx->buffer->buff + bheader_size,
                binlog_length) != binlog_length)
    {
        result = errno != 0 ? errno : EPERM;
        logError("file: "__FILE__", line: %d, "
                "write to file fail, errno: %d, error info: %s",
                __LINE__, result, STRERROR(result));
        return result;
    }

    return 0;
}

static int fetch_binlog_first_to_local(ConnectionInfo *conn,
        DataRecoveryContext *ctx, bool *is_last)
{
    FSProtoReplicaFetchBinlogFirstReq *req;
    char out_buff[sizeof(FSProtoHeader) + sizeof(
            FSProtoReplicaFetchBinlogFirstReq)];


    req = (FSProtoReplicaFetchBinlogFirstReq *)
        (out_buff + sizeof(FSProtoHeader));
    long2buff(ctx->fetch.last_data_version, req->last_data_version);
    int2buff(ctx->data_group_id, req->data_group_id);
    int2buff(CLUSTER_MYSELF_PTR->server->id, req->server_id);
    if (ctx->catch_up == DATA_RECOVERY_CATCH_UP_LAST_BATCH) {
        req->catch_up = 1;
    } else {
        req->catch_up = 0;
    }

    return fetch_binlog_to_local(conn, ctx,
            FS_REPLICA_PROTO_FETCH_BINLOG_FIRST_REQ,
            FS_REPLICA_PROTO_FETCH_BINLOG_FIRST_RESP,
            out_buff, sizeof(out_buff), is_last);
}

static int fetch_binlog_next_to_local(ConnectionInfo *conn,
        DataRecoveryContext *ctx, bool *is_last)
{
    char out_buff[sizeof(FSProtoHeader)];
    return fetch_binlog_to_local(conn, ctx,
            FS_REPLICA_PROTO_FETCH_BINLOG_NEXT_REQ,
            FS_REPLICA_PROTO_FETCH_BINLOG_NEXT_RESP,
            out_buff, sizeof(out_buff), is_last);
}

static int proto_fetch_binlog(ConnectionInfo *conn, DataRecoveryContext *ctx)
{
    int result;
    bool is_last;

    if ((result=fetch_binlog_first_to_local(conn, ctx, &is_last)) != 0) {
        return result;
    }
    if (is_last) {
        return 0;
    }

    do {
        if ((result=fetch_binlog_next_to_local(conn, ctx, &is_last)) != 0) {
            return result;
        }
    } while (!is_last);

    return 0;
}

static int do_fetch_binlog(DataRecoveryContext *ctx)
{
    int result;
    ConnectionInfo conn;

    if ((result=fc_server_make_connection_ex(&REPLICA_GROUP_ADDRESS_ARRAY(
                        ctx->master->cs->server), &conn,
                    SF_G_CONNECT_TIMEOUT, NULL, true)) != 0)
    {
        return result;
    }

    result = proto_fetch_binlog(&conn, ctx);
    conn_pool_disconnect_server(&conn);
    return result;
}

int data_recovery_fetch_binlog(DataRecoveryContext *ctx, int64_t *binlog_size)
{
    int result;
    BinlogFetchContext fetch_ctx;

    ctx->arg = &fetch_ctx;
    *binlog_size = 0;
    if ((result=check_and_open_binlog_file(ctx)) != 0) {
        return result;
    }

    fetch_ctx.buffer = replication_callee_alloc_shared_buffer(ctx->server_ctx);
    if (fetch_ctx.buffer == NULL) {
        close(fetch_ctx.fd);
        return ENOMEM;
    }

    if ((result=do_fetch_binlog(ctx)) == 0) {
        if ((*binlog_size=lseek(fetch_ctx.fd, 0, SEEK_END)) < 0) {
            result = errno != 0 ? errno : EIO;
            logError("file: "__FILE__", line: %d, "
                    "lseek fetched binlog fail, data group id: %d, "
                    "errno: %d, error info: %s", __LINE__,
                    ctx->data_group_id, result, STRERROR(result));
        }
    }

    close(fetch_ctx.fd);
    shared_buffer_release(fetch_ctx.buffer);

    if (result == 0 && *binlog_size > 0) {
        char full_filename[PATH_MAX];
        ReplicaBinlogRecord record;

        get_fetched_binlog_filename(ctx, full_filename, sizeof(full_filename));
        if ((result=replica_binlog_get_last_record(
                        full_filename, &record)) == 0)
        {
            ctx->fetch.last_data_version = record.data_version;
            ctx->fetch.last_bkey = record.bs_key.block;
        }
    }

    return result;
}

int data_recovery_unlink_fetched_binlog(DataRecoveryContext *ctx)
{
    char full_filename[PATH_MAX];

    get_fetched_binlog_filename(ctx, full_filename, sizeof(full_filename));
    return fc_delete_file(full_filename);
}
