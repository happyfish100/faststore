/*
 * Copyright (c) 2020 YuQing <384681@qq.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

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
#include "fastcommon/fc_atomic.h"
#include "sf/sf_func.h"
#include "../../common/fs_proto.h"
#include "../server_global.h"
#include "../cluster_relationship.h"
#include "../server_binlog.h"
#include "../server_replication.h"
#include "data_recovery.h"
#include "binlog_fetch.h"

typedef struct {
    int fd;
    int wait_count;
    uint64_t until_version;
    SharedBuffer *buffer;  //for network
} BinlogFetchContext;

static inline const char *data_recovery_get_fetched_binlog_filename(
        DataRecoveryContext *ctx, char *full_filename, const int size)
{
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];

    data_recovery_get_subdir_name(ctx, RECOVERY_BINLOG_SUBDIR_NAME_FETCH,
            subdir_name);
    binlog_reader_get_filename(subdir_name, 0, full_filename, size);
    return full_filename;
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
    data_recovery_get_fetched_binlog_filename(ctx,
            full_filename, sizeof(full_filename));
    unlink_flag = false;
    ctx->fetch.last_data_version = __sync_fetch_and_add(
            &ctx->ds->data.version, 0);
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
                    ctx->ds->dg->id, full_filename);
            unlink_flag = true;
            break;
        }

        if ((result=replica_binlog_get_last_data_version(
                        full_filename, &last_data_version)) != 0)
        {
            logWarning("file: "__FILE__", line: %d, "
                    "data_group_id: %d, binlog file: %s, get the last "
                    "data version fail, should fetch the data binlog again",
                    __LINE__, ctx->ds->dg->id, full_filename);
            unlink_flag = true;
            break;
        }

        if (last_data_version <= ctx->ds->data.version) {
            logWarning("file: "__FILE__", line: %d, "
                    "data_group_id: %d, binlog file: %s, the last data "
                    "version: %"PRId64" <= my current data version: %"PRId64
                    ", should fetch the data binlog again", __LINE__,
                    ctx->ds->dg->id, full_filename, last_data_version,
                    ctx->ds->data.version);
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
        ctx->fetch.last_data_version = ctx->ds->data.version;
    }

    if ((fetch_ctx->fd=open(full_filename, O_WRONLY |
                    O_CREAT | O_APPEND, 0644)) < 0)
    {
        logError("file: "__FILE__", line: %d, "
                "open binlog file %s fail, errno: %d, error info: %s",
                __LINE__, full_filename, errno, STRERROR(errno));
        return errno != 0 ? errno : EACCES;
    }

    return 0;
}

static int get_last_data_version(DataRecoveryContext *ctx,
        string_t *binlog, uint64_t *last_data_version)
{
    ReplicaBinlogRecord record;
    char error_info[256];
    string_t line;
    int result;

    line.str = (char *)fc_memrchr(binlog->str, '\n', binlog->len - 1);
    if (line.str == NULL) {
        line.str = binlog->str;
    } else {
        line.str += 1;  //skip \n
    }
    line.len = (binlog->str + binlog->len) - line.str;
    if ((result=replica_binlog_record_unpack(&line,
                    &record, error_info)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "unpack replica binlog fail, %s",
                __LINE__, error_info);
        return result;
    }

    *last_data_version = record.data_version;
    return 0;
}

static int find_binlog_length(DataRecoveryContext *ctx,
        string_t *binlog, bool *is_last)
{
    BinlogFetchContext *fetch_ctx;
    ReplicaBinlogRecord record;
    uint64_t last_data_version;
    char error_info[256];
    char *end;
    char *line_end;
    string_t line;
    int result;

    fetch_ctx = (BinlogFetchContext *)ctx->arg;
    if (binlog->len > 0) {
        if ((result=get_last_data_version(ctx, binlog,
                        &last_data_version)) != 0)
        {
            return result;
        }
    } else {
        last_data_version = ctx->fetch.last_data_version;
    }

    logDebug("data group id: %d, current binlog length: %d, is_online: %d, "
            "last_data_version: %"PRId64", until_version: %"PRId64,
            ctx->ds->dg->id, binlog->len, ctx->is_online,
            ctx->fetch.last_data_version, fetch_ctx->until_version);

    if (last_data_version == fetch_ctx->until_version) {
        *is_last = true;
        return 0;
    } else if (last_data_version < fetch_ctx->until_version) {
        *is_last = false;
        if (binlog->len == 0) {
            if (++(fetch_ctx->wait_count) >= 5) {
                logError("file: "__FILE__", line: %d, "
                        "data group id: %d, master server id: %d, "
                        "waiting replica binlog timeout, "
                        "current data version: %"PRId64", waiting/until "
                        "data version: %"PRId64, __LINE__, ctx->ds->dg->id,
                        ctx->master->cs->server->id, last_data_version,
                        fetch_ctx->until_version);
                return ETIMEDOUT;
            }

            logInfo("file: "__FILE__", line: %d, "
                    "data group id: %d, master server id: %d, "
                    "%dth waiting replica binlog ..., "
                    "current data version: %"PRId64", waiting/until data "
                    "version: %"PRId64, __LINE__, ctx->ds->dg->id,
                    ctx->master->cs->server->id, fetch_ctx->wait_count,
                    last_data_version, fetch_ctx->until_version);
            fc_sleep_ms(500);
        } else {
            fetch_ctx->wait_count = 0;
        }

        return 0;
    }

    *is_last = true;
    line.str = binlog->str;
    end = binlog->str + binlog->len;
    while (line.str < end) {
        line_end = (char *)memchr(line.str, '\n', end - line.str);
        if (line_end == NULL) {
            logError("file: "__FILE__", line: %d, "
                    "data group id: %d, master server id: %d, "
                    "expect end line char (\\n)", __LINE__,
                    ctx->ds->dg->id, ctx->master->cs->server->id);
            return EINVAL;
        }

        line_end += 1;  //skip \n
        line.len = line_end - line.str;
        if ((result=replica_binlog_record_unpack(&line,
                        &record, error_info)) != 0)
        {
            logError("file: "__FILE__", line: %d, "
                    "data group id: %d, master server id: %d, "
                    "unpack replica binlog fail, %s", __LINE__,
                    ctx->ds->dg->id, ctx->master->cs->server->id,
                    error_info);
            return result;
        }

        if (record.data_version == fetch_ctx->until_version) {
            binlog->len = line_end - binlog->str;
            break;
        } else if (record.data_version > fetch_ctx->until_version) {
            binlog->len = line.str - binlog->str;
            break;
        }

        line.str = line_end;
    }

    return 0;
}

static int fetch_binlog_to_local(ConnectionInfo *conn,
        DataRecoveryContext *ctx, const unsigned char req_cmd,
        const unsigned char resp_cmd, char *out_buff,
        const int out_bytes, const bool last_retry, bool *is_last)
{
    int result;
    int bheader_size;
    string_t binlog;
    BinlogFetchContext *fetch_ctx;
    FSProtoHeader *header;
    FSProtoReplicaFetchBinlogRespBodyHeader *common_bheader;
    SFResponseInfo response;

    if (req_cmd == FS_REPLICA_PROTO_FETCH_BINLOG_FIRST_REQ) {
        bheader_size = sizeof(FSProtoReplicaFetchBinlogFirstRespBodyHeader);
    } else {
        bheader_size = sizeof(FSProtoReplicaFetchBinlogNextRespBodyHeader);
    }

    fetch_ctx = (BinlogFetchContext *)ctx->arg;
    response.error.length = 0;
    header = (FSProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, req_cmd, out_bytes - sizeof(FSProtoHeader));
    if ((result=sf_send_and_check_response_header(conn, out_buff,
            out_bytes, &response, SF_G_NETWORK_TIMEOUT, resp_cmd)) != 0)
    {
        int log_level;
        if (result == EOVERFLOW) {
            result = EAGAIN;
        }
        if (result == EAGAIN || result == EINPROGRESS) {
            log_level = last_retry ? LOG_WARNING : LOG_DEBUG;
        } else {
            log_level = LOG_ERR;
        }
        fs_log_network_error_ex(&response, conn, result, log_level);
        return result;
    }

    if (response.header.body_len < bheader_size) {
        logError("file: "__FILE__", line: %d, "
                "server %s:%u, response body length: %d is too short, "
                "the min body length is %d", __LINE__, conn->ip_addr,
                conn->port, response.header.body_len, bheader_size);
        return EINVAL;
    }
    if (response.header.body_len > fetch_ctx->buffer->capacity) {
        logError("file: "__FILE__", line: %d, "
                "server %s:%u, response body length: %d is too large, "
                "the max body length is %d", __LINE__, conn->ip_addr,
                conn->port, response.header.body_len,
                fetch_ctx->buffer->capacity);
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
    binlog.len = buff2int(common_bheader->binlog_length);
    *is_last = common_bheader->is_last;
    if (response.header.body_len != bheader_size + binlog.len) {
        logError("file: "__FILE__", line: %d, "
                "server %s:%u, response body length: %d != body header "
                "size: %d + binlog_length: %d ", __LINE__, conn->ip_addr,
                conn->port, response.header.body_len, bheader_size, binlog.len);
        return EINVAL;
    }

    if (req_cmd == FS_REPLICA_PROTO_FETCH_BINLOG_FIRST_REQ) {
        FSProtoReplicaFetchBinlogFirstRespBodyHeader *first_bheader;

        first_bheader = (FSProtoReplicaFetchBinlogFirstRespBodyHeader *)
            fetch_ctx->buffer->buff;
        ctx->master_repl_version = buff2int(first_bheader->repl_version);
        ctx->is_full_dump = (first_bheader->is_full_dump == 1);
        fetch_ctx->until_version = buff2long(first_bheader->until_version);
        if (ctx->is_online) {
            if (!first_bheader->is_online) {
                int old_status;
                old_status = FC_ATOMIC_GET(ctx->ds->status);
                logWarning("file: "__FILE__", line: %d, "
                        "server %s:%u, data group id: %d, "
                        "my status: %d (%s), unexpect is_online: %d",
                        __LINE__, conn->ip_addr, conn->port, ctx->ds->dg->id,
                        old_status, fs_get_server_status_caption(old_status),
                        first_bheader->is_online);
                return EBUSY;
            }

            FC_ATOMIC_SET(ctx->ds->recovery.until_version,
                    fetch_ctx->until_version);
            data_recovery_notify_replication(ctx->ds);
        }

        /*
        logInfo("data group id: %d, is_full_dump: %d, is_online: %d, "
                "last_data_version: %"PRId64", until_version: %"PRId64,
                ctx->ds->dg->id, ctx->is_full_dump, ctx->is_online,
                ctx->fetch.last_data_version, fetch_ctx->until_version);
                */
    }

    binlog.str = fetch_ctx->buffer->buff + bheader_size;
    if (ctx->is_online) {
        if ((result=find_binlog_length(ctx, &binlog, is_last)) != 0) {
            return result;
        }
    }

    if (binlog.len == 0) {
        return 0;
    }

    if (fc_safe_write(((BinlogFetchContext *)ctx->arg)->fd,
                binlog.str, binlog.len) != binlog.len)
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
#define FETCH_BINLOG_RETRY_TIMES  10
    int result;
    int log_level;
    int my_status;
    int i;
    int retry_count;
    int remove_count;
    int binlog_count;
    int binlog_length;
    int buffer_size;
    int pkg_len;
    int sleep_ms;
    int max_sleep_ms;
    int64_t start_time_ms;
    int64_t time_used;
    char time_buff[32];
    FSProtoReplicaFetchBinlogFirstReqHeader *rheader;
    char out_buff[sizeof(FSProtoHeader) + sizeof(
            FSProtoReplicaFetchBinlogFirstReqHeader) +
        FS_MAX_SLAVE_BINLOG_CHECK_LAST_ROWS *
        FS_REPLICA_BINLOG_MAX_RECORD_SIZE];

    start_time_ms = get_current_time_ms();
    rheader = (FSProtoReplicaFetchBinlogFirstReqHeader *)
        (out_buff + sizeof(FSProtoHeader));
    long2buff(ctx->fetch.last_data_version, rheader->last_data_version);
    int2buff(ctx->ds->dg->id, rheader->data_group_id);
    int2buff(CLUSTER_MYSELF_PTR->server->id, rheader->server_id);
    if (ctx->catch_up == DATA_RECOVERY_CATCH_UP_LAST_BATCH) {
        rheader->catch_up = 1;
    } else {
        rheader->catch_up = 0;
    }

    pkg_len = sizeof(FSProtoHeader) + sizeof(*rheader);
    if (SLAVE_BINLOG_CHECK_LAST_ROWS > 0) {
        binlog_count = SLAVE_BINLOG_CHECK_LAST_ROWS;
        buffer_size = sizeof(out_buff) - pkg_len;
        if ((result=replica_binlog_get_last_lines(ctx->ds->dg->id,
                (char *)(rheader + 1), buffer_size, &binlog_count,
                &binlog_length)) != 0)
        {
            return result;
        }
    } else {
        binlog_length = 0;
    }
    pkg_len += binlog_length;
    int2buff(binlog_length, rheader->binlog_length);

    result = EINTR;
    sleep_ms = 100;
    i = 0;
    retry_count = 1;
    while (retry_count <= FETCH_BINLOG_RETRY_TIMES && SF_G_CONTINUE_FLAG) {
        my_status = __sync_add_and_fetch(&ctx->ds->status, 0);
        if (!(my_status == FS_DS_STATUS_REBUILDING ||
                my_status == FS_DS_STATUS_RECOVERING ||
                (my_status == FS_DS_STATUS_ONLINE && ctx->is_online)))
        {
            logWarning("file: "__FILE__", line: %d, "
                    "data group id: %d, my status: %d (%s) "
                    "is unexpected, skip data recovery!",
                    __LINE__, ctx->ds->dg->id, my_status,
                    fs_get_server_status_caption(my_status));
            result = EINVAL;
            break;
        }

        result = fetch_binlog_to_local(conn, ctx,
                FS_REPLICA_PROTO_FETCH_BINLOG_FIRST_REQ,
                FS_REPLICA_PROTO_FETCH_BINLOG_FIRST_RESP, out_buff,
                pkg_len, retry_count == FETCH_BINLOG_RETRY_TIMES, is_last);
        if (!(result == EAGAIN || result == EINPROGRESS)) {
            if (result == SF_CLUSTER_ERROR_BINLOG_INCONSISTENT) {
                logCrit("file: "__FILE__", line: %d, "
                        "data group id: %d, the replica binlog is "
                        "NOT consistent with the master server %d, "
                        "some mistake happen, program exit abnormally!",
                        __LINE__, ctx->ds->dg->id,
                        ctx->master->cs->server->id);
                sf_terminate_myself();
            } else if (result == SF_CLUSTER_ERROR_BINLOG_MISSED) {
                logWarning("file: "__FILE__", line: %d, "
                        "data group id: %d, the replica binlog is "
                        "missed on the master server %d, delete all "
                        "binlog files ...", __LINE__, ctx->ds->dg->id,
                        ctx->master->cs->server->id);
                if (replica_binlog_remove_all_files(ctx->ds->dg->id,
                            &remove_count) == 0)
                {
                    logWarning("file: "__FILE__", line: %d, "
                            "data group id: %d, delete %d binlog files",
                            __LINE__, ctx->ds->dg->id, remove_count);
                    FC_ATOMIC_SET(ctx->ds->data.version, 0);
                }
            }

            break;
        }

        if (result == EAGAIN) {
            max_sleep_ms = 1000;
            cluster_relationship_trigger_report_ds_status(ctx->ds);
            ++retry_count;
        } else {   //EINPROGRESS
            if (sleep_ms < 500) {
                sleep_ms = 500;
            }
            max_sleep_ms = 10000;
        }
        fc_sleep_ms(sleep_ms);  //waiting for ds status ready on the master

        //some thing goes wrong, trigger report to the leader again
        if (sleep_ms < max_sleep_ms) {
            sleep_ms *= 2;
            if (sleep_ms > max_sleep_ms) {
                sleep_ms = max_sleep_ms;
            }
        }
        ++i;
    }


    time_used = get_current_time_ms() - start_time_ms;
    long_to_comma_str(time_used, time_buff);
    if (result != 0) {
        log_level = LOG_WARNING;
    } else if (time_used > 3000) {
        log_level = LOG_INFO;
    } else {
        log_level = LOG_DEBUG;
    }
    log_it_ex(&g_log_context, log_level, "file: "__FILE__", line: %d, "
            "data group id: %d, waiting count: %d, result: %d, "
            "time used: %s ms", __LINE__, ctx->ds->dg->id, i,
            result, time_buff);

    return result == 0 ? 0 : EINVAL;
}

static int fetch_binlog_next_to_local(ConnectionInfo *conn,
        DataRecoveryContext *ctx, bool *is_last)
{
    char out_buff[sizeof(FSProtoHeader)];
    return fetch_binlog_to_local(conn, ctx,
            FS_REPLICA_PROTO_FETCH_BINLOG_NEXT_REQ,
            FS_REPLICA_PROTO_FETCH_BINLOG_NEXT_RESP,
            out_buff, sizeof(out_buff), true, is_last);
}

static int proto_fetch_binlog(ConnectionInfo *conn, DataRecoveryContext *ctx)
{
    int result;
    bool is_last;

    if ((result=fetch_binlog_first_to_local(conn, ctx, &is_last)) != 0) {
        return result;
    }

    while (!is_last) {
        if ((result=fetch_binlog_next_to_local(conn, ctx, &is_last)) != 0) {
            return result;
        }
    }

    return 0;
}

static int do_fetch_binlog(DataRecoveryContext *ctx)
{
    int result;
    ConnectionInfo conn;

    if ((result=fc_server_make_connection_ex(&REPLICA_GROUP_ADDRESS_ARRAY(
                        ctx->master->cs->server), &conn, "fstore",
                    SF_G_CONNECT_TIMEOUT, NULL, true)) != 0)
    {
        return result;
    }

    result = proto_fetch_binlog(&conn, ctx);
    conn_pool_disconnect_server(&conn);
    return result;
}

static int check_online_me(DataRecoveryContext *ctx)
{
#define RETRY_TIMES  3
    uint64_t old_until_version;
    int old_status;
    int i;

    ctx->is_online = (ctx->catch_up == DATA_RECOVERY_CATCH_UP_LAST_BATCH);
    if (!ctx->is_online) {
        return 0;
    }

    old_status = FC_ATOMIC_GET(ctx->ds->status);
    if (!(old_status == FS_DS_STATUS_REBUILDING ||
                old_status == FS_DS_STATUS_RECOVERING))
    {
        logError("file: "__FILE__", line: %d, "
                "data group id: %d, unexpect my status %d (%s)",
                __LINE__, ctx->ds->dg->id, old_status,
                fs_get_server_status_caption(old_status));
        return EBUSY;
    }

    for (i=0; i<RETRY_TIMES; i++) {
        if (replication_channel_is_all_ready(ctx->master)) {
            break;
        }

        sleep(1);
    }

    if (i > 0) {
        char prompt[64];
        int log_level;
        if (i < RETRY_TIMES) {
            strcpy(prompt, "success");
            log_level = (i == 1 ? LOG_DEBUG : LOG_INFO);
        } else {
            strcpy(prompt, "timeout");
            log_level = LOG_WARNING;
        }
        log_it_ex(&g_log_context, log_level, "file: "__FILE__", line: %d, "
                "data group id: %d, wait replica channel of master id %d "
                "ready %s, waiting count: %d", __LINE__, ctx->ds->dg->id,
                ctx->master->cs->server->id, prompt, i);
    }

    old_until_version = FC_ATOMIC_GET(ctx->ds->recovery.until_version);
    if (old_until_version != 0) {
        /* for hold RPC replication */
        FC_ATOMIC_CAS(ctx->ds->recovery.until_version, old_until_version, 0);
    }
    if (!cluster_relationship_swap_report_ds_status(ctx->ds,
                old_status, FS_DS_STATUS_ONLINE,
                FS_EVENT_SOURCE_SELF_REPORT))
    {
        logError("file: "__FILE__", line: %d, "
                "data group id: %d, change my status from %d (%s) "
                "to %d (%s) fail", __LINE__, ctx->ds->dg->id,
                old_status, fs_get_server_status_caption(old_status),
                FS_DS_STATUS_ONLINE,
                fs_get_server_status_caption(FS_DS_STATUS_ONLINE));
        return EBUSY;
    }

    return 0;
}

int data_recovery_fetch_binlog(DataRecoveryContext *ctx, int64_t *binlog_size)
{
    int result;
    BinlogFetchContext fetch_ctx;

    ctx->arg = &fetch_ctx;
    memset(&fetch_ctx, 0, sizeof(fetch_ctx));
    *binlog_size = 0;
    if ((result=check_and_open_binlog_file(ctx)) != 0) {
        return result;
    }

    fetch_ctx.buffer = replication_callee_alloc_shared_buffer(ctx->server_ctx);
    if (fetch_ctx.buffer == NULL) {
        close(fetch_ctx.fd);
        return ENOMEM;
    }

    do {
        if ((result=check_online_me(ctx)) != 0) {
            break;
        }

        if ((result=do_fetch_binlog(ctx)) != 0) {
            break;
        }

        if ((*binlog_size=lseek(fetch_ctx.fd, 0, SEEK_END)) < 0) {
            result = errno != 0 ? errno : EIO;
            logError("file: "__FILE__", line: %d, "
                    "lseek fetched binlog fail, data group id: %d, "
                    "errno: %d, error info: %s", __LINE__,
                    ctx->ds->dg->id, result, STRERROR(result));
            break;
        }
    } while (0);

    close(fetch_ctx.fd);
    shared_buffer_release(fetch_ctx.buffer);

    if (result == 0 && *binlog_size > 0) {
        char full_filename[PATH_MAX];
        ReplicaBinlogRecord record;

        data_recovery_get_fetched_binlog_filename(ctx,
                full_filename, sizeof(full_filename));
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

    data_recovery_get_fetched_binlog_filename(ctx,
            full_filename, sizeof(full_filename));
    return fc_delete_file(full_filename);
}
