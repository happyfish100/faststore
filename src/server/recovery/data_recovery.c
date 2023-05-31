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
#include "fastcommon/fc_atomic.h"
#include "sf/sf_global.h"
#include "sf/sf_service.h"
#include "../../common/fs_proto.h"
#include "../../common/fs_func.h"
#include "../server_global.h"
#include "../server_group_info.h"
#include "../cluster_relationship.h"
#include "../server_binlog.h"
#include "../server_replication.h"
#include "binlog_fetch.h"
#include "binlog_dedup.h"
#include "binlog_replay.h"
#include "binlog_sync.h"
#include "data_recovery.h"

#define DATA_RECOVERY_SYS_DATA_FILENAME        "data_recovery.dat"
#define DATA_RECOVERY_SYS_DATA_SECTION_FETCH   "fetch"
#define DATA_RECOVERY_SYS_DATA_SECTION_RESTORE "restore"
#define DATA_RECOVERY_SYS_DATA_ITEM_STAGE      "stage"
#define DATA_RECOVERY_SYS_DATA_ITEM_NEXT_STAGE "next_stage"
#define DATA_RECOVERY_SYS_DATA_ITEM_FULL_DUMP  "full_dump"
#define DATA_RECOVERY_SYS_DATA_ITEM_LAST_DV    "last_data_version"
#define DATA_RECOVERY_SYS_DATA_ITEM_LAST_BKEY  "last_bkey"
#define DATA_RECOVERY_SYS_DATA_ITEM_RESTORE_FLAG     "flag"
#define DATA_RECOVERY_SYS_DATA_ITEM_RESTORE_START_DV "start_dv"

#define DATA_RECOVERY_STAGE_NONE    '-'
#define DATA_RECOVERY_STAGE_FETCH   'F'
#define DATA_RECOVERY_STAGE_DEDUP   'D'
#define DATA_RECOVERY_STAGE_SYNC    'S'  //sync existing binlogs after full dump
#define DATA_RECOVERY_STAGE_REPLAY  'R'
#define DATA_RECOVERY_STAGE_RESTORE 'T'
#define DATA_RECOVERY_STAGE_LOG     'L'  //log to replica binlog
#define DATA_RECOVERY_STAGE_CLEANUP 'C'

int data_recovery_init(const char *config_filename)
{
    int result;

    if ((result=binlog_replay_init(config_filename)) != 0) {
        return result;
    }

    return 0;
}

void data_recovery_destroy()
{
}

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

    gid_len = sprintf(data_group_id, "%d", ctx->ds->dg->id);
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
        if ((result=fc_check_mkdir_ex(filepath, 0775, &create)) != 0) {
            return result;
        }
        if (create) {
            SF_CHOWN_RETURN_ON_ERROR(filepath, geteuid(), getegid());
        }
    }

    return 0;
}

static FSClusterDataServerInfo *data_recovery_get_master(
        DataRecoveryContext *ctx, int *err_no)
{
    FSClusterDataServerInfo *master;

    if (CLUSTER_LEADER_ATOM_PTR == NULL) {
        logWarning("file: "__FILE__", line: %d, "
                "data group id: %d, no leader",
                __LINE__, ctx->ds->dg->id);
        *err_no = ENOENT;
        return NULL;
    }

    master = (FSClusterDataServerInfo *)
        FC_ATOMIC_GET(ctx->ds->dg->master);
    if (master == NULL) {
        logWarning("file: "__FILE__", line: %d, "
                "data group id: %d, no master",
                __LINE__, ctx->ds->dg->id);
        *err_no = ENOENT;
        return NULL;
    }

    if (ctx->ds->dg->myself == NULL) {
        logError("file: "__FILE__", line: %d, "
                "data group id: %d NOT belongs to me",
                __LINE__, ctx->ds->dg->id);
        *err_no = ENOENT;
        return NULL;
    }

    if (ctx->ds->dg->myself == master) {
        logWarning("file: "__FILE__", line: %d, "
                "data group id: %d, i am already master, "
                "do NOT need recovery!", __LINE__, ctx->ds->dg->id);
        *err_no = EBUSY;
        return NULL;
    }

    if (ctx->ds->dg->myself == FC_ATOMIC_GET(ctx->ds->dg->old_master)) {
        logWarning("file: "__FILE__", line: %d, "
                "data group id: %d, rollback in progress, "
                "can't recovery!", __LINE__, ctx->ds->dg->id);
        *err_no = EBUSY;
        return NULL;
    }

    *err_no = 0;
    return master;
}

static void data_recovery_get_sys_data_filename(DataRecoveryContext *ctx,
        char *filename, const int size)
{
    snprintf(filename, size, "%s/%s/%d/%s", DATA_PATH_STR,
            FS_RECOVERY_BINLOG_SUBDIR_NAME, ctx->ds->dg->id,
            DATA_RECOVERY_SYS_DATA_FILENAME);
}

static int data_recovery_save_sys_data(DataRecoveryContext *ctx)
{
    char filename[PATH_MAX];
    char buff[256];
    int len;

    data_recovery_get_sys_data_filename(ctx, filename, sizeof(filename));
    len = sprintf(buff, "%s=%c\n"
            "%s=%c\n"
            "%s=%d\n"
            "[%s]\n"
            "%s=%"PRId64"\n"
            "%s=%"PRId64",%"PRId64"\n"
            "[%s]\n"
            "%s=%d\n"
            "%s=%"PRId64"\n",
            DATA_RECOVERY_SYS_DATA_ITEM_STAGE, ctx->stage,
            DATA_RECOVERY_SYS_DATA_ITEM_NEXT_STAGE, ctx->next_stage,
            DATA_RECOVERY_SYS_DATA_ITEM_FULL_DUMP, ctx->is_full_dump ? 1 : 0,
            DATA_RECOVERY_SYS_DATA_SECTION_FETCH,
            DATA_RECOVERY_SYS_DATA_ITEM_LAST_DV, ctx->fetch.last_data_version,
            DATA_RECOVERY_SYS_DATA_ITEM_LAST_BKEY, ctx->fetch.last_bkey.oid,
            ctx->fetch.last_bkey.offset, DATA_RECOVERY_SYS_DATA_SECTION_RESTORE,
            DATA_RECOVERY_SYS_DATA_ITEM_RESTORE_FLAG, ctx->is_restore ? 1 : 0,
            DATA_RECOVERY_SYS_DATA_ITEM_RESTORE_START_DV, ctx->restore.start_dv
            );

    return safeWriteToFile(filename, buff, len);
}

int data_recovery_unlink_sys_data(DataRecoveryContext *ctx)
{
    char filename[PATH_MAX];

    data_recovery_get_sys_data_filename(ctx, filename, sizeof(filename));
    return fc_delete_file(filename);
}

static int data_recovery_load_sys_data(DataRecoveryContext *ctx)
{
    IniContext ini_context;
    char filename[PATH_MAX];
    char *last_bkey;
    int result;

    data_recovery_get_sys_data_filename(ctx, filename, sizeof(filename));
    if (access(filename, F_OK) != 0) {
        result = errno != 0 ? errno : EPERM;
        if (result != ENOENT) {
            logError("file: "__FILE__", line: %d, "
                    "access file: %s fail, errno: %d, error info: %s",
                    __LINE__, filename, result, STRERROR(result));
            return result;
        }

        ctx->stage = DATA_RECOVERY_STAGE_FETCH;
        ctx->next_stage = DATA_RECOVERY_STAGE_NONE;
        return data_recovery_save_sys_data(ctx);
    }

    if ((result=iniLoadFromFile(filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load conf file \"%s\" fail, ret code: %d",
                __LINE__, filename, result);
        return result;
    }

    ctx->stage = iniGetCharValue(NULL, DATA_RECOVERY_SYS_DATA_ITEM_STAGE,
            &ini_context, DATA_RECOVERY_STAGE_FETCH);
    ctx->next_stage = iniGetCharValue(NULL,
            DATA_RECOVERY_SYS_DATA_ITEM_NEXT_STAGE,
            &ini_context, DATA_RECOVERY_STAGE_NONE);
    ctx->is_full_dump = iniGetBoolValue(NULL,
            DATA_RECOVERY_SYS_DATA_ITEM_FULL_DUMP,
            &ini_context, false);

    ctx->is_restore = iniGetBoolValue(DATA_RECOVERY_SYS_DATA_SECTION_RESTORE,
            DATA_RECOVERY_SYS_DATA_ITEM_RESTORE_FLAG, &ini_context, false);
    ctx->restore.start_dv = iniGetInt64Value(
            DATA_RECOVERY_SYS_DATA_SECTION_RESTORE,
            DATA_RECOVERY_SYS_DATA_ITEM_RESTORE_START_DV,
            &ini_context, 0);

    ctx->fetch.last_data_version = iniGetInt64Value(
            DATA_RECOVERY_SYS_DATA_SECTION_FETCH,
            DATA_RECOVERY_SYS_DATA_ITEM_LAST_DV,
            &ini_context, 0);
    last_bkey = iniGetStrValue(DATA_RECOVERY_SYS_DATA_SECTION_FETCH,
            DATA_RECOVERY_SYS_DATA_ITEM_LAST_BKEY, &ini_context);
    if (last_bkey != NULL && *last_bkey != '\0') {
        char value[64];
        char *cols[2];
        int count;

        snprintf(value, sizeof(value), "%s", last_bkey);
        count = splitEx(value, ',', cols, 2);
        if (count == 2) {
            ctx->fetch.last_bkey.oid = strtoll(cols[0], NULL, 10);
            ctx->fetch.last_bkey.offset = strtoll(cols[1], NULL, 10);
        } else {
            logError("file: "__FILE__", line: %d, "
                    "load conf file \"%s\" fail, invalid %s: %s",
                    __LINE__, filename, DATA_RECOVERY_SYS_DATA_ITEM_LAST_BKEY,
                    last_bkey);
            result = EINVAL;
        }
    }

    iniFreeContext(&ini_context);
    return result;
}

static int init_data_recovery_ctx(DataRecoveryContext *ctx,
        FSClusterDataServerInfo *ds)
{
    int result;
    struct nio_thread_data *thread_data;

    ctx->ds = ds;
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

    thread_data = sf_get_random_thread_data_ex(&REPLICA_SF_CTX);
    ctx->server_ctx = (FSServerContext *)thread_data->arg;
    if ((result=data_recovery_load_sys_data(ctx)) != 0) {
        return result;
    }

    if ((ctx->tallocator_info=binlog_replay_get_task_allocator()) == NULL) {
        logError("file: "__FILE__", line: %d, "
                "get_task_allocator fail", __LINE__);
        return ENOSPC;
    }

    return 0;
}

static void destroy_data_recovery_ctx(DataRecoveryContext *ctx)
{
    if (ctx->tallocator_info != NULL) {
        binlog_replay_release_task_allocator(ctx->tallocator_info);
    }
}

static int deal_binlog_buffer(BinlogReadThreadContext *rdthread_ctx,
        BinlogReadThreadResult *r)
{
    char *p;
    char *line_end;
    char *end;
    BufferInfo *buffer;
    ReplicaBinlogRecord record;
    string_t line;
    char error_info[256];
    int result;

    result = 0;
    *error_info = '\0';
    buffer = &r->buffer;
    end = buffer->buff + buffer->length;
    p = buffer->buff;
    while (p < end) {
        line_end = (char *)memchr(p, '\n', end - p);
        if (line_end == NULL) {
            strcpy(error_info, "expect end line (\\n)");
            result = EINVAL;
            break;
        }

        line.str = p;
        line.len = ++line_end - p;
        if ((result=replica_binlog_record_unpack(&line,
                        &record, error_info)) != 0)
        {
            break;
        }

        fs_calc_block_hashcode(&record.bs_key.block, FILE_BLOCK_SIZE);
        switch (record.op_type) {
            case BINLOG_OP_TYPE_WRITE_SLICE:
            case BINLOG_OP_TYPE_ALLOC_SLICE:
            case BINLOG_OP_TYPE_DEL_SLICE:
                result = replica_binlog_log_slice(record.timestamp,
                        FS_DATA_GROUP_ID(record.bs_key.block),
                        record.data_version, &record.bs_key,
                        BINLOG_SOURCE_REPLAY, record.op_type);
                break;
            case BINLOG_OP_TYPE_DEL_BLOCK:
            case BINLOG_OP_TYPE_NO_OP:
                result = replica_binlog_log_block(record.timestamp,
                        FS_DATA_GROUP_ID(record.bs_key.block),
                        record.data_version, &record.bs_key.block,
                        BINLOG_SOURCE_REPLAY, record.op_type);
                break;
            default:
                break;
        }

        if (result != 0) {
            snprintf(error_info, sizeof(error_info),
                    "%s fail, errno: %d, error info: %s",
                    replica_binlog_get_op_type_caption(record.op_type),
                    result, STRERROR(result));
            break;
        }

        p = line_end;
    }

    if (result != 0) {
        ServerBinlogReader *reader;
        int64_t offset;
        int64_t line_count;

        reader = &rdthread_ctx->reader;
        offset = reader->position.offset + (p - buffer->buff);
        fc_get_file_line_count_ex(reader->filename, offset, &line_count);

        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", %s",
                __LINE__, reader->filename,
                line_count + 1, error_info);
        abort();
    }

    return result;
}

static int log_to_replica_binlog(DataRecoveryContext *ctx)
{
    BinlogReadThreadContext rdthread_ctx;
    BinlogReadThreadResult *r;
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];
    SFBinlogFilePosition position;
    uint64_t last_data_version;
    int result;

    data_recovery_get_subdir_name(ctx, RECOVERY_BINLOG_SUBDIR_NAME_FETCH,
            subdir_name);
    if ((result=replica_binlog_get_last_dv(ctx->ds->dg->id,
                    &last_data_version)) != 0)
    {
        if (result != ENOENT) {
            logError("file: "__FILE__", line: %d, "
                    "data group id: %d, get replica last data version "
                    "fail, errno: %d, error info: %s", __LINE__,
                    ctx->ds->dg->id, result, STRERROR(result));
            return result;
        }

        position.index = 0;
        position.offset = 0;
    } else {
        if ((result=replica_binlog_get_position_by_dv_ex(subdir_name,
                        NULL, last_data_version, &position, true)) != 0)
        {
            return result;
        }
    }

    if ((result=binlog_read_thread_init(&rdthread_ctx, subdir_name,
                    NULL, &position, BINLOG_BUFFER_SIZE)) != 0)
    {
        return result;
    }

    result = 0;
    while (SF_G_CONTINUE_FLAG) {
        if ((r=binlog_read_thread_fetch_result(&rdthread_ctx)) == NULL) {
            result = EINTR;
            break;
        }

        /*
        logInfo("errno: %d, buffer length: %d", r->err_no,
                r->buffer.length);
                */
        if (r->err_no == ENOENT) {
            break;
        } else if (r->err_no != 0) {
            result = r->err_no;
            break;
        }

        if ((result=deal_binlog_buffer(&rdthread_ctx, r)) != 0) {
            break;
        }

        binlog_read_thread_return_result_buffer(&rdthread_ctx, r);
    }

    binlog_read_thread_terminate(&rdthread_ctx);
    return result;
}

static int data_recovery_log_to_replica(DataRecoveryContext *ctx,
        const uint64_t old_data_version)
{
    int result;

    if ((result=replica_binlog_waiting_write_done(ctx->ds->dg->id,
                    old_data_version, "old")) != 0)
    {
        return result;
    }

    if ((result=replica_binlog_writer_change_order_by(ctx->ds,
                    SF_BINLOG_WRITER_TYPE_ORDER_BY_NONE)) != 0)
    {
        return result;
    }

    if ((result=log_to_replica_binlog(ctx)) != 0) {
        return result;
    }

    if ((result=replica_binlog_waiting_write_done(ctx->ds->dg->id,
                    ctx->fetch.last_data_version, "replay")) != 0)
    {
        return result;
    }

    if ((result=replica_binlog_writer_change_order_by(ctx->ds,
                    SF_BINLOG_WRITER_TYPE_ORDER_BY_VERSION)) != 0)
    {
        return result;
    }

    return 0;
}

static int proto_active_confirm(ConnectionInfo *conn,
        DataRecoveryContext *ctx, const bool last_retry)
{
    int result;
    FSProtoHeader *header;
    SFResponseInfo response;
    FSProtoReplicaActiveConfirmReq *req;
    char out_buff[sizeof(FSProtoHeader) + sizeof(
            FSProtoReplicaActiveConfirmReq)];

    header = (FSProtoHeader *)out_buff;
    req = (FSProtoReplicaActiveConfirmReq *)(header + 1);
    int2buff(ctx->ds->dg->id, req->data_group_id);
    int2buff(CLUSTER_MYSELF_PTR->server->id, req->server_id);
    int2buff(ctx->master_repl_version, req->repl_version);
    SF_PROTO_SET_HEADER(header, FS_REPLICA_PROTO_ACTIVE_CONFIRM_REQ,
            sizeof(FSProtoReplicaActiveConfirmReq));

    response.error.length = 0;
    if ((result=sf_send_and_recv_none_body_response(conn, out_buff,
                    sizeof(out_buff), &response, SF_G_NETWORK_TIMEOUT,
                    FS_REPLICA_PROTO_ACTIVE_CONFIRM_RESP)) != 0)
    {
        int log_level;
        if (result == EAGAIN) {
            log_level = last_retry ? LOG_WARNING : LOG_DEBUG;
        } else {
            log_level = LOG_ERR;
        }
        fs_log_network_error_ex(&response, conn, result, log_level);
    }

    return result;
}

static int active_confirm(DataRecoveryContext *ctx)
{
#define ACTIVE_CONFIRM_RETRY_TIMES  3
    int result;
    int i;
    ConnectionInfo conn;

    if ((result=fc_server_make_connection_ex(&REPLICA_GROUP_ADDRESS_ARRAY(
                        ctx->master->cs->server), &conn, "fstore",
                    SF_G_CONNECT_TIMEOUT, NULL, true)) != 0)
    {
        return result;
    }

    i = 0;
    while ((result=proto_active_confirm(&conn, ctx, i ==
                    ACTIVE_CONFIRM_RETRY_TIMES)) == EAGAIN)
    {
        if (i++ == ACTIVE_CONFIRM_RETRY_TIMES) {
            break;
        }

        sleep(1);
    }

    conn_pool_disconnect_server(&conn);
    return result;
}

static int active_me(DataRecoveryContext *ctx)
{
    int result;

    if ((result=active_confirm(ctx)) != 0) {
        return result;
    }

    if (cluster_relationship_swap_report_ds_status(ctx->ds,
                FS_DS_STATUS_ONLINE, FS_DS_STATUS_ACTIVE,
                FS_EVENT_SOURCE_SELF_REPORT))
    {
        return 0;
    } else {
        int status;
        status = __sync_add_and_fetch(&ctx->ds->status, 0);
        logError("file: "__FILE__", line: %d, "
                "data group id: %d, change my status to ACTIVE fail, "
                "current status is %d (%s)", __LINE__, ctx->ds->dg->id,
                status, fs_get_server_status_caption(status));
        return EBUSY;
    }
}

static void find_restore_start_dv(DataRecoveryContext *ctx)
{
    int result;
    int first_unmatched_index;
    char full_filename[PATH_MAX];
    string_t mbuffer;
    int64_t file_size;

    data_recovery_get_fetched_binlog_filename(ctx,
            full_filename, sizeof(full_filename));
    if ((result=getFileContent(full_filename, &mbuffer.str,
                    &file_size)) != 0)
    {
        return;
    }
    mbuffer.len = file_size;

    result = replica_binlog_slave_check_consistency(ctx->ds->dg->id,
            &mbuffer, &first_unmatched_index, &ctx->restore.start_dv);
    free(mbuffer.str);
    if (result != SF_CLUSTER_ERROR_BINLOG_INCONSISTENT) {
        if (result == 0) {
            logError("file: "__FILE__", line: %d, "
                    "data group id: %d, unexpect check consistency "
                    "result: 0, some mistake happen?", __LINE__,
                    ctx->ds->dg->id);
        }
        return;
    }

    if (first_unmatched_index < SLAVE_BINLOG_CHECK_LAST_ROWS) {
        logError("file: "__FILE__", line: %d, "
                "data group id: %d, first_unmatched_index: %d < "
                "slave_binlog_check_last_rows: %d, delete all "
                "binlog files ...", __LINE__, ctx->ds->dg->id,
                first_unmatched_index, SLAVE_BINLOG_CHECK_LAST_ROWS);
        if (replica_binlog_remove_all_files(ctx->ds->dg->id) == 0) {
            replica_binlog_set_data_version(ctx->ds, 0);
        }

        ctx->restore.start_dv = 0;
    }
}

static int do_data_recovery(DataRecoveryContext *ctx)
{
    int result;
    int64_t binlog_count;
    int64_t binlog_size;
    int64_t start_time;
    int64_t dedup_start_time;
    uint64_t old_data_version;

    ctx->start_time = get_current_time_ms();
    start_time = 0;
    binlog_count = 0;
    result = 0;
    switch (ctx->stage) {
        case DATA_RECOVERY_STAGE_FETCH:
            start_time = get_current_time_ms();
            if ((result=data_recovery_fetch_binlog(ctx, &binlog_size)) != 0) {
                return result;
            }
            ctx->time_used.fetch = get_current_time_ms() - start_time;

            /*
               logInfo("data group id: %d, last_data_version: %"PRId64", "
               "binlog_size: %"PRId64", time used: %"PRId64" ms",
               ctx->ds->dg->id, ctx->fetch.last_data_version,
               binlog_size, ctx->time_used.fetch);
             */

            if (ctx->is_restore || binlog_size == 0) {
                if (ctx->is_restore) {
                    ctx->stage = DATA_RECOVERY_STAGE_RESTORE;
                } else if (ctx->is_full_dump) {
                    ctx->stage = DATA_RECOVERY_STAGE_SYNC;
                } else {
                    ctx->stage = DATA_RECOVERY_STAGE_CLEANUP;
                }

                if ((result=data_recovery_save_sys_data(ctx)) != 0) {
                    return result;
                }
                break;
            }

            ctx->stage = DATA_RECOVERY_STAGE_DEDUP;
            if ((result=data_recovery_save_sys_data(ctx)) != 0) {
                return result;
            }
        case DATA_RECOVERY_STAGE_DEDUP:
            dedup_start_time = get_current_time_ms();
            if ((result=data_recovery_dedup_binlog(ctx, &binlog_count)) != 0) {
                return result;
            }
            ctx->time_used.dedup = get_current_time_ms() - dedup_start_time;

            if (binlog_count == 0) {  //no binlog to replay
                if (ctx->is_full_dump) {
                    ctx->stage = DATA_RECOVERY_STAGE_SYNC;
                } else {
                    ctx->stage = DATA_RECOVERY_STAGE_LOG;
                }
            } else {
                if (ctx->is_full_dump) {
                    ctx->stage = DATA_RECOVERY_STAGE_REPLAY;
                    ctx->next_stage = DATA_RECOVERY_STAGE_SYNC;
                } else {
                    ctx->stage = DATA_RECOVERY_STAGE_REPLAY;
                    ctx->next_stage = DATA_RECOVERY_STAGE_LOG;
                }
            }
            if ((result=data_recovery_save_sys_data(ctx)) != 0) {
                return result;
            }
            break;
        default:
            break;
    }

    old_data_version = FC_ATOMIC_GET(ctx->ds->data.current_version);
    if (ctx->stage == DATA_RECOVERY_STAGE_REPLAY) {
        if ((result=data_recovery_replay_binlog(ctx)) != 0) {
            return result;
        }

        ctx->stage = ctx->next_stage;
        ctx->next_stage = DATA_RECOVERY_STAGE_NONE;
        if ((result=data_recovery_save_sys_data(ctx)) != 0) {
            return result;
        }
    }

    if (ctx->stage == DATA_RECOVERY_STAGE_SYNC ||
            ctx->stage == DATA_RECOVERY_STAGE_LOG)
    {
        if (ctx->stage == DATA_RECOVERY_STAGE_SYNC) {
            if ((result=data_recovery_sync_binlog(ctx)) != 0) {
                return result;
            }
        } else {
            if ((result=data_recovery_log_to_replica(ctx,
                            old_data_version)) != 0)
            {
                return result;
            }
        }

        if ((result=replica_binlog_set_data_version(ctx->ds,
                        ctx->fetch.last_data_version)) != 0)
        {
            return result;
        }

        ctx->stage = DATA_RECOVERY_STAGE_CLEANUP;
        if ((result=data_recovery_save_sys_data(ctx)) != 0) {
            return result;
        }
    } else if (ctx->stage == DATA_RECOVERY_STAGE_RESTORE) {
        bool is_redo;
        if (ctx->restore.start_dv == 0) {
            find_restore_start_dv(ctx);
            is_redo = false;
        } else {
            is_redo = true;
        }

        if (ctx->restore.start_dv > 0) {
            int64_t confirmed_version;

            confirmed_version = ctx->restore.start_dv - 1;
            binlog_rollback(ctx->ds, confirmed_version,
                    is_redo, FS_WHICH_SIDE_SLAVE);
        }

        ctx->stage = DATA_RECOVERY_STAGE_CLEANUP;
        if ((result=data_recovery_save_sys_data(ctx)) != 0) {
            return result;
        }
    }

    if (ctx->stage == DATA_RECOVERY_STAGE_CLEANUP) {
        if ((result=data_recovery_unlink_fetched_binlog(ctx)) != 0) {
            return result;
        }
        if ((result=data_recovery_unlink_replay_binlog(ctx)) != 0) {
            return result;
        }
        if ((result=data_recovery_unlink_sys_data(ctx)) != 0) {
            return result;
        }
    } else {
        logError("file: "__FILE__", line: %d, "
                "invalid stage value: 0x%02x",
                __LINE__, ctx->stage);
        return EINVAL;
    }

    if (!SF_G_CONTINUE_FLAG) {
        return EINTR;
    }

    if (ctx->is_online) {
        if ((result=active_me(ctx)) != 0) {
            return result;
        }
    } else {
        if (ctx->catch_up == DATA_RECOVERY_CATCH_UP_DOING) {
            if (get_current_time_ms() - start_time < 1000) {
                ctx->catch_up = DATA_RECOVERY_CATCH_UP_LAST_BATCH;
            }
        }
    }

    return 0;
}

static int data_recovery_waiting_rpc_done(FSClusterDataServerInfo *ds)
{
#define MAX_WAITING_COUNT  50
    uint64_t rpc_last_version;
    uint64_t current_version;
    int result;
    int count;
    int log_level;
    char prompt[256];

    rpc_last_version = FC_ATOMIC_GET(ds->replica.rpc_last_version);
    if (rpc_last_version == 0) {
        return 0;
    }

    count = 0;
    while ((current_version=FC_ATOMIC_GET(ds->data.current_version)) <
            rpc_last_version && count++ < MAX_WAITING_COUNT)
    {
        fc_sleep_ms(100);
    }

    if (count < MAX_WAITING_COUNT) {
        result = 0;
        if (count < 5) {
            log_level = LOG_DEBUG;
        } else if (count < 10) {
            log_level = LOG_INFO;
        } else {
            log_level = LOG_WARNING;
        }
        sprintf(prompt, "time count: %d, current data version: %"PRId64,
                count, current_version);
    } else {
        result = ETIMEDOUT;
        log_level = LOG_ERR;
        sprintf(prompt, "timeout, rpc last version: %"PRId64", "
                "current data version: %"PRId64, rpc_last_version,
                current_version);
    }

    log_it_ex(&g_log_context, log_level, "file: "__FILE__", line: %d, "
            "data group id: %d, waiting rpc deal done %s", __LINE__,
            ds->dg->id, prompt);
    return result;
}

int data_recovery_start(FSClusterDataServerInfo *ds)
{
    DataRecoveryContext ctx;
    int result;

    data_recovery_waiting_rpc_done(ds);

    memset(&ctx, 0, sizeof(ctx));
    if ((result=init_data_recovery_ctx(&ctx, ds)) != 0) {
        return result;
    }

    FC_ATOMIC_SET(ds->recovery.until_version, 0);
    ctx.catch_up = DATA_RECOVERY_CATCH_UP_DOING;
    do {
        if ((ctx.master=data_recovery_get_master(&ctx, &result)) == NULL) {
            break;
        }

        ctx.loop_count++;
        if ((result=do_data_recovery(&ctx)) != 0) {
            break;
        }

        /*
        logInfo("data group id: %d, stage: %d, catch_up: %d, "
                "is_online: %d, last_data_version: %"PRId64,
                ds->dg->id, ctx.stage, ctx.catch_up, ctx.is_online,
                ctx.fetch.last_data_version);
                */

        ctx.stage = DATA_RECOVERY_STAGE_FETCH;
    } while (!ctx.is_online);

    if (result == 0) {
        FC_ATOMIC_SET(ds->replica.rpc_last_version,
                ctx.fetch.last_data_version);
    }

    destroy_data_recovery_ctx(&ctx);
    return result;
}
