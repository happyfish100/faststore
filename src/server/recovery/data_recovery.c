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
#include "sf/sf_global.h"
#include "sf/sf_service.h"
#include "../../common/fs_proto.h"
#include "../server_global.h"
#include "../server_group_info.h"
#include "../cluster_relationship.h"
#include "../server_binlog.h"
#include "../server_replication.h"
#include "binlog_fetch.h"
#include "binlog_dedup.h"
#include "binlog_replay.h"
#include "data_recovery.h"

#define DATA_RECOVERY_SYS_DATA_FILENAME       "data_recovery.dat"
#define DATA_RECOVERY_SYS_DATA_SECTION_FETCH  "fetch"
#define DATA_RECOVERY_SYS_DATA_ITEM_STAGE     "stage"
#define DATA_RECOVERY_SYS_DATA_ITEM_LAST_DV   "last_data_version"
#define DATA_RECOVERY_SYS_DATA_ITEM_LAST_BKEY "last_bkey"

#define DATA_RECOVERY_STAGE_FETCH   'F'
#define DATA_RECOVERY_STAGE_DEDUP   'D'
#define DATA_RECOVERY_STAGE_REPLAY  'R'

int data_recovery_init()
{
    int result;

    if ((result=binlog_replay_init()) != 0) {
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

FSClusterDataServerInfo *data_recovery_get_master(
        DataRecoveryContext *ctx, int *err_no)
{
    FSClusterDataGroupInfo *group;
    FSClusterDataServerInfo *master;

    if ((group=fs_get_data_group(ctx->ds->dg->id)) == NULL) {
        *err_no = ENOENT;
        return NULL;
    }
    master = (FSClusterDataServerInfo *)
        __sync_fetch_and_add(&group->master, 0);
    if (master == NULL) {
        logError("file: "__FILE__", line: %d, "
                "data group id: %d, no master",
                __LINE__, ctx->ds->dg->id);
        *err_no = ENOENT;
        return NULL;
    }

    if (group->myself == NULL) {
        logError("file: "__FILE__", line: %d, "
                "data group id: %d NOT belongs to me",
                __LINE__, ctx->ds->dg->id);
        *err_no = ENOENT;
        return NULL;
    }

    if (group->myself == master) {
        logError("file: "__FILE__", line: %d, "
                "data group id: %d, i am already master, "
                "do NOT need recovery!", __LINE__, ctx->ds->dg->id);
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
            "[%s]\n"
            "%s=%"PRId64"\n"
            "%s=%"PRId64",%"PRId64"\n",
            DATA_RECOVERY_SYS_DATA_ITEM_STAGE, ctx->stage,
            DATA_RECOVERY_SYS_DATA_SECTION_FETCH,
            DATA_RECOVERY_SYS_DATA_ITEM_LAST_DV, ctx->fetch.last_data_version,
            DATA_RECOVERY_SYS_DATA_ITEM_LAST_BKEY, ctx->fetch.last_bkey.oid,
            ctx->fetch.last_bkey.offset);

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
    char *stage;
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
        return data_recovery_save_sys_data(ctx);
    }

    if ((result=iniLoadFromFile(filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load conf file \"%s\" fail, ret code: %d",
                __LINE__, filename, result);
        return result;
    }

    stage = iniGetStrValue(NULL, DATA_RECOVERY_SYS_DATA_ITEM_STAGE,
            &ini_context);
    if (stage == NULL || *stage == '\0') {
        ctx->stage = DATA_RECOVERY_STAGE_FETCH;
    } else {
        ctx->stage = stage[0];
    }

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
    ctx->start_time = get_current_time_ms();

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
    return data_recovery_load_sys_data(ctx);
}

static void destroy_data_recovery_ctx(DataRecoveryContext *ctx)
{
}

static int waiting_binlog_write_done(DataRecoveryContext *ctx,
        const uint64_t waiting_data_version)
{
#define MAX_WAITING_COUNT  50
    int result;
    int r;
    int count;
    int log_level;
    uint64_t data_version;
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];
    char filename[PATH_MAX];
    char prompt[64];

    result = ETIMEDOUT;
    replica_binlog_get_subdir_name(subdir_name, ctx->ds->dg->id);
    for (count=0; count<MAX_WAITING_COUNT && SF_G_CONTINUE_FLAG; count++) {
        sf_binlog_writer_get_filename(subdir_name,
                replica_binlog_get_current_write_index(
                    ctx->ds->dg->id), filename, sizeof(filename));
        if ((r=replica_binlog_get_last_data_version(
                        filename, &data_version)) != 0)
        {
            result = r;
            break;
        }

        if (data_version >= waiting_data_version) {
            result = 0;
            break;
        }
        fc_sleep_ms(100);
    }

    if (result == 0) {
        if (count < 3) {
            log_level = LOG_DEBUG;
        } else if (count < 10) {
            log_level = LOG_INFO;
        } else {
            log_level = LOG_WARNING;
        }
        sprintf(prompt, "time count: %d", count);
    } else {
        log_level = LOG_ERR;
        if (result == ETIMEDOUT) {
            sprintf(prompt, "timeout");
        } else {
            sprintf(prompt, "fail, result: %d", result);
        }
    }

    log_it_ex(&g_log_context, log_level, "file: "__FILE__", line: %d, "
            "data group id: %d, waiting data version: %"PRId64", "
            "waiting binlog write done %s", __LINE__, ctx->ds->dg->id,
            waiting_data_version, prompt);
    return result;
}

static int waiting_replay_binlog_write_done(DataRecoveryContext *ctx)
{
    int result;
    uint64_t last_data_version;
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];
    char filename[PATH_MAX];

    data_recovery_get_subdir_name(ctx, RECOVERY_BINLOG_SUBDIR_NAME_REPLAY,
            subdir_name);
    binlog_reader_get_filename(subdir_name, 0, filename, sizeof(filename));
    if ((result=replica_binlog_get_last_data_version(
                    filename, &last_data_version)) != 0)
    {
        return result;
    }

    return waiting_binlog_write_done(ctx, last_data_version);
}

static int replica_binlog_log_padding(DataRecoveryContext *ctx)
{
    int result;
    uint64_t current_version;

    current_version = __sync_fetch_and_add(&ctx->ds->data.version, 0);
    if (ctx->fetch.last_data_version > current_version) {
        replica_binlog_set_data_version(ctx->ds,
                ctx->fetch.last_data_version - 1);
        if ((result=replica_binlog_log_no_op(ctx->ds->dg->id,
                        ctx->fetch.last_data_version,
                        &ctx->fetch.last_bkey)) == 0)
        {
            __sync_fetch_and_add(&ctx->ds->data.version, 1);
            result = waiting_binlog_write_done(ctx,
                    ctx->fetch.last_data_version);
        }
    } else {
        result = 0;
    }

    return result;
}

static int proto_active_confirm(ConnectionInfo *conn,
        DataRecoveryContext *ctx)
{
    int result;
    SFResponseInfo response;
    FSProtoReplicaActiveConfirmReq *req;
    char out_buff[sizeof(FSProtoHeader) + sizeof(
            FSProtoReplicaActiveConfirmReq)];

    req = (FSProtoReplicaActiveConfirmReq *)
        (out_buff + sizeof(FSProtoHeader));
    int2buff(ctx->ds->dg->id, req->data_group_id);
    int2buff(CLUSTER_MYSELF_PTR->server->id, req->server_id);
    SF_PROTO_SET_HEADER((FSProtoHeader *)out_buff,
            FS_REPLICA_PROTO_ACTIVE_CONFIRM_REQ,
            sizeof(FSProtoReplicaActiveConfirmReq));

    response.error.length = 0;
    if ((result=sf_send_and_recv_none_body_response(conn, out_buff,
                    sizeof(out_buff), &response, SF_G_NETWORK_TIMEOUT,
                    FS_REPLICA_PROTO_ACTIVE_CONFIRM_RESP)) != 0)
    {
        sf_log_network_error(&response, conn, result);
    }

    return result;
}

static int active_confirm(DataRecoveryContext *ctx)
{
    int result;
    ConnectionInfo conn;

    if ((result=fc_server_make_connection_ex(&REPLICA_GROUP_ADDRESS_ARRAY(
                        ctx->master->cs->server), &conn,
                    SF_G_CONNECT_TIMEOUT, NULL, true)) != 0)
    {
        return result;
    }

    result = proto_active_confirm(&conn, ctx);
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
                FS_SERVER_STATUS_ONLINE, FS_SERVER_STATUS_ACTIVE,
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

static int do_data_recovery(DataRecoveryContext *ctx)
{
    int result;
    int64_t binlog_count;
    int64_t binlog_size;
    int64_t start_time;

    start_time = 0;
    binlog_count = 0;
    result = 0;
    switch (ctx->stage) {
        case DATA_RECOVERY_STAGE_FETCH:
            start_time = get_current_time_ms();
            if ((result=data_recovery_fetch_binlog(ctx, &binlog_size)) != 0) {
                break;
            }

            /*
            logInfo("data group id: %d, last_data_version: %"PRId64", "
                    "binlog_size: %"PRId64, ctx->ds->dg->id,
                    ctx->fetch.last_data_version, binlog_size);
                    */

            if (binlog_size == 0) {
                break;
            }

            ctx->stage = DATA_RECOVERY_STAGE_DEDUP;
            if ((result=data_recovery_save_sys_data(ctx)) != 0) {
                break;
            }
        case DATA_RECOVERY_STAGE_DEDUP:
            if ((result=data_recovery_dedup_binlog(ctx, &binlog_count)) != 0) {
                break;
            }

            if (binlog_count == 0) {  //no binlog to replay
                result = replica_binlog_log_padding(ctx);
                break;
            }

            ctx->stage = DATA_RECOVERY_STAGE_REPLAY;
            if ((result=data_recovery_save_sys_data(ctx)) != 0) {
                break;
            }
        case DATA_RECOVERY_STAGE_REPLAY:
            if ((result=data_recovery_replay_binlog(ctx)) != 0) {
                break;
            }
            if ((result=waiting_replay_binlog_write_done(ctx)) == 0) {
                result = replica_binlog_log_padding(ctx);
            }
            break;
        default:
            logError("file: "__FILE__", line: %d, "
                    "invalid stage value: 0x%02x",
                    __LINE__, ctx->stage);
            result = EINVAL;
            break;
    }

    if (result != 0) {
        return result;
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

    rpc_last_version = __sync_fetch_and_add(
            &ds->replica.rpc_last_version, 0);
    if (rpc_last_version == 0) {
        return 0;
    }

    count = 0;
    while ((current_version=__sync_fetch_and_add(&ds->data.version, 0)) <
            rpc_last_version && ++count < MAX_WAITING_COUNT)
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

    if ((result=data_recovery_waiting_rpc_done(ds)) != 0) {
        return result;
    }

    memset(&ctx, 0, sizeof(ctx));
    if ((result=init_data_recovery_ctx(&ctx, ds)) != 0) {
        return result;
    }

    ctx.catch_up = DATA_RECOVERY_CATCH_UP_DOING;
    do {
        if ((ctx.master=data_recovery_get_master(&ctx, &result)) == NULL) {
            break;
        }

        if ((result=do_data_recovery(&ctx)) != 0) {
            break;
        }

        /*
        logInfo("======= data group id: %d, stage: %d, catch_up: %d, "
                "is_online: %d, last_data_version: %"PRId64,
                ds->dg->id, ctx.stage, ctx.catch_up, ctx.is_online,
                ctx.fetch.last_data_version);
                */

        if ((result=data_recovery_unlink_sys_data(&ctx)) != 0) {
            break;
        }
        if ((result=data_recovery_unlink_replay_binlog(&ctx)) != 0) {
            break;
        }
        if ((result=data_recovery_unlink_fetched_binlog(&ctx)) != 0) {
            break;
        }

        ctx.stage = DATA_RECOVERY_STAGE_FETCH;
    } while (!ctx.is_online);

    destroy_data_recovery_ctx(&ctx);
    return result;
}
