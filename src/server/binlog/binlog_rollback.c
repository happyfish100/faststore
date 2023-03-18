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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include "fastcommon/logger.h"
#include "fastcommon/shared_func.h"
#include "sf/sf_global.h"
#include "../../common/fs_func.h"
#include "../../client/fs_client.h"
#include "../server_global.h"
#include "../data_thread.h"
#include "binlog_func.h"
#include "binlog_reader.h"
#include "slice_binlog.h"
#include "replica_binlog.h"
#include "binlog_rollback.h"

typedef struct {
    FSSliceOpContext op_ctx;
    pthread_lock_cond_pair_t lcp;  //for notify
    struct {
        bool done;
    } notify;
} DataRollbackContext;

static int compare_version_and_source(const BinlogCommonFields *p1,
        const BinlogCommonFields *p2)
{
    int sub;
    if ((sub=fc_compare_int64(p1->data_version, p2->data_version)) != 0) {
        return sub;
    }

    return (int)p1->source - (int)p2->source;
}

static int load_slice_binlogs(const int data_group_id,
        const int64_t my_confirmed_version,
        BinlogCommonFieldsArray *array)
{
    int result;

    if ((result=slice_binlog_load_records(data_group_id,
                    my_confirmed_version, array)) != 0)
    {
        return result;
    }

    if (array->count > 1) {
        qsort(array->records, array->count,
                sizeof(BinlogCommonFields),
                (int (*)(const void *, const void *))
                compare_version_and_source);
    }

    return 0;
}

static int get_master(FSClusterDataServerInfo *myself,
        FSClusterDataServerInfo **master)
{
    int64_t start_time_ms;
    int64_t time_used;
    char time_buff[32];

    if ((*master =(FSClusterDataServerInfo *)FC_ATOMIC_GET(
                    myself->dg->master)) != NULL)
    {
        return 0;
    }

    start_time_ms = get_current_time_ms();
    logInfo("file: "__FILE__", line: %d, "
            "data group id: %d, try to get master ...",
            __LINE__, myself->dg->id);
    do {
        fc_sleep_ms(100);
        *master = (FSClusterDataServerInfo *)
            FC_ATOMIC_GET(myself->dg->master);
        if (*master != NULL) {
            time_used = get_current_time_ms() - start_time_ms;
            long_to_comma_str(time_used, time_buff);
            logInfo("file: "__FILE__", line: %d, "
                    "data group id: %d, get master done, master id: %d, "
                    "time used: %s ms", __LINE__, myself->dg->id,
                    (*master)->cs->server->id, time_buff);
            return 0;
        }
    } while (SF_G_CONTINUE_FLAG);

    return EINTR;
}

static void slice_write_done_notify(FSDataOperation *op)
{
    DataRollbackContext *rollback_ctx;

    rollback_ctx = (DataRollbackContext *)op->arg;
    PTHREAD_MUTEX_LOCK(&rollback_ctx->lcp.lock);
    rollback_ctx->notify.done = true;
    pthread_cond_signal(&rollback_ctx->lcp.cond);
    PTHREAD_MUTEX_UNLOCK(&rollback_ctx->lcp.lock);
}

static int init_rollback_context(DataRollbackContext *rollback_ctx,
        FSClusterDataServerInfo *myself)
{
    int result;

    memset(rollback_ctx, 0, sizeof(*rollback_ctx));
    rollback_ctx->op_ctx.info.buff = fc_malloc(FS_FILE_BLOCK_SIZE);
    if (rollback_ctx->op_ctx.info.buff == NULL) {
        return ENOMEM;
    }

    if ((result=fs_slice_array_init(&rollback_ctx->
                    op_ctx.update.sarray)) != 0)
    {
        return result;
    }

    if ((result=init_pthread_lock_cond_pair(&rollback_ctx->lcp)) != 0) {
        return result;
    }

    rollback_ctx->op_ctx.notify_func = slice_write_done_notify;
    rollback_ctx->op_ctx.info.is_update = true;
    rollback_ctx->op_ctx.info.source = BINLOG_SOURCE_RESTORE;
    rollback_ctx->op_ctx.info.write_binlog.log_replica = false;
    rollback_ctx->op_ctx.info.myself = myself;
    rollback_ctx->op_ctx.info.data_group_id = myself->dg->id;
    return 0;
}

static void destroy_rollback_context(DataRollbackContext *rollback_ctx)
{
    if (rollback_ctx->op_ctx.info.buff != NULL) {
        free(rollback_ctx->op_ctx.info.buff);
        rollback_ctx->op_ctx.info.buff = NULL;
    }

    fs_slice_array_destroy(&rollback_ctx->op_ctx.update.sarray);
    destroy_pthread_lock_cond_pair(&rollback_ctx->lcp);
}

static int fetch_data(DataRollbackContext *rollback_ctx)
{
    FSClusterDataServerInfo *master;
    int read_bytes;
    int result;

    result = EINTR;
    while (SF_G_CONTINUE_FLAG) {
        if ((result=get_master(rollback_ctx->op_ctx.
                        info.myself, &master)) != 0)
        {
            break;
        }

        if (master == rollback_ctx->op_ctx.info.myself) {
            result = EEXIST;
            break;
        }

        if ((result=fs_client_slice_read_by_slave(&g_fs_client_vars.client_ctx,
                        CLUSTER_MY_SERVER_ID, &rollback_ctx->op_ctx.info.bs_key,
                        rollback_ctx->op_ctx.info.buff, &read_bytes)) == 0)
        {
            if (read_bytes != rollback_ctx->op_ctx.info.bs_key.slice.length) {
                logDebug("file: "__FILE__", line: %d, "
                        "data group id: %d, block {oid: %"PRId64", "
                        "offset: %"PRId64"}, slice {offset: %d, length: %d}, "
                        "read bytes: %d != slice length, "
                        "maybe delete later?", __LINE__,
                        rollback_ctx->op_ctx.info.myself->dg->id,
                        rollback_ctx->op_ctx.info.bs_key.block.oid,
                        rollback_ctx->op_ctx.info.bs_key.block.offset,
                        rollback_ctx->op_ctx.info.bs_key.slice.offset,
                        rollback_ctx->op_ctx.info.bs_key.slice.length,
                        read_bytes);
                rollback_ctx->op_ctx.info.bs_key.slice.length = read_bytes;
            }
            break;
        } else if (result == ENODATA) {
            logDebug("file: "__FILE__", line: %d, "
                    "data group id: %d, block {oid: %"PRId64", "
                    "offset: %"PRId64"}, slice {offset: %d, "
                    "length: %d}, slice not exist, "
                    "maybe delete later?", __LINE__,
                    rollback_ctx->op_ctx.info.myself->dg->id,
                    rollback_ctx->op_ctx.info.bs_key.block.oid,
                    rollback_ctx->op_ctx.info.bs_key.block.offset,
                    rollback_ctx->op_ctx.info.bs_key.slice.offset,
                    rollback_ctx->op_ctx.info.bs_key.slice.length);
            break;
        } else {
            logError("file: "__FILE__", line: %d, "
                    "data group id: %d, block {oid: %"PRId64", "
                    "offset: %"PRId64"}, slice {offset: %d, length: %d}, "
                    "fetch data fail, errno: %d, error info: %s", __LINE__,
                    rollback_ctx->op_ctx.info.myself->dg->id,
                    rollback_ctx->op_ctx.info.bs_key.block.oid,
                    rollback_ctx->op_ctx.info.bs_key.block.offset,
                    rollback_ctx->op_ctx.info.bs_key.slice.offset,
                    rollback_ctx->op_ctx.info.bs_key.slice.length,
                    result, STRERROR(result));
            sleep(1);
        }
    }

    return result;
}

static int write_data(DataRollbackContext *rollback_ctx)
{
    int result;

    if ((result=push_to_data_thread_queue(DATA_OPERATION_SLICE_WRITE,
                    BINLOG_SOURCE_ROLLBACK, rollback_ctx,
                    &rollback_ctx->op_ctx)) == 0)
    {
        PTHREAD_MUTEX_LOCK(&rollback_ctx->lcp.lock);
        while (!rollback_ctx->notify.done) {
            pthread_cond_wait(&rollback_ctx->lcp.cond,
                    &rollback_ctx->lcp.lock);
        }
        rollback_ctx->notify.done = false;  /* reset for next */
        PTHREAD_MUTEX_UNLOCK(&rollback_ctx->lcp.lock);
        result = rollback_ctx->op_ctx.result;
    }

    if (result != 0) {
        logError("file: "__FILE__", line: %d, "
                "data group id: %d, write fail, "
                "block {oid: %"PRId64", offset: %"PRId64"}, "
                "slice {offset: %d, length: %d}, "
                "errno: %d, error info: %s",
                __LINE__, rollback_ctx->op_ctx.info.myself->dg->id,
                rollback_ctx->op_ctx.info.bs_key.block.oid,
                rollback_ctx->op_ctx.info.bs_key.block.offset,
                rollback_ctx->op_ctx.info.bs_key.slice.offset,
                rollback_ctx->op_ctx.info.bs_key.slice.length,
                result, STRERROR(result));
    }

    return result;
}

static int do_rollback(DataRollbackContext *rollback_ctx,
        ReplicaBinlogRecord *record, bool *data_restored)
{
    int result;
    int r;
    int dec_alloc;
    uint64_t sn = 0;
    struct fc_queue_info space_chain;

    fs_calc_block_hashcode(&record->bs_key.block);
    rollback_ctx->op_ctx.info.bs_key.block = record->bs_key.block;
    if (record->op_type == BINLOG_OP_TYPE_DEL_BLOCK) {
        rollback_ctx->op_ctx.info.bs_key.slice.offset = 0;
        rollback_ctx->op_ctx.info.bs_key.slice.length = FS_FILE_BLOCK_SIZE;
    } else {
        rollback_ctx->op_ctx.info.bs_key.slice = record->bs_key.slice;
    }

    *data_restored = false;
    result = fetch_data(rollback_ctx);
    if (result == EEXIST) {
        return 0;
    } else if (!(result == 0 || result == ENODATA)) {
        return result;
    }

    if (record->op_type == BINLOG_OP_TYPE_WRITE_SLICE ||
            record->op_type == BINLOG_OP_TYPE_ALLOC_SLICE)
    {
        space_chain.head = space_chain.tail = NULL;
        if ((r=ob_index_delete_slice(&record->bs_key, &sn,
                        &dec_alloc, &space_chain)) == 0)
        {
            //TODO
            r = slice_binlog_log_del_slice(&record->bs_key,
                    g_current_time, sn, record->data_version,
                    BINLOG_SOURCE_ROLLBACK);
        } else if (r == ENOENT) {
            r = 0;
        }
    } else {
        r = slice_binlog_log_no_op(&record->bs_key.block, g_current_time,
                ob_index_generate_alone_sn(), record->data_version,
                BINLOG_SOURCE_ROLLBACK);
    }
    if (r != 0) {
        return r;
    }

    if (result == ENODATA) {
        return 0;
    } else {
        *data_restored = true;
        rollback_ctx->op_ctx.info.data_version = record->data_version;
        rollback_ctx->op_ctx.update.timestamp = g_current_time;
        return write_data(rollback_ctx);
    }
}

static int rollback_data(FSClusterDataServerInfo *myself,
        const uint64_t last_data_version,
        const ReplicaBinlogRecordArray *replica_array,
        const BinlogCommonFieldsArray *slice_array,
        const bool is_redo, const char which_side)
{
    int result;
    bool data_restored;
    int skip_count;
    int restore_count;
    DataRollbackContext rollback_ctx;
    ReplicaBinlogRecord *record;
    ReplicaBinlogRecord *end;
    BinlogCommonFields target;
    int64_t start_time_ms;
    int64_t time_used;
    char time_buff[32];
    char slice_prompt[64];

    start_time_ms = get_current_time_ms();
    if (replica_array->count == 0) {
        return 0;
    }

    if ((result=init_rollback_context(&rollback_ctx, myself)) != 0) {
        return result;
    }

    skip_count = 0;
    restore_count = 0;
    end = replica_array->records + replica_array->count;
    for (record=replica_array->records; record<end; record++) {
        if (record->op_type == BINLOG_OP_TYPE_NO_OP) {
            ++skip_count;
            continue;
        }

        if (is_redo) {
            target.data_version = record->data_version;
            target.source = BINLOG_SOURCE_ROLLBACK;
            if (bsearch(&target, slice_array->records, slice_array->count,
                        sizeof(target), (int (*)(const void *, const void *))
                        compare_version_and_source) != NULL)
            {
                ++skip_count;
                continue;
            }

            target.source = BINLOG_SOURCE_RESTORE;
            if (bsearch(&target, slice_array->records, slice_array->count,
                        sizeof(target), (int (*)(const void *, const void *))
                        compare_version_and_source) != NULL)
            {
                ++skip_count;
                continue;
            }
        }

        if ((result=do_rollback(&rollback_ctx, record,
                        &data_restored)) != 0)
        {
            break;
        }

        if (data_restored) {
            ++restore_count;
        }
    }

    time_used = get_current_time_ms() - start_time_ms;
    long_to_comma_str(time_used, time_buff);
    if (is_redo) {
        sprintf(slice_prompt, ", slice record count: %d", slice_array->count);
    } else {
        *slice_prompt = '\0';
    }
    logInfo("file: "__FILE__", line: %d, "
            "data group id: %d, %s rollback data version from %"PRId64" "
            "to %"PRId64", replica record count: %d%s, skip count: %d, "
            "restore count: %d, time used: %s ms", __LINE__, myself->dg->id,
            which_side == FS_WHICH_SIDE_MASTER ?  "master" : "slave",
            FC_ATOMIC_GET(myself->data.current_version), last_data_version,
            replica_array->count, slice_prompt, skip_count, restore_count,
            time_buff);

    destroy_rollback_context(&rollback_ctx);
    return result;
}

static int rollback_slice_binlog_and_data(FSClusterDataServerInfo *myself,
        const uint64_t last_data_version, const bool is_redo,
        const char which_side)
{
    int result;
    ReplicaBinlogRecordArray replica_array;
    BinlogCommonFieldsArray slice_array;

    replica_binlog_init_record_array(&replica_array);
    slice_binlog_init_record_array(&slice_array);

    if ((result=replica_binlog_load_records(myself->dg->id,
                    last_data_version, &replica_array)) != 0)
    {
        return result;
    }

    if (is_redo) {
        result = load_slice_binlogs(myself->dg->id,
                last_data_version, &slice_array);
        if (result != 0) {
            return result;
        }
    }

    result = rollback_data(myself, last_data_version, &replica_array,
            &slice_array, is_redo, which_side);

    replica_binlog_free_record_array(&replica_array);
    slice_binlog_free_record_array(&slice_array);
    return result;
}

int binlog_rollback(FSClusterDataServerInfo *myself, const uint64_t
        my_confirmed_version, const bool is_redo, const char which_side)
{
    const bool ignore_dv_overflow = false;
    int result;
    int start_index;
    int last_index;
    int binlog_index;
    uint64_t last_data_version;
    SFBinlogFilePosition position;
    char filename[PATH_MAX];

    if (!is_redo) {
        if ((result=replica_binlog_waiting_write_done(
                        myself->dg->id, FC_ATOMIC_GET(myself->data.
                            current_version), "replica")) != 0)
        {
            return result;
        }
    }

    if ((result=replica_binlog_get_last_dv(myself->dg->id,
                    &last_data_version)) != 0)
    {
        return result;
    }

    if (my_confirmed_version >= last_data_version) {
        return 0;
    }

    if ((result=replica_binlog_get_binlog_indexes(myself->dg->id,
                    &start_index, &last_index)) != 0)
    {
        return result;
    }

    if ((result=replica_binlog_get_position_by_dv(myself->dg->id,
                    my_confirmed_version, &position,
                    ignore_dv_overflow)) != 0)
    {
        return result;
    }

    result = rollback_slice_binlog_and_data(myself,
            my_confirmed_version, is_redo, which_side);
    if (result != 0) {
        return result;
    }

    if (position.index < last_index) {
        if ((result=replica_binlog_set_binlog_indexes(myself->dg->id,
                        start_index, position.index)) != 0)
        {
            return result;
        }

        for (binlog_index=position.index+1;
                binlog_index<=last_index;
                binlog_index++)
        {
            replica_binlog_get_filename(myself->dg->id, binlog_index,
                    filename, sizeof(filename));
            if ((result=fc_delete_file_ex(filename, "binlog")) != 0) {
                return result;
            }
        }
    }

    replica_binlog_get_filename(myself->dg->id, position.index,
            filename, sizeof(filename));
    if (truncate(filename, position.offset) != 0) {
        result = (errno != 0 ? errno : EPERM);
        logError("file: "__FILE__", line: %d, "
                "truncate file %s to length: %"PRId64" fail, "
                "errno: %d, error info: %s", __LINE__, filename,
                position.offset, result, STRERROR(result));
        return result;
    }

    if ((result=replica_binlog_get_last_dv(myself->dg->id,
                    &last_data_version)) != 0)
    {
        return result;
    }
    if (last_data_version != my_confirmed_version) {
        logError("file: "__FILE__", line: %d, "
                "binlog last_data_version: %"PRId64" != "
                "confirmed data version: %"PRId64", program exit!",
                __LINE__, last_data_version, my_confirmed_version);
        return EBUSY;
    }

    if ((result=replica_binlog_writer_change_write_index(
                    myself->dg->id, position.index)) != 0)
    {
        return result;
    }

    if ((result=replica_binlog_set_data_version(myself,
                    my_confirmed_version)) != 0)
    {
        return result;
    }

    return 0;
}
