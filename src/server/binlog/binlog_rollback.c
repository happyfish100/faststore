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
#include "../server_global.h"
#include "binlog_func.h"
#include "binlog_reader.h"
#include "slice_binlog.h"
#include "replica_binlog.h"
#include "binlog_rollback.h"

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

    start_time_ms = get_current_time_ms();
    logInfo("file: "__FILE__", line: %d, "
            "data group id: %d, try to get master ...",
            __LINE__, myself->dg->id);
    do {
        *master = (FSClusterDataServerInfo *)
            FC_ATOMIC_GET(myself->dg->master);
        if (*master != NULL) {
            time_used = get_current_time_ms() - start_time_ms;
            long_to_comma_str(time_used, time_buff);
            logInfo("file: "__FILE__", line: %d, "
                    "data group id: %d, get master done, time used: %s ms",
                    __LINE__, myself->dg->id, time_buff);
            return 0;
        }
        fc_sleep_ms(100);
    } while (SF_G_CONTINUE_FLAG);

    return EINTR;
}

static int rollback_data(FSClusterDataServerInfo *myself,
        const ReplicaBinlogRecordArray *replica_array,
        const BinlogCommonFieldsArray *slice_array,
        const bool is_redo)
{
    FSClusterDataServerInfo *master;
    ReplicaBinlogRecord *record;
    ReplicaBinlogRecord *end;
    BinlogCommonFields target;
    char buff[256];
    int len;
    int result;

    if ((result=get_master(myself, &master)) != 0) {
        return result;
    }

    end = replica_array->records + replica_array->count;
    for (record=replica_array->records; record<end; record++) {
        if (record->op_type == BINLOG_OP_TYPE_NO_OP) {
            continue;
        }

        if (is_redo) {
            target.data_version = record->data_version;
            target.source = BINLOG_SOURCE_ROLLBACK;
            if (bsearch(&target, slice_array->records, slice_array->count,
                        sizeof(target), (int (*)(const void *, const void *))
                        compare_version_and_source) != NULL)
            {
                continue;
            }
        }

        if (record->op_type == BINLOG_OP_TYPE_WRITE_SLICE ||
                record->op_type == BINLOG_OP_TYPE_ALLOC_SLICE)
        {
        }

        if (record->op_type == BINLOG_OP_TYPE_DEL_BLOCK) {
            len = replica_binlog_log_block_to_buff(record->timestamp,
                    record->data_version, &record->bs_key.block,
                    record->source, record->op_type, buff);
        } else {
            len = replica_binlog_log_slice_to_buff(record->timestamp,
                    record->data_version, &record->bs_key, record->source,
                    record->op_type, buff);
        }

        logInfo("%.*s", len, buff);
    }

    return 0;
}

int rollback_slice_binlog_and_data(FSClusterDataServerInfo *myself,
        const uint64_t last_data_version, const bool is_redo)
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

    logInfo("data_group_id: %d, last_data_version: %"PRId64", replica record count: %d",
            myself->dg->id, last_data_version, replica_array.count);

    if (is_redo) {
        result = load_slice_binlogs(myself->dg->id,
                last_data_version, &slice_array);
        if (result != 0) {
            return result;
        }
    }

    /*
    {
        BinlogCommonFields *record;
        BinlogCommonFields *end;

        end = slice_array.records + slice_array.count;
        for (record=slice_array.records; record<end; record++) {
            logInfo("%ld %"PRId64" %c %c %"PRId64" "
                    "%"PRId64"\n", (long)record->timestamp, record->data_version,
                    record->source, record->op_type, record->bkey.oid,
                    record->bkey.offset);
        }
    }
    */
    logInfo("data_group_id: %d, last_data_version: %"PRId64", slice record count: %d",
            myself->dg->id, last_data_version, slice_array.count);

    result = rollback_data(myself, &replica_array,
            &slice_array, is_redo);

    replica_binlog_free_record_array(&replica_array);
    slice_binlog_free_record_array(&slice_array);
    return result;
}

int binlog_rollback(FSClusterDataServerInfo *myself, const uint64_t
        my_confirmed_version, const bool is_redo)
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

    if ((result=rollback_slice_binlog_and_data(myself,
                    my_confirmed_version, is_redo)) != 0)
    {
        return result;
    }

    if (position.index < last_index) {
        if ((result=replica_binlog_set_binlog_indexes(myself->dg->id,
                        start_index, position.index)) != 0)
        {
            return result;
        }

        for (binlog_index=position.index+1;
                binlog_index<last_index;
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
