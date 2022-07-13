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

int rollback_slice_binlogs(const int data_group_id,
        const uint64_t last_data_version,
        const bool detect_slice_binlog)
{
    int result;
    ReplicaBinlogRecordArray replica_array;
    BinlogCommonFieldsArray slice_array;

    replica_binlog_init_record_array(&replica_array);
    slice_binlog_init_record_array(&slice_array);

    if ((result=replica_binlog_load_records(data_group_id,
                    last_data_version, &replica_array)) != 0)
    {
        return result;
    }

    {
        ReplicaBinlogRecord *record;
        ReplicaBinlogRecord *end;
        char buff[256];
        int len;

        end = replica_array.records + replica_array.count;
        for (record=replica_array.records; record<end; record++) {
            if (record->op_type == BINLOG_OP_TYPE_DEL_BLOCK ||
                    record->op_type == BINLOG_OP_TYPE_NO_OP)
            {
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
    }


    logInfo("data_group_id: %d, last_data_version: %"PRId64", replica record count: %d",
            data_group_id, last_data_version, replica_array.count);

    if ((result=load_slice_binlogs(data_group_id,
                    last_data_version,
                    &slice_array)) != 0)
    {
        return result;
    }

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
    logInfo("data_group_id: %d, last_data_version: %"PRId64", slice record count: %d",
            data_group_id, last_data_version, slice_array.count);

    replica_binlog_free_record_array(&replica_array);
    slice_binlog_free_record_array(&slice_array);
    return 0;
}

int binlog_rollback(FSClusterDataServerInfo *myself, const uint64_t
        my_confirmed_version, const bool detect_slice_binlog)
{
    const bool ignore_dv_overflow = false;
    int result;
    int start_index;
    int last_index;
    int binlog_index;
    uint64_t last_data_version;
    SFBinlogFilePosition position;
    char filename[PATH_MAX];

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

    if ((result=rollback_slice_binlogs(myself->dg->id,
                    my_confirmed_version,
                    detect_slice_binlog)) != 0)
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
