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


#ifndef _SLICE_BINLOG_H
#define _SLICE_BINLOG_H

#include "fastcommon/sched_thread.h"
#include "sf/sf_binlog_writer.h"
#include "binlog_types.h"
#include "../storage/object_block_index.h"
#include "../server_global.h"

typedef struct slice_binlog_record {
    char source;
    char op_type;
    OBSliceType slice_type;   //add slice only
    FSBlockSliceKeyInfo bs_key;
    FSTrunkSpaceInfo space;   //add slice only
    uint64_t sn;
    int64_t data_version;
} SliceBinlogRecord;

#ifdef __cplusplus
extern "C" {
#endif

    int slice_binlog_init();
    void slice_binlog_destroy();

    int slice_binlog_migrate_redo();
    int slice_binlog_get_last_sn_from_file();

    int slice_binlog_load();
    int slice_binlog_get_current_write_index();

    int slice_binlog_get_binlog_start_index();

    int slice_binlog_get_binlog_indexes(int *start_index, int *last_index);

    struct sf_binlog_writer_info *slice_binlog_get_writer();

    int slice_binlog_set_binlog_start_index(const int start_index);

    int slice_binlog_set_binlog_write_index(const int last_index);

    int slice_binlog_set_binlog_indexes(const int start_index,
            const int last_index);

    void slice_binlog_writer_set_flags(const short flags);

    int slice_binlog_set_next_version();

    int slice_binlog_rotate_file();

    int slice_binlog_get_position_by_dv(const int data_group_id,
            const uint64_t last_data_version, SFBinlogFilePosition *pos);

    static inline void slice_binlog_init_record_array(
            BinlogCommonFieldsArray *array)
    {
        array->alloc = array->count = 0;
        array->records = NULL;
    }

    static inline void slice_binlog_free_record_array(
            BinlogCommonFieldsArray *array)
    {
        if (array->records != NULL) {
            free(array->records);
            array->records = NULL;
            array->alloc = array->count = 0;
        }
    }

    int slice_binlog_load_records(const int data_group_id,
            const uint64_t last_data_version,
            BinlogCommonFieldsArray *array);

    static inline const char *slice_binlog_get_filepath(
            char *filepath, const int size)
    {
        return sf_binlog_writer_get_filepath(DATA_PATH_STR,
                FS_SLICE_BINLOG_SUBDIR_NAME, filepath, size);
    }

    static inline const char *slice_binlog_get_filename(const
            int binlog_index, char *filename, const int size)
    {
        return sf_binlog_writer_get_filename(DATA_PATH_STR,
                FS_SLICE_BINLOG_SUBDIR_NAME, binlog_index,
                filename, size);
    }

    static inline const char *slice_binlog_get_index_filename(
            char *filename, const int size)
    {
        return sf_binlog_writer_get_index_filename(DATA_PATH_STR,
                FS_SLICE_BINLOG_SUBDIR_NAME, filename, size);
    }

    static inline int slice_binlog_log_add_slice_to_buff_ex(const OBSliceEntry
            *slice, const time_t current_time, const uint64_t sn,
            const uint64_t data_version, const int source, char *buff)
    {
        return sprintf(buff, "%"PRId64" %"PRId64" %"PRId64" %c %c %"PRId64" "
                "%"PRId64" %d %d %d %"PRId64" %"PRId64" %"PRId64" %"PRId64"\n",
                (int64_t)current_time, sn, data_version, source,
                slice->type == OB_SLICE_TYPE_ALLOC ?
                BINLOG_OP_TYPE_ALLOC_SLICE :
                BINLOG_OP_TYPE_WRITE_SLICE,
                slice->ob->bkey.oid, slice->ob->bkey.offset,
                slice->ssize.offset, slice->ssize.length,
                slice->space.store->index, slice->space.id_info.id,
                slice->space.id_info.subdir, slice->space.offset,
                slice->space.size);
    }

    static inline int slice_binlog_log_add_slice_to_buff(const OBSliceEntry
            *slice, const time_t current_time, const uint64_t data_version,
            const int source, char *buff)
    {
        return slice_binlog_log_add_slice_to_buff_ex(slice, current_time,
                __sync_add_and_fetch(&SLICE_BINLOG_SN, 1), data_version,
                source, buff);
    }

    static inline int slice_binlog_log_no_op_to_buff(const FSBlockKey *bkey,
            const time_t current_time, const uint64_t data_version,
            const int source, char *buff)
    {
        return sprintf(buff, "%"PRId64" %"PRId64" %"PRId64" %c %c %"PRId64" "
                "%"PRId64"\n", (int64_t)current_time, __sync_add_and_fetch(
                    &SLICE_BINLOG_SN, 1), data_version, source,
                BINLOG_OP_TYPE_NO_OP, bkey->oid, bkey->offset);
    }

    int slice_binlog_log_add_slice(const OBSliceEntry *slice,
            const time_t current_time, const uint64_t sn,
            const uint64_t data_version, const int source);

    int slice_binlog_log_del_slice(const FSBlockSliceKeyInfo *bs_key,
            const time_t current_time, const uint64_t sn,
            const uint64_t data_version, const int source);

    int slice_binlog_log_del_block(const FSBlockKey *bkey,
            const time_t current_time, const uint64_t sn,
            const uint64_t data_version, const int source);

    int slice_binlog_log_no_op(const FSBlockKey *bkey,
            const time_t current_time, const uint64_t sn,
            const uint64_t data_version, const int source);

    int slice_binlog_padding_for_check(const int source);

    void slice_binlog_writer_stat(FSBinlogWriterStat *stat);

    int slice_binlog_record_unpack(const string_t *line,
            SliceBinlogRecord *record, char *error_info);

#ifdef __cplusplus
}
#endif

#endif
