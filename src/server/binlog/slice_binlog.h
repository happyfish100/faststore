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
    int64_t data_version;
} SliceBinlogRecord;

#ifdef __cplusplus
extern "C" {
#endif

    int slice_binlog_init();
    void slice_binlog_destroy();

    int slice_binlog_load();
    int slice_binlog_get_current_write_index();

    struct sf_binlog_writer_info *slice_binlog_get_writer();

    int slice_binlog_set_binlog_index(const int binlog_index);

    void slice_binlog_writer_set_flags(const short flags);

    int slice_binlog_set_next_version();

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

    static inline int slice_binlog_log_add_slice_to_buff(const OBSliceEntry
            *slice, const time_t current_time, const uint64_t data_version,
            const int source, char *buff)
    {
        return sprintf(buff, "%"PRId64" %"PRId64" %c %c %"PRId64" %"PRId64
                " %d %d %d %"PRId64" %"PRId64" %"PRId64" %"PRId64"\n",
                (int64_t)current_time, data_version, source,
                slice->type == OB_SLICE_TYPE_FILE ?
                BINLOG_OP_TYPE_WRITE_SLICE :
                BINLOG_OP_TYPE_ALLOC_SLICE,
                slice->ob->bkey.oid, slice->ob->bkey.offset,
                slice->ssize.offset, slice->ssize.length,
                slice->space.store->index, slice->space.id_info.id,
                slice->space.id_info.subdir, slice->space.offset,
                slice->space.size);
    }

    static inline int slice_binlog_log_no_op_to_buff(const FSBlockKey *bkey,
            const time_t current_time, const uint64_t data_version,
            const int source, char *buff)
    {
        return sprintf(buff, "%"PRId64" %"PRId64" %c %c %"PRId64" "
                "%"PRId64"\n", (int64_t)current_time, data_version,
                source, BINLOG_OP_TYPE_NO_OP, bkey->oid, bkey->offset);
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

    void slice_binlog_writer_stat(FSBinlogWriterStat *stat);

    int slice_binlog_record_unpack(const string_t *line,
            SliceBinlogRecord *record, char *error_info);

#ifdef __cplusplus
}
#endif

#endif
