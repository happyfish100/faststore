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
    time_t timestamp;
    char source;
    unsigned char op_type;
    DASliceType slice_type;   //add slice only
    FSBlockSliceKeyInfo bs_key;
    DATrunkSpaceInfo space;   //add slice only
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

    static inline SFBinlogWriterInfo *slice_binlog_get_writer()
    {
        return &SLICE_BINLOG_WRITER.writer;
    }

    int slice_binlog_set_binlog_start_index(const int start_index);

    int slice_binlog_set_binlog_write_index(const int last_index);

    int slice_binlog_set_binlog_indexes(const int start_index,
            const int last_index);

    void slice_binlog_writer_set_flags(const short flags);

    static inline int slice_binlog_set_next_version_ex(const int64_t next_sn)
    {
        return sf_binlog_writer_change_next_version(
                &SLICE_BINLOG_WRITER.writer, next_sn);
    }

    static inline int slice_binlog_set_next_version()
    {
        return slice_binlog_set_next_version_ex(
                FC_ATOMIC_GET(SLICE_BINLOG_SN) + 1);
    }

    static inline int slice_binlog_set_sn_ex(const int64_t sn,
            const bool reset_binlog_sn)
    {
        int result;

        if ((result=committed_version_reinit(sn)) != 0) {
            return result;
        }
        if (reset_binlog_sn) {
            if ((result=slice_binlog_set_next_version_ex(sn + 1)) != 0) {
                return result;
            }
        }

        FC_ATOMIC_SET(SLICE_BINLOG_SN, sn);
        return 0;
    }

    static inline int slice_binlog_set_sn(const int64_t sn)
    {
        const bool reset_binlog_sn = true;
        return slice_binlog_set_sn_ex(sn, reset_binlog_sn);
    }

    int slice_binlog_rotate_file();

    int slice_binlog_get_position_by_dv(const int data_group_id,
            const uint64_t last_data_version, SFBinlogFilePosition *pos);

    int slice_binlog_get_position_by_sn(const uint64_t last_sn,
            SFBinlogFilePosition *pos);

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

    static inline int slice_binlog_log_add_slice_to_buff1(
            const DASliceType slice_type, const FSBlockKey *bkey,
            const FSSliceSize *ssize, const DATrunkSpaceInfo *space,
            const time_t current_time, const uint64_t sn,
            const uint64_t data_version, const int source, char *buff)
    {
        return sprintf(buff, "%"PRId64" %"PRId64" %"PRId64" %c %c %"PRId64" "
                "%"PRId64" %d %d %d %"PRId64" %u %u %u\n",
                (int64_t)current_time, sn, data_version, source,
                slice_type == DA_SLICE_TYPE_ALLOC ?
                BINLOG_OP_TYPE_ALLOC_SLICE :
                BINLOG_OP_TYPE_WRITE_SLICE,
                bkey->oid, bkey->offset,
                ssize->offset, ssize->length,
                space->store->index, space->id_info.id,
                space->id_info.subdir, space->offset,
                space->size);
    }

    static inline int slice_binlog_log_add_slice_to_buff_ex(
            const OBSliceEntry *slice, const time_t current_time,
            const uint64_t sn, const uint64_t data_version,
            const int source, char *buff)
    {
        return slice_binlog_log_add_slice_to_buff1(slice->type,
                &slice->ob->bkey, &slice->ssize, &slice->space,
                current_time, sn, data_version, source, buff);
    }

    static inline int slice_binlog_log_add_slice_to_buff(const OBSliceEntry
            *slice, const time_t current_time, const uint64_t data_version,
            const int source, char *buff)
    {
        int64_t sn;
        sn = ob_index_generate_alone_sn();
        return slice_binlog_log_add_slice_to_buff_ex(slice,
                current_time, sn, data_version, source, buff);
    }

    static inline int slice_binlog_log_update_block_to_buff(
            const FSBlockKey *bkey, const time_t current_time,
            const char op_type, const int64_t sn, const uint64_t
            data_version, const int source, char *buff)
    {
        return sprintf(buff, "%"PRId64" %"PRId64" %"PRId64" %c %c %"PRId64" "
                "%"PRId64"\n", (int64_t)current_time, sn, data_version,
                source, op_type, bkey->oid, bkey->offset);
    }

    static inline int slice_binlog_log_del_block_to_buff(
            const FSBlockKey *bkey, const time_t current_time,
            const int64_t sn, const uint64_t data_version,
            const int source, char *buff)
    {
        return slice_binlog_log_update_block_to_buff(bkey, current_time,
                BINLOG_OP_TYPE_DEL_BLOCK, sn, data_version, source, buff);
    }

    static inline int slice_binlog_log_no_op_to_buff_ex(
            const FSBlockKey *bkey, const time_t current_time,
            const int64_t sn, const uint64_t data_version,
            const int source, char *buff)
    {
        return slice_binlog_log_update_block_to_buff(bkey, current_time,
                BINLOG_OP_TYPE_NO_OP, sn, data_version, source, buff);
    }

    static inline int slice_binlog_log_no_op_to_buff(const FSBlockKey *bkey,
            const time_t current_time, const uint64_t data_version,
            const int source, char *buff)
    {
        int64_t sn;
        sn = ob_index_generate_alone_sn();
        return slice_binlog_log_no_op_to_buff_ex(bkey, current_time,
                sn, data_version, source, buff);
    }

    static inline int slice_binlog_log_del_slice_to_buff(
            const FSBlockSliceKeyInfo *bs_key, const time_t current_time,
            const uint64_t sn, const uint64_t data_version,
            const int source, char *buff)
    {
        return sprintf(buff, "%"PRId64" %"PRId64" %"PRId64" %c %c "
                "%"PRId64" %"PRId64" %d %d\n", (int64_t)current_time, sn,
                data_version, source, BINLOG_OP_TYPE_DEL_SLICE,
                bs_key->block.oid, bs_key->block.offset,
                bs_key->slice.offset, bs_key->slice.length);
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

    int slice_binlog_padding(const int row_count, const int source);

    static inline int slice_binlog_padding_one(const int source)
    {
        const int row_count = 1;
        return slice_binlog_padding(row_count, source);
    }

    static inline int slice_binlog_padding_for_check(const int source)
    {
        const int row_count = LOCAL_BINLOG_CHECK_LAST_SECONDS + 1;
        return slice_binlog_padding(row_count, source);
    }

    void slice_binlog_writer_stat(FSBinlogWriterStat *stat);

    int slice_binlog_record_unpack(const string_t *line,
            SliceBinlogRecord *record, char *error_info);

    int slice_migrate_done_callback(const DATrunkFileInfo *trunk,
            const DAPieceFieldInfo *field, struct fc_queue_info *space_chain,
            SFSynchronizeContext *sctx, int *flags);

    int slice_binlog_cached_slice_write_done(const DASliceEntry *se,
            const DAFullTrunkSpace *ts, void *arg1, void *arg2);

    int slice_binlog_del_slice_push(const FSBlockSliceKeyInfo *bs_key,
            const time_t current_time, const uint64_t sn,
            const uint64_t data_version, const int source,
            struct fc_queue_info *space_chain);

#ifdef __cplusplus
}
#endif

#endif
