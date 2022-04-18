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


#ifndef _REPLICA_BINLOG_H
#define _REPLICA_BINLOG_H

#include "fastcommon/sched_thread.h"
#include "sf/sf_binlog_writer.h"
#include "../server_global.h"
#include "../storage/object_block_index.h"
#include "binlog_types.h"

struct server_binlog_reader;

typedef struct replica_binlog_record {
    short op_type;
    short source;
    FSBlockSliceKeyInfo bs_key;
    int64_t data_version;
} ReplicaBinlogRecord;

#ifdef __cplusplus
extern "C" {
#endif

    int replica_binlog_init();
    void replica_binlog_destroy();

    static inline void replica_binlog_get_subdir_name(
            char *subdir_name, const int data_group_id)
    {
        sprintf(subdir_name, "%s/%d", FS_REPLICA_BINLOG_SUBDIR_NAME,
                data_group_id);
    }

    SFBinlogWriterInfo *replica_binlog_get_writer(
            const int data_group_id);

    static inline const char *replica_binlog_get_filepath(
            const int data_group_id, char *filepath, const int size)
    {
        char subdir_name[64];

        replica_binlog_get_subdir_name(subdir_name, data_group_id);
        return sf_binlog_writer_get_filepath(DATA_PATH_STR,
                subdir_name, filepath, size);
    }

    static inline const char *replica_binlog_get_base_path(
            char *filepath, const int size)
    {
        return sf_binlog_writer_get_filepath(DATA_PATH_STR,
                FS_REPLICA_BINLOG_SUBDIR_NAME, filepath, size);
    }

    int replica_binlog_get_current_write_index(const int data_group_id);

    int replica_binlog_get_first_record(const char *filename,
            ReplicaBinlogRecord *record);

    static inline int replica_binlog_get_first_data_version(
            const char *filename, uint64_t *data_version)
    {
        ReplicaBinlogRecord record;
        int result;

        if ((result=replica_binlog_get_first_record(
                        filename, &record)) == 0)
        {
            *data_version = record.data_version;
        } else {
            *data_version = 0;
        }

        return result;
    }

    int replica_binlog_get_last_record_ex(const char *filename,
            ReplicaBinlogRecord *record, SFBinlogFilePosition *position,
            int *record_len);

    static inline int replica_binlog_get_last_record(const char *filename,
            ReplicaBinlogRecord *record)
    {
        SFBinlogFilePosition position;
        int record_len;

        return replica_binlog_get_last_record_ex(filename,
                record, &position, &record_len);
    }

    static inline int replica_binlog_get_last_data_version_ex(
            const char *filename, uint64_t *data_version,
            SFBinlogFilePosition *position, int *record_len)
    {
        ReplicaBinlogRecord record;
        int result;

        if ((result=replica_binlog_get_last_record_ex(filename,
                        &record, position, record_len)) == 0)
        {
            *data_version = record.data_version;
        } else {
            *data_version = 0;
        }

        return result;
    }

    static inline int replica_binlog_get_last_data_version(
            const char *filename, uint64_t *data_version)
    {
        SFBinlogFilePosition position;
        int record_len;

        return replica_binlog_get_last_data_version_ex(filename,
                data_version, &position, &record_len);
    }

    static inline int replica_binlog_get_last_dv(const int data_group_id,
            uint64_t *data_version)
    {
        char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];
        char filename[PATH_MAX];

        replica_binlog_get_subdir_name(subdir_name, data_group_id);
        sf_binlog_writer_get_filename(DATA_PATH_STR, subdir_name,
                replica_binlog_get_current_write_index(
                    data_group_id), filename, sizeof(filename));
        return replica_binlog_get_last_data_version(filename, data_version);
    }

    int replica_binlog_get_position_by_dv(const char *subdir_name,
            SFBinlogWriterInfo *writer, const uint64_t last_data_version,
            SFBinlogFilePosition *pos, const bool ignore_dv_overflow);

    int replica_binlog_record_unpack(const string_t *line,
            ReplicaBinlogRecord *record, char *error_info);

    int replica_binlog_unpack_records(const string_t *buffer,
            ReplicaBinlogRecord *records, const int size, int *count);

    static inline int replica_binlog_log_slice_to_buff(const time_t
            current_time, const int64_t data_version,
            const FSBlockSliceKeyInfo *bs_key, const int source,
            const int op_type, char *buff)
    {
        return sprintf(buff, "%"PRId64" %"PRId64" %c %c %"PRId64" "
                "%"PRId64" %d %d\n", (int64_t)current_time, data_version,
                source, op_type, bs_key->block.oid, bs_key->block.offset,
                bs_key->slice.offset, bs_key->slice.length);
    }

    static inline int replica_binlog_log_block_to_buff(const time_t
            current_time, const int64_t data_version,
            const FSBlockKey *bkey, const int source,
            const int op_type, char *buff)
    {
        return sprintf(buff, "%"PRId64" %"PRId64" %c %c %"PRId64" "
                "%"PRId64"\n", (int64_t)current_time, data_version,
                source, op_type, bkey->oid, bkey->offset);
    }

    int replica_binlog_log_slice(const time_t current_time,
            const int data_group_id, const int64_t data_version,
            const FSBlockSliceKeyInfo *bs_key, const int source,
            const int op_type);

    int replica_binlog_log_block(const time_t current_time,
            const int data_group_id, const int64_t data_version,
            const FSBlockKey *bkey, const int source, const int op_type);

    static inline int replica_binlog_log_del_block(const time_t current_time,
            const int data_group_id, const int64_t data_version,
            const FSBlockKey *bkey, const int source)
    {
        return replica_binlog_log_block(current_time, data_group_id,
                data_version, bkey, source,
                BINLOG_OP_TYPE_DEL_BLOCK);
    }

    static inline int replica_binlog_log_no_op(const int data_group_id,
            const int64_t data_version, const FSBlockKey *bkey)
    {
        return replica_binlog_log_block(g_current_time, data_group_id,
                data_version, bkey, BINLOG_SOURCE_REPLAY,
                BINLOG_OP_TYPE_NO_OP);
    }

    static inline int replica_binlog_log_write_slice(const time_t current_time,
            const int data_group_id, const int64_t data_version,
            const FSBlockSliceKeyInfo *bs_key, const int source)
    {
        return replica_binlog_log_slice(current_time, data_group_id,
                data_version, bs_key, source,
                BINLOG_OP_TYPE_WRITE_SLICE);
    }

    static inline int replica_binlog_log_alloc_slice(const time_t current_time,
            const int data_group_id, const int64_t data_version,
            const FSBlockSliceKeyInfo *bs_key, const int source)
    {
        return replica_binlog_log_slice(current_time, data_group_id,
                data_version, bs_key, source,
                BINLOG_OP_TYPE_ALLOC_SLICE);
    }

    static inline int replica_binlog_log_del_slice(const time_t current_time,
            const int data_group_id, const int64_t data_version,
            const FSBlockSliceKeyInfo *bs_key, const int source)
    {
        return replica_binlog_log_slice(current_time, data_group_id,
                data_version, bs_key, source,
                BINLOG_OP_TYPE_DEL_SLICE);
    }

    const char *replica_binlog_get_op_type_caption(const int op_type);

    int replica_binlog_reader_init(struct server_binlog_reader *reader,
            const int data_group_id, const uint64_t last_data_version);

    int replica_binlog_set_data_version(FSClusterDataServerInfo *myself,
            const uint64_t new_version);

    int replica_binlog_set_my_data_version(const int data_group_id);

    int replica_binlog_writer_change_order_by(FSClusterDataServerInfo
            *myself, const short order_by);

    int replica_binlog_get_last_lines(const int data_group_id, char *buff,
            const int buff_size, int *count, int *length);

    int replica_binlog_check_consistency(const int data_group_id,
            string_t *buffer, uint64_t *first_unmatched_dv);

    void replica_binlog_writer_stat(const int data_group_id,
            FSBinlogWriterStat *stat);

#ifdef __cplusplus
}
#endif

#endif
