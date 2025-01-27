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

#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "sf/sf_global.h"
#include "../../common/fs_func.h"
#include "../server_global.h"
#include "../server_group_info.h"
#include "../shared_thread_pool.h"
#include "../db/change_notify.h"
#include "binlog_func.h"
#include "binlog_loader.h"
#include "replica_binlog.h"

#define SLICE_EXPECT_FIELD_COUNT           8
#define BLOCK_EXPECT_FIELD_COUNT           6

#define MAX_BINLOG_FIELD_COUNT  8

int replica_binlog_get_first_record(const char *filename,
        ReplicaBinlogRecord *record)
{
    char buff[FS_REPLICA_BINLOG_MAX_RECORD_SIZE];
    char error_info[256];
    string_t line;
    int result;

    if ((result=fc_get_first_line(filename, buff,
                    sizeof(buff), &line)) != 0)
    {
        return result;
    }

    if ((result=replica_binlog_record_unpack(&line,
                    record, error_info)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: 1, %s",
                __LINE__, filename, error_info);
    }
    return result;
}

int replica_binlog_get_last_record_ex(const char *filename,
        ReplicaBinlogRecord *record, SFBinlogFilePosition *position,
        int *record_len, const int log_level)
{
    char buff[FS_REPLICA_BINLOG_MAX_RECORD_SIZE];
    char error_info[256];
    string_t line;
    int64_t file_size;
    int result;

    if (access(filename, F_OK) != 0) {
        result = errno != 0 ? errno : EPERM;
        if (result == ENOENT) {
            *record_len = 0;
            position->offset = 0;
            return result;
        }
    }

    if ((result=fc_get_last_line(filename, buff, sizeof(buff),
                    &file_size, &line)) != 0)
    {
        *record_len = 0;
        position->offset = 0;
        return result;
    }

    *record_len = line.len;
    position->offset = file_size - *record_len;
    if ((result=replica_binlog_record_unpack(&line,
                    record, error_info)) != 0)
    {
        int64_t line_count;
        fc_get_file_line_count(filename, &line_count);
        log_it_ex(&g_log_context, log_level,
                "file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", %s",
                __LINE__, filename, line_count, error_info);

        return result;
    }

    return 0;
}

static int get_last_data_version_from_file_ex(const int data_group_id,
        uint64_t *data_version, SFBinlogFilePosition *position,
        int *record_len)
{
    const int log_level = LOG_ERR;
    SFBinlogWriterInfo *writer;
    char filename[PATH_MAX];
    int result;

    *data_version = 0;
    *record_len = 0;
    writer = REPLICA_BINLOG_WRITER_ARRAY.writers[data_group_id -
        REPLICA_BINLOG_WRITER_ARRAY.base_id];
    position->index = sf_binlog_get_current_write_index(writer);
    while (position->index >= 0) {
        sf_binlog_writer_get_filename(writer->fw.cfg.data_path,
                writer->fw.cfg.subdir_name, position->index,
                filename, sizeof(filename));

        if ((result=replica_binlog_get_last_data_version_ex(filename,
                        data_version, position, record_len, log_level)) == 0)
        {
            return 0;
        }

        if (result == ENOENT && position->offset == 0) {
            if (position->index > 0) {
                position->index--;
                continue;
            } else {
                return 0;
            }
        }

        return result;
    }

    return 0;
}

static inline int get_last_data_version_from_file(const int data_group_id,
        uint64_t *data_version)
{
    SFBinlogFilePosition position;
    int record_len;

    return get_last_data_version_from_file_ex(data_group_id,
            data_version, &position, &record_len);
}

static int alloc_binlog_writer_array(const int my_data_group_count)
{
    int bytes;

    bytes = sizeof(SFBinlogWriterInfo) * my_data_group_count;
    REPLICA_BINLOG_WRITER_ARRAY.holders = (SFBinlogWriterInfo *)fc_malloc(bytes);
    if (REPLICA_BINLOG_WRITER_ARRAY.holders == NULL) {
        return ENOMEM;
    }
    memset(REPLICA_BINLOG_WRITER_ARRAY.holders, 0, bytes);

    bytes = sizeof(SFBinlogWriterInfo *) * CLUSTER_DATA_GROUP_ARRAY.count;
    REPLICA_BINLOG_WRITER_ARRAY.writers = (SFBinlogWriterInfo **)fc_malloc(bytes);
    if (REPLICA_BINLOG_WRITER_ARRAY.writers == NULL) {
        return ENOMEM;
    }
    memset(REPLICA_BINLOG_WRITER_ARRAY.writers, 0, bytes);

    REPLICA_BINLOG_WRITER_ARRAY.count = CLUSTER_DATA_GROUP_ARRAY.count;
    return 0;
}

int replica_binlog_set_data_version(FSClusterDataServerInfo *myself,
        const uint64_t new_version)
{
    SFBinlogWriterInfo *writer;
    uint64_t old_version;

    writer = REPLICA_BINLOG_WRITER_ARRAY.writers[myself->dg->id -
        REPLICA_BINLOG_WRITER_ARRAY.base_id];
    FC_ATOMIC_SET(myself->data.current_version, new_version);

    old_version = FC_ATOMIC_GET(myself->data.confirmed_version);
    if (REPLICA_QUORUM_ROLLBACK_DONE || old_version == 0 ||
            old_version > new_version)
    {
        FC_ATOMIC_CAS(myself->data.confirmed_version,
                old_version, new_version);
    }

    return sf_binlog_writer_change_next_version(writer, new_version + 1);
}

static int set_my_data_version(FSClusterDataServerInfo *myself)
{
    uint64_t old_version;
    uint64_t new_version;
    uint64_t confirmed_version;
    int result;

    if ((result=get_last_data_version_from_file(myself->dg->id,
                    &new_version)) != 0)
    {
        return result;
    }

    old_version = FC_ATOMIC_GET(myself->data.current_version);
    if (new_version == old_version) {
        confirmed_version = FC_ATOMIC_GET(myself->data.confirmed_version);
        if (confirmed_version == 0 || confirmed_version > new_version) {
            FC_ATOMIC_CAS(myself->data.confirmed_version,
                    confirmed_version, new_version);
        }
    } else {
        result = replica_binlog_set_data_version(myself, new_version);
        logDebug("file: "__FILE__", line: %d, data_group_id: %d, "
                "old version: %"PRId64", new version: %"PRId64,
                __LINE__, myself->dg->id, old_version,
                myself->data.current_version);
    }

    return result;
}

int replica_binlog_set_my_data_version(const int data_group_id)
{
    FSClusterDataServerInfo *myself;

    if ((myself=fs_get_data_server(data_group_id, CLUSTER_MYSELF_PTR->
                    server->id)) == NULL)
    {
        return ENOENT;
    }
    return set_my_data_version(myself);
}

int replica_binlog_writer_change_order_by(FSClusterDataServerInfo
        *myself, const short order_by)
{
    SFBinlogWriterInfo *writer;

    writer = REPLICA_BINLOG_WRITER_ARRAY.writers[myself->dg->id -
        REPLICA_BINLOG_WRITER_ARRAY.base_id];
    return sf_binlog_writer_change_order_by(writer, order_by);
}

int replica_binlog_writer_change_write_index(const int data_group_id,
        const int write_index)
{
    SFBinlogWriterInfo *writer;

    writer = REPLICA_BINLOG_WRITER_ARRAY.writers[data_group_id -
        REPLICA_BINLOG_WRITER_ARRAY.base_id];
    return sf_binlog_writer_change_write_index(writer, write_index);
}

static inline const char *replica_binlog_get_mark_filename(
        const int data_group_id, const int slave_id,
        char *filename, const int size)
{
    char subdir_name[64];
    char filepath[PATH_MAX];

    replica_binlog_get_dump_subdir_name(subdir_name,
            data_group_id, slave_id);
    sf_binlog_writer_get_filepath(DATA_PATH_STR,
            subdir_name, filepath, sizeof(filepath));
    snprintf(filename, size, "%s/.dump.mark", filepath);
    return filename;
}

static int replica_binlog_delete_mark_filenames(const int data_group_id)
{
    int result;
    FSClusterServerInfo *cs;
    FSClusterServerInfo *end;
    char mark_filename[PATH_MAX];

    end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (cs=CLUSTER_SERVER_ARRAY.servers; cs<end; cs++) {
        if (cs != CLUSTER_MYSELF_PTR) {
            replica_binlog_get_mark_filename(data_group_id, cs->server->id,
                    mark_filename, sizeof(mark_filename));
            if ((result=fc_delete_file(mark_filename)) != 0) {
                return result;
            }
        }
    }

    return 0;
}

int replica_binlog_init()
{
    const int write_interval_ms = 0;
    const bool use_fixed_buffer_size = true;
    const bool passive_write = false;
    FSIdArray *id_array;
    FSClusterDataServerInfo *myself;
    SFBinlogWriterInfo *writer;
    int data_group_id;
    int min_id;
    int max_delay;
    char filepath[PATH_MAX];
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];
    int result;
    int i;
    bool create;

    snprintf(filepath, sizeof(filepath), "%s/%s",
            DATA_PATH_STR, FS_REPLICA_BINLOG_SUBDIR_NAME);
    if ((result=fc_check_mkdir_ex(filepath, 0775, &create)) != 0) {
        return result;
    }
    if (create) {
        SF_CHOWN_RETURN_ON_ERROR(filepath, geteuid(), getegid());
    }

    if ((id_array=fs_cluster_cfg_get_my_data_group_ids(&CLUSTER_CONFIG_CTX,
                    CLUSTER_MYSELF_PTR->server->id)) == NULL)
    {
        logError("file: "__FILE__", line: %d, "
                "cluster config file no data group", __LINE__);
        return ENOENT;
    }

    if ((min_id=fs_cluster_cfg_get_min_data_group_id(id_array)) <= 0) {
        logError("file: "__FILE__", line: %d, "
                "cluster config file no data group", __LINE__);
        return ENOENT;
    }

    if ((result=alloc_binlog_writer_array(id_array->count)) != 0) {
        return result;
    }

    if (LOCAL_BINLOG_CHECK_LAST_SECONDS > 0) {
        max_delay = (LOCAL_BINLOG_CHECK_LAST_SECONDS + 1) / 2;
    } else {
        max_delay = 60;
    }
    REPLICA_BINLOG_WRITER_ARRAY.base_id = min_id;
    writer = REPLICA_BINLOG_WRITER_ARRAY.holders;
    if ((result=sf_binlog_writer_init_thread_ex(&REPLICA_BINLOG_WRITER_THREAD,
                    "replica", writer, SF_BINLOG_THREAD_ORDER_MODE_VARY,
                    write_interval_ms, max_delay,
                    FS_REPLICA_BINLOG_MAX_RECORD_SIZE,
                    use_fixed_buffer_size, passive_write)) != 0)
    {
        return result;
    }

    for (i=0; i<id_array->count; i++) {
        data_group_id = id_array->ids[i];
        if ((myself=fs_get_data_server(data_group_id,
                        CLUSTER_MYSELF_PTR->
                        server->id)) == NULL)
        {
            return ENOENT;
        }

        writer->thread = &REPLICA_BINLOG_WRITER_THREAD;
        REPLICA_BINLOG_WRITER_ARRAY.writers[data_group_id - min_id] = writer;
        replica_binlog_get_subdir_name(subdir_name, data_group_id);
        if ((result=sf_binlog_writer_init_by_version_ex(writer, DATA_PATH_STR,
                        subdir_name, SF_BINLOG_FILE_PREFIX,
                        FS_REPLICA_BINLOG_MAX_RECORD_SIZE, myself->data.
                        current_version + 1, BINLOG_BUFFER_SIZE, 1024,
                        SF_BINLOG_DEFAULT_ROTATE_SIZE,
                        BINLOG_CALL_FSYNC)) != 0)
        {
            return result;
        }

        if ((result=set_my_data_version(myself)) != 0) {
            return result;
        }

        if ((result=replica_binlog_delete_mark_filenames(
                        data_group_id)) != 0)
        {
            return result;
        }

        writer++;
    }

    return 0;
}

void replica_binlog_destroy()
{
    if (REPLICA_BINLOG_WRITER_ARRAY.count > 0) {
        sf_binlog_writer_finish(REPLICA_BINLOG_WRITER_ARRAY.writers[0]);
    }
}

int replica_binlog_get_current_write_index(const int data_group_id)
{
    SFBinlogWriterInfo *writer;
    writer = replica_binlog_get_writer(data_group_id);
    return sf_binlog_get_current_write_index(writer);
}

int replica_binlog_get_binlog_indexes(const int data_group_id,
        int *start_index, int *last_index)
{
    SFBinlogWriterInfo *writer;
    writer = replica_binlog_get_writer(data_group_id);
    return sf_binlog_get_indexes(writer, start_index, last_index);
}

int replica_binlog_set_binlog_indexes(const int data_group_id,
        const int start_index, const int last_index)
{
    SFBinlogWriterInfo *writer;
    writer = replica_binlog_get_writer(data_group_id);
    return sf_binlog_set_indexes(writer, start_index, last_index);
}

int replica_binlog_set_binlog_start_index(const int data_group_id,
        const int start_index)
{
    SFBinlogWriterInfo *writer;
    writer = replica_binlog_get_writer(data_group_id);
    return sf_binlog_writer_set_binlog_start_index(writer, start_index);
}

static inline int unpack_slice_record(string_t *cols, const int count,
        ReplicaBinlogRecord *record, char *error_info)
{
    char *endptr;

    if (count != SLICE_EXPECT_FIELD_COUNT) {
        sprintf(error_info, "field count: %d != %d",
                count, SLICE_EXPECT_FIELD_COUNT);
        return EINVAL;
    }

    BINLOG_PARSE_INT_SILENCE(record->bs_key.block.oid, "object ID",
            REPLICA_BINLOG_FIELD_INDEX_BLOCK_OID, ' ', 1);
    BINLOG_PARSE_INT_SILENCE(record->bs_key.block.offset, "block offset",
            REPLICA_BINLOG_FIELD_INDEX_BLOCK_OFFSET, ' ', 0);
    BINLOG_PARSE_INT_SILENCE(record->bs_key.slice.offset, "slice offset",
            REPLICA_BINLOG_FIELD_INDEX_SLICE_OFFSET, ' ', 0);
    BINLOG_PARSE_INT_SILENCE(record->bs_key.slice.length, "slice length",
            REPLICA_BINLOG_FIELD_INDEX_SLICE_LENGTH, '\n', 1);
    return 0;
}

static inline int unpack_block_record(string_t *cols, const int count,
        ReplicaBinlogRecord *record, char *error_info)
{
    char *endptr;

    if (count != BLOCK_EXPECT_FIELD_COUNT) {
        sprintf(error_info, "field count: %d != %d",
                count, BLOCK_EXPECT_FIELD_COUNT);
        return EINVAL;
    }

    BINLOG_PARSE_INT_SILENCE(record->bs_key.block.oid, "object ID",
            REPLICA_BINLOG_FIELD_INDEX_BLOCK_OID, ' ', 1);
    BINLOG_PARSE_INT_SILENCE(record->bs_key.block.offset, "block offset",
            REPLICA_BINLOG_FIELD_INDEX_BLOCK_OFFSET, '\n', 0);
    return 0;
}

int replica_binlog_record_unpack(const string_t *line,
        ReplicaBinlogRecord *record, char *error_info)
{
    int count;
    int result;
    char *endptr;
    string_t cols[MAX_BINLOG_FIELD_COUNT];

    count = split_string_ex(line, ' ', cols,
            MAX_BINLOG_FIELD_COUNT, false);
    if (count < REPLICA_MIN_FIELD_COUNT) {
        sprintf(error_info, "field count: %d < %d",
                count, REPLICA_MIN_FIELD_COUNT);
        return EINVAL;
    }

    BINLOG_PARSE_INT_SILENCE(record->timestamp, "timestamp",
            REPLICA_BINLOG_FIELD_INDEX_TIMESTAMP, ' ', 0);
    record->source = cols[REPLICA_BINLOG_FIELD_INDEX_SOURCE].str[0];
    record->op_type = cols[REPLICA_BINLOG_FIELD_INDEX_OP_TYPE].str[0];
    BINLOG_PARSE_INT_SILENCE(record->data_version, "data version",
            REPLICA_BINLOG_FIELD_INDEX_DATA_VERSION, ' ', 1);
    switch (record->op_type) {
        case BINLOG_OP_TYPE_WRITE_SLICE:
        case BINLOG_OP_TYPE_ALLOC_SLICE:
        case BINLOG_OP_TYPE_DEL_SLICE:
            result = unpack_slice_record(cols, count, record, error_info);
            break;
        case BINLOG_OP_TYPE_DEL_BLOCK:
        case BINLOG_OP_TYPE_NO_OP:
            result = unpack_block_record(cols, count, record, error_info);
            break;
        default:
            sprintf(error_info, "invalid op_type: %c (0x%02x)",
                    record->op_type, (unsigned char)record->op_type);
            result = EINVAL;
            break;
    }

    return result;
}

static SFBinlogWriterBuffer *alloc_binlog_buffer(const int data_group_id,
        const int64_t data_version, SFBinlogWriterInfo **writer)
{
    *writer = REPLICA_BINLOG_WRITER_ARRAY.writers[data_group_id -
        REPLICA_BINLOG_WRITER_ARRAY.base_id];
    return sf_binlog_writer_alloc_one_version_buffer(*writer, data_version);
}

int replica_binlog_log_slice(const time_t current_time,
        const int data_group_id, const int64_t data_version,
        const FSBlockSliceKeyInfo *bs_key, const int source,
        const int op_type)
{
    SFBinlogWriterInfo *writer;
    SFBinlogWriterBuffer *wbuffer;

    if ((wbuffer=alloc_binlog_buffer(data_group_id,
                    data_version, &writer)) == NULL)
    {
        return ENOMEM;
    }

    wbuffer->bf.length = replica_binlog_log_slice_to_buff(current_time,
            data_version, bs_key, source, op_type, wbuffer->bf.buff);
    sf_push_to_binlog_write_queue(writer, wbuffer);
    return 0;
}

int replica_binlog_log_block(const time_t current_time,
        const int data_group_id, const int64_t data_version,
        const FSBlockKey *bkey, const int source, const int op_type)
{
    SFBinlogWriterInfo *writer;
    SFBinlogWriterBuffer *wbuffer;

    if ((wbuffer=alloc_binlog_buffer(data_group_id,
                    data_version, &writer)) == NULL)
    {
        return ENOMEM;
    }

    wbuffer->bf.length = replica_binlog_log_block_to_buff(current_time,
            data_version, bkey, source, op_type, wbuffer->bf.buff);
    sf_push_to_binlog_write_queue(writer, wbuffer);
    return 0;
}

static int find_position_by_buffer(ServerBinlogReader *reader,
        const uint64_t data_version, SFBinlogFilePosition *pos)
{
    int result;
    char error_info[256];
    string_t line;
    char *line_end;
    ReplicaBinlogRecord record;

    while (reader->binlog_buffer.current < reader->binlog_buffer.data_end) {
        line_end = (char *)memchr(reader->binlog_buffer.current, '\n',
                reader->binlog_buffer.data_end - reader->binlog_buffer.current);
        if (line_end == NULL) {
            return EAGAIN;
        }

        ++line_end;   //skip \n
        line.str = reader->binlog_buffer.current;
        line.len = line_end - reader->binlog_buffer.current;
        if ((result=replica_binlog_record_unpack(&line,
                        &record, error_info)) != 0)
        {
            int64_t file_offset;
            int64_t line_count;

            file_offset = reader->position.offset - (reader-> binlog_buffer.
                    data_end - reader->binlog_buffer.current);
            fc_get_file_line_count_ex(reader->filename,
                    file_offset, &line_count);
            logError("file: "__FILE__", line: %d, "
                    "binlog file %s, line no: %"PRId64", %s",
                    __LINE__, reader->filename, line_count, error_info);
            return result;
        }

        if (data_version < record.data_version) {
            pos->index = reader->position.index;
            pos->offset = reader->position.offset - (reader->binlog_buffer.
                    data_end - reader->binlog_buffer.current);
            return 0;
        }

        reader->binlog_buffer.current = line_end;
    }

    return EAGAIN;
}

static int find_position_by_reader(ServerBinlogReader *reader,
        const uint64_t data_version, SFBinlogFilePosition *pos)
{
    int result;

    while ((result=binlog_reader_read(reader)) == 0) {
        result = find_position_by_buffer(reader, data_version, pos);
        if (result != EAGAIN) {
            break;
        }
    }

    return result;
}

static int find_position(const char *subdir_name, SFBinlogWriterInfo *writer,
        const uint64_t target_data_version, SFBinlogFilePosition *pos,
        const bool ignore_dv_overflow)
{
    const int log_level = LOG_ERR;
    int result;
    int record_len;
    uint64_t last_data_version;
    char filename[PATH_MAX];
    ServerBinlogReader reader;

    sf_binlog_writer_get_filename(DATA_PATH_STR, subdir_name,
            pos->index, filename, sizeof(filename));
    if ((result=replica_binlog_get_last_data_version_ex(filename,
                    &last_data_version, pos, &record_len, log_level)) != 0)
    {
        return result;
    }

    if (target_data_version == last_data_version) {  //match the last record
        if (pos->index < sf_binlog_get_current_write_index(writer)) {
            pos->index++; //skip to next binlog
            pos->offset = 0;
        } else {
            pos->offset += record_len;
        }
        return 0;
    }

    if (target_data_version > last_data_version) {
        if (pos->index < sf_binlog_get_current_write_index(writer)) {
            pos->index++;   //skip to next binlog
            pos->offset = 0;
            return 0;
        }

        if (ignore_dv_overflow) {
            pos->offset += record_len;
            return 0;
        }

        logWarning("file: "__FILE__", line: %d, subdir_name: %s, "
                "target_data_version: %"PRId64" is too large, which "
                " > the last data version %"PRId64" in the binlog file %s, "
                "binlog index: %d", __LINE__, subdir_name, target_data_version,
                last_data_version, filename, pos->index);
        return EOVERFLOW;
    }

    pos->offset = 0;
    if ((result=binlog_reader_init(&reader, subdir_name, writer, pos)) != 0) {
        return result;
    }

    result = find_position_by_reader(&reader, target_data_version, pos);
    binlog_reader_destroy(&reader);
    return result;
}

int replica_binlog_get_position_by_dv_ex(const char *subdir_name,
            SFBinlogWriterInfo *writer, const uint64_t last_data_version,
            SFBinlogFilePosition *pos, const bool ignore_dv_overflow)
{
    int result;
    int start_index;
    int last_index;
    int binlog_index;
    char filename[PATH_MAX];
    uint64_t first_data_version;

    if ((result=sf_binlog_get_indexes(writer, &start_index,
                    &last_index)) != 0)
    {
        return result;
    }
    if (last_data_version == 0) {
        if (start_index == 0) {
            pos->index = 0;
            pos->offset = 0;
            return 0;
        } else {
            return SF_CLUSTER_ERROR_BINLOG_MISSED;
        }
    }

    binlog_index = last_index;
    while (binlog_index >= start_index) {
        sf_binlog_writer_get_filename(DATA_PATH_STR, subdir_name,
                binlog_index, filename, sizeof(filename));
        if ((result=replica_binlog_get_first_data_version(
                        filename, &first_data_version)) != 0)
        {
            if (result == ENOENT) {
                --binlog_index;
                continue;
            }

            return result;
        }

        if (last_data_version + 1 >= first_data_version) {
            pos->index = binlog_index;
            pos->offset = 0;
            if (last_data_version + 1 == first_data_version) {
                return 0;
            }
            return find_position(subdir_name, writer, last_data_version,
                    pos, ignore_dv_overflow);
        }

        --binlog_index;
    }

    if (start_index == 0) {
        pos->index = 0;
        pos->offset = 0;
        return 0;
    } else {
        return SF_CLUSTER_ERROR_BINLOG_MISSED;
    }
}

int replica_binlog_get_position_by_dv(const int data_group_id,
        const uint64_t last_data_version, SFBinlogFilePosition *pos,
        const bool ignore_dv_overflow)
{
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];
    SFBinlogWriterInfo *writer;

    replica_binlog_get_subdir_name(subdir_name, data_group_id);
    writer = replica_binlog_get_writer(data_group_id);
    return replica_binlog_get_position_by_dv_ex(subdir_name, writer,
            last_data_version, pos, ignore_dv_overflow);
}

int replica_binlog_reader_init(struct server_binlog_reader *reader,
        const int data_group_id, const uint64_t last_data_version)
{
    int result;
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];
    SFBinlogWriterInfo *writer;
    SFBinlogFilePosition position;

    replica_binlog_get_subdir_name(subdir_name, data_group_id);
    writer = replica_binlog_get_writer(data_group_id);
    if ((result=replica_binlog_get_position_by_dv_ex(subdir_name, writer,
                    last_data_version, &position, false)) != 0)
    {
        return result;
    }

    return binlog_reader_init(reader, subdir_name, writer, &position);
}

const char *replica_binlog_get_op_type_caption(const int op_type)
{
    switch (op_type) {
        case BINLOG_OP_TYPE_WRITE_SLICE:
            return "write slice";
        case BINLOG_OP_TYPE_ALLOC_SLICE:
            return "alloc slice";
        case BINLOG_OP_TYPE_DEL_SLICE:
            return "delete slice";
        case BINLOG_OP_TYPE_DEL_BLOCK:
            return "delete block";
        case BINLOG_OP_TYPE_NO_OP:
            return "no op";
        default:
            return "unkown";
    }
}

int replica_binlog_get_last_lines(const int data_group_id, char *buff,
        const int buff_size, int *count, int *length)
{
    int current_windex;
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];

    replica_binlog_get_subdir_name(subdir_name, data_group_id);
    current_windex = replica_binlog_get_current_write_index(data_group_id);
    return sf_binlog_writer_get_last_lines(DATA_PATH_STR, subdir_name,
            current_windex, buff, buff_size, count, length);
}

static int replica_binlog_unpack_records(const int data_group_id,
        const string_t *buffer, ReplicaBinlogRecord *records,
        const int size, int *count)
{
    int result;
    char error_info[256];
    char *p;
    char *end;
    string_t line;
    char *line_end;
    ReplicaBinlogRecord *record;

    *count = 0;
    record = records;
    p = buffer->str;
    end = buffer->str + buffer->len;
    while (p < end) {
        line_end = (char *)memchr(p, '\n', end - p);
        if (line_end == NULL) {
            logError("file: "__FILE__", line: %d, "
                    "data group id: %d, unpack replica binlog fail, expect "
                    "line end char (\\n), binlog line: %.*s", __LINE__,
                    data_group_id, (int)(end - p), p);
            return EINVAL;
        }

        ++line_end;   //skip \n
        line.str = p;
        line.len = line_end - p;
        if ((result=replica_binlog_record_unpack(&line,
                        record++, error_info)) != 0)
        {
            logError("file: "__FILE__", line: %d, "
                    "data group id: %d, unpack reliplica binlog fail, %s, "
                    "binlog line: %.*s", __LINE__, data_group_id,
                    error_info, line.len, line.str);
            return result;
        }

        if (++(*count) == size) {
            break;
        }
        p = line_end;
    }

    return 0;
}

static int compare_record(ReplicaBinlogRecord *r1, ReplicaBinlogRecord *r2)
{
    int sub;

    if ((sub=(int)r1->op_type - (int)r2->op_type) != 0) {
        return sub;
    }

    if ((sub=fc_compare_int64(r1->bs_key.block.oid,
                    r2->bs_key.block.oid)) != 0)
    {
        return sub;
    }

    if ((sub=fc_compare_int64(r1->bs_key.block.offset,
                    r2->bs_key.block.offset)) != 0)
    {
        return sub;
    }

    if (r1->op_type == BINLOG_OP_TYPE_DEL_BLOCK ||
        r1->op_type == BINLOG_OP_TYPE_NO_OP)
    {
        return 0;
    }

    if ((sub=(int)r1->bs_key.slice.offset -
                (int)r2->bs_key.slice.offset) != 0)
    {
        return sub;
    }

    return (int)r1->bs_key.slice.length - (int)r2->bs_key.slice.length;
}

static int check_records_consistency(ReplicaBinlogRecord *slave_records,
        const int slave_rows, ReplicaBinlogRecord *master_records,
        const int master_rows, int *first_unmatched_index,
        uint64_t *first_unmatched_dv)
{
    ReplicaBinlogRecord *sr;
    ReplicaBinlogRecord *mr;
    ReplicaBinlogRecord *send;
    ReplicaBinlogRecord *mend;

    sr = slave_records;
    mr = master_records;
    send = slave_records + slave_rows;
    mend = master_records + master_rows;
    while ((sr < send) && (mr < mend)) {
        if (sr->data_version != mr->data_version) {
            break;
        }

        if (compare_record(sr, mr) != 0) {
            break;
        }
        sr++;
        mr++;
    }

    if (sr < send) {
        *first_unmatched_index = sr - slave_records;
        *first_unmatched_dv = sr->data_version;
        return SF_CLUSTER_ERROR_BINLOG_INCONSISTENT;
    }

    return 0;
}

int replica_binlog_master_check_consistency(const int data_group_id,
        string_t *sbuffer, uint64_t *first_unmatched_dv)
{
    int result;
    struct server_binlog_reader reader;
    ReplicaBinlogRecord slave_records[FS_MAX_SLAVE_BINLOG_CHECK_LAST_ROWS];
    ReplicaBinlogRecord master_records[FS_MAX_SLAVE_BINLOG_CHECK_LAST_ROWS];
    char buff[FS_MAX_SLAVE_BINLOG_CHECK_LAST_ROWS *
        FS_REPLICA_BINLOG_MAX_RECORD_SIZE];
    string_t mbuffer;
    int slave_rows;
    int master_rows;
    int first_unmatched_index;

    if (sbuffer->len == 0) {
        return 0;
    }

    *first_unmatched_dv = 0;
    if ((result=replica_binlog_unpack_records(data_group_id, sbuffer,
                    slave_records, FS_MAX_SLAVE_BINLOG_CHECK_LAST_ROWS,
                    &slave_rows)) != 0)
    {
        return result;
    }

    if ((result=replica_binlog_reader_init(&reader, data_group_id,
                    slave_records[0].data_version - 1)) != 0)
    {
        return result;
    }

    mbuffer.str = buff;
    result = binlog_reader_integral_full_read(&reader,
            buff, sizeof(buff), &mbuffer.len);
    binlog_reader_destroy(&reader);

    if (result != 0) {
        return result;
    }

    if ((result=replica_binlog_unpack_records(data_group_id, &mbuffer,
                    master_records, FS_MAX_SLAVE_BINLOG_CHECK_LAST_ROWS,
                    &master_rows)) != 0)
    {
        return result;
    }

    return check_records_consistency(slave_records, slave_rows,
            master_records, master_rows, &first_unmatched_index,
            first_unmatched_dv);
}

int replica_binlog_slave_check_consistency(const int data_group_id,
        string_t *mbuffer, int *first_unmatched_index,
        uint64_t *first_unmatched_dv)
{
    int result;
    struct server_binlog_reader reader;
    ReplicaBinlogRecord *slave_records;
    ReplicaBinlogRecord *master_records;
    char *buff;
    string_t sbuffer;
    int max_rows;
    int buff_size;
    int slave_rows;
    int master_rows;

    *first_unmatched_index = -1;
    *first_unmatched_dv = 0;
    if (mbuffer->len == 0) {
        return 0;
    }

    max_rows = mbuffer->len / FS_REPLICA_BINLOG_MIN_RECORD_SIZE + 1;
    master_records = fc_malloc(sizeof(ReplicaBinlogRecord) * max_rows);
    if (master_records == NULL) {
        return ENOMEM;
    }

    slave_records = fc_malloc(sizeof(ReplicaBinlogRecord) * max_rows);
    if (slave_records == NULL) {
        free(master_records);
        return ENOMEM;
    }

    buff_size = FS_REPLICA_BINLOG_MAX_RECORD_SIZE * max_rows;
    buff = fc_malloc(buff_size);
    if (buff == NULL) {
        free(master_records);
        free(slave_records);
        return ENOMEM;
    }

    do {
        if ((result=replica_binlog_unpack_records(data_group_id, mbuffer,
                        master_records, max_rows, &master_rows)) != 0)
        {
            break;
        }

        if ((result=replica_binlog_reader_init(&reader, data_group_id,
                        master_records[0].data_version - 1)) != 0)
        {
            break;
        }

        sbuffer.str = buff;
        result = binlog_reader_integral_full_read(&reader,
                buff, buff_size, &sbuffer.len);
        binlog_reader_destroy(&reader);

        if (result != 0) {
            break;
        }

        if ((result=replica_binlog_unpack_records(data_group_id, &sbuffer,
                        slave_records, max_rows, &slave_rows)) != 0)
        {
            break;
        }

        result = check_records_consistency(slave_records, slave_rows,
                master_records, master_rows, first_unmatched_index,
                first_unmatched_dv);
    } while (0);

    free(master_records);
    free(slave_records);
    free(buff);
    return result;
}

void replica_binlog_writer_stat(const int data_group_id,
        FSBinlogWriterStat *stat)
{
    SFBinlogWriterInfo *writer;

    writer = REPLICA_BINLOG_WRITER_ARRAY.writers[data_group_id -
        REPLICA_BINLOG_WRITER_ARRAY.base_id];
    stat->total_count = writer->fw.total_count;
    stat->next_version = writer->version_ctx.next;
    stat->waiting_count = writer->version_ctx.ring.waiting_count;
    stat->max_waitings = writer->version_ctx.ring.max_waitings;
}

static int replica_binlog_dump(const int data_group_id,
        const int slave_id)
{
    int result;
    int i;
    uint64_t current_data_version;
    uint64_t last_data_version;
    int64_t total_slice_count;
    int64_t total_replica_count;
    char tmp_filename[PATH_MAX];
    char dump_filename[PATH_MAX];
    int64_t start_time_ms;
    int64_t time_used;
    char time_buff[32];

    start_time_ms = get_current_time_ms();
    current_data_version = fs_get_my_ds_data_version(data_group_id);
    if (STORAGE_ENABLED) {
        if ((result=change_notify_waiting_consume_done()) != 0) {
            return result;
        }
    }
    for (i=0; i<=30; i++) {
        if ((result=replica_binlog_get_last_dv(data_group_id,
                        &last_data_version)) != 0)
        {
            return result;
        }

        if (last_data_version >= current_data_version) {
            break;
        }
        fc_sleep_ms(100);
    }

    if (last_data_version < current_data_version) {
        logError("file: "__FILE__", line: %d, "
                "data_group_id: %d, waiting replica binlog write done "
                "timeout, waiting data version: %"PRId64" > the last "
                "data version: %"PRId64, __LINE__, data_group_id,
                current_data_version, last_data_version);
        return EBUSY;
    }

    replica_binlog_get_dump_filename(data_group_id, slave_id,
            dump_filename, sizeof(dump_filename));
    snprintf(tmp_filename, sizeof(tmp_filename), "%s.tmp", dump_filename);
    if ((result=ob_index_dump_replica_binlog_to_file(data_group_id,
                    current_data_version, tmp_filename, &total_slice_count,
                    &total_replica_count)) != 0)
    {
        return result;
    }

    if (rename(tmp_filename, dump_filename) != 0) {
        result = errno != 0 ? errno : EPERM;
        logError("file: "__FILE__", line: %d, "
                "rename file %s to %s fail, "
                "errno: %d, error info: %s",
                __LINE__, tmp_filename, dump_filename,
                result, STRERROR(result));
        return result;
    }

    time_used = get_current_time_ms() - start_time_ms;
    long_to_comma_str(time_used, time_buff);
    logInfo("file: "__FILE__", line: %d, "
            "data_group_id: %d, slave_id: %d, dump replica binlog success. "
            "total_slice_count: %"PRId64", total_replica_count: %"PRId64", "
            "time used: %s ms", __LINE__, data_group_id, slave_id,
            total_slice_count, total_replica_count, time_buff);
    return 0;
}

typedef union {
    int64_t n;
    struct {
        int data_group_id;
        int slave_id;
    };
} ReplicaBinlogDumpArgs;

static void replica_binlog_dump_func(void *arg, void *thread_data)
{
    int result;
    ReplicaBinlogDumpArgs args;
    char mark_filename[PATH_MAX];

    args.n = (long)arg;
    if ((result=replica_binlog_dump(args.data_group_id,
                    args.slave_id)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "data_group_id: %d, slave_id: %d, dump replica binlog "
                "fail, result: %d", __LINE__, args.data_group_id,
                args.slave_id, result);
    }

    replica_binlog_get_mark_filename(args.data_group_id,
            args.slave_id, mark_filename, sizeof(mark_filename));
    fc_delete_file(mark_filename);
}

int replica_binlog_init_dump_reader(const int data_group_id,
        const int slave_id, struct server_binlog_reader *reader)
{
    int result;
    char subdir_name[64];
    struct stat stbuf;
    ReplicaBinlogDumpArgs args;
    char filepath[PATH_MAX];
    char dump_filename[PATH_MAX];
    char mark_filename[PATH_MAX];

    replica_binlog_get_dump_subdir_name(subdir_name,
            data_group_id, slave_id);
    sf_binlog_writer_get_filepath(DATA_PATH_STR,
            subdir_name, filepath, sizeof(filepath));
    if ((result=fc_check_mkdir(filepath, 0775)) != 0) {
        return result;
    }

    replica_binlog_get_dump_filename(data_group_id, slave_id,
            dump_filename, sizeof(dump_filename));
    if (stat(dump_filename, &stbuf) == 0) {
        if (g_current_time - stbuf.st_mtime <= 86400) {
            return binlog_reader_init(reader, subdir_name, NULL, NULL);
        } else {
            if ((result=fc_delete_file(dump_filename)) != 0) {
                return result;
            }
        }
    } else if (errno != ENOENT) {
        result = errno != 0 ? errno : EPERM;
        logError("file: "__FILE__", line: %d, "
                "stat file %s fail, errno: %d, error info: %s",
                __LINE__, dump_filename, result, STRERROR(result));
        return result;
    }

    replica_binlog_get_mark_filename(data_group_id, slave_id,
            mark_filename, sizeof(mark_filename));
    if (access(mark_filename, F_OK) == 0) {
        return EINPROGRESS;
    }

    if ((result=writeToFile(mark_filename, "OK", 2)) != 0) {
        return result;
    }

    args.data_group_id = data_group_id;
    args.slave_id = slave_id;
    if ((result=shared_thread_pool_run(replica_binlog_dump_func,
                    (void *)args.n)) != 0)
    {
        fc_delete_file(mark_filename);
        return result;
    } else {
        return EINPROGRESS;
    }
}

int replica_binlog_remove_all_files(const int data_group_id)
{
    int result;
    int start_index;
    int last_index;
    int binlog_index;
    int remove_count;
    char binlog_filename[PATH_MAX];

    if ((result=replica_binlog_get_binlog_indexes(data_group_id,
                    &start_index, &last_index)) != 0)
    {
        return result;
    }

    for (binlog_index=start_index; binlog_index<=last_index; binlog_index++) {
        replica_binlog_get_filename(data_group_id, binlog_index,
                binlog_filename, sizeof(binlog_filename));
        if ((result=fc_delete_file_ex(binlog_filename,
                        "replica binlog")) != 0)
        {
            return result;
        }
    }

    remove_count = (last_index - start_index) + 1;
    if ((result=replica_binlog_set_binlog_indexes(
                    data_group_id, 0, 0)) != 0)
    {
        logCrit("file: "__FILE__", line: %d, "
                "replica_binlog_set_binlog_indexes fail, errno: %d, "
                "program terminate!", __LINE__, result);
        sf_terminate_myself();
        return result;
    }

    logWarning("file: "__FILE__", line: %d, "
            "data group id: %d, delete %d replica binlog files",
            __LINE__, data_group_id, remove_count);
    return replica_binlog_writer_change_write_index(data_group_id, 0);
}

int replica_binlog_waiting_write_done(const int data_group_id,
        const uint64_t waiting_data_version, const char *caption)
{
#define MAX_WAITING_COUNT  10000
    int result;
    int r;
    int count;
    int record_len;
    int log_level;
    int64_t start_time_ms;
    uint64_t data_version;
    SFBinlogFilePosition position;
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];
    char filename[PATH_MAX];
    char prompt[64];
    char time_buff[32];

    if (waiting_data_version == 0) {
        return 0;
    }

    start_time_ms = get_current_time_ms();
    result = ETIMEDOUT;
    replica_binlog_get_subdir_name(subdir_name, data_group_id);
    for (count=0; count<MAX_WAITING_COUNT && SF_G_CONTINUE_FLAG; count++) {
        log_level =  count < MAX_WAITING_COUNT - 1 ? LOG_NOTHING : LOG_WARNING;
        sf_binlog_writer_get_filename(DATA_PATH_STR, subdir_name,
                replica_binlog_get_current_write_index(
                    data_group_id), filename, sizeof(filename));
        if ((r=replica_binlog_get_last_data_version_ex(filename, &data_version,
                        &position, &record_len, log_level)) != 0)
        {
            if (!(r == ENOENT || r == EINVAL)) {
                result = r;
                break;
            }
        } else if (data_version >= waiting_data_version) {
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

    long_to_comma_str(get_current_time_ms() - start_time_ms, time_buff);
    log_it_ex(&g_log_context, log_level, "file: "__FILE__", line: %d, "
            "data group id: %d, waiting %s last data version: %"PRId64", "
            "waiting binlog write done %s, time used: %s ms", __LINE__,
            data_group_id, caption, waiting_data_version, prompt, time_buff);
    return result;
}

static int check_alloc_record_array(ReplicaBinlogRecordArray *array)
{
    ReplicaBinlogRecord *records;
    int64_t new_alloc;
    int64_t bytes;

    if (array->alloc > array->count) {
        return 0;
    }

    new_alloc = (array->alloc > 0) ? 2 * array->alloc : 64;
    bytes = sizeof(ReplicaBinlogRecord) * new_alloc;
    records = (ReplicaBinlogRecord *)fc_malloc(bytes);
    if (records == NULL) {
        return ENOMEM;
    }

    if (array->records != NULL) {
        if (array->count > 0) {
            memcpy(records, array->records, array->count *
                    sizeof(ReplicaBinlogRecord));
        }
        free(array->records);
    }

    array->alloc = new_alloc;
    array->records = records;
    return 0;
}

static int binlog_parse_buffer(ServerBinlogReader *reader,
        const int length, ReplicaBinlogRecordArray *array)
{
    int result;
    string_t line;
    ReplicaBinlogRecord *record;
    char *buff;
    char *buff_end;
    char *line_end;
    char error_info[256];

    *error_info = '\0';
    result = 0;
    buff = reader->binlog_buffer.buff;
    line.str = buff;
    buff_end = buff + length;
    while (line.str < buff_end) {
        line_end = (char *)memchr(line.str, '\n', buff_end - line.str);
        if (line_end == NULL) {
            result = EINVAL;
            sprintf(error_info, "expect line end char (\\n)");
            break;
        }

        ++line_end;
        line.len = line_end - line.str;

        if ((result=check_alloc_record_array(array)) != 0) {
            return result;
        }
        record = array->records + array->count;
        if ((result=replica_binlog_record_unpack(&line,
                        record, error_info)) != 0)
        {
            break;
        }

        array->count++;
        line.str = line_end;
    }

    if (result != 0) {
        int64_t file_offset;
        int64_t line_count;
        int remain_bytes;

        remain_bytes = length - (line.str - buff);
        file_offset = reader->position.offset - remain_bytes;
        fc_get_file_line_count_ex(reader->filename,
                file_offset, &line_count);
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", %s",
                __LINE__, reader->filename, line_count, error_info);
    }

    return result;
}

int replica_binlog_load_records(const int data_group_id,
        const uint64_t last_data_version,
        ReplicaBinlogRecordArray *array)
{
    int result;
    int read_bytes;
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];
    SFBinlogWriterInfo *writer;
    SFBinlogFilePosition pos;
    ServerBinlogReader reader;

    array->count = 0;
    if ((result=replica_binlog_get_position_by_dv(data_group_id,
                    last_data_version, &pos, true)) != 0)
    {
        return (result == ENOENT ? 0 : result);
    }

    replica_binlog_get_subdir_name(subdir_name, data_group_id);
    writer = replica_binlog_get_writer(data_group_id);
    if ((result=binlog_reader_init(&reader, subdir_name,
                    writer, &pos)) != 0)
    {
        return result;
    }

    while ((result=binlog_reader_integral_read(&reader,
                    reader.binlog_buffer.buff,
                    reader.binlog_buffer.size,
                    &read_bytes)) == 0)
    {
        if ((result=binlog_parse_buffer(&reader, read_bytes, array)) != 0) {
            break;
        }
    }

    binlog_reader_destroy(&reader);
    return (result == ENOENT ? 0 : result);
}

typedef struct {
    SFBinlogFilePosition position;
    char filename[PATH_MAX];
    int fd;
    int length;
} ReplicaBinlogReader;

static int replica_file_reader_init(ReplicaBinlogReader *reader,
        const int data_group_id, const SFBinlogFilePosition *pos,
        const bool find_newline)
{
    int result;
    int read_bytes;
    char line[FS_REPLICA_BINLOG_MAX_RECORD_SIZE];

    replica_binlog_get_filename(data_group_id, pos->index,
            reader->filename, sizeof(reader->filename));
    if ((reader->fd=open(reader->filename, O_RDONLY | O_CLOEXEC)) < 0) {
        result = errno != 0 ? errno : ENOENT;
        logError("file: "__FILE__", line: %d, "
                "open file %s fail, errno: %d, error info: %s",
                __LINE__, reader->filename, result, STRERROR(result));
        return result;
    }

    if ((reader->position.offset=lseek(reader->fd, pos->offset,
                    pos->offset >= 0 ? SEEK_SET : SEEK_END)) < 0)
    {
        result = (errno != 0 ? errno : EIO);
        logError("file: "__FILE__", line: %d, "
                "lseek file %s fail, offset: %"PRId64", errno: %d, "
                "error info: %s", __LINE__, reader->filename,
                pos->offset, result, STRERROR(result));
        close(reader->fd);
        return result;
    }

    if (find_newline) {
        if ((read_bytes=fd_gets(reader->fd, line, sizeof(line),
                        FS_REPLICA_BINLOG_MAX_RECORD_SIZE)) < 0)
        {
            result = errno != 0 ? errno : EIO;
            logError("file: "__FILE__", line: %d, "
                    "file %s, fd gets fail, errno: %d, error info: %s",
                    __LINE__, reader->filename, result, STRERROR(result));
            close(reader->fd);
            return result;
        }
        reader->position.offset += read_bytes;
    }

    reader->position.index = pos->index;
    return 0;
}

static inline void replica_binlog_reader_destroy(ReplicaBinlogReader *reader)
{
    if (reader->fd >= 0) {
        close(reader->fd);
    }
}

int replica_binlog_load_until_dv(const int data_group_id,
        const uint64_t last_data_version, char *buff,
        const int size, int *length)
{
    int result;
    int read_bytes;
    int start_index;
    int last_index;
    int i;
    int64_t until_offset;
    ReplicaBinlogReader readers[2];
    SFBinlogFilePosition pos;
    char line[FS_REPLICA_BINLOG_MAX_RECORD_SIZE];

    if ((result=replica_binlog_get_binlog_indexes(data_group_id,
            &start_index, &last_index)) != 0)
    {
        return result;
    }

    if ((result=replica_binlog_get_position_by_dv(data_group_id,
                    last_data_version, &pos, true)) != 0)
    {
        return result;
    }

    if ((result=replica_file_reader_init(readers + 1,
                    data_group_id, &pos, false)) != 0)
    {
        return result;
    }

    if (readers[1].position.offset >= size) {
        readers[0].fd = -1;
        until_offset = readers[1].position.offset;
        readers[1].position.offset -= size;
        lseek(readers[1].fd, readers[1].position.offset, SEEK_SET);
        if ((read_bytes=fd_gets(readers[1].fd, line, sizeof(line),
                        FS_REPLICA_BINLOG_MAX_RECORD_SIZE)) < 0)
        {
            result = errno != 0 ? errno : EIO;
            logError("file: "__FILE__", line: %d, "
                    "file %s, fd gets fail, errno: %d, error info: %s",
                    __LINE__, readers[1].filename, result, STRERROR(result));
            replica_binlog_reader_destroy(readers + 1);
            return result;
        }

        readers[1].position.offset += read_bytes;
        readers[1].length = until_offset - readers[1].position.offset;
    } else {
        readers[1].position.offset = 0;
        readers[1].length = pos.offset;
        if (pos.index > start_index) {
            readers[0].length = size - readers[1].length;
            pos.index--;
            pos.offset = -1 * readers[0].length;
            if ((result=replica_file_reader_init(readers + 0,
                            data_group_id, &pos, true)) != 0)
            {
                replica_binlog_reader_destroy(readers + 1);
                return result;
            }
        } else {
            readers[0].fd = -1;
        }
    }

    *length = 0;
    for (i=0; i<2; i++) {
        if (readers[i].fd < 0) {
            continue;
        }

        if ((read_bytes=pread(readers[i].fd, buff + (*length), readers[i].
                        length, readers[i].position.offset)) < 0)
        {
            result = errno != 0 ? errno : EIO;
            logError("file: "__FILE__", line: %d, "
                    "read from file %s fail, offset: %"PRId64", "
                    "length: %d, errno: %d, error info: %s", __LINE__,
                    readers[i].filename, readers[i].position.offset,
                    readers[i].length, result, STRERROR(result));
            break;
        }

        *length += read_bytes;
    }

    replica_binlog_reader_destroy(readers + 0);
    replica_binlog_reader_destroy(readers + 1);
    return result;
}
