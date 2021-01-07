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
#include <fcntl.h>
#include <sys/stat.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "sf/sf_global.h"
#include "../../common/fs_func.h"
#include "../server_global.h"
#include "../server_group_info.h"
#include "../dio/trunk_io_thread.h"
#include "../storage/storage_allocator.h"
#include "../storage/trunk_id_info.h"
#include "binlog_func.h"
#include "binlog_reader.h"
#include "binlog_loader.h"
#include "replica_binlog.h"

#define SLICE_EXPECT_FIELD_COUNT           8
#define BLOCK_EXPECT_FIELD_COUNT           6

#define MAX_BINLOG_FIELD_COUNT  8
#define MIN_EXPECT_FIELD_COUNT  BLOCK_EXPECT_FIELD_COUNT

typedef struct {
    SFBinlogWriterInfo **writers;
    SFBinlogWriterInfo *holders;
    int count;
    int base_id;
} BinlogWriterArray;

static BinlogWriterArray binlog_writer_array = {NULL, 0};
static SFBinlogWriterThread binlog_writer_thread;   //only one write thread

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
        int *record_len)
{
    char buff[FS_REPLICA_BINLOG_MAX_RECORD_SIZE];
    char error_info[256];
    string_t line;
    int64_t file_size;
    int result;

    if ((result=fc_get_last_line(filename, buff,
                    sizeof(buff), &file_size, &line)) != 0)
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
        logError("file: "__FILE__", line: %d, "
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
    SFBinlogWriterInfo *writer;
    char filename[PATH_MAX];
    int result;

    *data_version = 0;
    *record_len = 0;
    writer = binlog_writer_array.writers[data_group_id -
        binlog_writer_array.base_id];
    position->index = sf_binlog_get_current_write_index(writer);
    while (position->index >= 0) {
        sf_binlog_writer_get_filename(writer->cfg.subdir_name,
                position->index, filename, sizeof(filename));

        if ((result=replica_binlog_get_last_data_version_ex(filename,
                        data_version, position, record_len)) == 0)
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
    binlog_writer_array.holders = (SFBinlogWriterInfo *)fc_malloc(bytes);
    if (binlog_writer_array.holders == NULL) {
        return ENOMEM;
    }
    memset(binlog_writer_array.holders, 0, bytes);

    bytes = sizeof(SFBinlogWriterInfo *) * CLUSTER_DATA_RGOUP_ARRAY.count;
    binlog_writer_array.writers = (SFBinlogWriterInfo **)fc_malloc(bytes);
    if (binlog_writer_array.writers == NULL) {
        return ENOMEM;
    }
    memset(binlog_writer_array.writers, 0, bytes);

    binlog_writer_array.count = CLUSTER_DATA_RGOUP_ARRAY.count;
    return 0;
}

bool replica_binlog_set_data_version(FSClusterDataServerInfo *myself,
        const uint64_t new_version)
{
    SFBinlogWriterInfo *writer;
    uint64_t old_version;

    writer = binlog_writer_array.writers[myself->dg->id -
        binlog_writer_array.base_id];
    while (1) {
        old_version = __sync_fetch_and_add(&myself->data.version, 0);
        if (old_version == new_version) {
            break;
        }

        if (__sync_bool_compare_and_swap(&myself->data.version,
                    old_version, new_version))
        {
            sf_binlog_writer_change_next_version(writer, new_version + 1);
            return true;
        }
    }

    return false;
}

static int set_my_data_version(FSClusterDataServerInfo *myself)
{
    uint64_t old_version;
    uint64_t new_version;
    int result;

    if ((result=get_last_data_version_from_file(myself->dg->id,
                    &new_version)) != 0)
    {
        return result;
    }

    old_version = __sync_fetch_and_add(&myself->data.version, 0);
    if (replica_binlog_set_data_version(myself, new_version)) {
        logDebug("file: "__FILE__", line: %d, data_group_id: %d, "
                "old version: %"PRId64", new version: %"PRId64,
                __LINE__, myself->dg->id, old_version,
                myself->data.version);
    }

    return 0;
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

int replica_binlog_init()
{
    const bool use_fixed_buffer_size = true;
    FSIdArray *id_array;
    FSClusterDataServerInfo *myself;
    SFBinlogWriterInfo *writer;
    int data_group_id;
    int min_id;
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

    binlog_writer_array.base_id = min_id;
    writer = binlog_writer_array.holders;
    if ((result=sf_binlog_writer_init_thread_ex(&binlog_writer_thread,
                    writer, SF_BINLOG_THREAD_ORDER_MODE_FIXED,
                    SF_BINLOG_THREAD_TYPE_ORDER_BY_VERSION,
                    FS_REPLICA_BINLOG_MAX_RECORD_SIZE, id_array->count,
                    use_fixed_buffer_size)) != 0)
    {
        return result;
    }

    for (i=0; i<id_array->count; i++) {
        data_group_id = id_array->ids[i];
        if ((myself=fs_get_data_server(data_group_id, CLUSTER_MYSELF_PTR->
                        server->id)) == NULL)
        {
            return ENOENT;
        }

        writer->thread = &binlog_writer_thread;
        binlog_writer_array.writers[data_group_id - min_id] = writer;
        replica_binlog_get_subdir_name(subdir_name, data_group_id);
        if ((result=sf_binlog_writer_init_by_version(writer,
                        subdir_name, myself->data.version + 1,
                        BINLOG_BUFFER_SIZE, 1024)) != 0)
        {
            return result;
        }

        if ((result=set_my_data_version(myself)) != 0) {
            return result;
        }

        writer++;
    }

    return 0;
}

void replica_binlog_destroy()
{
    if (binlog_writer_array.count > 0) {
        sf_binlog_writer_finish(binlog_writer_array.writers[0]);
    }
}

SFBinlogWriterInfo *replica_binlog_get_writer(const int data_group_id)
{
    return binlog_writer_array.writers[data_group_id -
        binlog_writer_array.base_id];
}

int replica_binlog_get_current_write_index(const int data_group_id)
{
    SFBinlogWriterInfo *writer;
    writer = replica_binlog_get_writer(data_group_id);
    return sf_binlog_get_current_write_index(writer);
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
            BINLOG_COMMON_FIELD_INDEX_BLOCK_OID, ' ', 1);
    BINLOG_PARSE_INT_SILENCE(record->bs_key.block.offset, "block offset",
            BINLOG_COMMON_FIELD_INDEX_BLOCK_OFFSET, ' ', 0);
    BINLOG_PARSE_INT_SILENCE(record->bs_key.slice.offset, "slice offset",
            BINLOG_COMMON_FIELD_INDEX_SLICE_OFFSET, ' ', 0);
    BINLOG_PARSE_INT_SILENCE(record->bs_key.slice.length, "slice length",
            BINLOG_COMMON_FIELD_INDEX_SLICE_LENGTH, '\n', 1);
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
            BINLOG_COMMON_FIELD_INDEX_BLOCK_OID, ' ', 1);
    BINLOG_PARSE_INT_SILENCE(record->bs_key.block.offset, "block offset",
            BINLOG_COMMON_FIELD_INDEX_BLOCK_OFFSET, '\n', 0);
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
    if (count < MIN_EXPECT_FIELD_COUNT) {
        sprintf(error_info, "field count: %d < %d",
                count, MIN_EXPECT_FIELD_COUNT);
        return EINVAL;
    }

    record->source = cols[BINLOG_COMMON_FIELD_INDEX_SOURCE].str[0];
    record->op_type = cols[BINLOG_COMMON_FIELD_INDEX_OP_TYPE].str[0];
    BINLOG_PARSE_INT_SILENCE(record->data_version, "data version",
            BINLOG_COMMON_FIELD_INDEX_DATA_VERSION, ' ', 1);
    switch (record->op_type) {
        case REPLICA_BINLOG_OP_TYPE_WRITE_SLICE:
        case REPLICA_BINLOG_OP_TYPE_ALLOC_SLICE:
        case REPLICA_BINLOG_OP_TYPE_DEL_SLICE:
            result = unpack_slice_record(cols, count, record, error_info);
            break;
        case REPLICA_BINLOG_OP_TYPE_DEL_BLOCK:
        case REPLICA_BINLOG_OP_TYPE_NO_OP:
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
    *writer = binlog_writer_array.writers[data_group_id -
        binlog_writer_array.base_id];
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

    wbuffer->tag = source;
    wbuffer->bf.length = sprintf(wbuffer->bf.buff,
            "%"PRId64" %"PRId64" %c %c %"PRId64" %"PRId64" %d %d\n",
            (int64_t)current_time, data_version, source,
            op_type, bs_key->block.oid, bs_key->block.offset,
            bs_key->slice.offset, bs_key->slice.length);
    sf_push_to_binlog_thread_queue(writer->thread, wbuffer);
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

    wbuffer->tag = source;
    wbuffer->bf.length = sprintf(wbuffer->bf.buff,
            "%"PRId64" %"PRId64" %c %c %"PRId64" %"PRId64"\n",
            (int64_t)current_time, data_version,
            source, op_type, bkey->oid, bkey->offset);
    sf_push_to_binlog_thread_queue(writer->thread, wbuffer);
    return 0;
}

static int find_position_by_buffer(ServerBinlogReader *reader,
        const uint64_t last_data_version, SFBinlogFilePosition *pos)
{
    int result;
    char error_info[256];
    string_t line;
    char *line_end;
    ReplicaBinlogRecord record;

    while (reader->binlog_buffer.current < reader->binlog_buffer.end) {
        line_end = (char *)memchr(reader->binlog_buffer.current, '\n',
                reader->binlog_buffer.end - reader->binlog_buffer.current);
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

            file_offset = reader->position.offset - (reader->
                    binlog_buffer.end - reader->binlog_buffer.current);
            fc_get_file_line_count_ex(reader->filename,
                    file_offset, &line_count);
            logError("file: "__FILE__", line: %d, "
                    "binlog file %s, line no: %"PRId64", %s",
                    __LINE__, reader->filename, line_count, error_info);
            return result;
        }

        if (last_data_version < record.data_version) {
            pos->index = reader->position.index;
            pos->offset = reader->position.offset - (reader->
                    binlog_buffer.end - reader->binlog_buffer.current);
            return 0;
        }

        reader->binlog_buffer.current = line_end;
    }

    return EAGAIN;
}

static int find_position_by_reader(ServerBinlogReader *reader,
        const uint64_t last_data_version, SFBinlogFilePosition *pos)
{
    int result;

    while ((result=binlog_reader_read(reader)) == 0) {
        result = find_position_by_buffer(reader, last_data_version, pos);
        if (result != EAGAIN) {
            break;
        }
    }

    return result;
}

static int find_position(const char *subdir_name, SFBinlogWriterInfo *writer,
        const uint64_t last_data_version, SFBinlogFilePosition *pos,
        const bool ignore_dv_overflow)
{
    int result;
    int record_len;
    uint64_t data_version;
    char filename[PATH_MAX];
    ServerBinlogReader reader;

    sf_binlog_writer_get_filename(subdir_name, pos->index,
            filename, sizeof(filename));
    if ((result=replica_binlog_get_last_data_version_ex(filename,
                    &data_version, pos, &record_len)) != 0)
    {
        return result;
    }

    if (last_data_version == data_version) {  //match the last record
        if (pos->index < sf_binlog_get_current_write_index(writer)) {
            pos->index++; //skip to next binlog
            pos->offset = 0;
        } else {
            pos->offset += record_len;
        }
        return 0;
    }

    if (last_data_version > data_version) {
        if (pos->index < sf_binlog_get_current_write_index(writer)) {
            pos->index++;   //skip to next binlog
            pos->offset = 0;
            return 0;
        }

        if (ignore_dv_overflow) {
            pos->offset += record_len;
            return 0;
        }

        logError("file: "__FILE__", line: %d, subdir_name: %s, "
                "last_data_version: %"PRId64" is too large, which "
                " > the last data version %"PRId64" in the binlog file %s, "
                "binlog index: %d", __LINE__, subdir_name,
                last_data_version, data_version, filename, pos->index);
        return EOVERFLOW;
    }

    pos->offset = 0;
    if ((result=binlog_reader_init(&reader, subdir_name,
                    writer, pos)) != 0)
    {
        return result;
    }

    result = find_position_by_reader(&reader, last_data_version, pos);
    binlog_reader_destroy(&reader);
    return result;
}

int replica_binlog_get_position_by_dv(const char *subdir_name,
            SFBinlogWriterInfo *writer, const uint64_t last_data_version,
            SFBinlogFilePosition *pos, const bool ignore_dv_overflow)
{
    int result;
    int binlog_index;
    char filename[PATH_MAX];
    uint64_t first_data_version;

    binlog_index = sf_binlog_get_current_write_index(writer);
    while (binlog_index >= 0) {
        sf_binlog_writer_get_filename(subdir_name, binlog_index,
                filename, sizeof(filename));
        if ((result=replica_binlog_get_first_data_version(
                        filename, &first_data_version)) != 0)
        {
            if (result == ENOENT) {
                --binlog_index;
                continue;
            }

            return result;
        }

        if (last_data_version >= first_data_version) {
            pos->index = binlog_index;
            pos->offset = 0;
            return find_position(subdir_name, writer, last_data_version,
                    pos, ignore_dv_overflow);
        }

        --binlog_index;
    }

    pos->index = 0;
    pos->offset = 0;
    return 0;
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
    if (last_data_version == 0) {
        return binlog_reader_init(reader, subdir_name, writer, NULL);
    }

    if ((result=replica_binlog_get_position_by_dv(subdir_name,
                    writer, last_data_version, &position, false)) != 0)
    {
        return result;
    }

    return binlog_reader_init(reader, subdir_name, writer, &position);
}

const char *replica_binlog_get_op_type_caption(const int op_type)
{
    switch (op_type) {
        case REPLICA_BINLOG_OP_TYPE_WRITE_SLICE:
            return "write slice";
        case REPLICA_BINLOG_OP_TYPE_ALLOC_SLICE:
            return "alloc slice";
        case REPLICA_BINLOG_OP_TYPE_DEL_SLICE:
            return "delete slice";
        case REPLICA_BINLOG_OP_TYPE_DEL_BLOCK:
            return "delete block";
        case REPLICA_BINLOG_OP_TYPE_NO_OP:
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
    return sf_binlog_writer_get_last_lines(subdir_name,
            current_windex, buff, buff_size, count, length);
}

int replica_binlog_unpack_records(const string_t *buffer,
        ReplicaBinlogRecord *records, const int size, int *count)
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
            return EINVAL;
        }

        ++line_end;   //skip \n
        line.str = p;
        line.len = line_end - p;
        if ((result=replica_binlog_record_unpack(&line,
                        record++, error_info)) != 0)
        {
            logError("file: "__FILE__", line: %d, "
                    "binlog unpack fail, %s, binlog line: %.*s",
                    __LINE__, error_info, line.len, line.str);
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

    if (r1->op_type == REPLICA_BINLOG_OP_TYPE_DEL_BLOCK ||
        r1->op_type == REPLICA_BINLOG_OP_TYPE_NO_OP)
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
        const int master_rows, uint64_t *first_unmatched_dv)
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
        if (sr->data_version < mr->data_version) {
            sr++;
        } else if (sr->data_version == mr->data_version) {
            if (FS_IS_BINLOG_SOURCE_RPC(sr->source) &&
                    FS_IS_BINLOG_SOURCE_RPC(mr->source))
            {
                if (compare_record(sr, mr) != 0) {
                    *first_unmatched_dv = sr->data_version;
                    return SF_CLUSTER_ERROR_BINLOG_INCONSISTENT;
                }
            }
            sr++;
            mr++;
        } else {
            mr++;
        }
    }

    return 0;
}

int replica_binlog_check_consistency(const int data_group_id,
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

    if (sbuffer->len == 0) {
        return 0;
    }

    *first_unmatched_dv = 0;
    if ((result=replica_binlog_unpack_records(sbuffer, slave_records,
                    FS_MAX_SLAVE_BINLOG_CHECK_LAST_ROWS, &slave_rows)) != 0)
    {
        return result;
    }

    if ((result=replica_binlog_reader_init(&reader, data_group_id,
                    slave_records[0].data_version - 1)) != 0)
    {
        return result;
    }

    mbuffer.str = buff;
    result = binlog_reader_integral_read(&reader,
            buff, sizeof(buff), &mbuffer.len);
    binlog_reader_destroy(&reader);

    if (result == ENOENT || result == EAGAIN) {
        return 0;
    }

    if ((result=replica_binlog_unpack_records(&mbuffer, master_records,
                    FS_MAX_SLAVE_BINLOG_CHECK_LAST_ROWS, &master_rows)) != 0)
    {
        return result;
    }

    return check_records_consistency(slave_records, slave_rows,
            master_records, master_rows, first_unmatched_dv);
}
