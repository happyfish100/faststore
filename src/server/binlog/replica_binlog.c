#include <limits.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "../../common/fs_func.h"
#include "../server_global.h"
#include "../server_group_info.h"
#include "../dio/trunk_io_thread.h"
#include "../storage/storage_allocator.h"
#include "../storage/trunk_id_info.h"
#include "binlog_reader.h"
#include "binlog_writer.h"
#include "binlog_loader.h"
#include "replica_binlog.h"

#define BINLOG_COMMON_FIELD_INDEX_TIMESTAMP    0
#define BINLOG_COMMON_FIELD_INDEX_DATA_VERSION 1
#define BINLOG_COMMON_FIELD_INDEX_OP_TYPE      2

#define SLICE_FIELD_INDEX_BLOCK_OID        3
#define SLICE_FIELD_INDEX_BLOCK_OFFSET     4
#define SLICE_FIELD_INDEX_SLICE_OFFSET     5
#define SLICE_FIELD_INDEX_SLICE_LENGTH     6
#define SLICE_EXPECT_FIELD_COUNT           7

#define BLOCK_FIELD_INDEX_BLOCK_OID        3
#define BLOCK_FIELD_INDEX_BLOCK_OFFSET     4
#define BLOCK_EXPECT_FIELD_COUNT           5

#define MAX_BINLOG_FIELD_COUNT  8
#define MIN_EXPECT_FIELD_COUNT  BLOCK_EXPECT_FIELD_COUNT

#define REPLICA_BINLOG_PARSE_INT(var, caption, index, endchr, min_val) \
    do {   \
        var = strtol(cols[index].str, &endptr, 10);  \
        if (*endptr != endchr || var < min_val) {    \
            sprintf(error_info, "invalid %s: %.*s",  \
                    caption, cols[index].len, cols[index].str); \
            return EINVAL;  \
        }  \
    } while (0)

typedef struct {
    BinlogWriterInfo **writers;
    BinlogWriterInfo *holders;
    int count;
    int base_id;
} BinlogWriterArray;

static BinlogWriterArray binlog_writer_array = {NULL, 0};
static BinlogWriterThread binlog_writer_thread;   //only one write thread

static int get_first_data_version_from_file(const int data_group_id,
        const int binlog_index, uint64_t *data_version)
{
    BinlogWriterInfo *writer;
    char filename[PATH_MAX];
    char buff[FS_REPLICA_BINLOG_MAX_RECORD_SIZE];
    char error_info[256];
    string_t line;
    char *line_end;
    ReplicaBinlogRecord record;
    int result;
    int64_t read_bytes;

    *data_version = 0;
    writer = binlog_writer_array.writers[data_group_id -
        binlog_writer_array.base_id];
    binlog_writer_get_filename(writer->cfg.subdir_name,
            binlog_index, filename, sizeof(filename));

    read_bytes = FS_REPLICA_BINLOG_MAX_RECORD_SIZE - 1;
    if ((result=getFileContentEx(filename, buff, 0, &read_bytes)) != 0) {
        return result;
    }
    if (read_bytes == 0) {
        return ENOENT;
    }

    line_end = (char *)memchr(buff, '\n', read_bytes);
    if (line_end == NULL) {
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: 1, "
                "expect new line char \"\\n\"",
                __LINE__, filename);
        return EINVAL;
    }
    line.str = buff;
    line.len = line_end - buff + 1;
    if ((result=replica_binlog_record_unpack(&line,
                    &record, error_info)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: 1, %s",
                __LINE__, filename, error_info);
        return result;
    }

    *data_version = record.data_version;
    return 0;
}

int replica_binlog_get_last_record_ex(const char *filename,
        ReplicaBinlogRecord *record, FSBinlogFilePosition *position,
        int *record_len)
{
    char buff[FS_REPLICA_BINLOG_MAX_RECORD_SIZE];
    char error_info[256];
    string_t line;
    int64_t file_size;
    int64_t offset;
    int64_t read_bytes;
    int result;

    position->offset = 0;
    *record_len = 0;
    if ((result=getFileSize(filename, &file_size)) != 0) {
        return result;
    }

    if (file_size == 0) {
        return ENOENT;
    }

    if (file_size >= FS_REPLICA_BINLOG_MAX_RECORD_SIZE) {
        offset = file_size - FS_REPLICA_BINLOG_MAX_RECORD_SIZE + 1;
    } else {
        offset = 0;
    }
    read_bytes = (file_size - offset) + 1;
    if ((result=getFileContentEx(filename, buff,
                    offset, &read_bytes)) != 0)
    {
        return result;
    }

    line.str = (char *)fc_memrchr(buff, '\n', read_bytes - 1);
    if (line.str == NULL) {
        line.str = buff;
    } else {
        line.str += 1;  //skip \n
    }
    line.len = *record_len = (buff + read_bytes) - line.str;
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
        uint64_t *data_version, FSBinlogFilePosition *position,
        int *record_len)
{
    BinlogWriterInfo *writer;
    char filename[PATH_MAX];
    int result;

    *data_version = 0;
    *record_len = 0;
    writer = binlog_writer_array.writers[data_group_id -
        binlog_writer_array.base_id];
    position->index = binlog_get_current_write_index(writer);
    while (position->index >= 0) {
        binlog_writer_get_filename(writer->cfg.subdir_name,
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
    FSBinlogFilePosition position;
    int record_len;

    return get_last_data_version_from_file_ex(data_group_id,
            data_version, &position, &record_len);
}

static int alloc_binlog_writer_array(const int my_data_group_count)
{
    int bytes;

    bytes = sizeof(BinlogWriterInfo) * my_data_group_count;
    binlog_writer_array.holders = (BinlogWriterInfo *)fc_malloc(bytes);
    if (binlog_writer_array.holders == NULL) {
        return ENOMEM;
    }
    memset(binlog_writer_array.holders, 0, bytes);

    bytes = sizeof(BinlogWriterInfo *) * CLUSTER_DATA_RGOUP_ARRAY.count;
    binlog_writer_array.writers = (BinlogWriterInfo **)fc_malloc(bytes);
    if (binlog_writer_array.writers == NULL) {
        return ENOMEM;
    }
    memset(binlog_writer_array.writers, 0, bytes);

    binlog_writer_array.count = CLUSTER_DATA_RGOUP_ARRAY.count;
    return 0;
}

int replica_binlog_init()
{
    FSIdArray *id_array;
    FSClusterDataServerInfo *myself;
    BinlogWriterInfo *writer;
    int data_group_id;
    int min_id;
    uint64_t data_version;
    char filepath[PATH_MAX];
    char subdir_name[64];
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
    if ((result=binlog_writer_init_thread_ex(&binlog_writer_thread,
                    writer, FS_BINLOG_WRITER_TYPE_ORDER_BY_VERSION,
                    FS_REPLICA_BINLOG_MAX_RECORD_SIZE, id_array->count)) != 0)
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
        sprintf(subdir_name, "%s/%d", FS_REPLICA_BINLOG_SUBDIR_NAME,
                data_group_id);

        if ((result=binlog_writer_init_by_version(writer, subdir_name,
                        myself->replica.data_version + 1, 1024)) != 0)
        {
            return result;
        }

        if ((result=get_last_data_version_from_file(data_group_id,
                        &data_version)) != 0)
        {
            return result;
        }

        replica_binlog_set_data_version(myself, data_version);
        if (myself->replica.data_version > 0) {
            logInfo("=====line: %d, data_group_id: %d, data_version: %"PRId64" =====",
                    __LINE__, data_group_id, myself->replica.data_version);
        }

        writer++;
    }

    return 0;
}

void replica_binlog_destroy()
{
    if (binlog_writer_array.count > 0) {
        binlog_writer_finish(binlog_writer_array.writers[0]);
    }
}

struct binlog_writer_info *replica_binlog_get_writer(const int data_group_id)
{
    return binlog_writer_array.writers[data_group_id -
        binlog_writer_array.base_id];
}

int replica_binlog_get_current_write_index(const int data_group_id)
{
    BinlogWriterInfo *writer;
    writer = replica_binlog_get_writer(data_group_id);
    return binlog_get_current_write_index(writer);
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

    REPLICA_BINLOG_PARSE_INT(record->bs_key.block.oid, "object ID",
            SLICE_FIELD_INDEX_BLOCK_OID, ' ', 1);
    REPLICA_BINLOG_PARSE_INT(record->bs_key.block.offset, "block offset",
            SLICE_FIELD_INDEX_BLOCK_OFFSET, ' ', 0);
    REPLICA_BINLOG_PARSE_INT(record->bs_key.slice.offset, "slice offset",
            SLICE_FIELD_INDEX_SLICE_OFFSET, ' ', 0);
    REPLICA_BINLOG_PARSE_INT(record->bs_key.slice.length, "slice length",
            SLICE_FIELD_INDEX_SLICE_LENGTH, '\n', 1);
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

    REPLICA_BINLOG_PARSE_INT(record->bs_key.block.oid, "object ID",
            BLOCK_FIELD_INDEX_BLOCK_OID, ' ', 1);
    REPLICA_BINLOG_PARSE_INT(record->bs_key.block.offset, "block offset",
            BLOCK_FIELD_INDEX_BLOCK_OFFSET, '\n', 0);
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

    record->op_type = cols[BINLOG_COMMON_FIELD_INDEX_OP_TYPE].str[0];
    REPLICA_BINLOG_PARSE_INT(record->data_version, "data version",
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

static BinlogWriterBuffer *alloc_binlog_buffer(const int data_group_id,
        const int64_t data_version, BinlogWriterInfo **writer)
{
    *writer = binlog_writer_array.writers[data_group_id -
        binlog_writer_array.base_id];
    return binlog_writer_alloc_versioned_buffer(*writer, data_version);
}

int replica_binlog_log_slice(const int data_group_id, const int64_t data_version,
        const FSBlockSliceKeyInfo *bs_key, const int op_type)
{
    BinlogWriterInfo *writer;
    BinlogWriterBuffer *wbuffer;

    if ((wbuffer=alloc_binlog_buffer(data_group_id,
                    data_version, &writer)) == NULL)
    {
        return ENOMEM;
    }

    wbuffer->bf.length = sprintf(wbuffer->bf.buff,
            "%d %"PRId64" %c %"PRId64" %"PRId64" %d %d\n",
            (int)g_current_time, data_version, op_type,
            bs_key->block.oid, bs_key->block.offset,
            bs_key->slice.offset, bs_key->slice.length);
    push_to_binlog_write_queue(writer->thread, wbuffer);
    return 0;
}

int replica_binlog_log_block(const int data_group_id,
        const int64_t data_version, const FSBlockKey *bkey,
        const int op_type)
{
    BinlogWriterInfo *writer;
    BinlogWriterBuffer *wbuffer;

    if ((wbuffer=alloc_binlog_buffer(data_group_id,
                    data_version, &writer)) == NULL)
    {
        return ENOMEM;
    }

    wbuffer->bf.length = sprintf(wbuffer->bf.buff,
            "%d %"PRId64" %c %"PRId64" %"PRId64"\n",
            (int)g_current_time, data_version,
            op_type, bkey->oid, bkey->offset);
    push_to_binlog_write_queue(writer->thread, wbuffer);
    return 0;
}

static int find_position_by_buffer(ServerBinlogReader *reader,
        const uint64_t last_data_version, FSBinlogFilePosition *pos)
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
        const uint64_t last_data_version, FSBinlogFilePosition *pos)
{
    int result;

    while ((result=binlog_reader_read(reader)) == 0) {
        result = find_position_by_buffer(reader, last_data_version, pos);
        if (result != EAGAIN) {
            return result;
        }
    }

    return result;
}

static int find_position(const int data_group_id,
        const uint64_t last_data_version, FSBinlogFilePosition *pos)
{
    int result;
    int record_len;
    uint64_t data_version;
    ServerBinlogReader reader;
    BinlogWriterInfo *writer;
    char subdir_name[64];

    if ((result=get_last_data_version_from_file_ex(data_group_id,
                    &data_version, pos, &record_len)) != 0)
    {
        return result;
    }

    if (last_data_version == data_version) {
        pos->offset += record_len;
        return 0;
    }

    if (last_data_version > data_version) {
        logError("file: "__FILE__", line: %d, data_group_id: %d, "
                "last_data_version: %"PRId64" is too large "
                " > the last data version %"PRId64" in the binlog file, "
                "binlog index: %d", __LINE__, data_group_id,
                last_data_version, data_version, pos->index);
        return EINVAL;
    }

    sprintf(subdir_name, "%s/%d", FS_REPLICA_BINLOG_SUBDIR_NAME,
            data_group_id);
    writer = replica_binlog_get_writer(data_group_id);
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

static int find_position_by_data_version(const int data_group_id,
        const uint64_t last_data_version, FSBinlogFilePosition *pos)
{
    int result;
    int binlog_index;
    BinlogWriterInfo *writer;
    uint64_t first_data_version;

    writer = binlog_writer_array.writers[data_group_id -
        binlog_writer_array.base_id];
    binlog_index = binlog_get_current_write_index(writer);
    while (binlog_index >= 0) {
        if ((result=get_first_data_version_from_file(data_group_id,
                        binlog_index, &first_data_version)) != 0)
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
            return find_position(data_group_id, last_data_version, pos);
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
    char subdir_name[64];
    BinlogWriterInfo *writer;
    FSBinlogFilePosition position;

    sprintf(subdir_name, "%s/%d", FS_REPLICA_BINLOG_SUBDIR_NAME,
            data_group_id);
    writer = replica_binlog_get_writer(data_group_id);
    if (last_data_version == 0) {
        return binlog_reader_init(reader, subdir_name, writer, NULL);
    }

    if ((result=find_position_by_data_version(data_group_id,
                    last_data_version, &position)) != 0)
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

void replica_binlog_set_data_version(FSClusterDataServerInfo *myself,
        const uint64_t new_version)
{
    BinlogWriterInfo *writer;
    uint64_t old_version;

    writer = binlog_writer_array.writers[myself->dg->id -
        binlog_writer_array.base_id];

    while (1) {
        old_version = __sync_fetch_and_add(&myself->replica.data_version, 0);
        if (old_version == new_version) {
            break;
        }

        if (__sync_bool_compare_and_swap(&myself->replica.data_version,
                    old_version, new_version))
        {
            binlog_writer_change_next_version(writer, new_version + 1);
            break;
        }
    }
}
