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

static int get_last_data_version_from_file(const int data_group_id,
        uint64_t *data_version)
{
    BinlogWriterInfo *writer;
    char filename[PATH_MAX];
    char buff[FS_REPLICA_BINLOG_MAX_RECORD_SIZE];
    char error_info[256];
    string_t line;
    ReplicaBinlogRecord record;
    int result;
    int binlog_index;
    int64_t file_size;
    int64_t offset;
    int64_t read_bytes;

    *data_version = 0;
    writer = binlog_writer_array.writers[data_group_id -
        binlog_writer_array.base_id];
    binlog_index = binlog_get_current_write_index(writer);
    while (binlog_index >= 0) {
        binlog_writer_get_filename(writer, binlog_index,
                filename, sizeof(filename));
        if ((result=getFileSize(filename, &file_size)) != 0) {
            return result;
        }
        if (file_size == 0) {
            binlog_index--;
            continue;
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
        }
        line.len = (buff + read_bytes) - line.str;
        if ((result=replica_binlog_record_unpack(&line,
                        &record, error_info)) != 0)
        {
            int64_t line_count;
            fc_get_file_line_count(filename, &line_count);
            logError("file: "__FILE__", line: %d, "
                    "binlog file %s, line no: %"PRId64", %s",
                    __LINE__, filename, line_count, error_info);

            return result;
        }

        *data_version = record.data_version;
        break;
    }

    return 0;
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
    FSClusterDataServerInfo *cs;
    BinlogWriterInfo *writer;
    int data_group_id;
    int min_id;
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
        if ((cs=fs_get_data_server(data_group_id, CLUSTER_MYSELF_PTR->
                        server->id)) == NULL)
        {
            return ENOENT;
        }

        binlog_writer_array.writers[data_group_id - min_id] = writer;
        sprintf(subdir_name, "%s/%d", FS_REPLICA_BINLOG_SUBDIR_NAME,
                data_group_id);
        if ((result=binlog_writer_init_by_version(writer,
                        subdir_name, cs->data_version + 1, 1024)) != 0)
        {
            return result;
        }

        if ((result=get_last_data_version_from_file(data_group_id,
                        &cs->data_version)) != 0)
        {
            return result;
        }

        if (cs->data_version > 0) {
            logInfo("=====data_group_id: %d, data_version: %"PRId64" =====",
                    data_group_id, cs->data_version);
        }

        writer->thread = &binlog_writer_thread;
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

int replica_binlog_get_current_write_index(const int data_group_id)
{
    BinlogWriterInfo *writer;
    writer = binlog_writer_array.writers[data_group_id -
        binlog_writer_array.base_id];
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
    BinlogWriterBuffer *wbuffer;

    *writer = binlog_writer_array.writers[data_group_id -
        binlog_writer_array.base_id];
    if ((wbuffer=binlog_writer_alloc_buffer((*writer)->thread)) != NULL) {
        wbuffer->writer = *writer;
        wbuffer->version = data_version;
    }

    return wbuffer;
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

int replica_binlog_log_del_block(const int data_group_id,
        const int64_t data_version, const FSBlockKey *bkey)
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
            REPLICA_BINLOG_OP_TYPE_DEL_BLOCK,
            bkey->oid, bkey->offset);
    push_to_binlog_write_queue(writer->thread, wbuffer);
    return 0;
}
