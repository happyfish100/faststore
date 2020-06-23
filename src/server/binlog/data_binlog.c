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
#include "data_binlog.h"

#define DATA_BINLOG_OP_TYPE_WRITE_SLICE  'w'
#define DATA_BINLOG_OP_TYPE_ALLOC_SLICE  'a'
#define DATA_BINLOG_OP_TYPE_DEL_SLICE    'd'
#define DATA_BINLOG_OP_TYPE_DEL_BLOCK    'D'

#define BINLOG_COMMON_FIELD_INDEX_TIMESTAMP    0
#define BINLOG_COMMON_FIELD_INDEX_DATA_VERSION 1
#define BINLOG_COMMON_FIELD_INDEX_OP_TYPE      2

#define WRITE_SLICE_FIELD_INDEX_SLICE_TYPE       2
#define WRITE_SLICE_FIELD_INDEX_BLOCK_OID        3
#define WRITE_SLICE_FIELD_INDEX_BLOCK_OFFSET     4
#define WRITE_SLICE_FIELD_INDEX_SLICE_OFFSET     5
#define WRITE_SLICE_FIELD_INDEX_SLICE_LENGTH     6
#define WRITE_SLICE_FIELD_INDEX_SPACE_PATH_INDEX 7
#define WRITE_SLICE_FIELD_INDEX_SPACE_TRUNK_ID   8
#define WRITE_SLICE_FIELD_INDEX_SPACE_SUBDIR     9
#define WRITE_SLICE_FIELD_INDEX_SPACE_OFFSET    10
#define WRITE_SLICE_FIELD_INDEX_SPACE_SIZE      11
#define WRITE_SLICE_EXPECT_FIELD_COUNT          12

#define DEL_SLICE_FIELD_INDEX_BLOCK_OID        2
#define DEL_SLICE_FIELD_INDEX_BLOCK_OFFSET     3
#define DEL_SLICE_FIELD_INDEX_SLICE_OFFSET     4
#define DEL_SLICE_FIELD_INDEX_SLICE_LENGTH     5
#define DEL_SLICE_EXPECT_FIELD_COUNT           6

#define DEL_BLOCK_FIELD_INDEX_BLOCK_OID        2
#define DEL_BLOCK_FIELD_INDEX_BLOCK_OFFSET     3
#define DEL_BLOCK_EXPECT_FIELD_COUNT           4

#define MAX_BINLOG_FIELD_COUNT  16
#define MIN_EXPECT_FIELD_COUNT  DEL_BLOCK_EXPECT_FIELD_COUNT

typedef struct {
    BinlogWriterInfo **writers;
    BinlogWriterInfo *holders;
    int count;
    int base_id;
} BinlogWriterArray;

static BinlogWriterArray binlog_writer_array = {NULL, 0};
static BinlogWriterThread binlog_writer_thread;

static int alloc_binlog_writer_array(const int my_data_group_count)
{
    int bytes;

    bytes = sizeof(BinlogWriterInfo) * my_data_group_count;
    binlog_writer_array.holders = (BinlogWriterInfo *)malloc(bytes);
    if (binlog_writer_array.holders == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(binlog_writer_array.holders, 0, bytes);

    bytes = sizeof(BinlogWriterInfo *) * CLUSTER_DATA_RGOUP_ARRAY.count;
    binlog_writer_array.writers = (BinlogWriterInfo **)malloc(bytes);
    if (binlog_writer_array.writers == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(binlog_writer_array.writers, 0, bytes);

    binlog_writer_array.count = CLUSTER_DATA_RGOUP_ARRAY.count;
    return 0;
}

int data_binlog_init()
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
                        subdir_name, cs->data_version, 1024)) != 0)
        {
            return result;
        }
        writer->thread = &binlog_writer_thread;
        writer++;
    }

    return 0;
}

int data_binlog_get_current_write_index()
{
    /*
    BinlogWriterInfo *writer;
    BinlogWriterInfo *end;
    */
    
    //TODO
    //return binlog_get_current_write_index(&binlog_writer);
    return 0;
}

void data_binlog_destroy()
{
    /*
    BinlogWriterInfo *writer;
    BinlogWriterInfo *end;

    end = binlog_writer_array.writers + binlog_writer_array.count;
    for (writer=binlog_writer_array.writers; writer<end; writer++) {
        binlog_writer_finish(writer);
    }
    */
}

int data_binlog_log_write_slice(const int data_group_id,
        const int64_t data_version, const OBSliceEntry *slice)
{
    BinlogWriterInfo *writer;
    BinlogWriterBuffer *wbuffer;

    writer = binlog_writer_array.writers[data_group_id -
        binlog_writer_array.base_id];
    if ((wbuffer=binlog_writer_alloc_buffer(writer->thread)) == NULL) {
        return ENOMEM;
    }

    wbuffer->version = data_version;
    wbuffer->bf.length = sprintf(wbuffer->bf.buff,
            "%d %c %c %"PRId64" %"PRId64" %d %d "
            "%d %"PRId64" %"PRId64" %"PRId64" %"PRId64"\n",
            (int)g_current_time, DATA_BINLOG_OP_TYPE_WRITE_SLICE,
            slice->type, slice->ob->bkey.oid, slice->ob->bkey.offset,
            slice->ssize.offset, slice->ssize.length,
            slice->space.store->index, slice->space.id_info.id,
            slice->space.id_info.subdir, slice->space.offset,
            slice->space.size);
    push_to_binlog_write_queue(writer->thread, wbuffer);
    return 0;
}

int data_binlog_log_del_slice(const int data_group_id,
        const int64_t data_version, const FSBlockSliceKeyInfo *bs_key)
{
    BinlogWriterInfo *writer;
    BinlogWriterBuffer *wbuffer;

    writer = binlog_writer_array.writers[data_group_id -
        binlog_writer_array.base_id];
    if ((wbuffer=binlog_writer_alloc_buffer(writer->thread)) == NULL) {
        return ENOMEM;
    }

    wbuffer->version = data_version;
    wbuffer->bf.length = sprintf(wbuffer->bf.buff,
            "%d %c %"PRId64" %"PRId64" %d %d\n",
            (int)g_current_time, DATA_BINLOG_OP_TYPE_DEL_SLICE,
            bs_key->block.oid, bs_key->block.offset,
            bs_key->slice.offset, bs_key->slice.length);
    push_to_binlog_write_queue(writer->thread, wbuffer);
    return 0;
}

int data_binlog_log_del_block(const int data_group_id,
        const int64_t data_version, const FSBlockKey *bkey)
{
    BinlogWriterInfo *writer;
    BinlogWriterBuffer *wbuffer;

    writer = binlog_writer_array.writers[data_group_id -
        binlog_writer_array.base_id];
    if ((wbuffer=binlog_writer_alloc_buffer(writer->thread)) == NULL) {
        return ENOMEM;
    }

    wbuffer->version = data_version;
    wbuffer->bf.length = sprintf(wbuffer->bf.buff,
            "%d %c %"PRId64" %"PRId64"\n",
            (int)g_current_time, DATA_BINLOG_OP_TYPE_DEL_BLOCK,
            bkey->oid, bkey->offset);
    push_to_binlog_write_queue(writer->thread, wbuffer);
    return 0;
}
