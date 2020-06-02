#include <limits.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "../dio/trunk_io_thread.h"
#include "storage_allocator.h"
#include "trunk_id_info.h"
#include "../binlog/binlog_writer.h"
#include "../binlog/binlog_loader.h"
#include "slice_binlog.h"

#define SLICE_BINLOG_MAX_RECORD_SIZE   256
#define SLICE_BINLOG_SUBDIR_NAME     "slice"

#define SLICE_BINLOG_OP_TYPE_ADD_SLICE  'a'
#define SLICE_BINLOG_OP_TYPE_DEL_SLICE  'd'
#define SLICE_BINLOG_OP_TYPE_DEL_BLOCK  'D'

static BinlogWriterContext binlog_writer = {NULL, NULL, 0, 0};

#define SLICE_GET_FILENAME_LINE_COUNT(r, binlog_filename, \
        line_str, line_count) \
        BINLOG_GET_FILENAME_LINE_COUNT(r, SLICE_BINLOG_SUBDIR_NAME, \
        binlog_filename, line_str, line_count)

#define SLICE_PARSE_INT_EX(var, caption, index, endchr, min_val) \
    BINLOG_PARSE_INT_EX(SLICE_BINLOG_SUBDIR_NAME, var, caption,  \
            index, endchr, min_val)

#define SLICE_PARSE_INT(var, index, endchr, min_val)  \
    BINLOG_PARSE_INT_EX(SLICE_BINLOG_SUBDIR_NAME, var, #var, \
            index, endchr, min_val)

static int slice_parse_line(BinlogReadThreadResult *r,
        string_t *line, char *line_end)
{
    /*
#define MAX_FIELD_COUNT     8
#define EXPECT_FIELD_COUNT  6
#define FIELD_INDEX_TIMESTAMP   0
#define FIELD_INDEX_OP_TYPE     1
#define FIELD_INDEX_PATH_INDEX  2
#define FIELD_INDEX_TRUNK_ID    3
#define FIELD_INDEX_SUBDIR      4
#define FIELD_INDEX_TRUNK_SIZE  5

    int result;
    int count;
    int64_t line_count;
    string_t cols[MAX_FIELD_COUNT];
    char binlog_filename[PATH_MAX];
    char *endptr;
    char op_type;
    int path_index;
    FSTrunkIdInfo id_info;
    int64_t trunk_size;

    count = split_string_ex(line, ' ', cols,
            MAX_FIELD_COUNT, false);
    if (count < EXPECT_FIELD_COUNT) {
        SLICE_GET_FILENAME_LINE_COUNT(r, binlog_filename,
                line->str, line_count);
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", "
                "field count: %d < %d", __LINE__,
                binlog_filename, line_count,
                count, EXPECT_FIELD_COUNT);
        return EINVAL;
    }

    op_type = cols[FIELD_INDEX_OP_TYPE].str[0];
    SLICE_PARSE_INT(path_index, FIELD_INDEX_PATH_INDEX, ' ', 0);
    if (path_index > STORAGE_CFG.max_store_path_index) {
        SLICE_GET_FILENAME_LINE_COUNT(r, binlog_filename,
                line->str, line_count);
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", "
                "invalid path_index: %d > max_store_path_index: %d",
                __LINE__, binlog_filename, line_count,
                path_index, STORAGE_CFG.max_store_path_index);
        return EINVAL;
    }

    SLICE_PARSE_INT_EX(id_info.id, "trunk_id", FIELD_INDEX_TRUNK_ID, ' ', 1);
    SLICE_PARSE_INT_EX(id_info.subdir, "subdir", FIELD_INDEX_SUBDIR, ' ', 1);
    SLICE_PARSE_INT(trunk_size, FIELD_INDEX_TRUNK_SIZE,
            '\n', FS_TRUNK_FILE_MIN_SIZE);
    if (trunk_size > FS_TRUNK_FILE_MAX_SIZE) {
        SLICE_GET_FILENAME_LINE_COUNT(r, binlog_filename,
                line->str, line_count);
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", "
                "invalid trunk size: %"PRId64, __LINE__,
                binlog_filename, line_count, trunk_size);
        return EINVAL;
    }

    if (op_type == FS_IO_TYPE_CREATE_TRUNK) {
       if ((result=storage_allocator_add_trunk(path_index,
                       &id_info, trunk_size)) != 0)
       {
           return result;
       }
    } else if (op_type == FS_IO_TYPE_DELETE_TRUNK) {
        if ((result=storage_allocator_delete_trunk(path_index,
                        &id_info)) != 0)
        {
            return result;
        }
    } else {
        SLICE_GET_FILENAME_LINE_COUNT(r, binlog_filename,
                line->str, line_count);
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", "
                "invalid op_type: %c (0x%02x)", __LINE__,
                binlog_filename, line_count,
                op_type, (unsigned char)op_type);
        return EINVAL;
    }
*/

    return 0;
}

static int init_binlog_writer()
{
    return binlog_writer_init(&binlog_writer, SLICE_BINLOG_SUBDIR_NAME,
            SLICE_BINLOG_MAX_RECORD_SIZE);
}

int slice_binlog_get_current_write_index()
{
    return binlog_get_current_write_index(&binlog_writer);
}

int slice_binlog_init()
{
    int result;

    if ((result=init_binlog_writer()) != 0) {
        return result;
    }

    return binlog_loader_load(SLICE_BINLOG_SUBDIR_NAME,
                    slice_binlog_get_current_write_index,
                    slice_parse_line);
}

void slice_binlog_destroy()
{
    binlog_writer_finish(&binlog_writer);
}

#define COMMON_FIELD_INDEX_TIMESTAMP   0
#define COMMON_FIELD_INDEX_OP_TYPE     1

#define ADD_SLICE_FIELD_INDEX_SLICE_TYPE       2
#define ADD_SLICE_FIELD_INDEX_BLOCK_OID        3
#define ADD_SLICE_FIELD_INDEX_BLOCK_OFFSET     4
#define ADD_SLICE_FIELD_INDEX_SLICE_OFFSET     5
#define ADD_SLICE_FIELD_INDEX_SLICE_LENGTH     6
#define ADD_SLICE_FIELD_INDEX_SPACE_PATH_INDEX 7
#define ADD_SLICE_FIELD_INDEX_SPACE_TRUNK_ID   8
#define ADD_SLICE_FIELD_INDEX_SPACE_SUBDIR     9
#define ADD_SLICE_FIELD_INDEX_SPACE_OFFSET    10
#define ADD_SLICE_FIELD_INDEX_SPACE_SIZE      11
#define ADD_SLICE_EXPECT_FIELD_COUNT          12
#define ADD_SLICE_MAX_FIELD_COUNT             16

int slice_binlog_log_add_slice(const OBSliceEntry *slice)
{
    BinlogWriterBuffer *wbuffer;

    if ((wbuffer=binlog_writer_alloc_buffer(&binlog_writer)) == NULL) {
        return ENOMEM;
    }

    wbuffer->bf.length = sprintf(wbuffer->bf.buff,
            "%d %c %c %"PRId64" %"PRId64" %d %d "
            "%d %"PRId64" %"PRId64" %"PRId64" %"PRId64"\n",
            (int)g_current_time, SLICE_BINLOG_OP_TYPE_ADD_SLICE,
            slice->type, slice->ob->bkey.oid, slice->ob->bkey.offset,
            slice->ssize.offset, slice->ssize.length,
            slice->space.store->index, slice->space.id_info.id,
            slice->space.id_info.subdir, slice->space.offset,
            slice->space.size);
    push_to_binlog_write_queue(&binlog_writer, wbuffer);
    return 0;
}

int slice_binlog_log_del_slice(const FSBlockSliceKeyInfo *bs_key)
{
    BinlogWriterBuffer *wbuffer;

    if ((wbuffer=binlog_writer_alloc_buffer(&binlog_writer)) == NULL) {
        return ENOMEM;
    }

    wbuffer->bf.length = sprintf(wbuffer->bf.buff,
            "%d %c %"PRId64" %"PRId64" %d %d\n",
            (int)g_current_time, SLICE_BINLOG_OP_TYPE_DEL_SLICE,
            bs_key->block.oid, bs_key->block.offset,
            bs_key->slice.offset, bs_key->slice.length);
    push_to_binlog_write_queue(&binlog_writer, wbuffer);
    return 0;
}

int slice_binlog_log_del_block(const FSBlockKey *bkey)
{
    BinlogWriterBuffer *wbuffer;

    if ((wbuffer=binlog_writer_alloc_buffer(&binlog_writer)) == NULL) {
        return ENOMEM;
    }

    wbuffer->bf.length = sprintf(wbuffer->bf.buff,
            "%d %c %"PRId64" %"PRId64"\n",
            (int)g_current_time, SLICE_BINLOG_OP_TYPE_DEL_BLOCK,
            bkey->oid, bkey->offset);
    push_to_binlog_write_queue(&binlog_writer, wbuffer);
    return 0;
}
