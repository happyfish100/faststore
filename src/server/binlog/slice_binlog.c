#include <limits.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "../../common/fs_func.h"
#include "../server_global.h"
#include "../dio/trunk_io_thread.h"
#include "../storage/storage_allocator.h"
#include "../storage/trunk_id_info.h"
#include "binlog_writer.h"
#include "binlog_loader.h"
#include "slice_binlog.h"

#define SLICE_BINLOG_OP_TYPE_ADD_SLICE  'a'
#define SLICE_BINLOG_OP_TYPE_DEL_SLICE  'd'
#define SLICE_BINLOG_OP_TYPE_DEL_BLOCK  'D'

#define BINLOG_COMMON_FIELD_INDEX_TIMESTAMP    0
#define BINLOG_COMMON_FIELD_INDEX_OP_TYPE      1

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

static BinlogWriterContext binlog_writer = {NULL, NULL, 0, 0};

#define SLICE_GET_FILENAME_LINE_COUNT(r, binlog_filename, \
        line_str, line_count) \
        BINLOG_GET_FILENAME_LINE_COUNT(r, FS_SLICE_BINLOG_SUBDIR_NAME, \
        binlog_filename, line_str, line_count)

#define SLICE_PARSE_INT_EX(var, caption, index, endchr, min_val) \
    BINLOG_PARSE_INT_EX(FS_SLICE_BINLOG_SUBDIR_NAME, var, caption,  \
            index, endchr, min_val)

#define SLICE_PARSE_INT(var, index, endchr, min_val)  \
    BINLOG_PARSE_INT_EX(FS_SLICE_BINLOG_SUBDIR_NAME, var, #var, \
            index, endchr, min_val)

static int add_slice(BinlogReadThreadResult *r, string_t *line,
        string_t *cols, const int count)
{
    FSBlockKey bkey;
    OBSliceEntry *slice;
    int64_t line_count;
    char binlog_filename[PATH_MAX];
    char *endptr;
    int path_index;
    char slice_type;

    if (count != ADD_SLICE_EXPECT_FIELD_COUNT) {
        SLICE_GET_FILENAME_LINE_COUNT(r, binlog_filename,
                line->str, line_count);
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", "
                "field count: %d != %d", __LINE__,
                binlog_filename, line_count,
                count, ADD_SLICE_EXPECT_FIELD_COUNT);
        return EINVAL;
    }

    slice_type = cols[ADD_SLICE_FIELD_INDEX_SLICE_TYPE].str[0];
    if (!(slice_type == OB_SLICE_TYPE_FILE ||
                slice_type == OB_SLICE_TYPE_ALLOC))
    {
        SLICE_GET_FILENAME_LINE_COUNT(r, binlog_filename,
                line->str, line_count);
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", "
                "invalid slice type: %c (0x%02x)", __LINE__,
                binlog_filename, line_count, slice_type, slice_type);
        return EINVAL;
    }

    SLICE_PARSE_INT_EX(bkey.oid, "object ID",
            ADD_SLICE_FIELD_INDEX_BLOCK_OID, ' ', 1);
    SLICE_PARSE_INT_EX(bkey.offset, "block offset",
            ADD_SLICE_FIELD_INDEX_BLOCK_OFFSET, ' ', 0);
    fs_calc_block_hashcode(&bkey);
    if ((slice=ob_index_alloc_slice(&bkey)) == NULL) {
        return ENOMEM;
    }

    slice->read_offset = 0;
    slice->type = slice_type;
    SLICE_PARSE_INT_EX(slice->ssize.offset, "slice offset",
            ADD_SLICE_FIELD_INDEX_SLICE_OFFSET, ' ', 0);
    SLICE_PARSE_INT_EX(slice->ssize.length, "slice length",
            ADD_SLICE_FIELD_INDEX_SLICE_LENGTH, ' ', 1);

    SLICE_PARSE_INT(path_index, ADD_SLICE_FIELD_INDEX_SPACE_PATH_INDEX, ' ', 0);
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

    if (PATHS_BY_INDEX_PPTR[path_index] == NULL) {
        SLICE_GET_FILENAME_LINE_COUNT(r, binlog_filename,
                line->str, line_count);
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", "
                "path_index: %d not exist", __LINE__,
                binlog_filename, line_count, path_index);
        return ENOENT;
    }
    slice->space.store = &PATHS_BY_INDEX_PPTR[path_index]->store;
    SLICE_PARSE_INT_EX(slice->space.id_info.id, "trunk_id",
            ADD_SLICE_FIELD_INDEX_SPACE_TRUNK_ID, ' ', 1);
    SLICE_PARSE_INT_EX(slice->space.id_info.subdir, "subdir",
            ADD_SLICE_FIELD_INDEX_SPACE_SUBDIR, ' ', 1);
    SLICE_PARSE_INT(slice->space.offset,
            ADD_SLICE_FIELD_INDEX_SPACE_OFFSET, ' ', 0);
    SLICE_PARSE_INT(slice->space.size,
            ADD_SLICE_FIELD_INDEX_SPACE_SIZE, '\n', 0);

    return ob_index_add_slice_by_binlog(slice);
}

static int del_slice(BinlogReadThreadResult *r, string_t *line,
        string_t *cols, const int count)
{
    FSBlockSliceKeyInfo bs_key;
    int64_t line_count;
    char binlog_filename[PATH_MAX];
    char *endptr;

    if (count != DEL_SLICE_EXPECT_FIELD_COUNT) {
        SLICE_GET_FILENAME_LINE_COUNT(r, binlog_filename,
                line->str, line_count);
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", "
                "field count: %d != %d", __LINE__,
                binlog_filename, line_count,
                count, DEL_SLICE_EXPECT_FIELD_COUNT);
        return EINVAL;
    }

    SLICE_PARSE_INT_EX(bs_key.block.oid, "object ID",
            DEL_SLICE_FIELD_INDEX_BLOCK_OID, ' ', 1);
    SLICE_PARSE_INT_EX(bs_key.block.offset, "block offset",
            DEL_SLICE_FIELD_INDEX_BLOCK_OFFSET, ' ', 0);
    SLICE_PARSE_INT_EX(bs_key.slice.offset, "slice offset",
            DEL_SLICE_FIELD_INDEX_SLICE_OFFSET, ' ', 0);
    SLICE_PARSE_INT_EX(bs_key.slice.length, "slice length",
            DEL_SLICE_FIELD_INDEX_SLICE_LENGTH, '\n', 1);
    fs_calc_block_hashcode(&bs_key.block);
    return ob_index_delete_slices_by_binlog(&bs_key);
}

static int del_block(BinlogReadThreadResult *r, string_t *line,
        string_t *cols, const int count)
{
    FSBlockKey bkey;
    int64_t line_count;
    char binlog_filename[PATH_MAX];
    char *endptr;

    if (count != DEL_BLOCK_EXPECT_FIELD_COUNT) {
        SLICE_GET_FILENAME_LINE_COUNT(r, binlog_filename,
                line->str, line_count);
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", "
                "field count: %d != %d", __LINE__,
                binlog_filename, line_count,
                count, DEL_BLOCK_EXPECT_FIELD_COUNT);
        return EINVAL;
    }

    SLICE_PARSE_INT_EX(bkey.oid, "object ID",
            DEL_BLOCK_FIELD_INDEX_BLOCK_OID, ' ', 1);
    SLICE_PARSE_INT_EX(bkey.offset, "block offset",
            DEL_BLOCK_FIELD_INDEX_BLOCK_OFFSET, '\n', 0);
    fs_calc_block_hashcode(&bkey);
    return ob_index_delete_block_by_binlog(&bkey);
}

static int slice_parse_line(BinlogReadThreadResult *r, string_t *line)
{
    int count;
    int result;
    int64_t line_count;
    string_t cols[MAX_BINLOG_FIELD_COUNT];
    char binlog_filename[PATH_MAX];
    char op_type;

    count = split_string_ex(line, ' ', cols,
            MAX_BINLOG_FIELD_COUNT, false);
    if (count < MIN_EXPECT_FIELD_COUNT) {
        SLICE_GET_FILENAME_LINE_COUNT(r, binlog_filename,
                line->str, line_count);
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", "
                "field count: %d < %d", __LINE__,
                binlog_filename, line_count,
                count, MIN_EXPECT_FIELD_COUNT);
        return EINVAL;
    }

    op_type = cols[BINLOG_COMMON_FIELD_INDEX_OP_TYPE].str[0];
    switch (op_type) {
        case SLICE_BINLOG_OP_TYPE_ADD_SLICE:
            result = add_slice(r, line, cols, count);
            break;
        case SLICE_BINLOG_OP_TYPE_DEL_SLICE:
            result = del_slice(r, line, cols, count);
            break;
        case SLICE_BINLOG_OP_TYPE_DEL_BLOCK:
            result = del_block(r, line, cols, count);
            break;
        default:
            SLICE_GET_FILENAME_LINE_COUNT(r, binlog_filename,
                    line->str, line_count);
            logError("file: "__FILE__", line: %d, "
                    "binlog file %s, line no: %"PRId64", "
                    "invalid op_type: %c (0x%02x)", __LINE__,
                    binlog_filename, line_count,
                    op_type, (unsigned char)op_type);
            result = EINVAL;
            break;
    }

    if (result != 0) {
        if (result != EINVAL) {
            SLICE_GET_FILENAME_LINE_COUNT(r, binlog_filename,
                    line->str, line_count);
            logError("file: "__FILE__", line: %d, "
                    "binlog file %s, line no: %"PRId64", op_type: %c, "
                    "add to index fail, errno: %d", __LINE__,
                    binlog_filename, line_count, op_type, result);
        }
    }

    return result;
}

static int init_binlog_writer()
{
    return binlog_writer_init(&binlog_writer, FS_SLICE_BINLOG_SUBDIR_NAME,
            FS_SLICE_BINLOG_MAX_RECORD_SIZE);
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

    return binlog_loader_load(FS_SLICE_BINLOG_SUBDIR_NAME,
                    slice_binlog_get_current_write_index,
                    slice_parse_line);
}

void slice_binlog_destroy()
{
    binlog_writer_finish(&binlog_writer);
}

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
