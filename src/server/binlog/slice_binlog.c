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
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "sf/sf_binlog_writer.h"
#include "../../common/fs_func.h"
#include "../server_global.h"
#include "../dio/trunk_io_thread.h"
#include "../storage/storage_allocator.h"
#include "../storage/trunk_id_info.h"
#include "binlog_loader.h"
#include "slice_binlog.h"

#define ADD_SLICE_FIELD_INDEX_SPACE_PATH_INDEX 8
#define ADD_SLICE_FIELD_INDEX_SPACE_TRUNK_ID   9
#define ADD_SLICE_FIELD_INDEX_SPACE_SUBDIR    10
#define ADD_SLICE_FIELD_INDEX_SPACE_OFFSET    11
#define ADD_SLICE_FIELD_INDEX_SPACE_SIZE      12
#define ADD_SLICE_EXPECT_FIELD_COUNT          13

#define DEL_SLICE_EXPECT_FIELD_COUNT           8
#define DEL_BLOCK_EXPECT_FIELD_COUNT           6

#define MAX_BINLOG_FIELD_COUNT  16
#define MIN_EXPECT_FIELD_COUNT  DEL_BLOCK_EXPECT_FIELD_COUNT

static SFBinlogWriterContext binlog_writer;

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
        string_t *cols, const int count, const OBSliceType slice_type)
{
    FSBlockKey bkey;
    OBSliceEntry *slice;
    int64_t line_count;
    char binlog_filename[PATH_MAX];
    char *endptr;
    int path_index;

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

    SLICE_PARSE_INT_EX(bkey.oid, "object ID",
            BINLOG_COMMON_FIELD_INDEX_BLOCK_OID, ' ', 1);
    SLICE_PARSE_INT_EX(bkey.offset, "block offset",
            BINLOG_COMMON_FIELD_INDEX_BLOCK_OFFSET, ' ', 0);
    fs_calc_block_hashcode(&bkey);
    if ((slice=ob_index_alloc_slice(&bkey)) == NULL) {
        return ENOMEM;
    }

    slice->read_offset = 0;
    slice->type = slice_type;
    SLICE_PARSE_INT_EX(slice->ssize.offset, "slice offset",
            BINLOG_COMMON_FIELD_INDEX_SLICE_OFFSET, ' ', 0);
    SLICE_PARSE_INT_EX(slice->ssize.length, "slice length",
            BINLOG_COMMON_FIELD_INDEX_SLICE_LENGTH, ' ', 1);

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
            BINLOG_COMMON_FIELD_INDEX_BLOCK_OID, ' ', 1);
    SLICE_PARSE_INT_EX(bs_key.block.offset, "block offset",
            BINLOG_COMMON_FIELD_INDEX_BLOCK_OFFSET, ' ', 0);
    SLICE_PARSE_INT_EX(bs_key.slice.offset, "slice offset",
            BINLOG_COMMON_FIELD_INDEX_SLICE_OFFSET, ' ', 0);
    SLICE_PARSE_INT_EX(bs_key.slice.length, "slice length",
            BINLOG_COMMON_FIELD_INDEX_SLICE_LENGTH, '\n', 1);
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
            BINLOG_COMMON_FIELD_INDEX_BLOCK_OID, ' ', 1);
    SLICE_PARSE_INT_EX(bkey.offset, "block offset",
            BINLOG_COMMON_FIELD_INDEX_BLOCK_OFFSET, '\n', 0);
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
        case SLICE_BINLOG_OP_TYPE_WRITE_SLICE:
        case SLICE_BINLOG_OP_TYPE_ALLOC_SLICE:
            result = add_slice(r, line, cols, count,
                    op_type == SLICE_BINLOG_OP_TYPE_WRITE_SLICE ?
                    OB_SLICE_TYPE_FILE : OB_SLICE_TYPE_ALLOC);
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
    int result;

    if ((result=sf_binlog_writer_init_by_version(&binlog_writer.writer,
                    FS_SLICE_BINLOG_SUBDIR_NAME, SLICE_BINLOG_SN + 1,
                    BINLOG_BUFFER_SIZE, 4096)) != 0)
    {
        return result;
    }

    return sf_binlog_writer_init_thread(&binlog_writer.thread,
            &binlog_writer.writer, SF_BINLOG_THREAD_TYPE_ORDER_BY_VERSION,
            FS_SLICE_BINLOG_MAX_RECORD_SIZE);
}

struct sf_binlog_writer_info *slice_binlog_get_writer()
{
    return &binlog_writer.writer;
}

int slice_binlog_get_current_write_index()
{
    return sf_binlog_get_current_write_index(&binlog_writer.writer);
}

int slice_binlog_init()
{
    int result;

    if ((result=init_binlog_writer()) != 0) {
        return result;
    }

    return binlog_loader_load(FS_SLICE_BINLOG_SUBDIR_NAME,
            &binlog_writer.writer, slice_parse_line);
}

void slice_binlog_destroy()
{
    sf_binlog_writer_finish(&binlog_writer.writer);
}

int slice_binlog_log_add_slice(const OBSliceEntry *slice,
        const time_t current_time, const uint64_t sn,
        const uint64_t data_version, const int source)
{
    SFBinlogWriterBuffer *wbuffer;

    if ((wbuffer=sf_binlog_writer_alloc_buffer(&binlog_writer.thread)) == NULL) {
        return ENOMEM;
    }

    wbuffer->version = sn;
    wbuffer->bf.length = sprintf(wbuffer->bf.buff,
            "%"PRId64" %"PRId64" %c %c %"PRId64" %"PRId64" %d %d "
            "%d %"PRId64" %"PRId64" %"PRId64" %"PRId64"\n",
            (int64_t)current_time, data_version, source,
            slice->type == OB_SLICE_TYPE_FILE ?
            SLICE_BINLOG_OP_TYPE_WRITE_SLICE :
            SLICE_BINLOG_OP_TYPE_ALLOC_SLICE,
            slice->ob->bkey.oid, slice->ob->bkey.offset,
            slice->ssize.offset, slice->ssize.length,
            slice->space.store->index, slice->space.id_info.id,
            slice->space.id_info.subdir, slice->space.offset,
            slice->space.size);
    sf_push_to_binlog_write_queue(&binlog_writer.writer, wbuffer);
    return 0;
}

int slice_binlog_log_del_slice(const FSBlockSliceKeyInfo *bs_key,
        const time_t current_time, const uint64_t sn,
        const uint64_t data_version, const int source)
{
    SFBinlogWriterBuffer *wbuffer;

    if ((wbuffer=sf_binlog_writer_alloc_buffer(&binlog_writer.thread)) == NULL) {
        return ENOMEM;
    }

    wbuffer->version = sn;
    wbuffer->bf.length = sprintf(wbuffer->bf.buff,
            "%"PRId64" %"PRId64" %c %c %"PRId64" %"PRId64" %d %d\n",
            (int64_t)current_time, data_version, source,
            SLICE_BINLOG_OP_TYPE_DEL_SLICE, bs_key->block.oid,
            bs_key->block.offset, bs_key->slice.offset,
            bs_key->slice.length);
    sf_push_to_binlog_write_queue(&binlog_writer.writer, wbuffer);
    return 0;
}

int slice_binlog_log_del_block(const FSBlockKey *bkey,
        const time_t current_time, const uint64_t sn,
        const uint64_t data_version, const int source)
{
    SFBinlogWriterBuffer *wbuffer;

    if ((wbuffer=sf_binlog_writer_alloc_buffer(&binlog_writer.thread)) == NULL) {
        return ENOMEM;
    }

    wbuffer->version = sn;
    wbuffer->bf.length = sprintf(wbuffer->bf.buff,
            "%"PRId64" %"PRId64" %c %c %"PRId64" %"PRId64"\n",
            (int64_t)current_time, data_version, source,
            SLICE_BINLOG_OP_TYPE_DEL_BLOCK,
            bkey->oid, bkey->offset);
    sf_push_to_binlog_write_queue(&binlog_writer.writer, wbuffer);
    return 0;
}
