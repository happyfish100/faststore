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
#include "fastcommon/fc_atomic.h"
#include "sf/sf_global.h"
#include "sf/sf_binlog_writer.h"
#include "../../common/fs_func.h"
#include "../server_global.h"
#include "../storage/storage_allocator.h"
#include "../storage/trunk_id_info.h"
#include "binlog_loader.h"
#include "slice_loader.h"
#include "slice_binlog.h"

#define ADD_SLICE_FIELD_INDEX_SPACE_PATH_INDEX 8
#define ADD_SLICE_FIELD_INDEX_SPACE_TRUNK_ID   9
#define ADD_SLICE_FIELD_INDEX_SPACE_SUBDIR    10
#define ADD_SLICE_FIELD_INDEX_SPACE_OFFSET    11
#define ADD_SLICE_FIELD_INDEX_SPACE_SIZE      12
#define ADD_SLICE_EXPECT_FIELD_COUNT          13

#define DEL_SLICE_EXPECT_FIELD_COUNT           8
#define DEL_BLOCK_EXPECT_FIELD_COUNT           6
#define NO_OP_EXPECT_FIELD_COUNT               6

#define MAX_BINLOG_FIELD_COUNT  16
#define MIN_EXPECT_FIELD_COUNT  DEL_BLOCK_EXPECT_FIELD_COUNT

static SFBinlogWriterContext binlog_writer;

static int init_binlog_writer()
{
    int result;

    if ((result=sf_binlog_writer_init_by_version(&binlog_writer.writer,
                    DATA_PATH_STR, FS_SLICE_BINLOG_SUBDIR_NAME,
                    SLICE_BINLOG_SN + 1, BINLOG_BUFFER_SIZE, 10240)) != 0)
    {
        return result;
    }

    return sf_binlog_writer_init_thread(&binlog_writer.thread, "slice",
            &binlog_writer.writer, FS_SLICE_BINLOG_MAX_RECORD_SIZE);
}

struct sf_binlog_writer_info *slice_binlog_get_writer()
{
    return &binlog_writer.writer;
}

int slice_binlog_set_binlog_index(const int binlog_index)
{
    /* force write to binlog index file */
    binlog_writer.writer.fw.binlog.index = -1;
    return sf_binlog_writer_set_binlog_index(&binlog_writer.
            writer, binlog_index);
}

void slice_binlog_writer_set_flags(const short flags)
{
    sf_binlog_writer_set_flags(&binlog_writer.writer, flags);
}

int slice_binlog_set_next_version()
{
    return sf_binlog_writer_change_next_version(&binlog_writer.
            writer, FC_ATOMIC_GET(SLICE_BINLOG_SN) + 1);
}

int slice_binlog_get_current_write_index()
{
    return sf_binlog_get_current_write_index(&binlog_writer.writer);
}

int slice_binlog_init()
{
    return init_binlog_writer();
}

int slice_binlog_load()
{
    return slice_loader_load(&binlog_writer.writer);
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

    if ((wbuffer=sf_binlog_writer_alloc_buffer(
                    &binlog_writer.thread)) == NULL)
    {
        return ENOMEM;
    }

    wbuffer->tag = data_version;
    SF_BINLOG_BUFFER_SET_VERSION(wbuffer, sn);
    wbuffer->bf.length = slice_binlog_log_add_slice_to_buff(slice,
            current_time, data_version, source, wbuffer->bf.buff);
    sf_push_to_binlog_write_queue(&binlog_writer.writer, wbuffer);
    return 0;
}

int slice_binlog_log_del_slice(const FSBlockSliceKeyInfo *bs_key,
        const time_t current_time, const uint64_t sn,
        const uint64_t data_version, const int source)
{
    SFBinlogWriterBuffer *wbuffer;

    if ((wbuffer=sf_binlog_writer_alloc_buffer(
                    &binlog_writer.thread)) == NULL)
    {
        return ENOMEM;
    }

    wbuffer->tag = data_version;
    SF_BINLOG_BUFFER_SET_VERSION(wbuffer, sn);
    wbuffer->bf.length = sprintf(wbuffer->bf.buff,
            "%"PRId64" %"PRId64" %c %c %"PRId64" %"PRId64" %d %d\n",
            (int64_t)current_time, data_version, source,
            BINLOG_OP_TYPE_DEL_SLICE, bs_key->block.oid,
            bs_key->block.offset, bs_key->slice.offset,
            bs_key->slice.length);
    sf_push_to_binlog_write_queue(&binlog_writer.writer, wbuffer);
    return 0;
}

static inline int log_block_update(const FSBlockKey *bkey,
        const time_t current_time, const uint64_t sn,
        const uint64_t data_version, const int source,
        const char op_type)
{
    SFBinlogWriterBuffer *wbuffer;

    if ((wbuffer=sf_binlog_writer_alloc_buffer(
                    &binlog_writer.thread)) == NULL)
    {
        return ENOMEM;
    }

    wbuffer->tag = data_version;
    SF_BINLOG_BUFFER_SET_VERSION(wbuffer, sn);
    wbuffer->bf.length = sprintf(wbuffer->bf.buff,
            "%"PRId64" %"PRId64" %c %c %"PRId64" %"PRId64"\n",
            (int64_t)current_time, data_version, source,
            op_type, bkey->oid, bkey->offset);
    sf_push_to_binlog_write_queue(&binlog_writer.writer, wbuffer);
    return 0;
}

int slice_binlog_log_del_block(const FSBlockKey *bkey,
        const time_t current_time, const uint64_t sn,
        const uint64_t data_version, const int source)
{
    return log_block_update(bkey, current_time, sn, data_version,
            source, BINLOG_OP_TYPE_DEL_BLOCK);
}

int slice_binlog_log_no_op(const FSBlockKey *bkey,
        const time_t current_time, const uint64_t sn,
        const uint64_t data_version, const int source)
{
    return log_block_update(bkey, current_time, sn, data_version,
            source, BINLOG_OP_TYPE_NO_OP);
}

void slice_binlog_writer_stat(FSBinlogWriterStat *stat)
{
    stat->total_count = binlog_writer.writer.fw.total_count;
    stat->next_version = binlog_writer.writer.version_ctx.next;
    stat->waiting_count = binlog_writer.writer.version_ctx.ring.waiting_count;
    stat->max_waitings = binlog_writer.writer.version_ctx.ring.max_waitings;
}

static inline int unpack_add_slice_record(string_t *cols, const int count,
        SliceBinlogRecord *record, char *error_info)
{
    char *endptr;
    int path_index;

    if (count != ADD_SLICE_EXPECT_FIELD_COUNT) {
        sprintf(error_info, "field count: %d != %d",
                count, ADD_SLICE_EXPECT_FIELD_COUNT);
        return EINVAL;
    }

    BINLOG_PARSE_INT_SILENCE(record->bs_key.slice.offset, "slice offset",
            BINLOG_COMMON_FIELD_INDEX_SLICE_OFFSET, ' ', 0);
    BINLOG_PARSE_INT_SILENCE(record->bs_key.slice.length, "slice length",
            BINLOG_COMMON_FIELD_INDEX_SLICE_LENGTH, ' ', 1);

    BINLOG_PARSE_INT_SILENCE(path_index, "path index",
            ADD_SLICE_FIELD_INDEX_SPACE_PATH_INDEX, ' ', 0);
    if (path_index > STORAGE_CFG.max_store_path_index) {
        sprintf(error_info, "invalid path_index: %d > "
                "max_store_path_index: %d", path_index,
                STORAGE_CFG.max_store_path_index);
        return EINVAL;
    }
    if (PATHS_BY_INDEX_PPTR[path_index] == NULL) {
        sprintf(error_info, "path_index: %d not exist", path_index);
        return ENOENT;
    }
    record->space.store = &PATHS_BY_INDEX_PPTR[path_index]->store;

    BINLOG_PARSE_INT_SILENCE(record->space.id_info.id, "trunk id",
            ADD_SLICE_FIELD_INDEX_SPACE_TRUNK_ID, ' ', 1);
    BINLOG_PARSE_INT_SILENCE(record->space.id_info.subdir, "subdir",
            ADD_SLICE_FIELD_INDEX_SPACE_SUBDIR, ' ', 1);
    BINLOG_PARSE_INT_SILENCE(record->space.offset, "space offset",
            ADD_SLICE_FIELD_INDEX_SPACE_OFFSET, ' ', 0);
    BINLOG_PARSE_INT_SILENCE(record->space.size, "space size",
            ADD_SLICE_FIELD_INDEX_SPACE_SIZE, '\n', 0);
    return 0;
}

static inline int unpack_del_slice_record(string_t *cols, const int count,
        SliceBinlogRecord *record, char *error_info)
{
    char *endptr;

    if (count != DEL_SLICE_EXPECT_FIELD_COUNT) {
        sprintf(error_info, "field count: %d != %d",
                count, DEL_SLICE_EXPECT_FIELD_COUNT);
        return EINVAL;
    }

    BINLOG_PARSE_INT_SILENCE(record->bs_key.slice.offset, "slice offset",
            BINLOG_COMMON_FIELD_INDEX_SLICE_OFFSET, ' ', 0);
    BINLOG_PARSE_INT_SILENCE(record->bs_key.slice.length, "slice length",
            BINLOG_COMMON_FIELD_INDEX_SLICE_LENGTH, '\n', 1);
    return 0;
}

static inline int unpack_block_record(string_t *cols, const int count,
        SliceBinlogRecord *record, char *error_info)
{
    if (count != DEL_BLOCK_EXPECT_FIELD_COUNT) {
        sprintf(error_info, "field count: %d != %d",
                count, DEL_BLOCK_EXPECT_FIELD_COUNT);
        return EINVAL;
    }

    return 0;
}

int slice_binlog_record_unpack(const string_t *line,
        SliceBinlogRecord *record, char *error_info)
{
    int count;
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
            BINLOG_COMMON_FIELD_INDEX_DATA_VERSION, ' ', 0);
    BINLOG_PARSE_INT_SILENCE(record->bs_key.block.oid, "object ID",
            BINLOG_COMMON_FIELD_INDEX_BLOCK_OID, ' ', 1);
    BINLOG_PARSE_INT_SILENCE(record->bs_key.block.offset, "block offset",
            BINLOG_COMMON_FIELD_INDEX_BLOCK_OFFSET,
            ((record->op_type == BINLOG_OP_TYPE_DEL_BLOCK ||
              record->op_type == BINLOG_OP_TYPE_NO_OP) ?
             '\n' : ' '), 0);
    fs_calc_block_hashcode(&record->bs_key.block);

    switch (record->op_type) {
        case BINLOG_OP_TYPE_WRITE_SLICE:
            record->slice_type = OB_SLICE_TYPE_FILE;
            return unpack_add_slice_record(cols,
                    count, record, error_info);
        case BINLOG_OP_TYPE_ALLOC_SLICE:
            record->slice_type = OB_SLICE_TYPE_ALLOC;
            return unpack_add_slice_record(cols,
                    count, record, error_info);
        case BINLOG_OP_TYPE_DEL_SLICE:
            return unpack_del_slice_record(cols,
                    count, record, error_info);
        case BINLOG_OP_TYPE_DEL_BLOCK:
        case BINLOG_OP_TYPE_NO_OP:
            return unpack_block_record(cols,
                    count, record, error_info);
        default:
            sprintf(error_info, "invalid op_type: %c (0x%02x)",
                    record->op_type, (unsigned char)record->op_type);
            return EINVAL;
    }
}
