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
#include "binlog_func.h"
#include "binlog_loader.h"
#include "slice_loader.h"
#include "slice_binlog.h"

#define ADD_SLICE_FIELD_INDEX_SPACE_PATH_INDEX 9
#define ADD_SLICE_FIELD_INDEX_SPACE_TRUNK_ID  10
#define ADD_SLICE_FIELD_INDEX_SPACE_SUBDIR    11
#define ADD_SLICE_FIELD_INDEX_SPACE_OFFSET    12
#define ADD_SLICE_FIELD_INDEX_SPACE_SIZE      13
#define ADD_SLICE_EXPECT_FIELD_COUNT          14

#define DEL_SLICE_EXPECT_FIELD_COUNT           9
#define DEL_BLOCK_EXPECT_FIELD_COUNT           7
#define NO_OP_EXPECT_FIELD_COUNT               7

static SFBinlogWriterContext binlog_writer;

static int init_binlog_writer()
{
    int result;
    int ring_size;

    ring_size = (WRITE_TO_CACHE ? 102400 : 10240);
    if ((result=sf_binlog_writer_init_by_version(&binlog_writer.writer,
                    DATA_PATH_STR, FS_SLICE_BINLOG_SUBDIR_NAME,
                    SLICE_BINLOG_SN + 1, BINLOG_BUFFER_SIZE, ring_size)) != 0)
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

int slice_binlog_set_binlog_start_index(const int start_index)
{
    return sf_binlog_writer_set_binlog_start_index(
            &binlog_writer.writer, start_index);
}

int slice_binlog_set_binlog_write_index(const int last_index)
{
    /* force write to binlog index file */
    binlog_writer.writer.fw.binlog.last_index = -1;
    return sf_binlog_writer_set_binlog_write_index(
            &binlog_writer.writer, last_index);
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

int slice_binlog_get_binlog_start_index()
{
    return sf_binlog_get_start_index(&binlog_writer.writer);
}

int slice_binlog_get_current_write_index()
{
    return sf_binlog_get_current_write_index(&binlog_writer.writer);
}

int slice_binlog_get_binlog_indexes(int *start_index, int *last_index)
{
    return sf_binlog_get_indexes(&binlog_writer.writer,
            start_index, last_index);
}

int slice_binlog_set_binlog_indexes(const int start_index,
        const int last_index)
{
    return sf_binlog_set_indexes(&binlog_writer.writer,
            start_index, last_index);
}

int slice_binlog_rotate_file()
{
    return sf_binlog_writer_rotate_file(&binlog_writer.writer);
}

static int slice_check_redo_migrate()
{
    return 0;
}

static int slice_binlog_do_migrate()
{
    return slice_check_redo_migrate();
}

int slice_binlog_get_last_sn()
{
    char buff[FS_SLICE_BINLOG_MAX_RECORD_SIZE];
    string_t line;
    string_t cols[BINLOG_MAX_FIELD_COUNT];
    int last_index;
    int line_count;
    int col_count;
    int result;

    if ((result=sf_binlog_writer_get_binlog_last_index(DATA_PATH_STR,
                    FS_SLICE_BINLOG_SUBDIR_NAME, &last_index)) != 0)
    {
        return result;
    }

    line_count = 1;
    if ((result=sf_binlog_writer_get_last_lines(DATA_PATH_STR,
                    FS_SLICE_BINLOG_SUBDIR_NAME, last_index, buff,
                    sizeof(buff), &line_count, &line.len)) != 0)
    {
        return result;
    }

    if (line_count == 0) {
        SLICE_BINLOG_SN = 0;
        return 0;
    }

    line.str = buff;
    if (!(line.len > 0 && line.str[line.len - 1] == '\n')) {
        logError("file: "__FILE__", line: %d, "
                "the last line of slice binlog is invalid, "
                "line length: %d, last line: %.*s", __LINE__,
                line.len, line.len, line.str);
        return EINVAL;
    }

    col_count = split_string_ex(&line, ' ', cols,
            BINLOG_MAX_FIELD_COUNT, false);
    if (col_count == ADD_SLICE_EXPECT_FIELD_COUNT ||
            col_count == DEL_SLICE_EXPECT_FIELD_COUNT ||
            col_count == DEL_BLOCK_EXPECT_FIELD_COUNT)
    {
        string_t *sn;
        char tmp[32];

        sn = cols + SLICE_BINLOG_FIELD_INDEX_SN;
        snprintf(tmp, sizeof(tmp), "%.*s", sn->len, sn->str);
        SLICE_BINLOG_SN = strtoll(tmp, NULL, 10);
        return 0;
    }

    if (!(col_count == ADD_SLICE_EXPECT_FIELD_COUNT - 1 ||
                col_count == DEL_SLICE_EXPECT_FIELD_COUNT - 1 ||
                col_count == DEL_BLOCK_EXPECT_FIELD_COUNT - 1))
    {
        logError("file: "__FILE__", line: %d, "
                "the last line of slice binlog is invalid, "
                "field count: %d, last line: %.*s", __LINE__,
                col_count, line.len, line.str);
        return EINVAL;
    }

    return slice_binlog_do_migrate();
}

int slice_binlog_init()
{
    int result;

    if ((result=slice_check_redo_migrate()) != 0) {
        return result;
    }

    if ((result=slice_binlog_get_last_sn()) != 0) {
        return result;
    }

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
    wbuffer->bf.length = slice_binlog_log_add_slice_to_buff_ex(slice,
            current_time, sn, data_version, source, wbuffer->bf.buff);
    sf_push_to_binlog_write_queue(&binlog_writer.writer, wbuffer);
    FC_ATOMIC_INC(SLICE_BINLOG_COUNT);
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
    wbuffer->bf.length = sprintf(wbuffer->bf.buff, "%"PRId64" "
            "%"PRId64" %"PRId64" %c %c %"PRId64" %"PRId64" %d %d\n",
            (int64_t)current_time, sn, data_version, source,
            BINLOG_OP_TYPE_DEL_SLICE, bs_key->block.oid,
            bs_key->block.offset, bs_key->slice.offset,
            bs_key->slice.length);
    sf_push_to_binlog_write_queue(&binlog_writer.writer, wbuffer);
    FC_ATOMIC_INC(SLICE_BINLOG_COUNT);
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
    wbuffer->bf.length = sprintf(wbuffer->bf.buff, "%"PRId64" "
            "%"PRId64" %"PRId64" %c %c %"PRId64" %"PRId64"\n",
            (int64_t)current_time, sn, data_version, source,
            op_type, bkey->oid, bkey->offset);
    sf_push_to_binlog_write_queue(&binlog_writer.writer, wbuffer);
    FC_ATOMIC_INC(SLICE_BINLOG_COUNT);
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

int slice_binlog_padding_for_check(const int source)
{
    const int64_t data_version = 0;
    int result;
    int i;
    time_t current_time;
    FSBlockKey bkey;

    current_time = g_current_time;
    bkey.oid = 1;
    bkey.offset = 0;
    for (i=1; i<=LOCAL_BINLOG_CHECK_LAST_SECONDS + 1; i++) {
        if ((result=slice_binlog_log_no_op(&bkey, current_time + i,
                        __sync_add_and_fetch(&SLICE_BINLOG_SN, 1),
                        data_version, source)) != 0)
        {
            return result;
        }
    }

    return 0;
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
            SLICE_BINLOG_FIELD_INDEX_SLICE_OFFSET, ' ', 0);
    BINLOG_PARSE_INT_SILENCE(record->bs_key.slice.length, "slice length",
            SLICE_BINLOG_FIELD_INDEX_SLICE_LENGTH, ' ', 1);

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
            SLICE_BINLOG_FIELD_INDEX_SLICE_OFFSET, ' ', 0);
    BINLOG_PARSE_INT_SILENCE(record->bs_key.slice.length, "slice length",
            SLICE_BINLOG_FIELD_INDEX_SLICE_LENGTH, '\n', 1);
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
    string_t cols[BINLOG_MAX_FIELD_COUNT];

    count = split_string_ex(line, ' ', cols,
            BINLOG_MAX_FIELD_COUNT, false);
    if (count < SLICE_MIN_FIELD_COUNT) {
        sprintf(error_info, "field count: %d < %d",
                count, SLICE_MIN_FIELD_COUNT);
        return EINVAL;
    }

    BINLOG_PARSE_INT_SILENCE(record->sn, "sn",
            SLICE_BINLOG_FIELD_INDEX_SN, ' ', 1);
    record->source = cols[SLICE_BINLOG_FIELD_INDEX_SOURCE].str[0];
    record->op_type = cols[SLICE_BINLOG_FIELD_INDEX_OP_TYPE].str[0];
    BINLOG_PARSE_INT_SILENCE(record->data_version, "data version",
            SLICE_BINLOG_FIELD_INDEX_DATA_VERSION, ' ', 0);
    BINLOG_PARSE_INT_SILENCE(record->bs_key.block.oid, "object ID",
            SLICE_BINLOG_FIELD_INDEX_BLOCK_OID, ' ', 1);
    BINLOG_PARSE_INT_SILENCE(record->bs_key.block.offset, "block offset",
            SLICE_BINLOG_FIELD_INDEX_BLOCK_OFFSET,
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

static int slice_binlog_get_first_record(const char *filename,
        const int data_group_id, BinlogCommonFields *record,
        SFBinlogFilePosition *pos)
{
    char buff[64 * 1024];
    char error_info[256];
    string_t line;
    char *line_end;
    char *buff_end;
    int fd;
    int read_bytes;
    bool found;
    int result;

    pos->offset = 0;
    if ((fd=open(filename, O_RDONLY | O_CLOEXEC)) < 0) {
        result = errno != 0 ? errno : ENOENT;
        logError("file: "__FILE__", line: %d, "
                "open file %s fail, errno: %d, error info: %s",
                __LINE__, filename, result, STRERROR(result));
        return result;
    }

    found = false;
    result = ENOENT;
    while (1) {
        if ((read_bytes=fc_read_lines(fd, buff, sizeof(buff))) < 0) {
            result = errno != 0 ? errno : ENOENT;
            logError("file: "__FILE__", line: %d, "
                    "read from file %s fail, errno: %d, error info: %s",
                    __LINE__, filename, result, STRERROR(result));
            break;
        }
        if (read_bytes == 0) {
            result = ENOENT;
            break;
        }

        buff_end = buff + read_bytes;
        line.str = buff;
        while (line.str < buff_end) {
            line_end = memchr(line.str, '\n', buff_end - line.str);
            if (line_end == NULL) {
                result = ENOENT;
                break;
            }

            ++line_end;
            line.len = line_end - line.str;
            if ((result=binlog_unpack_slice_common_fields(&line,
                            record, error_info)) != 0)
            {
                logError("file: "__FILE__", line: %d, "
                        "binlog file %s, %s", __LINE__,
                        filename, error_info);
                break;
            }

            if (FS_IS_BINLOG_SOURCE_RPC(record->source)) {
                fs_calc_block_hashcode(&record->bkey);
                if (FS_DATA_GROUP_ID(record->bkey) == data_group_id) {
                    pos->offset += (line.str - buff);
                    found = true;
                    break;
                }
            }

            line.str = line_end;
        }

        if (result != 0 || found) {
            break;
        }
        pos->offset += read_bytes;
    }

    close(fd);
    return result;
}

static inline int slice_binlog_get_first_data_version(
        const char *filename, const int data_group_id,
        uint64_t *data_version, SFBinlogFilePosition *pos)
{
    BinlogCommonFields record;
    int result;

    if ((result=slice_binlog_get_first_record(filename,
                    data_group_id, &record, pos)) == 0)
    {
        *data_version = record.data_version;
    } else {
        *data_version = 0;
    }

    return result;
}

static int find_position(const char *filename, const int data_group_id,
        const uint64_t last_data_version, SFBinlogFilePosition *pos)
{
    char buff[64 * 1024];
    char error_info[256];
    string_t line;
    BinlogCommonFields record;
    char *line_end;
    char *buff_end;
    int fd;
    int read_bytes;
    bool found;
    int result;

    if ((fd=open(filename, O_RDONLY | O_CLOEXEC)) < 0) {
        result = errno != 0 ? errno : ENOENT;
        logError("file: "__FILE__", line: %d, "
                "open file %s fail, errno: %d, error info: %s",
                __LINE__, filename, result, STRERROR(result));
        return result;
    }

    if (lseek(fd, pos->offset, SEEK_SET) < 0) {
        result = errno != 0 ? errno : ENOENT;
        logError("file: "__FILE__", line: %d, "
                "lseek file %s fail, errno: %d, error info: %s",
                __LINE__, filename, result, STRERROR(result));
        close(fd);
        return result;
    }

    found = false;
    result = ENOENT;
    while (1) {
        if ((read_bytes=fc_read_lines(fd, buff, sizeof(buff))) < 0) {
            result = errno != 0 ? errno : ENOENT;
            logError("file: "__FILE__", line: %d, "
                    "read from file %s fail, errno: %d, error info: %s",
                    __LINE__, filename, result, STRERROR(result));
            break;
        }
        if (read_bytes == 0) {
            result = ENOENT;
            break;
        }

        buff_end = buff + read_bytes;
        line.str = buff;
        while (line.str < buff_end) {
            line_end = memchr(line.str, '\n', buff_end - line.str);
            if (line_end == NULL) {
                result = ENOENT;
                break;
            }

            ++line_end;
            line.len = line_end - line.str;
            if ((result=binlog_unpack_slice_common_fields(&line,
                            &record, error_info)) != 0)
            {
                logError("file: "__FILE__", line: %d, "
                        "binlog file %s, %s", __LINE__,
                        filename, error_info);
                break;
            }

            if (FS_IS_BINLOG_SOURCE_RPC(record.source)) {
                fs_calc_block_hashcode(&record.bkey);
                if (FS_DATA_GROUP_ID(record.bkey) == data_group_id &&
                        record.data_version > last_data_version)
                {
                    pos->offset += (line.str - buff);
                    found = true;
                    break;
                }
            }

            line.str = line_end;
        }

        if (result != 0 || found) {
            break;
        }
        pos->offset += read_bytes;
    }

    close(fd);
    return result;
}

int slice_binlog_get_position_by_dv(const int data_group_id,
        const uint64_t last_data_version, SFBinlogFilePosition *pos)
{
    int result;
    int start_index;
    int last_index;
    int binlog_index;
    SFBinlogWriterInfo *writer;
    char filename[PATH_MAX];
    uint64_t first_data_version;

    writer = slice_binlog_get_writer();
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
        sf_binlog_writer_get_filename(DATA_PATH_STR,
                FS_SLICE_BINLOG_SUBDIR_NAME, binlog_index,
                filename, sizeof(filename));
        pos->index = binlog_index;
        pos->offset = 0;
        if ((result=slice_binlog_get_first_data_version(filename,
                        data_group_id, &first_data_version, pos)) != 0)
        {
            if (result == ENOENT) {
                --binlog_index;
                continue;
            }

            return result;
        }

        if (last_data_version + 1 == first_data_version) {
            return 0;
        } else if (last_data_version + 1 > first_data_version) {
            return find_position(filename, data_group_id,
                    last_data_version, pos);
        }

        --binlog_index;
    }

    return ENOENT;
}

static int check_alloc_record_array(BinlogCommonFieldsArray *array)
{
    BinlogCommonFields *records;
    int64_t new_alloc;
    int64_t bytes;

    if (array->alloc > array->count) {
        return 0;
    }

    new_alloc = (array->alloc > 0) ? 2 * array->alloc : 256;
    bytes = sizeof(BinlogCommonFields) * new_alloc;
    records = (BinlogCommonFields *)fc_malloc(bytes);
    if (records == NULL) {
        return ENOMEM;
    }

    if (array->records != NULL) {
        if (array->count > 0) {
            memcpy(records, array->records, array->count *
                    sizeof(BinlogCommonFields));
        }
        free(array->records);
    }

    array->alloc = new_alloc;
    array->records = records;
    return 0;
}

static int slice_parse_to_array(const int data_group_id,
        ServerBinlogReader *reader, const int read_bytes,
        BinlogCommonFieldsArray *array)
{
    int result;
    string_t line;
    char *line_end;
    char *buff_end;
    char error_info[256];
    BinlogCommonFields *record;

    buff_end = reader->binlog_buffer.buff + read_bytes;
    line.str = reader->binlog_buffer.buff;
    while (line.str < buff_end) {
        line_end = memchr(line.str, '\n', buff_end - line.str);
        if (line_end == NULL) {
            break;
        }

        ++line_end;
        line.len = line_end - line.str;

        if ((result=check_alloc_record_array(array)) != 0) {
            return result;
        }
        record = array->records + array->count;
        if ((result=binlog_unpack_slice_common_fields(&line,
                        record, error_info)) != 0)
        {
            logError("file: "__FILE__", line: %d, "
                    "binlog file %s, %s", __LINE__,
                    reader->filename, error_info);
            return result;
        }

        if (FS_IS_BINLOG_SOURCE_RPC(record->source) ||
                record->source == BINLOG_SOURCE_ROLLBACK)
        {
            fs_calc_block_hashcode(&record->bkey);
            if (FS_DATA_GROUP_ID(record->bkey) == data_group_id) {
                array->count++;
            }
        }

        line.str = line_end;
    }

    return 0;
}

int slice_binlog_load_records(const int data_group_id,
        const uint64_t last_data_version,
        BinlogCommonFieldsArray *array)
{
    SFBinlogFilePosition pos;
    ServerBinlogReader reader;
    int read_bytes;
    int result;

    array->count = 0;
    if ((result=slice_binlog_get_position_by_dv(data_group_id,
                    last_data_version, &pos)) != 0)
    {
        return (result == ENOENT ? 0 : result);
    }

    if ((result=binlog_reader_init(&reader, FS_SLICE_BINLOG_SUBDIR_NAME,
                    slice_binlog_get_writer(), &pos)) != 0)
    {
        return result;
    }

    while ((result=binlog_reader_integral_read(&reader,
                    reader.binlog_buffer.buff,
                    reader.binlog_buffer.size,
                    &read_bytes)) == 0)
    {
        if ((result=slice_parse_to_array(data_group_id,
                        &reader, read_bytes, array)) != 0)
        {
            break;
        }
    }

    binlog_reader_destroy(&reader);
    return (result == ENOENT ? 0 : result);
}
