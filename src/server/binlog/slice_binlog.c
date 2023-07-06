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
#include "diskallocator/binlog/trunk/trunk_space_log.h"
#include "../../common/fs_func.h"
#include "../server_global.h"
#include "../storage/slice_space_log.h"
#include "binlog_func.h"
#include "binlog_loader.h"
#include "slice_loader.h"
#include "trunk_migrate.h"
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

#define FS_SLICE_MIGRATE_SUBDIR_NAME  FS_SLICE_BINLOG_SUBDIR_NAME"/migrate"

#define MIGRATE_REDO_ITEM_CURRENT_STAGE      "current_stage"

#define MIGRATE_REDO_STAGE_RENAME       1
#define MIGRATE_REDO_STAGE_MIGRATING    2
#define MIGRATE_REDO_STAGE_CLEANUP      3

typedef struct slice_binlog_migrate_redo_context {
    char mark_filename[PATH_MAX];
    int current_stage;
} SliceMigrateRedoContext;

static int init_binlog_writer()
{
    int result;
    int ring_size;
    int max_delay;

    ring_size = (WRITE_TO_CACHE ? 102400 : 10240);
    if ((result=sf_binlog_writer_init_by_version_ex(&SLICE_BINLOG_WRITER.
                    writer, DATA_PATH_STR, FS_SLICE_BINLOG_SUBDIR_NAME,
                    SF_BINLOG_FILE_PREFIX, SLICE_BINLOG_SN + 1,
                    BINLOG_BUFFER_SIZE, ring_size,
                    SF_BINLOG_DEFAULT_ROTATE_SIZE,
                    BINLOG_CALL_FSYNC)) != 0)
    {
        return result;
    }

    if (LOCAL_BINLOG_CHECK_LAST_SECONDS > 0) {
        max_delay = (LOCAL_BINLOG_CHECK_LAST_SECONDS + 1) / 2;
    } else {
        max_delay = 60;
    }
    slice_binlog_writer_set_flags(SF_FILE_WRITER_FLAGS_WANT_DONE_VERSION);
    return sf_binlog_writer_init_thread(&SLICE_BINLOG_WRITER.thread,
            "slice", &SLICE_BINLOG_WRITER.writer, max_delay,
            FS_SLICE_BINLOG_MAX_RECORD_SIZE);
}

int slice_binlog_set_binlog_start_index(const int start_index)
{
    return sf_binlog_writer_set_binlog_start_index(
            &SLICE_BINLOG_WRITER.writer, start_index);
}

int slice_binlog_set_binlog_write_index(const int last_index)
{
    /* force write to binlog index file */
    SLICE_BINLOG_WRITER.writer.fw.binlog.last_index = -1;
    return sf_binlog_writer_set_binlog_write_index(
            &SLICE_BINLOG_WRITER.writer, last_index);
}

void slice_binlog_writer_set_flags(const short flags)
{
    if (!(STORAGE_ENABLED && flags == 0)) {
        sf_binlog_writer_set_flags(&SLICE_BINLOG_WRITER.writer, flags);
    }
}

int slice_binlog_get_binlog_start_index()
{
    return sf_binlog_get_start_index(&SLICE_BINLOG_WRITER.writer);
}

int slice_binlog_get_current_write_index()
{
    return sf_binlog_get_current_write_index(&SLICE_BINLOG_WRITER.writer);
}

int slice_binlog_get_binlog_indexes(int *start_index, int *last_index)
{
    return sf_binlog_get_indexes(&SLICE_BINLOG_WRITER.writer,
            start_index, last_index);
}

int slice_binlog_set_binlog_indexes(const int start_index,
        const int last_index)
{
    return sf_binlog_set_indexes(&SLICE_BINLOG_WRITER.writer,
            start_index, last_index);
}

int slice_binlog_rotate_file()
{
    return sf_binlog_writer_rotate_file(&SLICE_BINLOG_WRITER.writer);
}

static inline const char *get_migrate_mark_filename(
        char *filename, const int size)
{
    snprintf(filename, size, "%s/%s/.migrate.flag",
            DATA_PATH_STR, FS_SLICE_BINLOG_SUBDIR_NAME);
    return filename;
}

static int write_to_redo_file(SliceMigrateRedoContext *redo_ctx)
{
    char buff[256];
    int result;
    int len;

    len = sprintf(buff, "%s=%d\n", MIGRATE_REDO_ITEM_CURRENT_STAGE,
            redo_ctx->current_stage);
    if ((result=safeWriteToFile(redo_ctx->mark_filename, buff, len)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "write to file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, redo_ctx->mark_filename, result, STRERROR(result));
    }

    return result;
}

static int load_from_redo_file(SliceMigrateRedoContext *redo_ctx)
{
    IniContext ini_context;
    int result;

    if ((result=iniLoadFromFile(redo_ctx->mark_filename,
                    &ini_context)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "load from file \"%s\" fail, error code: %d",
                __LINE__, redo_ctx->mark_filename, result);
        return result;
    }

    redo_ctx->current_stage = iniGetIntValue(NULL,
            MIGRATE_REDO_ITEM_CURRENT_STAGE, &ini_context, 0);

    iniFreeContext(&ini_context);
    return 0;
}

static int slice_migrate_rename()
{
    int result;
    int start_index;
    int last_index;
    int binlog_index;
    char slice_index_filename[PATH_MAX];
    char migrate_index_filename[PATH_MAX];
    char binlog_filename[PATH_MAX];
    char migrate_filename[PATH_MAX];

    sf_binlog_writer_get_index_filename(DATA_PATH_STR,
            FS_SLICE_BINLOG_SUBDIR_NAME, slice_index_filename,
            sizeof(slice_index_filename));
    sf_binlog_writer_get_index_filename(DATA_PATH_STR,
            FS_SLICE_MIGRATE_SUBDIR_NAME, migrate_index_filename,
            sizeof(migrate_index_filename));
    if (access(migrate_index_filename, F_OK) == 0) {
        return 0;
    }

    if ((result=sf_binlog_writer_get_binlog_indexes(DATA_PATH_STR,
                    FS_SLICE_BINLOG_SUBDIR_NAME,
                    &start_index, &last_index)) != 0)
    {
        return result;
    }

    for (binlog_index=start_index; binlog_index<=last_index; binlog_index++) {
        sf_binlog_writer_get_filename(DATA_PATH_STR,
                FS_SLICE_BINLOG_SUBDIR_NAME, binlog_index,
                binlog_filename, sizeof(binlog_filename));
        sf_binlog_writer_get_filename(DATA_PATH_STR,
                FS_SLICE_MIGRATE_SUBDIR_NAME, binlog_index,
                migrate_filename, sizeof(migrate_filename));
        if (access(migrate_filename, F_OK) == 0) {
            continue;
        }

        if (rename(binlog_filename, migrate_filename) != 0) {
            result = errno != 0 ? errno : EPERM;
            logError("file: "__FILE__", line: %d, rename file %s to %s fail, "
                    "errno: %d, error info: %s", __LINE__, binlog_filename,
                    migrate_filename, result, STRERROR(result));
            return result;
        }
    }

    if (rename(slice_index_filename, migrate_index_filename) != 0) {
        result = errno != 0 ? errno : EPERM;
        logError("file: "__FILE__", line: %d, rename file %s to %s fail, "
                "errno: %d, error info: %s", __LINE__, slice_index_filename,
                migrate_index_filename, result, STRERROR(result));
        return result;
    }

    return 0;
}

static int remove_slice_binlogs(const char *subdir_name)
{
    int result;
    int start_index;
    int last_index;
    int binlog_index;
    char index_filename[PATH_MAX];
    char binlog_filename[PATH_MAX];

    sf_binlog_writer_get_index_filename(DATA_PATH_STR,
            subdir_name, index_filename, sizeof(index_filename));
    if (access(index_filename, F_OK) == 0) {
        if ((result=sf_binlog_writer_get_binlog_indexes(DATA_PATH_STR,
                        subdir_name, &start_index, &last_index)) != 0)
        {
            return result;
        }

        for (binlog_index=start_index; binlog_index<=last_index;
                binlog_index++)
        {
            sf_binlog_writer_get_filename(DATA_PATH_STR,
                    subdir_name, binlog_index, binlog_filename,
                    sizeof(binlog_filename));
            if ((result=fc_delete_file_ex(binlog_filename,
                            "slice binlog")) != 0)
            {
                return result;
            }
        }

        return fc_delete_file_ex(index_filename, "slice index");
    } else {
        result = errno != 0 ? errno : EPERM;
        if (result != ENOENT) {
            logError("file: "__FILE__", line: %d, access file %s fail, "
                    "errno: %d, error info: %s", __LINE__,
                    index_filename, result, STRERROR(result));
            return result;
        }

        sf_binlog_writer_get_filename(DATA_PATH_STR, subdir_name,
                0, binlog_filename, sizeof(binlog_filename));
        return fc_delete_file_ex(binlog_filename, "slice binlog");
    }
}

static int slice_migrate_parse_buffer(ServerBinlogReader *reader,
        const int read_bytes)
{
    int front_len;
    int tail_len;
    int64_t sn;
    string_t line;
    char *line_end;
    char *buff_end;
    char *insert_point;
    char *p;
    SFBinlogWriterBuffer *wbuffer;

    buff_end = reader->binlog_buffer.buff + read_bytes;
    line.str = reader->binlog_buffer.buff;
    while (line.str < buff_end) {
        line_end = memchr(line.str, '\n', buff_end - line.str);
        if (line_end == NULL) {
            break;
        }

        ++line_end;
        line.len = line_end - line.str;
        if (line.len > FS_SLICE_BINLOG_MAX_RECORD_SIZE - 32) {
            logError("file: "__FILE__", line: %d, "
                    "binlog file %s, line length %d is too large",
                    __LINE__, reader->filename, line.len);
            return EOVERFLOW;
        }

        insert_point = memchr(line.str, ' ', line.len);
        if (insert_point == NULL) {
            logError("file: "__FILE__", line: %d, "
                    "binlog file %s, binlog line: %.*s, expect space char",
                    __LINE__, reader->filename, line.len, line.str);
            return EINVAL;
        }
        ++insert_point;
        front_len = insert_point - line.str;
        tail_len = line.len - front_len;

        if ((wbuffer=sf_binlog_writer_alloc_buffer(
                        &SLICE_BINLOG_WRITER.thread)) == NULL)
        {
            return ENOMEM;
        }

        sn = __sync_add_and_fetch(&SLICE_BINLOG_SN, 1);
        SF_BINLOG_BUFFER_SET_VERSION(wbuffer, sn);

        p = wbuffer->bf.buff;
        memcpy(p, line.str, front_len);
        p += front_len;
        p += fc_itoa(sn, p);
        *p++ = ' ';
        memcpy(p, insert_point, tail_len);
        p += tail_len;

        wbuffer->bf.length = p - wbuffer->bf.buff;
        sf_push_to_binlog_write_queue(&SLICE_BINLOG_WRITER.writer, wbuffer);

        line.str = line_end;
    }

    return 0;
}

static int slice_migrate_do()
{
    const int suggest_buffer_size = 4 * 1024 * 1024;
    int result;
    int start_index;
    int last_index;
    int buffer_size;
    int read_bytes;
    int64_t sn;
    int64_t waiting_count;
    int64_t start_time_ms;
    char time_buff[32];
    ServerBinlogReader reader;
    SFBinlogFilePosition position;

    start_time_ms = get_current_time_ms();
    if ((result=remove_slice_binlogs(FS_SLICE_BINLOG_SUBDIR_NAME)) != 0) {
        return result;
    }
    if ((result=slice_binlog_set_binlog_indexes(0, 0)) != 0) {
        return result;
    }
    if ((result=slice_binlog_set_binlog_write_index(0)) != 0) {
        return result;
    }

    if ((result=sf_binlog_writer_get_binlog_indexes(DATA_PATH_STR,
                    FS_SLICE_MIGRATE_SUBDIR_NAME,
                    &start_index, &last_index)) != 0)
    {
        return result;
    }
    sf_binlog_writer_change_passive_write(&SLICE_BINLOG_WRITER.writer, true);
    if (BINLOG_CALL_FSYNC) {
        sf_binlog_writer_change_call_fsync(&SLICE_BINLOG_WRITER.writer, false);
    }

    logInfo("file: "__FILE__", line: %d, "
            "begin migrate slice binlog, binlog count: %d ...",
            __LINE__, last_index - start_index + 1);

    position.index = start_index;
    position.offset = 0;
    buffer_size = FC_MAX(suggest_buffer_size, BINLOG_BUFFER_SIZE);
    if ((result=binlog_reader_init1_ex(&reader, FS_SLICE_MIGRATE_SUBDIR_NAME,
                    "", last_index, &position, buffer_size)) != 0)
    {
        return result;
    }

    waiting_count = 0;
    while (SF_G_CONTINUE_FLAG && (result=binlog_reader_integral_read(
                    &reader, reader.binlog_buffer.buff,
                    reader.binlog_buffer.size,
                    &read_bytes)) == 0)
    {
        if ((result=slice_migrate_parse_buffer(&reader, read_bytes)) != 0) {
            break;
        }

        sn = FC_ATOMIC_GET(SLICE_BINLOG_SN);
        while (sf_binlog_writer_get_last_version(
                    &SLICE_BINLOG_WRITER.writer) + 100000 < sn)
        {
            ++waiting_count;
            fc_sleep_ms(1);
        }
    }

    binlog_reader_destroy(&reader);
    if (!SF_G_CONTINUE_FLAG) {
        return EINTR;
    }
    if (result == ENOENT) {
        result = 0;
    }
    if (result != 0) {
        return result;
    }

    sf_binlog_writer_flush_file(&SLICE_BINLOG_WRITER.writer);
    sn = FC_ATOMIC_GET(SLICE_BINLOG_SN);
    while (SF_G_CONTINUE_FLAG && sf_binlog_writer_get_last_version(
                &SLICE_BINLOG_WRITER.writer) < sn)
    {
        ++waiting_count;
        fc_sleep_ms(1);
    }
    if (!SF_G_CONTINUE_FLAG) {
        return EINTR;
    }

    sf_binlog_writer_change_passive_write(&SLICE_BINLOG_WRITER.writer, false);
    if (BINLOG_CALL_FSYNC) {
        sf_binlog_writer_change_call_fsync(&SLICE_BINLOG_WRITER.writer, true);
    }

    long_to_comma_str(get_current_time_ms() - start_time_ms, time_buff);
    logInfo("file: "__FILE__", line: %d, "
            "migrate slice binlog done, time used: %s ms, waiting write "
            "count: %"PRId64, __LINE__, time_buff, waiting_count);
    return 0;
}

static int slice_migrate_cleanup()
{
    int result;
    char migrate_path[PATH_MAX];

    if ((result=remove_slice_binlogs(FS_SLICE_MIGRATE_SUBDIR_NAME)) != 0) {
        return result;
    }

    sf_binlog_writer_get_filepath(DATA_PATH_STR,
            FS_SLICE_MIGRATE_SUBDIR_NAME,
            migrate_path, sizeof(migrate_path));
    if (rmdir(migrate_path) != 0) {
        result = errno != 0 ? errno : EPERM;
        if (result != ENOENT) {
            logError("file: "__FILE__", line: %d, rmdir %s fail, "
                    "errno: %d, error info: %s", __LINE__,
                    migrate_path, result, STRERROR(result));
            return result;
        }
    }

    return 0;
}

static int slice_migrate_redo(SliceMigrateRedoContext *redo_ctx)
{
    int result;

    switch (redo_ctx->current_stage) {
        case MIGRATE_REDO_STAGE_RENAME:
            if ((result=slice_migrate_rename()) != 0) {
                break;
            }
            redo_ctx->current_stage = MIGRATE_REDO_STAGE_MIGRATING;
            if ((result=write_to_redo_file(redo_ctx)) != 0) {
                break;
            }
        case MIGRATE_REDO_STAGE_MIGRATING:
            if ((result=slice_migrate_do()) != 0) {
                break;
            }
            redo_ctx->current_stage = MIGRATE_REDO_STAGE_CLEANUP;
            if ((result=write_to_redo_file(redo_ctx)) != 0) {
                break;
            }
        case MIGRATE_REDO_STAGE_CLEANUP:
            if ((result=slice_migrate_cleanup()) != 0) {
                break;
            }
            if ((result=fc_delete_file_ex(redo_ctx->mark_filename,
                            "slice migrate mark")) != 0)
            {
                break;
            }
            break;
        default:
            logError("file: "__FILE__", line: %d, migrate mark file %s, "
                    "invalid stage: %d", __LINE__, redo_ctx->mark_filename,
                    redo_ctx->current_stage);
            result = EINVAL;
            break;
    }

    return result;
}

static int slice_binlog_do_migrate()
{
    int result;
    char migrate_path[PATH_MAX];
    SliceMigrateRedoContext redo_ctx;

    sf_binlog_writer_get_filepath(DATA_PATH_STR,
            FS_SLICE_MIGRATE_SUBDIR_NAME,
            migrate_path, sizeof(migrate_path));
    if ((result=fc_check_mkdir(migrate_path, 0755)) != 0) {
        return result;
    }

    get_migrate_mark_filename(redo_ctx.mark_filename,
            sizeof(redo_ctx.mark_filename));
    redo_ctx.current_stage = MIGRATE_REDO_STAGE_RENAME;
    if ((result=write_to_redo_file(&redo_ctx)) != 0) {
        return result;
    }

    return slice_migrate_redo(&redo_ctx);
}

static int parse_sn(string_t *line, volatile uint64_t *sn, bool *need_migrate)
{
    string_t cols[BINLOG_MAX_FIELD_COUNT];
    int col_count;

    if (!(line->len > 0 && line->str[line->len - 1] == '\n')) {
        logError("file: "__FILE__", line: %d, "
                "the last line of slice binlog is invalid, "
                "line length: %d, last line: %.*s", __LINE__,
                line->len, line->len, line->str);
        return EINVAL;
    }

    col_count = split_string_ex(line, ' ', cols,
            BINLOG_MAX_FIELD_COUNT, false);
    if (col_count == ADD_SLICE_EXPECT_FIELD_COUNT ||
            col_count == DEL_SLICE_EXPECT_FIELD_COUNT ||
            col_count == DEL_BLOCK_EXPECT_FIELD_COUNT)
    {
        string_t *s;
        char tmp[32];

        s = cols + SLICE_BINLOG_FIELD_INDEX_SN;
        snprintf(tmp, sizeof(tmp), "%.*s", s->len, s->str);
        *sn = strtoll(tmp, NULL, 10);
        *need_migrate = false;
        return 0;
    }

    if (!(col_count == ADD_SLICE_EXPECT_FIELD_COUNT - 1 ||
                col_count == DEL_SLICE_EXPECT_FIELD_COUNT - 1 ||
                col_count == DEL_BLOCK_EXPECT_FIELD_COUNT - 1))
    {
        logError("file: "__FILE__", line: %d, "
                "the last line of slice binlog is invalid, "
                "field count: %d, last line: %.*s", __LINE__,
                col_count, line->len, line->str);
        return EINVAL;
    }

    *need_migrate = true;
    return 0;
}

static int get_last_sn(bool *migrate_flag)
{
    char filename[PATH_MAX];
    char buff[FS_SLICE_BINLOG_MAX_RECORD_SIZE];
    string_t line;
    int start_index;
    int last_index;
    int line_count;
    bool need_migrate;
    int result;

    if ((result=sf_binlog_writer_get_binlog_indexes(DATA_PATH_STR,
                    FS_SLICE_BINLOG_SUBDIR_NAME, &start_index,
                    &last_index)) != 0)
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
    if ((result=parse_sn(&line, &SLICE_BINLOG_SN, &need_migrate)) != 0) {
        return result;
    }

    if (!need_migrate) {
        if (SLICE_BINLOG_SN > 0) {
            slice_binlog_set_next_version();
        }
        return 0;
    } else if (migrate_flag == NULL) {
        return EINVAL;
    }


    //check the first line also
    sf_binlog_writer_get_filename(DATA_PATH_STR, FS_SLICE_BINLOG_SUBDIR_NAME,
            start_index, filename, sizeof(filename));
    if ((result=fc_get_first_line(filename, buff, sizeof(buff), &line)) != 0) {
        return result;
    }
    if (line.len > 0) {
        if ((result=parse_sn(&line, &SLICE_BINLOG_SN, &need_migrate)) != 0) {
            return result;
        }

        if (!need_migrate) {
            logError("file: "__FILE__", line: %d, "
                    "slice binlog formats not consistent!", __LINE__);
            return EINVAL;
        }
    }

    if (STORAGE_ENABLED) {
        logError("file: "__FILE__", line: %d, "
                "can't enable storage engine plugin before trunk "
                "migrate! you should enable storage engine when "
                "trunk migrate done.", __LINE__);
        return EINVAL;
    }

    if ((result=trunk_migrate_create()) != 0) {
        return result;
    }

    *migrate_flag = true;
    return slice_binlog_do_migrate();
}

int slice_binlog_get_last_sn_from_file()
{
    int result;
    bool migrate_flag;

    migrate_flag = false;
    if ((result=get_last_sn(&migrate_flag)) != 0) {
        return result;
    }

    if (migrate_flag) {
        result = get_last_sn(NULL);
    }

    return result;
}

int slice_binlog_migrate_redo()
{
    int result;
    SliceMigrateRedoContext redo_ctx;

    get_migrate_mark_filename(redo_ctx.mark_filename,
            sizeof(redo_ctx.mark_filename));
    if (access(redo_ctx.mark_filename, F_OK) != 0) {
        result = errno != 0 ? errno : EPERM;
        if (result == ENOENT) {
            return 0;
        }

        logError("file: "__FILE__", line: %d, "
                "access file %s fail, errno: %d, error info: %s",
                __LINE__, redo_ctx.mark_filename, result, STRERROR(result));
        return result;
    }

    if ((result=load_from_redo_file(&redo_ctx)) != 0) {
        return result;
    }

    return slice_migrate_redo(&redo_ctx);
}

int slice_binlog_init()
{
    return init_binlog_writer();
}

int slice_binlog_load()
{
    return slice_loader_load(&SLICE_BINLOG_WRITER.writer);
}

void slice_binlog_destroy()
{
    sf_binlog_writer_finish(&SLICE_BINLOG_WRITER.writer);
}

static int slice_binlog_log_no_op(const FSBlockKey *bkey,
        const time_t current_time, const uint64_t sn,
        const uint64_t data_version, const int source)
{
    SFBinlogWriterBuffer *wbuffer;

    if ((wbuffer=sf_binlog_writer_alloc_buffer(
                    &SLICE_BINLOG_WRITER.thread)) == NULL)
    {
        return ENOMEM;
    }

    SF_BINLOG_BUFFER_SET_VERSION(wbuffer, sn);
    wbuffer->bf.length = slice_binlog_log_update_block_to_buff(
            bkey, current_time, BINLOG_OP_TYPE_NO_OP, sn,
            data_version, source, wbuffer->bf.buff);
    sf_push_to_binlog_write_queue(&SLICE_BINLOG_WRITER.writer, wbuffer);
    FC_ATOMIC_INC(SLICE_BINLOG_COUNT);
    SLICE_SPACE_LOG_CTX.last_sn = sn;
    return 0;
}

int slice_binlog_padding_ex(const int row_count,
        const int source, const bool direct_log)
{
    const int64_t data_version = 0;
    int result;
    int i;
    int64_t sn = 0;
    time_t current_time;
    FSBlockKey bkey;

    current_time = g_current_time;
    bkey.oid = 1;
    bkey.offset = 0;
    for (i=1; i<=row_count; i++) {
        if (committed_version_inited()) {
            sn = ob_index_generate_alone_sn();
        } else {
            sn = __sync_add_and_fetch(&SLICE_BINLOG_SN, 1);
        }
        if (direct_log) {
            result = slice_binlog_log_no_op(&bkey, current_time + i,
                    sn, data_version, source);
        } else {
            result = slice_binlog_no_op_push(&bkey, current_time + i,
                    sn, data_version, source);
        }
        if (result != 0) {
            return result;
        }
    }

    if (sn > 0) {
        while (sf_binlog_writer_get_last_version(
                    &SLICE_BINLOG_WRITER.writer) < sn)
        {
            fc_sleep_ms(1);
        }
    }

    return 0;
}

void slice_binlog_writer_stat(FSBinlogWriterStat *stat)
{
    stat->total_count = SLICE_BINLOG_WRITER.writer.fw.total_count;
    stat->next_version = SLICE_BINLOG_WRITER.writer.version_ctx.next;
    stat->waiting_count = SLICE_BINLOG_WRITER.writer.version_ctx.ring.waiting_count;
    stat->max_waitings = SLICE_BINLOG_WRITER.writer.version_ctx.ring.max_waitings;
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

    BINLOG_PARSE_INT_SILENCE(record->timestamp, "timestamp",
            SLICE_BINLOG_FIELD_INDEX_TIMESTAMP, ' ', 0);
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
    fs_calc_block_hashcode(&record->bs_key.block, FILE_BLOCK_SIZE);

    switch (record->op_type) {
        case BINLOG_OP_TYPE_WRITE_SLICE:
            record->slice_type = DA_SLICE_TYPE_FILE;
            return unpack_add_slice_record(cols,
                    count, record, error_info);
        case BINLOG_OP_TYPE_ALLOC_SLICE:
            record->slice_type = DA_SLICE_TYPE_ALLOC;
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

static int slice_binlog_get_first_sn(const char *filename, int64_t *sn)
{
    int result;
    char buff[FS_SLICE_BINLOG_MAX_RECORD_SIZE];
    char error_info[256];
    string_t line;
    SliceBinlogRecord record;

    if ((result=fc_get_first_line(filename, buff,
                    sizeof(buff), &line)) != 0)
    {
        return result;
    }

    if ((result=slice_binlog_record_unpack(&line,
                    &record, error_info)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "slice first line of binlog file %s, %s",
                __LINE__, filename, error_info);
        return result;
    }

    *sn = record.sn;
    return 0;
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
                fs_calc_block_hashcode(&record->bkey, FILE_BLOCK_SIZE);
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

static int find_position_by_dv(const char *filename, const int data_group_id,
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
                fs_calc_block_hashcode(&record.bkey, FILE_BLOCK_SIZE);
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
            return find_position_by_dv(filename, data_group_id,
                    last_data_version, pos);
        }

        --binlog_index;
    }

    return ENOENT;
}

static int find_position_by_sn(const char *filename,
        const uint64_t last_sn, SFBinlogFilePosition *pos)
{
    char buff[64 * 1024];
    char error_info[256];
    string_t line;
    SliceBinlogRecord record;
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
            if ((result=slice_binlog_record_unpack(&line,
                            &record, error_info)) != 0)
            {
                logError("file: "__FILE__", line: %d, "
                        "binlog file %s, %s", __LINE__,
                        filename, error_info);
                break;
            }

            if (record.sn > last_sn) {
                pos->offset += (line.str - buff);
                found = true;
                break;
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

int slice_binlog_get_position_by_sn(const uint64_t last_sn,
        SFBinlogFilePosition *pos)
{
    int result;
    int start_index;
    int last_index;
    int binlog_index;
    SFBinlogWriterInfo *writer;
    char filename[PATH_MAX];
    int64_t first_sn;

    writer = slice_binlog_get_writer();
    if ((result=sf_binlog_get_indexes(writer, &start_index,
                    &last_index)) != 0)
    {
        return result;
    }
    if (last_sn == 0) {
        pos->index = start_index;
        pos->offset = 0;
        return 0;
    }

    binlog_index = last_index;
    while (binlog_index >= start_index) {
        sf_binlog_writer_get_filename(DATA_PATH_STR,
                FS_SLICE_BINLOG_SUBDIR_NAME, binlog_index,
                filename, sizeof(filename));
        pos->index = binlog_index;
        pos->offset = 0;
        if ((result=slice_binlog_get_first_sn(filename, &first_sn)) != 0) {
            if (result == ENOENT) {
                --binlog_index;
                continue;
            }

            return result;
        }

        if (last_sn + 1 == first_sn) {
            return 0;
        } else if (last_sn + 1 > first_sn) {
            return find_position_by_sn(filename, last_sn, pos);
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
            fs_calc_block_hashcode(&record->bkey, FILE_BLOCK_SIZE);
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

int slice_migrate_done_callback(const DATrunkFileInfo *trunk,
        const DAPieceFieldInfo *field, struct fc_queue_info *space_chain,
        SFSynchronizeContext *sctx, int *flags)
{
    const bool call_by_reclaim = true;
    int result;
    int update_count;
    DASliceType slice_type;
    DATrunkSpaceInfo space;
    DASliceEntry se;
    FSSliceSpaceLogRecord *record;

    if ((record=slice_space_log_alloc_init_record()) == NULL) {
        return ENOMEM;
    }

    space.store = &trunk->allocator->path_info->store;
    space.id_info = trunk->id_info;
    space.offset = field->storage.offset;
    space.size = field->storage.size;
    se.timestamp = g_current_time;
    se.source = BINLOG_SOURCE_RECLAIM;
    se.data_version = field->storage.version;
    se.bs_key.block.oid = field->oid;
    se.bs_key.block.offset = field->fid;
    se.bs_key.slice.offset = field->extra;
    se.bs_key.slice.length = field->storage.length;
    se.sn = 0;
    fs_calc_block_hashcode(&se.bs_key.block, FILE_BLOCK_SIZE);
    slice_type = ((DATrunkSpaceLogRecord *)space_chain->tail)->slice_type;
    if ((result=ob_index_update_slice(&se, &space, &update_count,
                    record, slice_type, call_by_reclaim)) != 0)
    {
        return result;
    }

    da_trunk_space_log_free_chain(&DA_CTX, space_chain);
    if (record->slice_chain.head != NULL) {
        record->sctx = sctx;
        slice_space_log_push(record);
    } else {
        sf_synchronize_counter_notify(sctx, 1);
        slice_space_log_free_record(record);
    }

    *flags = update_count > 0 ? 0 : DA_REDO_QUEUE_PUSH_FLAGS_IGNORE;
    return 0;
}

static void dedup_space_chain(struct fc_queue_info *space_chain,
        const int update_count)
{
    DATrunkSpaceLogRecord *current;
    DATrunkSpaceLogRecord *next;
    DATrunkSpaceLogRecord *last;
    DATrunkSpaceLogRecord *deleted_records[2];

    current = space_chain->head;
    last = space_chain->tail;
    space_chain->head = space_chain->tail = NULL;
    while (1) {
        while (current != NULL && !(current->op_type ==
                    da_binlog_op_type_consume_space &&
                    current->slice_type == DA_SLICE_TYPE_CACHE))
        {
            next = current->next;
            DA_SPACE_LOG_ADD_TO_CHAIN(space_chain, current);
            current = next;
        }
        if (current == NULL) {
            break;
        }

        if ((next=current->next) == NULL) {
            DA_SPACE_LOG_ADD_TO_CHAIN(space_chain, current);
            break;
        }

        if (next->op_type == da_binlog_op_type_reclaim_space &&
                next->storage.trunk_id == current->storage.trunk_id &&
                next->storage.offset == current->storage.offset)
        {
            if (current->trunk != NULL) {
                if (update_count > 0) {
                    last->trunk = current->trunk;
                } else {
                    da_trunk_freelist_decrease_writing_count(current->trunk);
                }
                current->trunk = NULL;
            }

            deleted_records[0] = current;
            deleted_records[1] = next;
            current = next->next;
            da_trunk_space_log_free_records(&DA_CTX, deleted_records, 2);
            if (current == NULL) {
                break;
            }
        } else {
            DA_SPACE_LOG_ADD_TO_CHAIN(space_chain, current);
            current = next;
        }
    }
}

int slice_binlog_cached_slice_write_done(const DASliceEntry *se,
        const DATrunkSpaceInfo *space, void *arg)
{
    const bool call_by_reclaim = false;
    int result;
    int update_count;
    FSSliceSpaceLogRecord *record;
    SFBinlogWriterBuffer *wbuffer;

    record = arg;
    if ((result=ob_index_update_slice(se, space, &update_count, record,
                    DA_SLICE_TYPE_FILE, call_by_reclaim)) != 0)
    {
        return result;
    }

    if ((wbuffer=sf_binlog_writer_alloc_buffer(
                    &SLICE_BINLOG_WRITER.thread)) == NULL)
    {
        return ENOMEM;
    }

    SF_BINLOG_BUFFER_SET_VERSION(wbuffer, se->sn);
    wbuffer->bf.length = slice_binlog_log_add_slice_to_buff1(
            DA_SLICE_TYPE_FILE, &se->bs_key.block, &se->bs_key.slice,
            space, se->timestamp, se->sn, se->data_version, se->source,
            wbuffer->bf.buff);
    wbuffer->next = NULL;

    slice_space_log_queue_lock();
    record->slice_chain.head = wbuffer;
    record->slice_chain.count = 1;
    dedup_space_chain(&record->space_chain, update_count);
    record->status = FS_SLICE_SPACE_LOG_RECORD_STATUS_READY;
    slice_space_log_queue_unlock();

    return 0;
}

int slice_binlog_del_slice_push(const FSBlockSliceKeyInfo *bs_key,
        const time_t current_time, const uint64_t sn,
        const uint64_t data_version, const int source,
        struct fc_queue_info *space_chain)
{
    FSSliceSpaceLogRecord *record;
    int result;

    if ((record=slice_space_log_alloc_record()) == NULL) {
        return ENOMEM;
    }

    record->space_chain = *space_chain;
    if ((result=ob_index_del_slice_to_wbuffer_chain(record, bs_key,
                    current_time, sn, data_version, source)) != 0)
    {
        return result;
    }
    slice_space_log_push(record);
    return 0;
}

int slice_binlog_no_op_push(const FSBlockKey *bkey,
        const time_t current_time, const uint64_t sn,
        const uint64_t data_version, const int source)
{
    FSSliceSpaceLogRecord *record;
    SFBinlogWriterBuffer *wbuffer;

    if ((record=slice_space_log_alloc_record()) == NULL) {
        return ENOMEM;
    }

    record->space_chain.head = record->space_chain.tail = NULL;
    if ((wbuffer=sf_binlog_writer_alloc_buffer(
                    &SLICE_BINLOG_WRITER.thread)) == NULL)
    {
        return ENOMEM;
    }

    SF_BINLOG_BUFFER_SET_VERSION(wbuffer, sn);
    wbuffer->bf.length = slice_binlog_log_update_block_to_buff(
            bkey, current_time, BINLOG_OP_TYPE_NO_OP, sn,
            data_version, source, wbuffer->bf.buff);
    wbuffer->next = NULL;
    record->slice_chain.head = wbuffer;
    record->slice_chain.count = 1;
    record->last_sn = sn;
    slice_space_log_push(record);
    return 0;
}
