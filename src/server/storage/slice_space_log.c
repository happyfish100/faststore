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

#include "fastcommon/logger.h"
#include "sf/sf_func.h"
#include "diskallocator/binlog/trunk/trunk_space_log.h"
#include "../binlog/slice_binlog.h"
#include "slice_space_log.h"

#define FIELD_TMP_FILENAME  ".slice.tmp"
#define FIELD_REDO_FILENAME  "slice.redo"
#define SPACE_TMP_FILENAME  ".space.tmp"
#define SPACE_REDO_FILENAME  "space.redo"

typedef struct {
    SFBinlogWriterBuffer *head;
    int count;
    int64_t last_sn;
} SliceBinlogRecordChain;

static inline int buffer_to_file(FSBinlogWriteFileBufferPair *pair)
{
    int len;
    int result;

    if ((len=pair->buffer.length) == 0) {
        return 0;
    }

    pair->buffer.length = 0;
    if (fc_safe_write(pair->fi.fd, pair->buffer.data, len) != len) {
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "write to fd: %d fail, errno: %d, error info: %s",
                __LINE__, pair->fi.fd, result, STRERROR(result));
        return result;
    } else {
        return 0;
    }
}

static int write_slice_redo_log(FSSliceSpaceLogRecord *record)
{
    int result;
    SFBinlogWriterBuffer *wbuffer;

    wbuffer = record->slice_head;
    while (wbuffer != NULL) {
        if (SLICE_SPACE_LOG_CTX.slice_redo.buffer.alloc_size -
                SLICE_SPACE_LOG_CTX.slice_redo.buffer.length <
                wbuffer->bf.length)
        {
            if ((result=buffer_to_file(&SLICE_SPACE_LOG_CTX.
                            slice_redo)) != 0)
            {
                return result;
            }
        }

        memcpy(SLICE_SPACE_LOG_CTX.slice_redo.buffer.data +
                SLICE_SPACE_LOG_CTX.slice_redo.buffer.length,
                wbuffer->bf.buff, wbuffer->bf.length);
        SLICE_SPACE_LOG_CTX.slice_redo.buffer.length += wbuffer->bf.length;
        SLICE_SPACE_LOG_CTX.slice_redo.record_count++;

        wbuffer = wbuffer->next;
    }

    return 0;
}

static int write_space_redo_log(struct fc_queue_info *space_chain)
{
    int result;
    DATrunkSpaceLogRecord *space_log;

    space_log = space_chain->head;
    while (space_log != NULL) {
        if (SLICE_SPACE_LOG_CTX.space_redo.buffer.alloc_size -
                SLICE_SPACE_LOG_CTX.space_redo.buffer.length <
                FS_SLICE_BINLOG_MAX_RECORD_SIZE)
        {
            if ((result=buffer_to_file(&SLICE_SPACE_LOG_CTX.
                            space_redo)) != 0)
            {
                return result;
            }
        }

        da_trunk_space_log_pack(space_log, &SLICE_SPACE_LOG_CTX.
                space_redo.buffer, DA_CTX.storage.have_extra_field);
        SLICE_SPACE_LOG_CTX.space_redo.record_count++;
        space_log = space_log->next;
    }

    return 0;
}

static inline int write_record_redo_log(FSSliceSpaceLogRecord *record)
{
    int result;

    if ((result=write_space_redo_log(&record->space_chain)) != 0) {
        return result;
    }
    return write_slice_redo_log(record);
}

static inline int open_redo_logs()
{
    int result;

    if ((result=fc_safe_write_file_open(&SLICE_SPACE_LOG_CTX.
                    space_redo.fi)) != 0)
    {
        return result;
    }

    return fc_safe_write_file_open(&SLICE_SPACE_LOG_CTX.slice_redo.fi);
}

static inline int close_redo_logs()
{
    int result;

    if ((result=buffer_to_file(&SLICE_SPACE_LOG_CTX.space_redo)) != 0) {
        return result;
    }

    if ((result=buffer_to_file(&SLICE_SPACE_LOG_CTX.slice_redo)) != 0) {
        return result;
    }

    if ((result=fc_safe_write_file_close(&SLICE_SPACE_LOG_CTX.
                    space_redo.fi)) != 0)
    {
        return result;
    }
    return fc_safe_write_file_close(&SLICE_SPACE_LOG_CTX.slice_redo.fi);
}

static int write_redo_logs(const struct fc_queue_info *qinfo)
{
    int result;
    FSSliceSpaceLogRecord *record;

    if ((result=open_redo_logs()) != 0) {
        return result;
    }

    record = (FSSliceSpaceLogRecord *)qinfo->head;
    do {
        SLICE_SPACE_LOG_CTX.record_count++;
        if ((result=write_record_redo_log(record)) != 0) {
            return result;
        }
    } while ((record=record->next) != NULL);

    return close_redo_logs();
}

static inline void push_to_log_queues(struct fc_queue_info *qinfo)
{
    FSSliceSpaceLogRecord *record;
    SFBinlogWriterBuffer *wbuffer;

    record = (FSSliceSpaceLogRecord *)qinfo->head;
    do {
        while (record->slice_head != NULL) {
            wbuffer = record->slice_head;
            record->slice_head = record->slice_head->next;
            sf_push_to_binlog_write_queue(&SLICE_BINLOG_WRITER.
                    writer, wbuffer);
            FC_ATOMIC_INC(SLICE_BINLOG_COUNT);
        }

        da_trunk_space_log_push_chain(&DA_CTX, &record->space_chain);
    } while ((record=record->next) != NULL);
}

static int deal_records(struct fc_queue_info *qinfo)
{
    int result;

    SLICE_SPACE_LOG_CTX.slice_redo.record_count = 0;
    SLICE_SPACE_LOG_CTX.space_redo.record_count = 0;
    if ((result=write_redo_logs(qinfo)) != 0) {
        return result;
    }

    da_trunk_space_log_inc_waiting_count(&DA_CTX, SLICE_SPACE_LOG_CTX.
            space_redo.record_count);

    push_to_log_queues(qinfo);

    da_trunk_space_log_wait(&DA_CTX);

    sorted_queue_free_chain(&SLICE_SPACE_LOG_CTX.queue,
            &SLICE_SPACE_LOG_CTX.allocator, qinfo);
    return 0;
}

static void *slice_space_log_func(void *arg)
{
    FSSliceSpaceLogRecord less_equal;
    struct fc_queue_info qinfo;

#ifdef OS_LINUX
    prctl(PR_SET_NAME, "SE-binlog-write");
#endif

    while (SF_G_CONTINUE_FLAG) {
        less_equal.last_sn = FC_ATOMIC_GET(COMMITTED_VERSION_RING.next_sn) - 1;
        sorted_queue_try_pop_to_queue(&SLICE_SPACE_LOG_CTX.
                queue, &less_equal, &qinfo);
        SLICE_SPACE_LOG_CTX.record_count = 0;
        if (qinfo.head != NULL) {
            if (deal_records(&qinfo) != 0) {
                logCrit("file: "__FILE__", line: %d, "
                        "deal notify events fail, "
                        "program exit!", __LINE__);
                sf_terminate_myself();
                break;
            }
        }

        if (SLICE_SPACE_LOG_CTX.record_count <= 10) {
            fc_sleep_ms(1000);
        } else if (SLICE_SPACE_LOG_CTX.record_count <= 100) {
            fc_sleep_ms(100);
        } else if (SLICE_SPACE_LOG_CTX.record_count <= 1000) {
            fc_sleep_ms(10);
        } else if (SLICE_SPACE_LOG_CTX.record_count <= 10000) {
            fc_sleep_ms(1);
        }
    }

    return NULL;
}


static int init_file_buffer_pair(FSBinlogWriteFileBufferPair *pair,
        const char *file_path, const char *redo_filename,
        const char *tmp_filename)
{
    const int buffer_size = 64 * 1024;
    int result;

    if ((result=fc_safe_write_file_init(&pair->fi, file_path,
                    redo_filename, tmp_filename)) != 0)
    {
        return result;
    }

    return fast_buffer_init_ex(&pair->buffer, buffer_size);
}

static int slice_space_log_compare(const FSSliceSpaceLogRecord *record1,
        const FSSliceSpaceLogRecord *record2)
{
    return fc_compare_int64(record1->last_sn, record2->last_sn);
}

static int do_load(const char *filename, const string_t *content,
        SliceBinlogRecordChain *chain)
{
    int result;
    int line_count;
    string_t line;
    char *line_start;
    char *buff_end;
    char *line_end;
    SliceBinlogRecord record;
    SFBinlogWriterBuffer *wbuffer;
    SFBinlogWriterBuffer *tail;
    char error_info[256];

    line_count = 0;
    result = 0;
    tail = NULL;
    *error_info = '\0';
    line_start = content->str;
    buff_end = content->str + content->len;
    while (line_start < buff_end) {
        line_end = (char *)memchr(line_start, '\n', buff_end - line_start);
        if (line_end == NULL) {
            break;
        }

        ++line_count;
        ++line_end;
        line.str = line_start;
        line.len = line_end - line_start;
        if ((result=slice_binlog_record_unpack(&line,
                        &record, error_info)) != 0)
        {
            logError("file: "__FILE__", line: %d, "
                    "parse record fail, filename: %s, line no: %d%s%s",
                    __LINE__, filename, line_count, (*error_info != '\0' ?
                        ", error info: " : ""), error_info);
            break;
        }

        if (record.sn > SLICE_BINLOG_SN) {
            if (record.sn > chain->last_sn) {
                chain->last_sn = record.sn;
            }

            if ((wbuffer=sf_binlog_writer_alloc_buffer(
                            &SLICE_BINLOG_WRITER.thread)) == NULL)
            {
                result = ENOMEM;
                break;
            }

            SF_BINLOG_BUFFER_SET_VERSION(wbuffer, record.sn);
            memcpy(wbuffer->bf.buff, line.str, line.len);
            wbuffer->bf.length = line.len;
            wbuffer->next = NULL;
            if (chain->head == NULL) {
                chain->head = wbuffer;
            } else {
                tail->next = wbuffer;
            }
            tail = wbuffer;
            chain->count++;
        }

        line_start = line_end;
    }

    return result;
}

static int slice_redo_load(const char *filename,
        SliceBinlogRecordChain *chain)
{
    int result;
    int64_t file_size;
    string_t content;

    if (access(filename, F_OK) != 0) {
        result = errno != 0 ? errno : EPERM;
        if (result == ENOENT) {
            return 0;
        } else {
            logError("file: "__FILE__", line: %d, "
                    "access file %s fail, errno: %d, error info: %s",
                    __LINE__, filename, result, STRERROR(result));
            return result;
        }
    }

    if ((result=getFileContent(filename, &content.str, &file_size)) != 0) {
        return result;
    }
    content.len = file_size;
    result = do_load(filename, &content, chain);
    free(content.str);
    return result;
}

static int slice_log_redo(const char *slice_log_filename)
{
    int result;
    SliceBinlogRecordChain chain;
    SFBinlogWriterBuffer *wbuffer;

    chain.head = NULL;
    chain.count = 0;
    chain.last_sn = 0;
    if ((result=slice_redo_load(slice_log_filename, &chain)) != 0) {
        return result;
    }

    logInfo("slice redo record count: %d", chain.count);

    if (chain.count == 0) {
        return 0;
    }

    while (chain.head != NULL) {
        wbuffer = chain.head;
        chain.head = chain.head->next;
        sf_push_to_binlog_write_queue(&SLICE_BINLOG_WRITER.writer, wbuffer);
    }

    while (sf_binlog_writer_get_last_version(
                &SLICE_BINLOG_WRITER.
                writer) < chain.last_sn)
    {
        fc_sleep_ms(1);
    }
    SLICE_BINLOG_SN = chain.last_sn;

    return result;
}

static int slice_space_log_redo()
{
    int result;
    char space_tmp_filename[PATH_MAX];
    char field_tmp_filename[PATH_MAX];
    char space_log_filename[PATH_MAX];
    char slice_log_filename[PATH_MAX];

    snprintf(space_tmp_filename, sizeof(space_tmp_filename),
            "%s/%s", DATA_PATH_STR, SPACE_TMP_FILENAME);
    snprintf(field_tmp_filename, sizeof(field_tmp_filename),
            "%s/%s", DATA_PATH_STR, FIELD_TMP_FILENAME);
    snprintf(space_log_filename, sizeof(space_log_filename),
            "%s/%s", DATA_PATH_STR, SPACE_REDO_FILENAME);
    snprintf(slice_log_filename, sizeof(slice_log_filename),
            "%s/%s", DATA_PATH_STR, FIELD_REDO_FILENAME);
    if (access(space_tmp_filename, F_OK) != 0 &&
            access(field_tmp_filename, F_OK) == 0)
    {
        /* compensate for two phases renames */
        if (rename(field_tmp_filename, slice_log_filename) != 0) {
            result = errno != 0 ? errno : EIO;
            logError("file: "__FILE__", line: %d, "
                    "rename file \"%s\" to \"%s\" fail, "
                    "errno: %d, error info: %s", __LINE__,
                    field_tmp_filename, slice_log_filename,
                    result, STRERROR(result));
            return result;
        }
    }

    if ((result=da_trunk_space_log_redo(&DA_CTX, space_log_filename)) != 0) {
        return result;
    }

    if ((result=slice_log_redo(slice_log_filename)) != 0) {
        return result;
    }

    return 0;
}

int slice_space_log_init()
{
    int result;
    pthread_t tid;

    if ((result=init_file_buffer_pair(&SLICE_SPACE_LOG_CTX.
                    slice_redo, DATA_PATH_STR, FIELD_REDO_FILENAME,
                    FIELD_TMP_FILENAME)) != 0)
    {
        return result;
    }

    if ((result=init_file_buffer_pair(&SLICE_SPACE_LOG_CTX.
                    space_redo, DATA_PATH_STR, SPACE_REDO_FILENAME,
                    SPACE_TMP_FILENAME)) != 0)
    {
        return result;
    }

    if ((result=fast_mblock_init_ex1(&SLICE_SPACE_LOG_CTX.allocator,
                    "slice-space-log", sizeof(FSSliceSpaceLogRecord),
                    8 * 1024, 0, NULL , NULL, true)) != 0)
    {
        return result;
    }

    if ((result=sorted_queue_init(&SLICE_SPACE_LOG_CTX.queue, (long)
                    (&((FSSliceSpaceLogRecord *)NULL)->next),
                    (int (*)(const void *, const void *))
                    slice_space_log_compare)) != 0)
    {
        return result;
    }

    if ((result=slice_space_log_redo()) != 0) {
        return result;
    }

    return fc_create_thread(&tid, slice_space_log_func,
            NULL, SF_G_THREAD_STACK_SIZE);
}

void slice_space_log_destroy()
{
}
