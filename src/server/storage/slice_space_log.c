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
#include "slice_space_log.h"

#define FIELD_TMP_FILENAME  ".field.tmp"
#define FIELD_REDO_FILENAME  "field.redo"
#define SPACE_TMP_FILENAME  ".space.tmp"
#define SPACE_REDO_FILENAME  "space.redo"

typedef struct piece_field_with_version {
    DAPieceFieldInfo *field;
    int version;
} PieceFieldWithVersion;

typedef struct piece_field_with_version_array {
    PieceFieldWithVersion *records;
    int count;
} PieceFieldWithVersionArray;

static inline int buffer_to_file(FDIRBinlogWriteFileBufferPair *pair)
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

static int write_field_redo_log(FSSliceSpaceLogRecord *record)
{
    int result;

    inode_binlog_pack(&record->inode.field, &record->inode.buffer);
    if (SLICE_SPACE_LOG_CTX.field_redo.buffer.alloc_size -
            SLICE_SPACE_LOG_CTX.field_redo.buffer.length <
            record->inode.buffer.length)
    {
        if ((result=buffer_to_file(&SLICE_SPACE_LOG_CTX.
                        field_redo)) != 0)
        {
            return result;
        }
    }

    memcpy(SLICE_SPACE_LOG_CTX.field_redo.buffer.data +
            SLICE_SPACE_LOG_CTX.field_redo.buffer.length,
            record->inode.buffer.buff, record->inode.buffer.length);
    SLICE_SPACE_LOG_CTX.field_redo.buffer.length +=
        record->inode.buffer.length;
    SLICE_SPACE_LOG_CTX.field_redo.record_count++;
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
                FDIR_INODE_BINLOG_RECORD_MAX_SIZE)
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
    return write_field_redo_log(record);
}

static inline int open_redo_logs()
{
    int result;

    if ((result=fc_safe_write_file_open(&SLICE_SPACE_LOG_CTX.
                    space_redo.fi)) != 0)
    {
        return result;
    }

    return fc_safe_write_file_open(&SLICE_SPACE_LOG_CTX.field_redo.fi);
}

static inline int close_redo_logs()
{
    int result;

    if ((result=buffer_to_file(&SLICE_SPACE_LOG_CTX.space_redo)) != 0) {
        return result;
    }

    if ((result=buffer_to_file(&SLICE_SPACE_LOG_CTX.field_redo)) != 0) {
        return result;
    }

    if ((result=fc_safe_write_file_close(&SLICE_SPACE_LOG_CTX.
                    space_redo.fi)) != 0)
    {
        return result;
    }
    return fc_safe_write_file_close(&SLICE_SPACE_LOG_CTX.field_redo.fi);
}

/*
static FILE *fp = NULL;
static void write_debug(FSSliceSpaceLogRecord *record)
{
    const char *filename = "/tmp/wqueue.log";
    const char *bak_filename = "/tmp/wqueue.txt";
    int result;

    if (fp == NULL) {
        if (access(filename, F_OK) == 0) {
            rename(filename, bak_filename);
        }

        fp = fopen(filename, "w");
        if (fp == NULL) {
            result = errno != 0 ? errno : EIO;
            logError("file: "__FILE__", line: %d, "
                    "open file to write fail, error info: %s",
                    __LINE__, STRERROR(result));
            return;
        }
    }

    fprintf(fp, "%"PRId64". %"PRId64" %"PRId64" %c %d\n", record->version,
            record->inode.field.oid, record->inode.field.fid,
            record->inode.field.op_type, record->inode.buffer.length);
}

static void queue_debug(const struct fc_queue_info *qinfo)
{
    FSSliceSpaceLogRecord *record;

    record = (FSSliceSpaceLogRecord *)qinfo->head;
    do {
        write_debug(record);
    } while ((record=record->next) != NULL);

    if (fp != NULL) {
        fflush(fp);
    }
}
*/

static int write_redo_logs(const struct fc_queue_info *qinfo)
{
    int result;
    FSSliceSpaceLogRecord *record;

    if ((result=open_redo_logs()) != 0) {
        return result;
    }

    record = (FSSliceSpaceLogRecord *)qinfo->head;
    do {
        if ((result=write_record_redo_log(record)) != 0) {
            return result;
        }
    } while ((record=record->next) != NULL);

    return close_redo_logs();
}

static inline void push_to_log_queues(struct fc_queue_info *qinfo)
{
    FSSliceSpaceLogRecord *record;

    record = (FSSliceSpaceLogRecord *)qinfo->head;
    do {
        inode_binlog_writer_log(record->inode.segment,
                &record->inode.buffer);
        da_trunk_space_log_push_chain(&DA_CTX, &record->space_chain);
    } while ((record=record->next) != NULL);
}

static void notify_all(struct fc_queue_info *qinfo)
{
    FSSliceSpaceLogRecord *record;
    SFSynchronizeContext *sctx;
    int count;

    sctx = NULL;
    count = 0;
    record = qinfo->head;
    do {
        if (record->sctx != NULL) {
            if (sctx != record->sctx) {
                if (sctx != NULL) {
                    sf_synchronize_counter_notify(sctx, count);
                }

                sctx = record->sctx;
                count = 1;
            } else {
                ++count;
            }
        }
    } while ((record=record->next) != NULL);

    if (sctx != NULL) {
        sf_synchronize_counter_notify(sctx, count);
    }
}

static int deal_records(struct fc_queue_info *qinfo)
{
    int result;

    //queue_debug(qinfo);

    SLICE_SPACE_LOG_CTX.field_redo.record_count = 0;
    SLICE_SPACE_LOG_CTX.space_redo.record_count = 0;
    if ((result=write_redo_logs(qinfo)) != 0) {
        return result;
    }

    da_binlog_writer_inc_waiting_count(&INODE_BINLOG_WRITER,
            SLICE_SPACE_LOG_CTX.field_redo.record_count);
    da_trunk_space_log_inc_waiting_count(&DA_CTX, SLICE_SPACE_LOG_CTX.
            space_redo.record_count);

    push_to_log_queues(qinfo);

    da_binlog_writer_wait(&INODE_BINLOG_WRITER);
    da_trunk_space_log_wait(&DA_CTX);

    notify_all(qinfo);

    fc_queue_free_chain(&SLICE_SPACE_LOG_CTX.queue,
            &UPDATE_RECORD_ALLOCATOR, qinfo);
    return 0;
}

static void *slice_space_log_func(void *arg)
{
    struct fc_queue_info qinfo;

#ifdef OS_LINUX
    prctl(PR_SET_NAME, "SE-binlog-write");
#endif

    while (SF_G_CONTINUE_FLAG) {
        fc_queue_pop_to_queue(&SLICE_SPACE_LOG_CTX.queue, &qinfo);
        if (qinfo.head != NULL) {
            if (deal_records(&qinfo) != 0) {
                logCrit("file: "__FILE__", line: %d, "
                        "deal notify events fail, "
                        "program exit!", __LINE__);
                sf_terminate_myself();
            }
        }
    }

    return NULL;
}


static int init_file_buffer_pair(FDIRBinlogWriteFileBufferPair *pair,
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
    return fc_compare_int64(record1->sn, record2->sn);
}

int slice_space_log_init()
{
    int result;

    if ((result=init_file_buffer_pair(&SLICE_SPACE_LOG_CTX.
                    field_redo, STORAGE_PATH_STR, FIELD_REDO_FILENAME,
                    FIELD_TMP_FILENAME)) != 0)
    {
        return result;
    }

    if ((result=init_file_buffer_pair(&SLICE_SPACE_LOG_CTX.
                    space_redo, STORAGE_PATH_STR, SPACE_REDO_FILENAME,
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

    return 0;
}

static void field_log_to_ptr_array(const DAPieceFieldArray *array,
        PieceFieldWithVersionArray *parray)
{
    FDIRStorageInodeIndexInfo index;
    bool found;
    bool keep;
    DAPieceFieldInfo *field;
    DAPieceFieldInfo *end;
    PieceFieldWithVersion *dest;

    dest = parray->records;
    end = array->records + array->count;
    for (field=array->records; field<end; field++) {
        index.inode = field->oid;
        found = (inode_segment_index_get(&index) == 0);
        switch (field->op_type) {
            case da_binlog_op_type_create:
                keep = !found;
                break;
            case da_binlog_op_type_remove:
                keep = found;
                break;
            case da_binlog_op_type_update:
                if (found) {
                    if (field->source == DA_FIELD_UPDATE_SOURCE_NORMAL) {
                        keep = (field->storage.version > index.
                                fields[field->fid].version);
                    } else {
                        keep = ((field->storage.version == index.
                                    fields[field->fid].version) &&
                                (field->storage.trunk_id != index.
                                 fields[field->fid].trunk_id));
                    }
                } else {
                    keep = false;
                }
                break;
            default:
                keep = false;
                break;
        }

        if (keep) {
            dest->field = field;
            dest->version = dest - parray->records;
            dest++;
        }
    }

    parray->count = dest - parray->records;
}

static int field_with_version_compare(const PieceFieldWithVersion *record1,
        const PieceFieldWithVersion *record2)
{
    int sub;

    if ((sub=fc_compare_int64(record1->field->oid,
                    record2->field->oid)) != 0)
    {
        return sub;
    }

    return record1->version - record2->version;
}

static int redo_by_ptr_array(const PieceFieldWithVersionArray *parray)
{
    int result;
    bool normal_update;
    PieceFieldWithVersion *record;
    PieceFieldWithVersion *end;
    FDIRStorageInodeIndexInfo index;
    FDIRInodeUpdateResult r;
    char buff[FDIR_INODE_BINLOG_RECORD_MAX_SIZE];
    BufferInfo buffer;

    if (parray->count == 0) {
        return 0;
    } else if (parray->count > 1) {
        qsort(parray->records, parray->count,
                sizeof(PieceFieldWithVersion),
                (int (*)(const void *, const void *))
                field_with_version_compare);
    }

    da_binlog_writer_inc_waiting_count(
            &INODE_BINLOG_WRITER, parray->count);

    buffer.buff = buff;
    buffer.alloc_size = sizeof(buff);
    end = parray->records + parray->count;
    for (record=parray->records; record<end; record++) {
        switch (record->field->op_type) {
            case da_binlog_op_type_create:
                result = inode_segment_index_add(record->field, &r);
                break;
            case da_binlog_op_type_remove:
                index.inode = record->field->oid;
                result = inode_segment_index_delete(&index, &r);
                break;
            case da_binlog_op_type_update:
                normal_update = (record->field->source ==
                        DA_FIELD_UPDATE_SOURCE_NORMAL);
                result = inode_segment_index_update(record->field,
                        normal_update, &r);
                break;
            default:
                result = EINVAL;
                break;
        }

        if (result != 0) {
            return result;
        }

        inode_binlog_pack(record->field, &buffer);
        inode_binlog_writer_log(r.segment, &buffer);
    }

    da_binlog_writer_wait(&INODE_BINLOG_WRITER);
    return 0;
}

static int inode_field_log_redo(const char *field_log_filename)
{
    int result;
    DAPieceFieldArray array;
    PieceFieldWithVersionArray parray;

    if ((result=inode_binlog_reader_load(field_log_filename, &array)) != 0) {
        return result;
    }

    if (array.count == 0) {
        return 0;
    }

    parray.records = (PieceFieldWithVersion *)fc_malloc(
            sizeof(PieceFieldWithVersion) * array.count);
    if (parray.records == NULL) {
        result = ENOMEM;
    } else {
        field_log_to_ptr_array(&array, &parray);
        result = redo_by_ptr_array(&parray);

        /*
        logInfo("field record count: %d, redo count: %d",
                array.count, parray.count);
                */

        free(parray.records);
    }

    free(array.records);
    return result;
}

static int slice_space_log_redo()
{
    int result;
    char space_tmp_filename[PATH_MAX];
    char field_tmp_filename[PATH_MAX];
    char space_log_filename[PATH_MAX];
    char field_log_filename[PATH_MAX];

    snprintf(space_tmp_filename, sizeof(space_tmp_filename),
            "%s/%s", STORAGE_PATH_STR, SPACE_TMP_FILENAME);
    snprintf(field_tmp_filename, sizeof(field_tmp_filename),
            "%s/%s", STORAGE_PATH_STR, FIELD_TMP_FILENAME);
    snprintf(space_log_filename, sizeof(space_log_filename),
            "%s/%s", STORAGE_PATH_STR, SPACE_REDO_FILENAME);
    snprintf(field_log_filename, sizeof(field_log_filename),
            "%s/%s", STORAGE_PATH_STR, FIELD_REDO_FILENAME);
    if (access(space_tmp_filename, F_OK) != 0 &&
            access(field_tmp_filename, F_OK) == 0)
    {
        /* compensate for two phases renames */
        if (rename(field_tmp_filename, field_log_filename) != 0) {
            result = errno != 0 ? errno : EIO;
            logError("file: "__FILE__", line: %d, "
                    "rename file \"%s\" to \"%s\" fail, "
                    "errno: %d, error info: %s", __LINE__,
                    field_tmp_filename, field_log_filename,
                    result, STRERROR(result));
            return result;
        }
    }

    if ((result=da_trunk_space_log_redo(&DA_CTX, space_log_filename)) != 0) {
        return result;
    }

    if ((result=inode_field_log_redo(field_log_filename)) != 0) {
        return result;
    }

    return 0;
}

int slice_space_log_start()
{
    int result;
    pthread_t tid;

    if ((result=slice_space_log_redo()) != 0) {
        return result;
    }

    return fc_create_thread(&tid, slice_space_log_func,
            NULL, SF_G_THREAD_STACK_SIZE);
}

void slice_space_log_destroy()
{
}
