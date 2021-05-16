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
#include "../../common/fs_func.h"
#include "../server_global.h"
#include "../shared_thread_pool.h"
#include "../dio/trunk_io_thread.h"
#include "../storage/storage_allocator.h"
#include "../storage/trunk_id_info.h"
#include "binlog_loader.h"
#include "slice_binlog.h"
#include "slice_loader.h"

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

typedef struct fs_slice_binlog_record {
    char op_type;
    OBSliceType slice_type;   //add slice only
    FSBlockSliceKeyInfo bs_key;
    FSTrunkSpaceInfo space;   //add slice only
    struct fs_slice_binlog_record *next;  //for queue
} FSSliceBinlogRecord;

typedef struct fs_slice_loader_thread_context {
    volatile bool continue_flag;
    int64_t total_count;
    volatile int64_t done_count;
    struct fc_queue queue;
    struct fast_mblock_man record_allocator;  //element: FSSliceBinlogRecord 
} FSSliceLoaderThreadContext;

typedef struct fs_slice_loader_thread_ctx_array {
    FSSliceLoaderThreadContext *contexts;
    int count;
} FSSliceLoaderThreadCtxArray;

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

static inline int add_slice_set_fields(FSSliceBinlogRecord *record,
        BinlogReadThreadResult *r, string_t *line, string_t *cols,
        const int count)
{
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

    SLICE_PARSE_INT_EX(record->bs_key.slice.offset, "slice offset",
            BINLOG_COMMON_FIELD_INDEX_SLICE_OFFSET, ' ', 0);
    SLICE_PARSE_INT_EX(record->bs_key.slice.length, "slice length",
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
    record->space.store = &PATHS_BY_INDEX_PPTR[path_index]->store;
    SLICE_PARSE_INT_EX(record->space.id_info.id, "trunk_id",
            ADD_SLICE_FIELD_INDEX_SPACE_TRUNK_ID, ' ', 1);
    SLICE_PARSE_INT_EX(record->space.id_info.subdir, "subdir",
            ADD_SLICE_FIELD_INDEX_SPACE_SUBDIR, ' ', 1);
    SLICE_PARSE_INT(record->space.offset,
            ADD_SLICE_FIELD_INDEX_SPACE_OFFSET, ' ', 0);
    SLICE_PARSE_INT(record->space.size,
            ADD_SLICE_FIELD_INDEX_SPACE_SIZE, '\n', 0);
    return 0;
}

static inline int del_slice_set_fields(FSSliceBinlogRecord *record,
        BinlogReadThreadResult *r, string_t *line,
        string_t *cols, const int count)
{
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

    SLICE_PARSE_INT_EX(record->bs_key.slice.offset, "slice offset",
            BINLOG_COMMON_FIELD_INDEX_SLICE_OFFSET, ' ', 0);
    SLICE_PARSE_INT_EX(record->bs_key.slice.length, "slice length",
            BINLOG_COMMON_FIELD_INDEX_SLICE_LENGTH, '\n', 1);
    return 0;
}

static inline int del_block_set_fields(FSSliceBinlogRecord *record,
        BinlogReadThreadResult *r, string_t *line,
        string_t *cols, const int count)
{
    int64_t line_count;
    char binlog_filename[PATH_MAX];

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

    return 0;
}

static int slice_parse_line(BinlogReadThreadResult *r, string_t *line,
        FSSliceLoaderThreadCtxArray *ctx_array)
{
    int count;
    int result;
    int64_t line_count;
    string_t cols[MAX_BINLOG_FIELD_COUNT];
    char binlog_filename[PATH_MAX];
    FSSliceLoaderThreadContext *thread_ctx;
    FSSliceBinlogRecord *record;
    FSBlockKey bkey;
    char op_type;
    char *endptr;

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
    SLICE_PARSE_INT_EX(bkey.oid, "object ID",
            BINLOG_COMMON_FIELD_INDEX_BLOCK_OID, ' ', 1);
    SLICE_PARSE_INT_EX(bkey.offset, "block offset",
            BINLOG_COMMON_FIELD_INDEX_BLOCK_OFFSET,
            (op_type == SLICE_BINLOG_OP_TYPE_DEL_BLOCK ?
             '\n' : ' '), 0);
    fs_calc_block_hashcode(&bkey);

    thread_ctx = ctx_array->contexts + bkey.hash_code % ctx_array->count;
    record = (FSSliceBinlogRecord *)fast_mblock_alloc_object(
            &thread_ctx->record_allocator);
    if (record == NULL) {
        return ENOMEM;
    }

    record->op_type = op_type;
    record->bs_key.block = bkey;
    switch (op_type) {
        case SLICE_BINLOG_OP_TYPE_WRITE_SLICE:
            record->slice_type = OB_SLICE_TYPE_FILE;
            result = add_slice_set_fields(record, r, line, cols, count);
            break;
        case SLICE_BINLOG_OP_TYPE_ALLOC_SLICE:
            record->slice_type = OB_SLICE_TYPE_ALLOC;
            result = add_slice_set_fields(record, r, line, cols, count);
            break;
        case SLICE_BINLOG_OP_TYPE_DEL_SLICE:
            result = del_slice_set_fields(record, r, line, cols, count);
            break;
        case SLICE_BINLOG_OP_TYPE_DEL_BLOCK:
            result = del_block_set_fields(record, r, line, cols, count);
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

        return result;
    }

    thread_ctx->total_count++;
    fc_queue_push(&thread_ctx->queue, record);
    return 0;
}

static inline int slice_loader_deal_record(FSSliceBinlogRecord *record)
{
    OBSliceEntry *slice;

    switch (record->op_type) {
        case SLICE_BINLOG_OP_TYPE_WRITE_SLICE:
        case SLICE_BINLOG_OP_TYPE_ALLOC_SLICE:
            if ((slice=ob_index_alloc_slice(&record->bs_key.block)) == NULL) {
                return ENOMEM;
            }

            slice->type = record->slice_type;
            slice->ssize = record->bs_key.slice;
            slice->space = record->space;
            return ob_index_add_slice_by_binlog(slice);
        case SLICE_BINLOG_OP_TYPE_DEL_SLICE:
            return ob_index_delete_slices_by_binlog(&record->bs_key);
        case SLICE_BINLOG_OP_TYPE_DEL_BLOCK:
            return ob_index_delete_block_by_binlog(&record->bs_key.block);
        default:
            return 0;
    }
}

static inline void deal_records(FSSliceLoaderThreadContext *thread_ctx,
        FSSliceBinlogRecord *head)
{
    FSSliceBinlogRecord *record;
    do {
        record = head;
        head = head->next;

        if (slice_loader_deal_record(record) != 0) {
            SF_G_CONTINUE_FLAG = false;
            return;
        }
        fast_mblock_free_object(&thread_ctx->record_allocator, record);
        thread_ctx->done_count++;
    } while (head != NULL);
}

static void slice_loader_thread_run(FSSliceLoaderThreadContext *thread_ctx,
        void *thread_data)
{
    FSSliceBinlogRecord *head;

    while (SF_G_CONTINUE_FLAG && thread_ctx->continue_flag) {
        head = (FSSliceBinlogRecord *)fc_queue_pop_all(&thread_ctx->queue);
        if (head == NULL) {
            continue;
        }

        deal_records(thread_ctx, head);
    }
}

static int init_thread_context(FSSliceLoaderThreadContext *thread_ctx)
{
    const int alloc_elements_once = 4096;
    const int elements_limit = 2 * alloc_elements_once;
    int result;

    if ((result=fc_queue_init(&thread_ctx->queue, (long)(
                        &((FSSliceBinlogRecord *)NULL)->next))) != 0)
    {
        return result;
    }

    if ((result=fast_mblock_init_ex1(&thread_ctx->record_allocator,
                    "slice_record", sizeof(FSSliceBinlogRecord),
                    alloc_elements_once, elements_limit,
                    NULL, NULL, true)) != 0)
    {
        return result;
    }
    fast_mblock_set_need_wait(&thread_ctx->record_allocator,
            true, (bool *)&SF_G_CONTINUE_FLAG);

    thread_ctx->total_count = 0;
    thread_ctx->done_count = 0;
    return 0;
}

static int init_thread_ctx_array(FSSliceLoaderThreadCtxArray *ctx_array)
{
    int result;
    int bytes;
    FSSliceLoaderThreadContext *ctx;
    FSSliceLoaderThreadContext *end;

    bytes = sizeof(FSSliceLoaderThreadContext) * DATA_THREAD_COUNT;
    ctx_array->contexts = (FSSliceLoaderThreadContext *)fc_malloc(bytes);
    if (ctx_array->contexts == NULL) {
        return ENOMEM;
    }

    ctx_array->count = DATA_THREAD_COUNT;
    end = ctx_array->contexts + ctx_array->count;
    for (ctx=ctx_array->contexts; ctx<end; ctx++) {
        if ((result=init_thread_context(ctx)) != 0) {
            return result;
        }

        ctx->continue_flag = true;
        if ((result=shared_thread_pool_run((fc_thread_pool_callback)
                        slice_loader_thread_run, ctx)) != 0)
        {
            return result;
        }
    }

    return 0;
}

static void destroy_thread_ctx_array(FSSliceLoaderThreadCtxArray *ctx_array)
{
    FSSliceLoaderThreadContext *ctx;
    FSSliceLoaderThreadContext *end;

    end = ctx_array->contexts + ctx_array->count;
    for (ctx=ctx_array->contexts; ctx<end; ctx++) {
        fc_queue_destroy(&ctx->queue);
        fast_mblock_destroy(&ctx->record_allocator);
    }

    free(ctx_array->contexts);
}

static void waiting_threads_finish(FSSliceLoaderThreadCtxArray *ctx_array)
{
    FSSliceLoaderThreadContext *ctx;
    FSSliceLoaderThreadContext *end;
    bool all_done;

    end = ctx_array->contexts + ctx_array->count;
    while (1) {
        all_done = true;
        for (ctx=ctx_array->contexts; ctx<end; ctx++) {
            if (ctx->done_count < ctx->total_count) {
                all_done = false;
                break;
            }
        }
        if (all_done) {
            break;
        }
        fc_sleep_ms(10);
    }

    while (1) {
        for (ctx=ctx_array->contexts; ctx<end; ctx++) {
            ctx->continue_flag = false;
            fc_queue_terminate(&ctx->queue);
        }

        if (shared_thread_pool_dealing_count() == 0) {
            break;
        }

        fc_sleep_ms(1);
    }
}

int slice_loader_load(struct sf_binlog_writer_info *slice_writer)
{
    int result;
    int64_t slice_count;
    int64_t start_time;
    int64_t end_time;
    char time_buff[32];
    FSSliceLoaderThreadCtxArray ctx_array;

    if ((result=init_thread_ctx_array(&ctx_array)) != 0) {
        return result;
    }

    result = binlog_loader_load_ex(FS_SLICE_BINLOG_SUBDIR_NAME, slice_writer,
            (binlog_parse_line_func)slice_parse_line, &ctx_array);
    if (result == 0) {
        if (!SF_G_CONTINUE_FLAG) {
            result = EINTR;
        }
    }

    if (SF_G_CONTINUE_FLAG) {
        waiting_threads_finish(&ctx_array);
        destroy_thread_ctx_array(&ctx_array);
    }

    if (result == 0) {
        start_time = get_current_time_ms();
        if ((result=ob_index_dump_slices_to_trunk(&slice_count)) == 0) {
            end_time = get_current_time_ms();
            long_to_comma_str(end_time - start_time, time_buff);
            logInfo("file: "__FILE__", line: %d, "
                    "%"PRId64" slices to trunk done, time used: %s ms",
                    __LINE__, slice_count, time_buff);
        }
    }
    return result;
}
