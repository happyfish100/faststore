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
#include "sf/sf_func.h"
#include "../../common/fs_func.h"
#include "../server_global.h"
#include "../server_group_info.h"
#include "../shared_thread_pool.h"
#include "../storage/storage_allocator.h"
#include "../storage/trunk_id_info.h"
#include "../rebuild/store_path_rebuild.h"
#include "binlog_loader.h"
#include "migrate_clean.h"
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
#define NO_OP_EXPECT_FIELD_COUNT               6

#define MAX_BINLOG_FIELD_COUNT  16
#define MIN_EXPECT_FIELD_COUNT  DEL_BLOCK_EXPECT_FIELD_COUNT

#define MBLOCK_BATCH_ALLOC_SIZE  1024

struct slice_loader_context;

typedef struct slice_binlog_record {
    char op_type;
    OBSliceType slice_type;   //add slice only
    FSBlockSliceKeyInfo bs_key;
    FSTrunkSpaceInfo space;   //add slice only
    struct fast_mblock_man *allocator;
    struct slice_binlog_record *next;  //for queue
} SliceBinlogRecord;

typedef struct slice_record_chain {
    SliceBinlogRecord *head;
    SliceBinlogRecord *tail;
} SliceRecordChain;

typedef struct slice_parse_thread_context {
    int64_t total_count;
    struct {
        bool parse_done;
        pthread_lock_cond_pair_t lcp;
    } notify;
    SliceRecordChain slices;  //for output
    struct fast_mblock_man record_allocator;  //element: SliceBinlogRecord
    struct fast_mblock_node *freelist;  //for batch alloc
    BinlogReadThreadContext *read_thread_ctx;
    BinlogReadThreadResult *r;
    struct slice_loader_context *loader_ctx;
} SliceParseThreadContext;

typedef struct slice_parse_thread_ctx_array {
    SliceParseThreadContext *contexts;
    int count;
} SliceParseThreadCtxArray;

typedef struct slice_data_thread_context {
    int64_t total_count;
    int64_t skip_count;
    int64_t rebuild_count;   //for data rebuilding of the specify store path
    volatile int64_t done_count;
    SliceRecordChain slices;  //for enqueue
    struct fc_queue queue;   //element: SliceBinlogRecord
    struct slice_loader_context *loader_ctx;
} SliceDataThreadContext;

typedef struct slice_data_thread_ctx_array {
    SliceDataThreadContext *contexts;
    int count;
} SliceDataThreadCtxArray;

typedef struct slice_dump_thread_context {
    int64_t slice_count;
    int64_t start_index;
    int64_t end_index;
    struct slice_loader_context *loader_ctx;
} SliceDumpThreadContext;

typedef struct slice_dump_thread_ctx_array {
    SliceDumpThreadContext *contexts;
    int count;
} SliceDumpThreadCtxArray;

typedef struct slice_loader_context {
    struct {
        volatile int parse;
        volatile int data;
        volatile int dump;
    } thread_counts;
    volatile bool parse_continue_flag;
    volatile bool data_continue_flag;
    int dealing_threads;
    int64_t slice_count;
    SliceParseThreadCtxArray parse_thread_array;
    SliceDataThreadCtxArray data_thread_array;
} SliceLoaderContext;

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

static inline int add_slice_set_fields(SliceBinlogRecord *record,
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

static inline int del_slice_set_fields(SliceBinlogRecord *record,
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

static inline int del_block_set_fields(SliceBinlogRecord *record,
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

static inline int no_op_set_fields(SliceBinlogRecord *record,
        BinlogReadThreadResult *r, string_t *line,
        string_t *cols, const int count)
{
    int64_t line_count;
    char binlog_filename[PATH_MAX];

    if (count != NO_OP_EXPECT_FIELD_COUNT) {
        SLICE_GET_FILENAME_LINE_COUNT(r, binlog_filename,
                line->str, line_count);
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", "
                "field count: %d != %d", __LINE__,
                binlog_filename, line_count,
                count, NO_OP_EXPECT_FIELD_COUNT);
        return EINVAL;
    }

    return 0;
}

#define SLICE_ADD_TO_CHAIN(thread_ctx, record) \
    if (thread_ctx->slices.head == NULL) { \
        thread_ctx->slices.head = record;  \
    } else { \
        thread_ctx->slices.tail->next = record; \
    } \
    thread_ctx->slices.tail = record; \
    thread_ctx->total_count++


static int slice_parse_line(SliceParseThreadContext *thread_ctx,
        BinlogReadThreadResult *r, string_t *line,
        SliceBinlogRecord *record)
{
    int count;
    int result;
    int64_t line_count;
    string_t cols[MAX_BINLOG_FIELD_COUNT];
    char binlog_filename[PATH_MAX];
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
            ((op_type == SLICE_BINLOG_OP_TYPE_DEL_BLOCK ||
              op_type == SLICE_BINLOG_OP_TYPE_NO_OP) ?
             '\n' : ' '), 0);
    fs_calc_block_hashcode(&bkey);

    record->op_type = op_type;
    record->bs_key.block = bkey;
    switch (op_type) {
        case SLICE_BINLOG_OP_TYPE_WRITE_SLICE:
            record->slice_type = OB_SLICE_TYPE_FILE;
            result = add_slice_set_fields(record,
                    r, line, cols, count);
            break;
        case SLICE_BINLOG_OP_TYPE_ALLOC_SLICE:
            record->slice_type = OB_SLICE_TYPE_ALLOC;
            result = add_slice_set_fields(record,
                    r, line, cols, count);
            break;
        case SLICE_BINLOG_OP_TYPE_DEL_SLICE:
            result = del_slice_set_fields(record,
                    r, line, cols, count);
            break;
        case SLICE_BINLOG_OP_TYPE_DEL_BLOCK:
            result = del_block_set_fields(record,
                    r, line, cols, count);
            break;
        case SLICE_BINLOG_OP_TYPE_NO_OP:
            result = no_op_set_fields(record,
                    r, line, cols, count);
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

    SLICE_ADD_TO_CHAIN(thread_ctx, record);
    return 0;
}

static void waiting_and_process_parse_result(SliceLoaderContext
        *slice_ctx, SliceParseThreadContext *parse_thread)
{
    SliceDataThreadContext *data_thread;
    SliceBinlogRecord *record;

    PTHREAD_MUTEX_LOCK(&parse_thread->notify.lcp.lock);
    while (!parse_thread->notify.parse_done && SF_G_CONTINUE_FLAG) {
        pthread_cond_wait(&parse_thread->notify.lcp.cond,
                &parse_thread->notify.lcp.lock);
    }
    PTHREAD_MUTEX_UNLOCK(&parse_thread->notify.lcp.lock);

    if (!SF_G_CONTINUE_FLAG) {
        return;
    }

    record = parse_thread->slices.head;
    while (record != NULL) {
        data_thread = slice_ctx->data_thread_array.contexts + record->
            bs_key.block.hash_code % slice_ctx->data_thread_array.count;
        SLICE_ADD_TO_CHAIN(data_thread, record);

        record = record->next;
    }

    parse_thread->slices.head = parse_thread->slices.tail = NULL;
}

static void waiting_and_process_parse_outputs(SliceLoaderContext *slice_ctx)
{
    SliceParseThreadContext *parse_thread;
    SliceParseThreadContext *parse_end;
    SliceDataThreadContext *data_thread;
    SliceDataThreadContext *data_end;
    struct fc_queue_info qinfo;

    parse_end = slice_ctx->parse_thread_array.contexts +
        slice_ctx->dealing_threads;
    for (parse_thread=slice_ctx->parse_thread_array.contexts;
            parse_thread<parse_end; parse_thread++)
    {
        waiting_and_process_parse_result(slice_ctx, parse_thread);
    }

    if (!SF_G_CONTINUE_FLAG) {
        return;
    }

    data_end = slice_ctx->data_thread_array.contexts +
        slice_ctx->data_thread_array.count;
    for (data_thread=slice_ctx->data_thread_array.contexts;
            data_thread<data_end; data_thread++)
    {
        if (data_thread->slices.head != NULL) {
            data_thread->slices.tail->next = NULL;
            qinfo.head = data_thread->slices.head;
            qinfo.tail = data_thread->slices.tail;
            fc_queue_push_queue_to_tail(&data_thread->queue, &qinfo);
            data_thread->slices.head = data_thread->slices.tail = NULL;
        }
    }

    slice_ctx->dealing_threads = 0;
}

static int slice_parse_buffer(BinlogLoaderContext *ctx)
{
    SliceLoaderContext *slice_ctx;
    SliceParseThreadContext *thread_ctx;

    slice_ctx = (SliceLoaderContext *)ctx->arg;
    thread_ctx = slice_ctx->parse_thread_array.contexts +
        slice_ctx->dealing_threads;

    PTHREAD_MUTEX_LOCK(&thread_ctx->notify.lcp.lock);
    thread_ctx->read_thread_ctx = ctx->read_thread_ctx;
    thread_ctx->r = ctx->r;
    thread_ctx->notify.parse_done = false;
    pthread_cond_signal(&thread_ctx->notify.lcp.cond);
    PTHREAD_MUTEX_UNLOCK(&thread_ctx->notify.lcp.lock);

    if (++(slice_ctx->dealing_threads) ==
            slice_ctx->parse_thread_array.count)
    {
        waiting_and_process_parse_outputs(slice_ctx);
    }
    return 0;
}

static inline int slice_loader_deal_record(SliceDataThreadContext
        *thread_ctx, SliceBinlogRecord *record)
{
    OBSliceEntry *slice;
    int result;

    if (MIGRATE_CLEAN_ENABLED) {
        if (record->op_type == SLICE_BINLOG_OP_TYPE_NO_OP) {
            return 0;
        }
        if (!fs_is_my_data_group(FS_DATA_GROUP_ID(record->bs_key.block))) {
            thread_ctx->skip_count++;
            return 0;
        }
    }

    switch (record->op_type) {
        case SLICE_BINLOG_OP_TYPE_WRITE_SLICE:
        case SLICE_BINLOG_OP_TYPE_ALLOC_SLICE:
            if ((slice=ob_index_alloc_slice(&record->bs_key.block)) == NULL) {
                return ENOMEM;
            }

            if (record->space.store->index == DATA_REBUILD_PATH_INDEX) {
                thread_ctx->rebuild_count++;
            }

            slice->type = record->slice_type;
            slice->ssize = record->bs_key.slice;
            slice->space = record->space;
            return ob_index_add_slice_by_binlog(slice);
        case SLICE_BINLOG_OP_TYPE_DEL_SLICE:
            if ((result=ob_index_delete_slices_by_binlog(
                            &record->bs_key)) != 0)
            {
                logError("file: "__FILE__", line: %d, "
                        "delete slice fail, block {oid: %"PRId64", "
                        "offset: %"PRId64"}, slice {offset: %d, length: %d}"
                        ", errno: %d, error info: %s", __LINE__, record->
                        bs_key.block.oid, record->bs_key.block.offset,
                        record->bs_key.slice.offset, record->bs_key.
                        slice.length, result, STRERROR(result));
            }
            return result;
        case SLICE_BINLOG_OP_TYPE_DEL_BLOCK:
            if ((result=ob_index_delete_block_by_binlog(
                            &record->bs_key.block)) != 0)
            {
                logError("file: "__FILE__", line: %d, "
                        "delete block fail, {oid: %"PRId64", offset: %"
                        PRId64"}, errno: %d, error info: %s", __LINE__,
                        record->bs_key.block.oid, record->bs_key.block.
                        offset, result, STRERROR(result));
            }
            return result;
        case SLICE_BINLOG_OP_TYPE_NO_OP:
        default:
            return 0;
    }
}

static inline void deal_records(SliceDataThreadContext *thread_ctx,
        SliceBinlogRecord *head)
{
    SliceBinlogRecord *record;
    struct fast_mblock_node *node;
    struct fast_mblock_man *prev_allocator;
    struct fast_mblock_chain chain;
    int count;

    if (slice_loader_deal_record(thread_ctx, head) != 0) {
        SF_G_CONTINUE_FLAG = false;
        return;
    }

    count = 1;
    prev_allocator = head->allocator;
    chain.head = chain.tail = fast_mblock_to_node_ptr(head);
    thread_ctx->done_count++;
    record = head->next;
    while (record != NULL) {
        if (slice_loader_deal_record(thread_ctx, record) != 0) {
            SF_G_CONTINUE_FLAG = false;
            return;
        }

        ++count;
        node = fast_mblock_to_node_ptr(record);
        if (record->allocator != prev_allocator ||
                count == MBLOCK_BATCH_ALLOC_SIZE)
        {
            chain.tail->next = NULL;
            fast_mblock_batch_free(prev_allocator, &chain);

            count = 1;
            prev_allocator = record->allocator;
            chain.head = node;
        } else {
            chain.tail->next = node;
        }
        chain.tail = node;

        thread_ctx->done_count++;
        record = record->next;
    }

    chain.tail->next = NULL;
    fast_mblock_batch_free(prev_allocator, &chain);
}

static int parse_buffer(SliceParseThreadContext *thread_ctx)
{
    int result;
    string_t line;
    char *line_start;
    char *buff_end;
    char *line_end;
    struct fast_mblock_node *node;
    SliceBinlogRecord *record;

    result = 0;
    thread_ctx->slices.head = thread_ctx->slices.tail = NULL;
    line_start = thread_ctx->r->buffer.buff;
    buff_end = thread_ctx->r->buffer.buff + thread_ctx->r->buffer.length;
    while (line_start < buff_end) {
        line_end = (char *)memchr(line_start, '\n', buff_end - line_start);
        if (line_end == NULL) {
            break;
        }

        line.str = line_start;
        line.len = line_end - line_start;

        if (thread_ctx->freelist == NULL) {
            thread_ctx->freelist = fast_mblock_batch_alloc1(
                    &thread_ctx->record_allocator,
                    MBLOCK_BATCH_ALLOC_SIZE);
            if (thread_ctx->freelist == NULL) {
                result = ENOMEM;
                break;
            }
        }
        node = thread_ctx->freelist;
        thread_ctx->freelist = thread_ctx->freelist->next;
        record = (SliceBinlogRecord *)node->data;
        if ((result=slice_parse_line(thread_ctx, thread_ctx->r,
                        &line, record)) != 0)
        {
            break;
        }

        line_start = line_end + 1;
    }

    if (thread_ctx->slices.tail != NULL) {
        thread_ctx->slices.tail->next = NULL;
    }

    binlog_read_thread_return_result_buffer(
            thread_ctx->read_thread_ctx, thread_ctx->r);
    return result;
}

static void slice_parse_thread_run(SliceParseThreadContext *thread_ctx,
        void *thread_data)
{
    __sync_add_and_fetch(&thread_ctx->loader_ctx->thread_counts.parse, 1);

    while (SF_G_CONTINUE_FLAG && thread_ctx->
            loader_ctx->parse_continue_flag)
    {
        PTHREAD_MUTEX_LOCK(&thread_ctx->notify.lcp.lock);
        if (thread_ctx->r == NULL) {
            pthread_cond_wait(&thread_ctx->notify.lcp.cond,
                    &thread_ctx->notify.lcp.lock);
        }
        PTHREAD_MUTEX_UNLOCK(&thread_ctx->notify.lcp.lock);

        if (thread_ctx->r != NULL) {
            if (parse_buffer(thread_ctx) != 0) {
                sf_terminate_myself();
                break;
            }

            PTHREAD_MUTEX_LOCK(&thread_ctx->notify.lcp.lock);
            thread_ctx->r = NULL;
            thread_ctx->notify.parse_done = true;
            pthread_cond_signal(&thread_ctx->notify.lcp.cond);
            PTHREAD_MUTEX_UNLOCK(&thread_ctx->notify.lcp.lock);
        }
    }

    __sync_sub_and_fetch(&thread_ctx->loader_ctx->thread_counts.parse, 1);
}

static void slice_data_thread_run(SliceDataThreadContext *thread_ctx,
        void *thread_data)
{
    SliceBinlogRecord *head;

    __sync_add_and_fetch(&thread_ctx->loader_ctx->thread_counts.data, 1);
    while (SF_G_CONTINUE_FLAG && thread_ctx->
            loader_ctx->data_continue_flag)
    {
        head = (SliceBinlogRecord *)fc_queue_pop_all(&thread_ctx->queue);
        if (head != NULL) {
            deal_records(thread_ctx, head);
        }
    }
    __sync_sub_and_fetch(&thread_ctx->loader_ctx->thread_counts.data, 1);
}

static int slice_record_alloc_init(SliceBinlogRecord *record,
        struct fast_mblock_man *allocator)
{
    record->allocator = allocator;
    return 0;
}

static int init_parse_thread_context(SliceParseThreadContext *thread_ctx)
{
    const int alloc_elements_once = 8 * 1024;
    int elements_limit;
    int result;

    if ((result=init_pthread_lock_cond_pair(&thread_ctx->notify.lcp)) != 0) {
        return result;
    }

    elements_limit = (8 * BINLOG_BUFFER_SIZE) /
        FS_SLICE_BINLOG_MIN_RECORD_SIZE;
    if ((result=fast_mblock_init_ex1(&thread_ctx->record_allocator,
                    "slice_record", sizeof(SliceBinlogRecord),
                    alloc_elements_once, elements_limit,
                    (fast_mblock_alloc_init_func)slice_record_alloc_init,
                    &thread_ctx->record_allocator, true)) != 0)
    {
        return result;
    }
    fast_mblock_set_need_wait(&thread_ctx->record_allocator,
            true, (bool *)&SF_G_CONTINUE_FLAG);

    thread_ctx->total_count = 0;
    thread_ctx->notify.parse_done = false;
    thread_ctx->slices.head = thread_ctx->slices.tail = NULL;
    thread_ctx->read_thread_ctx = NULL;
    thread_ctx->r = NULL;
    thread_ctx->freelist = NULL;
    return 0;
}

static int init_data_thread_context(SliceDataThreadContext *thread_ctx)
{
    int result;

    if ((result=fc_queue_init(&thread_ctx->queue, (long)(
                        &((SliceBinlogRecord *)NULL)->next))) != 0)
    {
        return result;
    }

    thread_ctx->total_count = 0;
    thread_ctx->skip_count = 0;
    thread_ctx->rebuild_count = 0;
    thread_ctx->done_count = 0;
    thread_ctx->slices.head = thread_ctx->slices.tail = NULL;
    return 0;
}

static int init_parse_thread_ctx_array(SliceLoaderContext *loader_ctx,
        SliceParseThreadCtxArray *ctx_array, const int thread_count)
{
    int result;
    int bytes;
    SliceParseThreadContext *ctx;
    SliceParseThreadContext *end;

    bytes = sizeof(SliceParseThreadContext) * thread_count;
    ctx_array->contexts = (SliceParseThreadContext *)fc_malloc(bytes);
    if (ctx_array->contexts == NULL) {
        return ENOMEM;
    }

    ctx_array->count = thread_count;
    end = ctx_array->contexts + ctx_array->count;
    for (ctx=ctx_array->contexts; ctx<end; ctx++) {
        if ((result=init_parse_thread_context(ctx)) != 0) {
            return result;
        }

        ctx->loader_ctx = loader_ctx;
        if ((result=shared_thread_pool_run((fc_thread_pool_callback)
                        slice_parse_thread_run, ctx)) != 0)
        {
            return result;
        }
    }

    return 0;
}

static int init_data_thread_ctx_array(SliceLoaderContext *loader_ctx,
        SliceDataThreadCtxArray *ctx_array, const int thread_count)
{
    int result;
    int bytes;
    SliceDataThreadContext *ctx;
    SliceDataThreadContext *end;

    bytes = sizeof(SliceDataThreadContext) * thread_count;
    ctx_array->contexts = (SliceDataThreadContext *)fc_malloc(bytes);
    if (ctx_array->contexts == NULL) {
        return ENOMEM;
    }

    ctx_array->count = thread_count;
    end = ctx_array->contexts + ctx_array->count;
    for (ctx=ctx_array->contexts; ctx<end; ctx++) {
        if ((result=init_data_thread_context(ctx)) != 0) {
            return result;
        }

        ctx->loader_ctx = loader_ctx;
        if ((result=shared_thread_pool_run((fc_thread_pool_callback)
                        slice_data_thread_run, ctx)) != 0)
        {
            return result;
        }
    }

    return 0;
}

static int init_thread_ctx_array(SliceLoaderContext *ctx)
{
    int parse_threads;
    int data_threads;
    int result;

    parse_threads = SYSTEM_CPU_COUNT / 4;
    if (parse_threads == 0) {
        parse_threads = 1;
    }
    data_threads = parse_threads * 3;

    result = init_parse_thread_ctx_array(ctx, &ctx->
            parse_thread_array, parse_threads);
    if (result != 0) {
        return result;
    }

    return init_data_thread_ctx_array(ctx, &ctx->
            data_thread_array, data_threads);
}

static void destroy_parse_thread_ctx_array(SliceParseThreadCtxArray *ctx_array)
{
    SliceParseThreadContext *ctx;
    SliceParseThreadContext *end;

    end = ctx_array->contexts + ctx_array->count;
    for (ctx=ctx_array->contexts; ctx<end; ctx++) {
        destroy_pthread_lock_cond_pair(&ctx->notify.lcp);
        fast_mblock_destroy(&ctx->record_allocator);
    }

    free(ctx_array->contexts);
}

static void destroy_data_thread_ctx_array(SliceDataThreadCtxArray *ctx_array)
{
    SliceDataThreadContext *ctx;
    SliceDataThreadContext *end;

    end = ctx_array->contexts + ctx_array->count;
    for (ctx=ctx_array->contexts; ctx<end; ctx++) {
        fc_queue_destroy(&ctx->queue);
    }

    free(ctx_array->contexts);
}

static void destroy_loader_context(SliceLoaderContext *ctx)
{
    destroy_parse_thread_ctx_array(&ctx->parse_thread_array);
    destroy_data_thread_ctx_array(&ctx->data_thread_array);
}

static void terminate_parse_threads(SliceLoaderContext *slice_ctx)
{
    SliceParseThreadContext *parse_thread;
    SliceParseThreadContext *end;

    slice_ctx->parse_continue_flag = false;
    end = slice_ctx->parse_thread_array.contexts +
        slice_ctx->parse_thread_array.count;
    while (SF_G_CONTINUE_FLAG) {
        for (parse_thread=slice_ctx->parse_thread_array.contexts;
                parse_thread<end; parse_thread++)
        {
            pthread_cond_signal(&parse_thread->notify.lcp.cond);
        }

        if (__sync_add_and_fetch(&slice_ctx->thread_counts.parse, 0) == 0) {
            break;
        }
        fc_sleep_ms(1);
    }
}

static void waiting_data_threads_finish(SliceLoaderContext *slice_ctx,
        SliceDataThreadCtxArray *ctx_array)
{
    SliceDataThreadContext *ctx;
    SliceDataThreadContext *end;
    bool all_done;

    end = ctx_array->contexts + ctx_array->count;
    while (SF_G_CONTINUE_FLAG) {
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

    slice_ctx->data_continue_flag = false;
    while (SF_G_CONTINUE_FLAG) {
        for (ctx=ctx_array->contexts; ctx<end; ctx++) {
            fc_queue_terminate(&ctx->queue);
        }

        if (__sync_add_and_fetch(&slice_ctx->thread_counts.data, 0) == 0) {
            break;
        }

        fc_sleep_ms(1);
    }
}

static void slice_binlog_read_done(BinlogLoaderContext *ctx)
{
    SliceLoaderContext *slice_ctx;
    SliceParseThreadContext *parse_thread;
    SliceParseThreadContext *end;

    slice_ctx = (SliceLoaderContext *)ctx->arg;
    if (slice_ctx->dealing_threads > 0) {
        waiting_and_process_parse_outputs(slice_ctx);
    }

    terminate_parse_threads(slice_ctx);

    end = slice_ctx->parse_thread_array.contexts +
        slice_ctx->parse_thread_array.count;
    for (parse_thread=slice_ctx->parse_thread_array.contexts;
            parse_thread<end; parse_thread++)
    {
        ctx->total_count += parse_thread->total_count;
    }

    waiting_data_threads_finish(slice_ctx,
            &slice_ctx->data_thread_array);
}

static void slice_dump_thread_run(SliceDumpThreadContext *thread_ctx,
        void *thread_data)
{
    __sync_add_and_fetch(&thread_ctx->loader_ctx->thread_counts.dump, 1);
    if (ob_index_dump_slices_to_trunk(thread_ctx->start_index,
                thread_ctx->end_index, &thread_ctx->slice_count) != 0)
    {
        sf_terminate_myself();
    }
    __sync_sub_and_fetch(&thread_ctx->loader_ctx->thread_counts.dump, 1);
}

static int init_dump_thread_ctx_array(SliceLoaderContext *loader_ctx,
        SliceDumpThreadCtxArray *ctx_array)
{
#define MIN_SLICES_PER_THREAD  200000
    int result;
    int bytes;
    int thread_count;
    int64_t buckets_per_thread;
    int64_t ob_count;
    int64_t total_slice_count;
    int64_t start_index;
    SliceDumpThreadContext *ctx;
    SliceDumpThreadContext *end;

    ob_index_get_ob_and_slice_counts(&ob_count, &total_slice_count);
    if (total_slice_count == 0) {
        ctx_array->contexts = NULL;
        ctx_array->count = 0;
        return 0;
    }

    thread_count = (total_slice_count + MIN_SLICES_PER_THREAD - 1) /
        MIN_SLICES_PER_THREAD;
    if (thread_count > SYSTEM_CPU_COUNT) {
        thread_count = SYSTEM_CPU_COUNT;
    }

    if (thread_count == 1) {
        ctx_array->count = 0;
        ctx_array->contexts = NULL;
        return ob_index_dump_slices_to_trunk(0, g_ob_hashtable.
                capacity, &loader_ctx->slice_count);
    }

    bytes = sizeof(SliceDumpThreadContext) * thread_count;
    ctx_array->contexts = (SliceDumpThreadContext *)fc_malloc(bytes);
    if (ctx_array->contexts == NULL) {
        return ENOMEM;
    }

    buckets_per_thread = (g_ob_hashtable.capacity +
            thread_count - 1) / thread_count; 
    end = ctx_array->contexts + thread_count;
    for (ctx=ctx_array->contexts, start_index=0; ctx<end; ctx++) {
        ctx->slice_count = 0;
        ctx->start_index = start_index;
        ctx->end_index = start_index + buckets_per_thread;
        if (ctx->end_index > g_ob_hashtable.capacity) {
            ctx->end_index = g_ob_hashtable.capacity;
        }
        ctx->loader_ctx = loader_ctx;
        if ((result=shared_thread_pool_run((fc_thread_pool_callback)
                        slice_dump_thread_run, ctx)) != 0)
        {
            return result;
        }

        start_index += buckets_per_thread;
    }

    ctx_array->count = thread_count;
    return 0;
}

static int slice_dump_slices_to_trunk(SliceLoaderContext *loader_ctx)
{
    int result;
    SliceDumpThreadCtxArray dump_thread_array;
    SliceDumpThreadContext *ctx;
    SliceDumpThreadContext *end;
    int64_t start_time;
    int64_t end_time;
    char time_buff[32];

    start_time = get_current_time_ms();
    loader_ctx->slice_count = 0;
    if ((result=init_dump_thread_ctx_array(loader_ctx,
                    &dump_thread_array)) != 0)
    {
        return result;
    }

    if (dump_thread_array.count > 0) {
        fc_sleep_ms(100);
        while (SF_G_CONTINUE_FLAG) {
            if (__sync_add_and_fetch(&loader_ctx->
                        thread_counts.dump, 0) == 0)
            {
                break;
            }

            fc_sleep_ms(10);
        }

        loader_ctx->slice_count = 0;
        end = dump_thread_array.contexts + dump_thread_array.count;
        for (ctx=dump_thread_array.contexts; ctx<end; ctx++) {
            /*
            logInfo("thread %d. slice_count: %"PRId64,
                    (int)(ctx-dump_thread_array.contexts) + 1,
                    ctx->slice_count);
                    */

            loader_ctx->slice_count += ctx->slice_count;
        }

        free(dump_thread_array.contexts);
    }

    end_time = get_current_time_ms();
    long_to_comma_str(end_time - start_time, time_buff);
    logInfo("file: "__FILE__", line: %d, "
            "dump %"PRId64" slices to trunk done, time used: %s ms",
            __LINE__, loader_ctx->slice_count, time_buff);

    return result;
}

static int64_t get_total_count(SliceDataThreadCtxArray *ctx_array)
{
    int64_t total;
    SliceDataThreadContext *ctx;
    SliceDataThreadContext *end;

    total = 0;
    end = ctx_array->contexts + ctx_array->count;
    for (ctx=ctx_array->contexts; ctx<end; ctx++) {
        total += ctx->total_count;
    }

    return total;
}

static int64_t get_total_skip_count(SliceDataThreadCtxArray *ctx_array)
{
    int64_t total;
    SliceDataThreadContext *ctx;
    SliceDataThreadContext *end;

    total = 0;
    end = ctx_array->contexts + ctx_array->count;
    for (ctx=ctx_array->contexts; ctx<end; ctx++) {
        total += ctx->skip_count;
    }

    return total;
}

static int64_t get_total_rebuild_count(SliceDataThreadCtxArray *ctx_array)
{
    int64_t total;
    SliceDataThreadContext *ctx;
    SliceDataThreadContext *end;

    total = 0;
    end = ctx_array->contexts + ctx_array->count;
    for (ctx=ctx_array->contexts; ctx<end; ctx++) {
        total += ctx->rebuild_count;
    }

    return total;
}

int slice_loader_load(struct sf_binlog_writer_info *slice_writer)
{
    int result;
    SliceLoaderContext ctx;
    BinlogLoaderCallbacks callbacks;

    ctx.parse_continue_flag = true;
    ctx.data_continue_flag = true;
    ctx.dealing_threads = 0;
    ctx.thread_counts.parse = 0;
    ctx.thread_counts.data = 0;
    ctx.thread_counts.dump = 0;
    if ((result=init_thread_ctx_array(&ctx)) != 0) {
        return result;
    }

    callbacks.parse_buffer = slice_parse_buffer;
    callbacks.parse_line = NULL;
    callbacks.read_done = (binlog_read_done_func)slice_binlog_read_done;
    callbacks.arg = &ctx;
    result = binlog_loader_load_ex(FS_SLICE_BINLOG_SUBDIR_NAME,
            slice_writer, &callbacks, (ctx.parse_thread_array.count +
                ctx.data_thread_array.count) * 2);

    if (result == 0) {
        if (DATA_REBUILD_PATH_INDEX >= 0) {
            DATA_REBUILD_SLICE_COUNT = get_total_rebuild_count(
                    &ctx.data_thread_array);
            if (DATA_REBUILD_SLICE_COUNT > 0) {
                result = store_path_rebuild_dump_data(
                        get_total_count(&ctx.data_thread_array));

                logInfo("rebuild path: %s, total slice count: %"PRId64", "
                        "rebuild count: %"PRId64, DATA_REBUILD_PATH_STR,
                        get_total_count(&ctx.data_thread_array),
                        DATA_REBUILD_SLICE_COUNT);
            }
        }

        if (result == 0) {
            result = slice_dump_slices_to_trunk(&ctx);
        }
    }
    g_ob_hashtable.modify_sallocator = true;

    if (result == 0 && MIGRATE_CLEAN_ENABLED) {
        bool dump_slice_index;
        dump_slice_index = (get_total_skip_count(
                    &ctx.data_thread_array) > 0);
        result = migrate_clean_binlog(ctx.slice_count, dump_slice_index);
    }

    destroy_loader_context(&ctx);
    return result;
}
