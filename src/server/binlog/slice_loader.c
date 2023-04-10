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
#include "../rebuild/store_path_rebuild.h"
#include "../db/event_dealer.h"
#include "binlog_loader.h"
#include "migrate_clean.h"
#include "slice_binlog.h"
#include "slice_loader.h"

#define MBLOCK_BATCH_ALLOC_SIZE  1024
#define BINLOG_OP_TYPE_SET_SN     255

struct slice_loader_context;

typedef struct slice_loader_record {
    SliceBinlogRecord slice;
    SFBinlogFilePosition position;
    struct fast_mblock_man *allocator;
    struct slice_loader_record *next;  //for queue
} SliceLoaderRecord;

typedef struct slice_record_chain {
    SliceLoaderRecord *head;
    SliceLoaderRecord *tail;
} SliceRecordChain;

typedef struct slice_parse_thread_context {
    int thread_index;
    int64_t total_count;
    struct {
        bool parse_done;
        pthread_lock_cond_pair_t lcp;
    } notify;
    SliceRecordChain slices;  //for output
    struct fast_mblock_man record_allocator;  //element: SliceLoaderRecord
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
    int thread_index;
    int64_t total_count;
    int64_t skip_count;
    int64_t rebuild_count;   //for data rebuilding of the specify store path
    volatile int64_t done_count;
    struct {
        int64_t waiting;
        volatile int64_t done;
    } last_sns; //for storage engine
    SliceRecordChain slices;  //for enqueue
    struct fc_queue queue;   //element: SliceLoaderRecord
    struct slice_loader_context *loader_ctx;
} SliceDataThreadContext;

typedef struct slice_data_thread_ctx_array {
    SliceDataThreadContext *contexts;
    int count;
} SliceDataThreadCtxArray;

typedef struct slice_loader_context {
    struct {
        volatile int parse;
        volatile int data;
        volatile int dump;
    } thread_counts;
    volatile bool parse_continue_flag;
    volatile bool data_continue_flag;
    int dealing_threads;
    int64_t binlog_count;
    SliceParseThreadCtxArray parse_thread_array;
    SliceDataThreadCtxArray data_thread_array;
} SliceLoaderContext;

#define SLICE_GET_FILENAME_LINE_COUNT(r, binlog_filename, \
        line_str, line_count) \
        BINLOG_GET_FILENAME_LINE_COUNT(r, FS_SLICE_BINLOG_SUBDIR_NAME, \
        binlog_filename, line_str, line_count)

#define SLICE_ADD_TO_CHAIN(thread_ctx, record) \
    if (thread_ctx->slices.head == NULL) { \
        thread_ctx->slices.head = record;  \
    } else { \
        thread_ctx->slices.tail->next = record; \
    } \
    thread_ctx->slices.tail = record; \
    thread_ctx->total_count++


static int slice_parse_line(SliceParseThreadContext *thread_ctx,
        BinlogReadThreadResult *r, const string_t *line,
        SliceLoaderRecord *record)
{
    int result;
    int64_t line_count;
    char binlog_filename[PATH_MAX];
    char error_info[256];

    if ((result=slice_binlog_record_unpack(line,
                    &record->slice, error_info)) != 0)
    {
        SLICE_GET_FILENAME_LINE_COUNT(r, binlog_filename,
                line->str, line_count);
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", %s", __LINE__,
                binlog_filename, line_count, error_info);
        return result;
    }

    record->position.index = r->binlog_position.index;
    record->position.offset = r->binlog_position.offset +
        (line->str - r->buffer.buff);
    SLICE_ADD_TO_CHAIN(thread_ctx, record);
    return 0;
}

static void waiting_and_process_parse_result(SliceLoaderContext
        *slice_ctx, SliceParseThreadContext *parse_thread)
{
    SliceDataThreadContext *data_thread;
    SliceLoaderRecord *record;

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
        data_thread = slice_ctx->data_thread_array.contexts + record->slice.
            bs_key.block.hash_code % slice_ctx->data_thread_array.count;
        SLICE_ADD_TO_CHAIN(data_thread, record);
        if (record->slice.sn > data_thread->last_sns.waiting) {
            data_thread->last_sns.waiting = record->slice.sn;
        }

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
    SliceLoaderRecord *record;
    int64_t min_sn;

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

    min_sn = slice_ctx->data_thread_array.contexts->last_sns.done;
    data_end = slice_ctx->data_thread_array.contexts +
        slice_ctx->data_thread_array.count;
    for (data_thread=slice_ctx->data_thread_array.contexts;
            data_thread<data_end; data_thread++)
    {
        if (data_thread->slices.head != NULL) {
            if (STORAGE_ENABLED) {
                if ((record=fast_mblock_alloc_object(&slice_ctx->
                                parse_thread_array.contexts->
                                record_allocator)) != NULL)
                {
                    record->slice.op_type = BINLOG_OP_TYPE_SET_SN;
                    record->slice.sn = data_thread->last_sns.waiting;
                    data_thread->slices.tail->next = record;
                    data_thread->slices.tail = record;
                }
            }

            data_thread->slices.tail->next = NULL;
            qinfo.head = data_thread->slices.head;
            qinfo.tail = data_thread->slices.tail;
            fc_queue_push_queue_to_tail(&data_thread->queue, &qinfo);
            data_thread->slices.head = data_thread->slices.tail = NULL;
        }

        if (STORAGE_ENABLED && data_thread->last_sns.done < min_sn) {
            min_sn = data_thread->last_sns.done;
        }
    }

    if (STORAGE_ENABLED && min_sn > SLICE_LOAD_LAST_SN) {
        SLICE_LOAD_LAST_SN = min_sn;
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

#define SLICE_LOADER_GET_FILENAME_LINE_COUNT(position, \
        binlog_filename, line_count) \
    do { \
        binlog_reader_get_filename(FS_SLICE_BINLOG_SUBDIR_NAME, \
                position.index, binlog_filename, sizeof(binlog_filename)); \
        fc_get_file_line_count_ex(binlog_filename, \
                position.offset, &line_count); \
        line_count++;   \
    } while (0)

static inline int slice_loader_deal_record(SliceDataThreadContext
        *thread_ctx, SliceLoaderRecord *record)
{
    OBSliceEntry *slice;
    char binlog_filename[PATH_MAX];
    int64_t line_count;
    int log_level;
    int result;

    if (MIGRATE_CLEAN_ENABLED) {
        if (record->slice.op_type == BINLOG_OP_TYPE_NO_OP) {
            return 0;
        }
        if (!fs_is_my_data_group(FS_DATA_GROUP_ID(record->
                        slice.bs_key.block)))
        {
            thread_ctx->skip_count++;
        }
    }

    switch (record->slice.op_type) {
        case BINLOG_OP_TYPE_WRITE_SLICE:
        case BINLOG_OP_TYPE_ALLOC_SLICE:
            if ((slice=ob_index_alloc_slice(&record->
                            slice.bs_key.block)) == NULL)
            {
                return ENOMEM;
            }

            if (record->slice.space.store->index ==
                    DATA_REBUILD_PATH_INDEX)
            {
                thread_ctx->rebuild_count++;
            }

            slice->type = record->slice.slice_type;
            slice->ssize = record->slice.bs_key.slice;
            slice->space = record->slice.space;
            slice->data_version = record->slice.data_version;
            return ob_index_add_slice_by_binlog(record->slice.sn, slice);
        case BINLOG_OP_TYPE_DEL_SLICE:
            if ((result=ob_index_delete_slice_by_binlog(record->
                            slice.sn, &record->slice.bs_key)) == 0)
            {
                return 0;
            } else {
                log_level = (result == ENOENT ? LOG_WARNING : LOG_ERR);
                SLICE_LOADER_GET_FILENAME_LINE_COUNT(record->position,
                        binlog_filename, line_count);
                log_it_ex(&g_log_context, log_level,
                        "file: "__FILE__", line: %d, "
                        "delete slice fail, binlong index: %d, offset: %"
                        PRId64", line no: %"PRId64", block {oid: %"PRId64", "
                        "offset: %"PRId64"}, slice {offset: %d, length: %d}"
                        ", errno: %d, error info: %s", __LINE__, record->
                        position.index, record->position.offset, line_count,
                        record->slice.bs_key.  block.oid, record->slice.bs_key.
                        block.offset, record->slice.bs_key.slice.offset,
                        record->slice.bs_key.slice.length, result,
                        STRERROR(result));
                return (result == ENOENT ? 0 : result);
            }
        case BINLOG_OP_TYPE_DEL_BLOCK:
            if ((result=ob_index_delete_block_by_binlog(record->slice.sn,
                            &record->slice.bs_key.block)) == 0)
            {
                return 0;
            } else {
                log_level = (result == ENOENT ? LOG_WARNING : LOG_ERR);
                SLICE_LOADER_GET_FILENAME_LINE_COUNT(record->position,
                        binlog_filename, line_count);
                log_it_ex(&g_log_context, log_level,
                        "file: "__FILE__", line: %d, "
                        "delete block fail, binlong index: %d, line no: "
                        "%"PRId64", block {oid: %"PRId64", offset: %"PRId64
                        "}, errno: %d, error info: %s", __LINE__,
                        record->position.index, line_count,
                        record->slice.bs_key.block.oid,
                        record->slice.bs_key.block.offset,
                        result, STRERROR(result));
                return (result == ENOENT ? 0 : result);
            }
        case BINLOG_OP_TYPE_SET_SN:
            thread_ctx->last_sns.done = record->slice.sn;
            return 0;
        case BINLOG_OP_TYPE_NO_OP:
        default:
            return 0;
    }
}

static inline void deal_records(SliceDataThreadContext *thread_ctx,
        SliceLoaderRecord *head)
{
    SliceLoaderRecord *record;
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
    SliceLoaderRecord *record;

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
        record = (SliceLoaderRecord *)node->data;
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
    SliceLoaderRecord *head;

    while (SF_G_CONTINUE_FLAG && thread_ctx->
            loader_ctx->data_continue_flag)
    {
        head = (SliceLoaderRecord *)fc_queue_pop_all(&thread_ctx->queue);
        if (head != NULL) {
            deal_records(thread_ctx, head);
        }
    }
    __sync_sub_and_fetch(&thread_ctx->loader_ctx->thread_counts.data, 1);
}

static int slice_record_alloc_init(SliceLoaderRecord *record,
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
                    "slice_record", sizeof(SliceLoaderRecord),
                    alloc_elements_once, elements_limit,
                    (fast_mblock_object_init_func)slice_record_alloc_init,
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
                        &((SliceLoaderRecord *)NULL)->next))) != 0)
    {
        return result;
    }

    thread_ctx->total_count = 0;
    thread_ctx->skip_count = 0;
    thread_ctx->rebuild_count = 0;
    thread_ctx->done_count = 0;
    thread_ctx->last_sns.waiting = 0;
    thread_ctx->last_sns.done = 0;
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

    __sync_add_and_fetch(&loader_ctx->thread_counts.parse, thread_count);
    ctx_array->count = thread_count;
    end = ctx_array->contexts + ctx_array->count;
    for (ctx=ctx_array->contexts; ctx<end; ctx++) {
        if ((result=init_parse_thread_context(ctx)) != 0) {
            return result;
        }

        ctx->thread_index = ctx - ctx_array->contexts;
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

    __sync_add_and_fetch(&loader_ctx->thread_counts.data, thread_count);
    ctx_array->count = thread_count;
    end = ctx_array->contexts + ctx_array->count;
    for (ctx=ctx_array->contexts; ctx<end; ctx++) {
        if ((result=init_data_thread_context(ctx)) != 0) {
            return result;
        }

        ctx->thread_index = ctx - ctx_array->contexts;
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

        if (FC_ATOMIC_GET(slice_ctx->thread_counts.parse) == 0) {
            break;
        }
        fc_sleep_ms(1);
    }
}

static void waiting_data_threads_finish(SliceLoaderContext *slice_ctx,
        SliceDataThreadCtxArray *ctx_array)
{
    SliceDataThreadContext *data_thread;
    SliceDataThreadContext *end;
    int64_t min_sn;
    bool all_done;

    end = ctx_array->contexts + ctx_array->count;
    while (SF_G_CONTINUE_FLAG) {
        all_done = true;
        for (data_thread=ctx_array->contexts; data_thread<end; data_thread++) {
            if (data_thread->done_count < data_thread->total_count) {
                all_done = false;
                break;
            }
        }
        if (all_done) {
            break;
        }

        fc_sleep_ms(10);
        if (STORAGE_ENABLED) {
            min_sn = ctx_array->contexts->last_sns.done;
            for (data_thread=ctx_array->contexts + 1;
                    data_thread<end; data_thread++)
            {
                if (data_thread->last_sns.done < min_sn) {
                    min_sn = data_thread->last_sns.done;
                }
            }

            if (min_sn > SLICE_LOAD_LAST_SN) {
                SLICE_LOAD_LAST_SN = min_sn;
                logInfo("thread: #%d, SLICE_LOAD_LAST_SN: %"PRId64,
                        (int)(data_thread - ctx_array->contexts), SLICE_LOAD_LAST_SN);
            }
        }
    }

    slice_ctx->data_continue_flag = false;
    while (SF_G_CONTINUE_FLAG) {
        for (data_thread=ctx_array->contexts; data_thread<end; data_thread++) {
            fc_queue_terminate(&data_thread->queue);
        }

        if (FC_ATOMIC_GET(slice_ctx->thread_counts.data) == 0) {
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

    slice_ctx->binlog_count = ctx->total_count;
    waiting_data_threads_finish(slice_ctx,
            &slice_ctx->data_thread_array);
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
    SFBinlogFilePosition position;

    if (STORAGE_ENABLED) {
        logInfo("event_dealer_get_last_data_version: %"PRId64,
                event_dealer_get_last_data_version());
        if (event_dealer_get_last_data_version() >= SLICE_BINLOG_SN) {
            SLICE_LOAD_LAST_SN = SLICE_BINLOG_SN;
            SLICE_LOAD_DONE = true;
            return 0;
        }
    }

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
    if (STORAGE_ENABLED) {
        if ((result=slice_binlog_get_position_by_sn(
                        event_dealer_get_last_data_version(),
                        &position)) != 0)
        {
            return result;
        }
    } else {
        position.index = sf_binlog_get_start_index(slice_writer);
        position.offset = 0;
    }
    result = binlog_loader_load1(FS_SLICE_BINLOG_SUBDIR_NAME, slice_writer,
            &position, &callbacks, (ctx.parse_thread_array.count +
                ctx.data_thread_array.count) * 2);
    SLICE_LOAD_LAST_SN = SLICE_BINLOG_SN;
    if (STORAGE_ENABLED && result == 0) {
        if (event_dealer_get_last_data_version() < SLICE_BINLOG_SN) {
            change_notify_load_done_signal();
        }
    }
    SLICE_LOAD_DONE = true;

    if (result == 0) {
        __sync_add_and_fetch(&SLICE_BINLOG_COUNT, ctx.binlog_count);
        if (DATA_REBUILD_PATH_INDEX >= 0) {
            DATA_REBUILD_SLICE_COUNT = get_total_rebuild_count(
                    &ctx.data_thread_array);
            result = store_path_rebuild_dump_data(get_total_count(
                        &ctx.data_thread_array));
            logInfo("rebuild path: %s, total slice count: %"PRId64", "
                    "rebuild count: %"PRId64, DATA_REBUILD_PATH_STR,
                    get_total_count(&ctx.data_thread_array),
                    DATA_REBUILD_SLICE_COUNT);
        }
    }

    if (result == 0 && MIGRATE_CLEAN_ENABLED) {
        bool dump_slice_index;
        dump_slice_index = (get_total_skip_count(&ctx.data_thread_array) > 0);
        result = migrate_clean_binlog(get_total_count(&ctx.
                    data_thread_array), dump_slice_index);
    }

    destroy_loader_context(&ctx);
    return result;
}
