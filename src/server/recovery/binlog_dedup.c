#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <pthread.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "../../common/fs_func.h"
#include "../server_global.h"
#include "../binlog/replica_binlog.h"
#include "../binlog/binlog_read_thread.h"
#include "../storage/object_block_index.h"
#include "binlog_fetch.h"
#include "data_recovery.h"
#include "binlog_dedup.h"

typedef struct {
    OBHashtable htable;
    BinlogReadThreadContext rdthread_ctx;
    BinlogReadThreadResult *r;
    ReplicaBinlogRecord record;
    struct {
        int64_t total;
        int64_t success;
    } record_counts;
} BinlogDedupContext;

static int add_slice(BinlogDedupContext *dedup_ctx, const OBSliceType stype)
{
    OBSliceEntry *slice;
    int inc_alloc;

    slice = ob_index_alloc_slice_ex(&dedup_ctx->htable,
            &dedup_ctx->record.bs_key.block, 0);
    if (slice == NULL) {
        return ENOMEM;
    }

    slice->type = stype;
    slice->ssize = dedup_ctx->record.bs_key.slice;
    return ob_index_add_slice_ex(&dedup_ctx->htable,
            slice, NULL, &inc_alloc);
}

static int deal_binlog_buffer(BinlogDedupContext *dedup_ctx)
{
    char *p;
    char *line_end;
    char *end;
    BufferInfo *buffer;
    string_t line;
    char error_info[256];
    int result;
    int dec_alloc;

    result = 0;
    *error_info = '\0';
    buffer = &dedup_ctx->r->buffer;
    end = buffer->buff + buffer->length;
    p = buffer->buff;
    while (p < end) {
        line_end = (char *)memchr(p, '\n', end - p);
        if (line_end == NULL) {
            strcpy(error_info, "expect end line (\\n)");
            result = EINVAL;
            break;
        }

        line_end++;
        line.str = p;
        line.len = line_end - p;
        if ((result=replica_binlog_record_unpack(&line,
                        &dedup_ctx->record, error_info)) != 0)
        {
            break;
        }

        fs_calc_block_hashcode(&dedup_ctx->record.bs_key.block);
        switch (dedup_ctx->record.op_type) {
            case REPLICA_BINLOG_OP_TYPE_WRITE_SLICE:
                result = add_slice(dedup_ctx, OB_SLICE_TYPE_FILE);
                break;
            case REPLICA_BINLOG_OP_TYPE_ALLOC_SLICE:
                result = add_slice(dedup_ctx, OB_SLICE_TYPE_ALLOC);
                break;
            case REPLICA_BINLOG_OP_TYPE_DEL_SLICE:
                result = ob_index_delete_slices_ex(&dedup_ctx->htable,
                        &dedup_ctx->record.bs_key, NULL, &dec_alloc);
                break;
            case REPLICA_BINLOG_OP_TYPE_DEL_BLOCK:
                result = ob_index_delete_block_ex(&dedup_ctx->htable,
                        &dedup_ctx->record.bs_key.block, NULL, &dec_alloc);
                break;
        }

        dedup_ctx->record_counts.total++;
        if (result == 0) {
            dedup_ctx->record_counts.success++;
        } else {
            int op_type;
            op_type = dedup_ctx->record.op_type;
            if (!((result == ENOENT) &&
                        (op_type == REPLICA_BINLOG_OP_TYPE_DEL_SLICE ||
                         op_type == REPLICA_BINLOG_OP_TYPE_DEL_BLOCK)))
            {
                snprintf(error_info, sizeof(error_info),
                        "%s fail, errno: %d, error info: %s",
                        replica_binlog_get_op_type_caption(op_type),
                        result, STRERROR(result));
                break;
            }
        }

        p = line_end;
    }

    if (result != 0) {
        ServerBinlogReader *reader;
        int64_t offset;
        int64_t line_count;

        reader = &dedup_ctx->rdthread_ctx.reader;
        offset = reader->position.offset + (p - buffer->buff);
        fc_get_file_line_count_ex(reader->filename, offset, &line_count);

        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", %s",
                __LINE__, reader->filename,
                line_count + 1, error_info);
    }

    return result;
}

static int dedup_binlog(DataRecoveryContext *ctx,
        BinlogDedupContext *dedup_ctx)
{
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];
    int result;

    data_recovery_get_subdir_name(ctx, RECOVERY_BINLOG_SUBDIR_NAME_FETCH,
            subdir_name);
    if ((result=binlog_read_thread_init(&dedup_ctx->rdthread_ctx, subdir_name,
                    NULL, NULL, BINLOG_BUFFER_SIZE)) != 0)
    {
        return result;
    }

    logInfo("file: "__FILE__", line: %d, "
            "dedup %s data ...", __LINE__, subdir_name);

    result = 0;
    while (SF_G_CONTINUE_FLAG) {
        if ((dedup_ctx->r=binlog_read_thread_fetch_result(
                        &dedup_ctx->rdthread_ctx)) == NULL)
        {
            result = EINTR;
            break;
        }

        logInfo("errno: %d, buffer length: %d", dedup_ctx->r->err_no,
                dedup_ctx->r->buffer.length);
        if (dedup_ctx->r->err_no == ENOENT) {
            break;
        } else if (dedup_ctx->r->err_no != 0) {
            result = dedup_ctx->r->err_no;
            break;
        }

        if ((result=deal_binlog_buffer(dedup_ctx)) != 0) {
            break;
        }

        binlog_read_thread_return_result_buffer(&dedup_ctx->rdthread_ctx,
                dedup_ctx->r);
    }

    binlog_read_thread_terminate(&dedup_ctx->rdthread_ctx);
    return result;
}

static int do_output(BinlogDedupContext *dedup_ctx, UniqSkiplist *slices)
{
    return 0;
}

static int dedup_output(DataRecoveryContext *ctx,
        BinlogDedupContext *dedup_ctx, int64_t *binlog_count)
{
    OBEntry **bucket;
    OBEntry **end;
    OBEntry *ob;
    int result;

    result = 0;
    end = dedup_ctx->htable.buckets + dedup_ctx->htable.capacity;
    for (bucket = dedup_ctx->htable.buckets;
            bucket < end && result == 0; bucket++)
    {
        if (*bucket == NULL) {
            continue;
        }

        ob = *bucket;
        do {
            if (!uniq_skiplist_empty(ob->slices)) {
                if ((result=do_output(dedup_ctx, ob->slices)) != 0) {
                    break;
                }
            }
            ob = ob->next;
        } while (ob != NULL);
    }

    return 0;
}

int data_recovery_dedup_binlog(DataRecoveryContext *ctx, int64_t *binlog_count)
{
    int result;
    BinlogDedupContext dedup_ctx;
    int64_t start_time;
    int64_t end_time;
    char time_buff[32];

    start_time = get_current_time_ms();

    *binlog_count = 0;
    dedup_ctx.record_counts.total = 0;
    dedup_ctx.record_counts.success = 0;
    if ((result=ob_index_init_htable(&dedup_ctx.htable)) != 0) {
        return result;
    }

    if ((result=dedup_binlog(ctx, &dedup_ctx)) == 0) {
        if (dedup_ctx.record_counts.success > 0) {
            result = dedup_output(ctx, &dedup_ctx, binlog_count);
        }
    }
    ob_index_destroy_htable(&dedup_ctx.htable);

    if (result == 0) {
        result = data_recovery_unlink_fetched_binlog(ctx);

        end_time = get_current_time_ms();
        long_to_comma_str(end_time - start_time, time_buff);
        logInfo("file: "__FILE__", line: %d, "
                "dedup data group id: %d done. total record count: %"PRId64", "
                "success record count: %"PRId64", time used: %s ms", __LINE__,
                ctx->data_group_id, dedup_ctx.record_counts.total,
                dedup_ctx.record_counts.success, time_buff);
    } else {
        logError("file: "__FILE__", line: %d, "
                "result: %d", __LINE__, result);
    }

    return result;
}
