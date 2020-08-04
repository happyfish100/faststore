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
#include "../server_binlog.h"
#include "../binlog/replica_binlog.h"
#include "../binlog/binlog_read_thread.h"
#include "../storage/object_block_index.h"
#include "binlog_fetch.h"
#include "data_recovery.h"
#include "binlog_dedup.h"

typedef struct {
    FILE *fp;
    char filename[PATH_MAX];
} BinlogFileWriter;

typedef struct {
    OBHashtable create;   //create operation
    OBHashtable remove;   //remove operation
} BinlogHashtables;

typedef struct {
    int64_t total;
    int64_t success;
    int64_t ignore;
} BinlogCounterTripple;

typedef struct {
    BinlogHashtables htables;
    BinlogReadThreadContext rdthread_ctx;
    BinlogReadThreadResult *r;
    ReplicaBinlogRecord record;
    struct {
        BinlogCounterTripple create;  //add slice index
        BinlogCounterTripple remove;  //remove slice index
        int64_t partial_deletes;
    } rstat;  //record stat

    struct {
        BinlogFileWriter writer;
        int current_op_type;
        uint64_t current_version;
        struct {
            int64_t create;
            int64_t remove;
        } binlog_counts;
    } out;
} BinlogDedupContext;

static int add_slice(OBHashtable *htable, ReplicaBinlogRecord *record,
        const OBSliceType stype)
{
    OBSliceEntry *slice;
    int inc_alloc;

    slice = ob_index_alloc_slice_ex(htable, &record->bs_key.block, 0);
    if (slice == NULL) {
        return ENOMEM;
    }

    slice->type = stype;
    slice->ssize = record->bs_key.slice;
    return ob_index_add_slice_ex(htable, slice, NULL, &inc_alloc);
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
    int r;
    int op_type;
    int target_len;
    int dec_alloc;

    result = 0;
    dec_alloc = 0;
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

        op_type = dedup_ctx->record.op_type;
        fs_calc_block_hashcode(&dedup_ctx->record.bs_key.block);
        switch (op_type) {
            case REPLICA_BINLOG_OP_TYPE_WRITE_SLICE:
            case REPLICA_BINLOG_OP_TYPE_ALLOC_SLICE:
                if (op_type == REPLICA_BINLOG_OP_TYPE_WRITE_SLICE) {
                    result = add_slice(&dedup_ctx->htables.create,
                            &dedup_ctx->record, OB_SLICE_TYPE_FILE);
                } else {
                    result = add_slice(&dedup_ctx->htables.create,
                            &dedup_ctx->record, OB_SLICE_TYPE_ALLOC);
                }
                dedup_ctx->rstat.create.total++;
                if (result == 0) {
                    dedup_ctx->rstat.create.success++;
                }
                break;
            case REPLICA_BINLOG_OP_TYPE_DEL_SLICE:
            case REPLICA_BINLOG_OP_TYPE_DEL_BLOCK:
                if (op_type == REPLICA_BINLOG_OP_TYPE_DEL_SLICE) {
                    result = ob_index_delete_slices_ex(&dedup_ctx->
                            htables.create, &dedup_ctx->record.bs_key,
                            NULL, &dec_alloc);
                    target_len = dedup_ctx->record.bs_key.slice.length;
                } else {
                    result = ob_index_delete_block_ex(&dedup_ctx->
                            htables.create, &dedup_ctx->record.bs_key.
                            block, NULL, &dec_alloc);
                    target_len = FS_FILE_BLOCK_SIZE;
                }

                if (dec_alloc != target_len) {
                    if (op_type == REPLICA_BINLOG_OP_TYPE_DEL_BLOCK) {
                        dedup_ctx->record.bs_key.slice.offset = 0;
                        dedup_ctx->record.bs_key.slice.length =
                            FS_FILE_BLOCK_SIZE;
                    }

                    if ((r=add_slice(&dedup_ctx->htables.remove,
                                    &dedup_ctx->record,
                                    OB_SLICE_TYPE_FILE)) == 0)
                    {
                        dedup_ctx->rstat.partial_deletes++;
                    } else {
                        result = r;
                    }
                }

                dedup_ctx->rstat.remove.total++;
                if (result == 0) {
                    dedup_ctx->rstat.remove.success++;
                } else if (result == ENOENT) {
                    dedup_ctx->rstat.remove.ignore++;
                    result = 0;
                }
                break;
            default:
                break;
        }

        if (result != 0) {
            snprintf(error_info, sizeof(error_info),
                    "%s fail, errno: %d, error info: %s",
                    replica_binlog_get_op_type_caption(op_type),
                    result, STRERROR(result));
            break;
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

static int do_dedup_binlog(DataRecoveryContext *ctx)
{
    BinlogDedupContext *dedup_ctx;
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];
    int result;

    dedup_ctx = (BinlogDedupContext *)ctx->arg;
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

static int write_one_binlog(BinlogDedupContext *dedup_ctx,
        const OBSliceEntry *first, const OBSliceEntry *last)
{
    int length;
    int result;
    int op_type;
    uint64_t data_version;

    if (first == last) {
        length = first->ssize.length;
    } else {
        length = (last->ssize.offset - first->ssize.offset) +
            last->ssize.length;
    }

    if (dedup_ctx->out.current_op_type == SLICE_BINLOG_OP_TYPE_DEL_SLICE) {
        op_type = REPLICA_BINLOG_OP_TYPE_DEL_SLICE;
    } else {
        if (first->type == OB_SLICE_TYPE_FILE) {
            op_type = REPLICA_BINLOG_OP_TYPE_WRITE_SLICE;
        } else {
            op_type = REPLICA_BINLOG_OP_TYPE_ALLOC_SLICE;
        }
    }

    data_version = ++(dedup_ctx->out.current_version);
    if (fprintf(dedup_ctx->out.writer.fp,
                "%d %"PRId64" %c %"PRId64" %"PRId64" %d %d\n",
                (int)g_current_time, data_version,
                op_type, first->ob->bkey.oid,
                first->ob->bkey.offset,
                first->ssize.offset, length) > 0)
    {
        result = 0;
    } else {
        result = errno != 0 ? errno : EPERM;
        logError("file: "__FILE__", line: %d, "
                "write to file: %s fail, "
                "errno: %d, error info: %s", __LINE__,
                dedup_ctx->out.writer.filename,
                result, STRERROR(result));
    }

    return result;
}

static int do_output(BinlogDedupContext *dedup_ctx, const OBEntry *ob,
        int64_t *binlog_count)
{
    UniqSkiplistIterator it;
    OBSliceEntry *first;
    OBSliceEntry *previous;
    OBSliceEntry *slice;
    int result;

    uniq_skiplist_iterator(ob->slices, &it);
    first = previous = (OBSliceEntry *)uniq_skiplist_next(&it);
    while ((slice=(OBSliceEntry *)uniq_skiplist_next(&it)) != NULL) {
        if (!((previous->ssize.offset + previous->ssize.length ==
                        slice->ssize.offset) && (previous->type == slice->type)))
        {
            (*binlog_count)++;
            if ((result=write_one_binlog(dedup_ctx, first, previous)) != 0) {
                return result;
            }

            first = slice;
        }

        previous = slice;
    }

    (*binlog_count)++;
    return write_one_binlog(dedup_ctx, first, previous);
}

static int open_output_files(DataRecoveryContext *ctx)
{
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];
    BinlogDedupContext *dedup_ctx;
    int result;

    dedup_ctx = (BinlogDedupContext *)ctx->arg;
    data_recovery_get_subdir_name(ctx, RECOVERY_BINLOG_SUBDIR_NAME_REPLAY,
            subdir_name);

    binlog_reader_get_filename(subdir_name, 0, dedup_ctx->out.writer.filename,
            sizeof(dedup_ctx->out.writer.filename));
    dedup_ctx->out.writer.fp = fopen(dedup_ctx->out.writer.filename, "wb");
    if (dedup_ctx->out.writer.fp == NULL) {
        result = errno != 0 ? errno : EPERM;
        logError("file: "__FILE__", line: %d, "
                "open file: %s to write fail, "
                "errno: %d, error info: %s",
                __LINE__, dedup_ctx->out.writer.filename,
                result, STRERROR(result));
        return result;
    }

    return 0;
}

static void close_output_files(BinlogDedupContext *dedup_ctx)
{
    if (dedup_ctx->out.writer.fp != NULL) {
        fclose(dedup_ctx->out.writer.fp);
        dedup_ctx->out.writer.fp = NULL;
    }
}

static int htable_dump(BinlogDedupContext *dedup_ctx, OBHashtable *htable,
        int64_t *binlog_count)
{
    OBEntry **bucket;
    OBEntry **end;
    OBEntry *ob;
    int result;

    *binlog_count = 0;
    end = htable->buckets + htable->capacity;
    for (bucket = htable->buckets, result = 0;
            bucket < end && result == 0; bucket++)
    {
        if (*bucket == NULL) {
            continue;
        }

        ob = *bucket;
        do {
            if (!uniq_skiplist_empty(ob->slices)) {
                if ((result=do_output(dedup_ctx, ob, binlog_count)) != 0) {
                    break;
                }
            }
            ob = ob->next;
        } while (ob != NULL);
    }

    return result;
}

static void htable_reverse_remove(BinlogHashtables *htables)
{
    OBEntry **bucket;
    OBEntry **end;
    OBEntry *ob;
    OBSliceEntry *slice;
    UniqSkiplistIterator it;
    FSBlockSliceKeyInfo bs_key;
    int dec_alloc;

    end = htables->create.buckets + htables->create.capacity;
    for (bucket = htables->create.buckets; bucket < end; bucket++) {
        if (*bucket == NULL) {
            continue;
        }

        ob = *bucket;
        do {
            do {
                if (uniq_skiplist_empty(ob->slices)) {
                    break;
                }

                if (ob_index_get_ob_entry(&htables->remove,
                            &ob->bkey) == NULL)
                {
                    break;
                }

                uniq_skiplist_iterator(ob->slices, &it);
                while ((slice=(OBSliceEntry *)uniq_skiplist_next(&it)) != NULL) {
                    bs_key.block = slice->ob->bkey;
                    bs_key.slice = slice->ssize;
                    ob_index_delete_slices_ex(&htables->remove,
                            &bs_key, NULL, &dec_alloc);
                }
            } while (0);

            ob = ob->next;
        } while (ob != NULL);
    }
}

static int dedup_binlog(DataRecoveryContext *ctx)
{
    BinlogDedupContext *dedup_ctx;
    int result;

    dedup_ctx = (BinlogDedupContext *)ctx->arg;
    if ((result=do_dedup_binlog(ctx)) != 0) {
        return result;
    }

    if (dedup_ctx->rstat.create.success == 0 && 
            dedup_ctx->rstat.partial_deletes == 0)
    {
        return 0;
    }

    if ((result=open_output_files(ctx)) != 0) {
        return result;
    }

    if (dedup_ctx->rstat.partial_deletes > 0) {
        if (dedup_ctx->rstat.create.success > 0) {
            htable_reverse_remove(&dedup_ctx->htables);
        }

        dedup_ctx->out.current_op_type = SLICE_BINLOG_OP_TYPE_DEL_SLICE;
        result = htable_dump(dedup_ctx, &dedup_ctx->htables.remove,
                 &dedup_ctx->out.binlog_counts.remove);
    }

    if (dedup_ctx->rstat.create.success > 0) {
        dedup_ctx->out.current_op_type = SLICE_BINLOG_OP_TYPE_ADD_SLICE;
        result = htable_dump(dedup_ctx, &dedup_ctx->htables.create,
                &dedup_ctx->out.binlog_counts.create);
    }

    close_output_files(dedup_ctx);
    return result;
}

static int init_htables(DataRecoveryContext *ctx)
{
    BinlogDedupContext *dedup_ctx;
    int result;
    int64_t slice_capacity;
    int64_t deleted_capacity;

    dedup_ctx = (BinlogDedupContext *)ctx->arg;

    slice_capacity = ctx->master->replica.data_version -
        ctx->master->dg->myself->replica.data_version;
    if (slice_capacity < 256) {
        slice_capacity = 256;
    } else if (slice_capacity > STORAGE_CFG.object_block.hashtable_capacity) {
        slice_capacity = STORAGE_CFG.object_block.hashtable_capacity;
    }
    if ((result=ob_index_init_htable_ex(&dedup_ctx->htables.create,
                    slice_capacity, false, false)) != 0)
    {
        return result;
    }

    deleted_capacity = slice_capacity / 4;
    if (deleted_capacity > 10240) {
        deleted_capacity = 10240;
    }
    if ((result=ob_index_init_htable_ex(&dedup_ctx->htables.remove,
                    deleted_capacity, false, false)) != 0)
    {
        return result;
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
    memset(&dedup_ctx, 0, sizeof(dedup_ctx));
    ctx->arg = &dedup_ctx;

    if ((result=init_htables(ctx)) != 0) {
        return result;
    }
    
    dedup_ctx.out.current_version = __sync_fetch_and_add(
            &ctx->master->dg->myself->replica.data_version, 0);

    result = dedup_binlog(ctx);
    ob_index_destroy_htable(&dedup_ctx.htables.create);

    *binlog_count = dedup_ctx.out.binlog_counts.remove +
        dedup_ctx.out.binlog_counts.create;
    if (result == 0) {
        result = data_recovery_unlink_fetched_binlog(ctx);

        end_time = get_current_time_ms();
        long_to_comma_str(end_time - start_time, time_buff);

        logInfo("file: "__FILE__", line: %d, "
                "dedup data group id: %d done. "
                "input: {all : {total : %"PRId64", success : %"PRId64"}, "
                "create : {total : %"PRId64", success : %"PRId64"}, "
                "delete : {total : %"PRId64", success : %"PRId64", "
                "ignore : %"PRId64", partial : %"PRId64"}}, "
                "output: {create : %"PRId64", delete : %"PRId64"}, "
                "time used: %s ms", __LINE__, ctx->data_group_id,
                dedup_ctx.rstat.create.total + dedup_ctx.rstat.remove.total,
                dedup_ctx.rstat.create.success + dedup_ctx.rstat.remove.success,
                dedup_ctx.rstat.create.total, dedup_ctx.rstat.create.success,
                dedup_ctx.rstat.remove.total, dedup_ctx.rstat.remove.success,
                dedup_ctx.rstat.remove.ignore, dedup_ctx.rstat.partial_deletes,
                dedup_ctx.out.binlog_counts.create,
                dedup_ctx.out.binlog_counts.remove, time_buff);
    } else {
        logError("file: "__FILE__", line: %d, "
                "dedup binlog fail, result: %d",
                __LINE__, result);
    }

    return result;
}
