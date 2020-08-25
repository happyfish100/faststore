#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <pthread.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "binlog_func.h"
#include "binlog_reader.h"
#include "slice_binlog.h"
#include "replica_binlog.h"
#include "binlog_repair.h"

#define BINLOG_REPAIR_FILE_EXT_NAME  ".repair"

typedef struct {
    int binlog_index;
    int fd;
    int64_t file_size;
    char filename[PATH_MAX];
    ServerBinlogBuffer buffer;
} BinlogRepairWriter;

typedef struct {
    struct {
        const char *subdir_name;
        int obj_filed_skip;
        struct binlog_writer_info *writer;
        FSBinlogFilePosition *pos;
        BinlogDataGroupVersionArray *varray;
    } input;
    BinlogRepairWriter out_writer;
} BinlogRepairContext;

static int repair_open_file(BinlogRepairContext *ctx)
{
    binlog_reader_get_filename_ex(ctx->input.subdir_name,
            BINLOG_REPAIR_FILE_EXT_NAME,
            ctx->out_writer.binlog_index,
            ctx->out_writer.filename,
            sizeof(ctx->out_writer.filename));

    logInfo("repair filename ==== %s", ctx->out_writer.filename);

    if (ctx->out_writer.fd >= 0) {
        close(ctx->out_writer.fd);
    }

    ctx->out_writer.fd = open(ctx->out_writer.filename,
            O_WRONLY | O_CREAT | O_TRUNC | O_APPEND, 0644);
    if (ctx->out_writer.fd < 0) {
        logCrit("file: "__FILE__", line: %d, "
                "open file \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, ctx->out_writer.filename,
                errno, STRERROR(errno));
        return errno != 0 ? errno : EACCES;
    }

    ctx->out_writer.file_size = 0;
    return 0;
}

static int do_repair(BinlogRepairContext *ctx)
{
    int result;
    int read_bytes;
    ServerBinlogReader reader;

    ctx->out_writer.fd = -1;
    ctx->out_writer.binlog_index = ctx->input.pos->index;
    if ((result=repair_open_file(ctx)) != 0) {
        return result;
    }

    if ((result=binlog_reader_init(&reader, ctx->input.subdir_name,
                    ctx->input.writer, ctx->input.pos)) != 0)
    {
        return result;
    }

    while ((result=binlog_reader_integral_read(&reader,
                    reader.binlog_buffer.buff,
                    reader.binlog_buffer.size,
                    &read_bytes)) == 0)
    {
        /*
        if ((result=binlog_parse_buffer(&reader, read_bytes,
                        obj_filed_skip, varray)) != 0)
        {
            break;
        }
        */
    }

    if (result == ENOENT) {
        result = 0;
    }

    binlog_reader_destroy(&reader);
    return result;
}

static int binlog_repair(BinlogRepairContext *ctx)
{
    char filename[PATH_MAX];
    int result;
    int current_windex;
    int64_t file_size;

    current_windex = binlog_get_current_write_index(ctx->input.writer);
    if (ctx->input.pos->index == current_windex) {
        binlog_reader_get_filename(ctx->input.subdir_name,
                ctx->input.pos->index,
                filename, sizeof(filename));
        if ((result=getFileSize(filename, &file_size)) != 0) {
            return result;
        }

        if (ctx->input.pos->offset >= file_size) {
            return 0;
        }
    }

    return do_repair(ctx);
}

static int repair_context_init(BinlogRepairContext *ctx,
        const int obj_filed_skip, BinlogDataGroupVersionArray *varray)
{
    ctx->input.obj_filed_skip = obj_filed_skip;
    ctx->input.varray = varray;
    return binlog_buffer_init(&ctx->out_writer.buffer);
}

static void repair_context_destroy(BinlogRepairContext *ctx)
{
    binlog_buffer_destroy(&ctx->out_writer.buffer);
}

int binlog_consistency_repair_replica(BinlogConsistencyContext *ctx)
{
    int result;
    int data_group_id;
    int index;
    BinlogRepairContext repair_ctx;
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];
    FSBinlogFilePosition *replica;
    FSBinlogFilePosition *end;

    if ((result=repair_context_init(&repair_ctx, 0,
                    &ctx->version_arrays.slice)) != 0)
    {
        return result;
    }

    end = ctx->positions.replicas + ctx->positions.dg_count;
    for (replica=ctx->positions.replicas; replica<end; replica++) {
        index = replica - ctx->positions.replicas;
        data_group_id = ctx->positions.base_dg_id + index;
        sprintf(subdir_name, "%s/%d", FS_REPLICA_BINLOG_SUBDIR_NAME,
                data_group_id);

        repair_ctx.input.subdir_name = subdir_name;
        repair_ctx.input.writer = replica_binlog_get_writer(data_group_id);
        repair_ctx.input.pos = replica;
        if ((result=binlog_repair(&repair_ctx)) != 0) {
            break;
        }
    }

    repair_context_destroy(&repair_ctx);
    return result;
}

int binlog_consistency_repair_slice(BinlogConsistencyContext *ctx)
{
    int result;
    BinlogRepairContext repair_ctx;

    if ((result=repair_context_init(&repair_ctx, 1,
                    &ctx->version_arrays.replica)) != 0)
    {
        return result;
    }

    repair_ctx.input.subdir_name = FS_SLICE_BINLOG_SUBDIR_NAME;
    repair_ctx.input.writer = slice_binlog_get_writer();
    repair_ctx.input.pos = &ctx->positions.slice;
    result = binlog_repair(&repair_ctx);

    repair_context_destroy(&repair_ctx);
    return result;
}
