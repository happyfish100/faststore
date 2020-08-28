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
#include "../../common/fs_func.h"
#include "../server_global.h"
#include "binlog_func.h"
#include "binlog_reader.h"
#include "slice_binlog.h"
#include "replica_binlog.h"
#include "binlog_repair.h"

#define BINLOG_REPAIR_FILE_EXT_NAME  ".repair"

#define BINLOG_REPAIR_SYS_DATA_FILENAME           ".binlog_repair.dat"
#define BINLOG_REPAIR_SYS_DATA_ITEM_DG_ID         "data_group_id"
#define BINLOG_REPAIR_SYS_DATA_ITEM_START_BINDEX  "start_binlog_index"
#define BINLOG_REPAIR_SYS_DATA_ITEM_END_BINDEX    "end_binlog_index"

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
        int data_group_id;
        struct binlog_writer_info *writer;
        FSBinlogFilePosition *pos;
        BinlogDataGroupVersionArray *varray;
    } input;
    BinlogRepairWriter out_writer;
} BinlogRepairContext;

static void binlog_repair_get_sys_data_filename(char *filename, const int size)
{
    snprintf(filename, size, "%s/%s", DATA_PATH_STR,
            BINLOG_REPAIR_SYS_DATA_FILENAME);
}

static int binlog_repair_save_sys_data(BinlogRepairContext *ctx)
{
    char filename[PATH_MAX];
    char buff[256];
    int len;

    binlog_repair_get_sys_data_filename(filename, sizeof(filename));
    len = sprintf(buff, "%s=%d\n"
            "%s=%d\n"
            "%s=%d\n",
            BINLOG_REPAIR_SYS_DATA_ITEM_DG_ID,
            ctx->input.data_group_id,
            BINLOG_REPAIR_SYS_DATA_ITEM_START_BINDEX,
            ctx->input.pos->index,
            BINLOG_REPAIR_SYS_DATA_ITEM_END_BINDEX,
            ctx->out_writer.binlog_index);
    return safeWriteToFile(filename, buff, len);
}

static int binlog_repair_unlink_sys_data()
{
    char filename[PATH_MAX];
    binlog_repair_get_sys_data_filename(filename, sizeof(filename));
    return fc_delete_file_ex(filename, "repair sys");
}

static int binlog_repair_load_sys_data(int *data_group_id,
        int *start_binlog_index, int *end_binlog_index)
{
    IniContext ini_context;
    char filename[PATH_MAX];
    int result;

    binlog_repair_get_sys_data_filename(filename, sizeof(filename));
    if (access(filename, F_OK) != 0) {
        result = errno != 0 ? errno : EPERM;
        if (result != ENOENT) {
            logError("file: "__FILE__", line: %d, "
                    "access file: %s fail, errno: %d, error info: %s",
                    __LINE__, filename, result, STRERROR(result));
        }

        return result;
    }

    if ((result=iniLoadFromFile(filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load from ini file \"%s\" fail, ret code: %d",
                __LINE__, filename, result);
        return result;
    }

    *data_group_id = iniGetIntValue(NULL, BINLOG_REPAIR_SYS_DATA_ITEM_DG_ID,
            &ini_context, -1);
    *start_binlog_index = iniGetIntValue(NULL,
            BINLOG_REPAIR_SYS_DATA_ITEM_START_BINDEX, &ini_context, -1);
    *end_binlog_index = iniGetIntValue(NULL,
            BINLOG_REPAIR_SYS_DATA_ITEM_END_BINDEX, &ini_context, -1);
    iniFreeContext(&ini_context);

    if ((*data_group_id < 0) || (*start_binlog_index < 0) ||
            (*end_binlog_index < 0))
    {
        logError("file: "__FILE__", line: %d, "
                "sys data file \"%s\" is invalid, "
                "you should delete it manually and try again!",
                __LINE__, filename);
        return EINVAL;
    }

    return 0;
}

static int open_write_file(BinlogRepairContext *ctx)
{
    if (ctx->out_writer.fd >= 0) {
        close(ctx->out_writer.fd);
    }

    binlog_reader_get_filename_ex(ctx->input.subdir_name,
            BINLOG_REPAIR_FILE_EXT_NAME,
            ctx->out_writer.binlog_index,
            ctx->out_writer.filename,
            sizeof(ctx->out_writer.filename));

    logInfo("repair filename ==== %s", ctx->out_writer.filename);

    ctx->out_writer.fd = open(ctx->out_writer.filename,
            O_WRONLY | O_CREAT | O_TRUNC | O_APPEND, 0644);
    if (ctx->out_writer.fd < 0) {
        logError("file: "__FILE__", line: %d, "
                "open file \"%s\" to write fail, "
                "errno: %d, error info: %s",
                __LINE__, ctx->out_writer.filename,
                errno, STRERROR(errno));
        return errno != 0 ? errno : EACCES;
    }

    ctx->out_writer.file_size = 0;
    return 0;
}

static int copy_binlog_front_part(BinlogRepairContext *ctx)
{
    char filename[PATH_MAX];
    int64_t remain_bytes;
    int fd;
    int current_bytes;
    int read_bytes;
    int result;

    if (ctx->input.pos->offset == 0) {
        return 0;
    }

    binlog_reader_get_filename(ctx->input.subdir_name,
            ctx->out_writer.binlog_index, filename, sizeof(filename));
    fd = open(filename, O_RDONLY, 0644);
    if (fd < 0) {
        logError("file: "__FILE__", line: %d, "
                "open file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, filename, errno, STRERROR(errno));
        return errno != 0 ? errno : EACCES;
    }

    result = 0;
    remain_bytes = ctx->input.pos->offset;
    while (remain_bytes > 0) {
        if (remain_bytes > ctx->out_writer.buffer.size) {
            current_bytes = ctx->out_writer.buffer.size;
        } else {
            current_bytes = remain_bytes;
        }

        read_bytes = read(fd, ctx->out_writer.buffer.buff, current_bytes);
        if (read_bytes < 0) {
            result = errno != 0 ? errno : EACCES;
            logError("file: "__FILE__", line: %d, "
                    "read from file \"%s\" fail, "
                    "errno: %d, error info: %s", __LINE__,
                    filename, result, STRERROR(result));
            break;
        } else if (read_bytes == 0) {
            result = EIO;
            logError("file: "__FILE__", line: %d, "
                    "read from file \"%s\" fail, "
                    "expect bytes: %"PRId64", but EOF (0 byte read)",
                    __LINE__, filename, remain_bytes);
            break;
        } else if (read_bytes != current_bytes) {
            result = EIO;
            logError("file: "__FILE__", line: %d, "
                    "read from file \"%s\" fail, "
                    "current expect bytes: %d, but read %d bytes",
                    __LINE__, filename, current_bytes, read_bytes);
            break;
        }

        if (write(ctx->out_writer.fd, ctx->out_writer.buffer.
                    buff, read_bytes) != read_bytes)
        {
            result = errno != 0 ? errno : EACCES;
            logError("file: "__FILE__", line: %d, "
                    "write to file \"%s\" fail, "
                    "errno: %d, error info: %s",
                    __LINE__, ctx->out_writer.filename,
                    result, STRERROR(result));
            break;
        }

        remain_bytes -= read_bytes;
    }

    close(fd);
    return result;
}

static int do_write_to_file(BinlogRepairContext *ctx, const int len)
{
    int result;

    if (fc_safe_write(ctx->out_writer.fd, ctx->out_writer.
                buffer.buff, len) != len)
    {
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "write to binlog file \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, ctx->out_writer.filename,
                result, STRERROR(result));
        return result;
    }

    ctx->out_writer.file_size += len;
    ctx->out_writer.buffer.end = ctx->out_writer.buffer.buff;
    return 0;
}

static int binlog_write_to_file(BinlogRepairContext *ctx)
{
    int result;
    int len;

    len = BINLOG_BUFFER_LENGTH(ctx->out_writer.buffer);
    if (ctx->out_writer.file_size + len <= BINLOG_FILE_MAX_SIZE) {
        return do_write_to_file(ctx, len);
    }

    ctx->out_writer.binlog_index++;  //binlog rotate
    if ((result=open_write_file(ctx)) != 0) {
        return result;
    }

    return do_write_to_file(ctx, len);
}

static inline int write_one_line(BinlogRepairContext *ctx, string_t *line)
{
    int result;

    if (ctx->out_writer.file_size + BINLOG_BUFFER_LENGTH(ctx->out_writer.
                buffer) + line->len > BINLOG_FILE_MAX_SIZE)
    {
        if ((result=binlog_write_to_file(ctx)) != 0) {
            return result;
        }
    } else if (ctx->out_writer.buffer.size - BINLOG_BUFFER_LENGTH(
                ctx->out_writer.buffer) < line->len)
    {
        if ((result=binlog_write_to_file(ctx)) != 0) {
            return result;
        }
    }

    memcpy(ctx->out_writer.buffer.end, line->str, line->len);
    ctx->out_writer.buffer.end += line->len;
    return 0;
}

static int binlog_filter_buffer(BinlogRepairContext *ctx,
        ServerBinlogReader *reader, const int length)
{
    int result;
    bool keep;
    string_t line;
    char *buff;
    char *line_start;
    char *buff_end;
    char *line_end;
    BinlogDataGroupVersion dg_version;
    BinlogCommonFields fields;
    char error_info[256];

    result = 0;
    *error_info = '\0';
    buff = reader->binlog_buffer.buff;
    line_start = buff;
    buff_end = buff + length;
    while (line_start < buff_end) {
        line_end = (char *)memchr(line_start, '\n', buff_end - line_start);
        if (line_end == NULL) {
            result = EINVAL;
            sprintf(error_info, "expect line end char (\\n)");
            break;
        }

        line.str = line_start;
        line.len = ++line_end - line_start;
        if ((result=binlog_unpack_common_fields(&line,
                        &fields, error_info)) != 0)
        {
            break;
        }

        fs_calc_block_hashcode(&fields.bkey);
        if (BINLOG_REPAIR_KEEP_RECORD(fields.op_type, fields.data_version)) {
            keep = true;
        } else {
            dg_version.data_version = fields.data_version;
            dg_version.data_group_id = FS_DATA_GROUP_ID(fields.bkey);
            keep = bsearch(&dg_version, ctx->input.varray->versions,
                    ctx->input.varray->count, sizeof(BinlogDataGroupVersion),
                    (int (*)(const void *, const void *))
                    binlog_compare_dg_version) != NULL;
        }

        if (keep) {
            if ((result=write_one_line(ctx, &line)) != 0) {
                sprintf(error_info, "write to file fail");
                break;
            }
        }

        line_start = line_end;
    }

    if (result != 0) {
        int64_t file_offset;
        int64_t line_count;
        int remain_bytes;

        remain_bytes = length - (line_start - buff);
        file_offset = reader->position.offset - remain_bytes;
        fc_get_file_line_count_ex(reader->filename,
                file_offset, &line_count);
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", %s",
                __LINE__, reader->filename, line_count, error_info);
    }
    return result;
}

static int binlog_filter(BinlogRepairContext *ctx)
{
    int result;
    int read_bytes;
    ServerBinlogReader reader;

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
        if ((result=binlog_filter_buffer(ctx, &reader, read_bytes)) != 0) {
            break;
        }
    }

    binlog_reader_destroy(&reader);
    if (result == ENOENT) {
        result = 0;
    }
    
    if (result == 0 && BINLOG_BUFFER_LENGTH(ctx->out_writer.buffer) > 0) {
        result = binlog_write_to_file(ctx);
    }

    return result;
}

static int filter_to_files(BinlogRepairContext *ctx)
{
    int result;

    ctx->out_writer.fd = -1;
    ctx->out_writer.binlog_index = ctx->input.pos->index;
    if ((result=open_write_file(ctx)) != 0) {
        return result;
    }

    if ((result=copy_binlog_front_part(ctx)) != 0) {
        return result;
    }

    if ((result=binlog_filter(ctx)) != 0) {
        return result;
    }

    if (fsync(ctx->out_writer.fd) != 0) {
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "fsync to binlog file \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, ctx->out_writer.filename,
                result, STRERROR(result));
        return result;
    }
    close(ctx->out_writer.fd);
    ctx->out_writer.fd = -1;

    return result;
}

static int binlog_repair_finish(const int data_group_id,
        const int start_binlog_index, const int end_binlog_index)
{
    BinlogWriterInfo *writer;
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];
    char src_filename[PATH_MAX];
    char dest_filename[PATH_MAX];
    int index;
    int rename_count;
    int result;

    if (data_group_id == 0) {
        writer = slice_binlog_get_writer();
        strcpy(subdir_name, FS_SLICE_BINLOG_SUBDIR_NAME);
    } else {
        writer = replica_binlog_get_writer(data_group_id);
        replica_binlog_get_subdir_name(subdir_name, data_group_id);
    }

    rename_count = 0;
    for (index=start_binlog_index; index<=end_binlog_index; index++) {
        binlog_reader_get_filename_ex(subdir_name,
                BINLOG_REPAIR_FILE_EXT_NAME,
                index, src_filename, sizeof(src_filename));
        if (access(src_filename, F_OK) != 0) {
            result = errno != 0 ? errno : EPERM;
            if (result == ENOENT) {
                continue;
            }

            logError("file: "__FILE__", line: %d, "
                    "access file: %s fail, errno: %d, error info: %s",
                    __LINE__, src_filename, result, STRERROR(result));
            return result;
        }

        binlog_reader_get_filename(subdir_name, index,
                dest_filename, sizeof(dest_filename));

        logInfo("file: "__FILE__", line: %d, "
                "rename binlog from %s to %s", __LINE__,
                src_filename, dest_filename);

        if (rename(src_filename, dest_filename) != 0) {
            result = errno != 0 ? errno : EPERM;
            if (result == ENOENT) {
                continue;
            }
            logError("file: "__FILE__", line: %d, "
                    "rename file %s to %s fail, "
                    "errno: %d, error info: %s",
                    __LINE__, src_filename, dest_filename,
                    result, STRERROR(result));
            return result;
        }

        rename_count++;
    }

    logInfo("file: "__FILE__", line: %d, func: %s, "
            "===== data_group_id: %d, rename_count: %d, end_binlog_index: %d",
            __LINE__, __FUNCTION__, data_group_id,
            rename_count, end_binlog_index);

    if ((rename_count > 0) || (end_binlog_index !=
                binlog_get_current_write_index(writer)))
    {
        if ((result=binlog_writer_set_binlog_index(writer,
                        end_binlog_index)) != 0)
        {
            return result;
        }
        if (data_group_id > 0) {
            if ((result=replica_binlog_set_my_data_version(
                            data_group_id)) != 0)
            {
                return result;
            }
        }
        if (result == 0) {
            usleep(100 * 1000);
        }
    }

    return 0;
}

static int do_repair(BinlogRepairContext *ctx)
{
    int result;

    if ((result=filter_to_files(ctx)) != 0) {
        return result;
    }

    if ((result=binlog_repair_save_sys_data(ctx)) != 0) {
        return result;
    }

    if ((result=binlog_repair_finish(ctx->input.data_group_id,
                    ctx->input.pos->index,
                    ctx->out_writer.binlog_index)) != 0)
    {
        return result;
    }

    return binlog_repair_unlink_sys_data();
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
        BinlogDataGroupVersionArray *varray)
{
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

    if ((result=repair_context_init(&repair_ctx,
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

        repair_ctx.input.data_group_id = data_group_id;
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

    if ((result=repair_context_init(&repair_ctx,
                    &ctx->version_arrays.replica)) != 0)
    {
        return result;
    }

    repair_ctx.input.data_group_id = 0;
    repair_ctx.input.subdir_name = FS_SLICE_BINLOG_SUBDIR_NAME;
    repair_ctx.input.writer = slice_binlog_get_writer();
    repair_ctx.input.pos = &ctx->positions.slice;
    result = binlog_repair(&repair_ctx);

    repair_context_destroy(&repair_ctx);
    return result;
}

int binlog_consistency_repair_finish()
{
    int result;
    int data_group_id;
    int start_binlog_index;
    int end_binlog_index;

    result = binlog_repair_load_sys_data(&data_group_id,
            &start_binlog_index, &end_binlog_index);
    if (result != 0) {
        if (result == ENOENT) {
            return 0;
        }
        return result;
    }

    if ((result=binlog_repair_finish(data_group_id,
                    start_binlog_index, end_binlog_index)) != 0)
    {
        return result;
    }

    return binlog_repair_unlink_sys_data();
}
