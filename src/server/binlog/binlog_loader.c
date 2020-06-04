#include <limits.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "binlog_loader.h"

typedef struct {
    BinlogReadThreadResult *r;
    binlog_parse_line_func parse_line;
    int count;
} BinlogParseContext;

static int parse_binlog(BinlogParseContext *ctx)
{
    int result;
    string_t line;
    char *line_start;
    char *buff_end;
    char *line_end;

    ctx->count = 0;
    line_start = ctx->r->buffer.buff;
    buff_end = ctx->r->buffer.buff + ctx->r->buffer.length;
    while (line_start < buff_end) {
        line_end = (char *)memchr(line_start, '\n', buff_end - line_start);
        if (line_end == NULL) {
            break;
        }

        line.str = line_start;
        line.len = line_end - line_start;
        if ((result=ctx->parse_line(ctx->r, &line)) != 0) {
            break;
        }

        ctx->count++;
        line_start = line_end + 1;
    }

    return result;
}

int binlog_loader_load(const char *subdir_name,
        get_current_write_index_func get_current_write_index,
        binlog_parse_line_func parse_line)
{
    BinlogReadThreadContext read_thread_ctx;
    BinlogParseContext parse_ctx;
    int64_t total_count;
    int64_t start_time;
    int64_t end_time;
    char time_buff[32];
    int result;

    start_time = get_current_time_ms();

    if ((result=binlog_read_thread_init(&read_thread_ctx,
                    subdir_name, get_current_write_index,
                    NULL, BINLOG_BUFFER_SIZE)) != 0)
    {
        return result;
    }

    logInfo("file: "__FILE__", line: %d, "
            "loading %s data ...", __LINE__, subdir_name);

    parse_ctx.parse_line = parse_line;
    total_count = 0;
    result = 0;
    while (SF_G_CONTINUE_FLAG) {
        if ((parse_ctx.r=binlog_read_thread_fetch_result(
                        &read_thread_ctx)) == NULL)
        {
            result = EINTR;
            break;
        }

        logInfo("errno: %d, buffer length: %d", parse_ctx.r->err_no,
                parse_ctx.r->buffer.length);
        if (parse_ctx.r->err_no == ENOENT) {
            break;
        } else if (parse_ctx.r->err_no != 0) {
            result = parse_ctx.r->err_no;
            break;
        }

        if ((result=parse_binlog(&parse_ctx)) != 0) {
            break;
        }

        total_count += parse_ctx.count;
        binlog_read_thread_return_result_buffer(&read_thread_ctx, parse_ctx.r);
    }

    binlog_read_thread_terminate(&read_thread_ctx);
    if (result == 0) {
        end_time = get_current_time_ms();
        logInfo("file: "__FILE__", line: %d, "
                "load %s data done. record count: %"PRId64", "
                "time used: %s ms", __LINE__, subdir_name, total_count,
                long_to_comma_str(end_time - start_time, time_buff));
    }

    return result;
}
