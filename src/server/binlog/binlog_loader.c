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
#include "sf/sf_global.h"
#include "../server_global.h"
#include "../storage/object_block_index.h"
#include "../db/event_dealer.h"
#include "binlog_loader.h"

int binlog_loader_parse_buffer(BinlogLoaderContext *ctx)
{
    int result;
    string_t line;
    char *line_start;
    char *buff_end;
    char *line_end;

    result = 0;
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

        ctx->total_count++;
        line_start = line_end + 1;
    }

    binlog_read_thread_return_result_buffer(
            ctx->read_thread_ctx, ctx->r);
    return result;
}

int binlog_loader_load1(const char *subdir_name,
        struct sf_binlog_writer_info *writer,
        const SFBinlogFilePosition *position,
        BinlogLoaderCallbacks *callbacks,
        const int buffer_count)
{
    BinlogReadThreadContext read_thread_ctx;
    BinlogLoaderContext parse_ctx;
    int64_t start_time;
    int64_t end_time;
    int64_t db_last_sn;
    char time_buff[32];
    char other_prompt[128];
    int len;
    int result;

    start_time = get_current_time_ms();
    if ((result=binlog_read_thread_init_ex(&read_thread_ctx,
                    subdir_name, writer, position,
                    BINLOG_BUFFER_SIZE, buffer_count)) != 0)
    {
        return result;
    }

    if (strcmp(subdir_name, FS_SLICE_BINLOG_SUBDIR_NAME_STR) == 0) {
        len = sprintf(other_prompt, ", slice binlog sn: %"PRId64,
                SLICE_BINLOG_SN);
        if (STORAGE_ENABLED) {
            db_last_sn = event_dealer_get_last_data_version();
            sprintf(other_prompt + len, ", db last sn: %"PRId64", "
                    "record count: %"PRId64, db_last_sn,
                    SLICE_BINLOG_SN - db_last_sn);
        }
    } else {
        *other_prompt = '\0';
    }

    logInfo("file: "__FILE__", line: %d, "
            "loading %s data%s ...", __LINE__,
            subdir_name, other_prompt);

    parse_ctx.parse_line = callbacks->parse_line;
    parse_ctx.arg = callbacks->arg;
    parse_ctx.read_thread_ctx = &read_thread_ctx;
    parse_ctx.total_count = 0;
    result = 0;
    while (SF_G_CONTINUE_FLAG) {
        if ((parse_ctx.r=binlog_read_thread_fetch_result(
                        &read_thread_ctx)) == NULL)
        {
            result = EINTR;
            break;
        }

        /*
           logInfo("errno: %d, buffer length: %d", parse_ctx.r->err_no,
           parse_ctx.r->buffer.length);
         */
        if (parse_ctx.r->err_no == ENOENT) {
            break;
        } else if (parse_ctx.r->err_no != 0) {
            result = parse_ctx.r->err_no;
            break;
        }

        if ((result=callbacks->parse_buffer(&parse_ctx)) != 0) {
            break;
        }
    }

    if (callbacks->read_done != NULL) {
        callbacks->read_done(&parse_ctx);
    }

    binlog_read_thread_terminate(&read_thread_ctx);
    if (result == 0) {
        char extra_buff[128];
        int64_t ob_count;
        int64_t slice_count;

        end_time = get_current_time_ms();
        long_to_comma_str(end_time - start_time, time_buff);

        if (strcmp(subdir_name, FS_SLICE_BINLOG_SUBDIR_NAME_STR) == 0) {
            ob_index_get_ob_and_slice_counts(&ob_count, &slice_count);
            sprintf(extra_buff, ", output object count: %"PRId64", "
                    "slice count: %"PRId64, ob_count, slice_count);
        } else {
            *extra_buff = '\0';
        }

        if (SF_G_CONTINUE_FLAG) {
            logInfo("file: "__FILE__", line: %d, "
                    "load %s data done. record count: %"PRId64"%s, "
                    "time used: %s ms", __LINE__, subdir_name,
                    parse_ctx.total_count, extra_buff, time_buff);
        }
    } else {
        logError("file: "__FILE__", line: %d, "
                "result: %d", __LINE__, result);
    }

    return SF_G_CONTINUE_FLAG ? result : EINTR;
}

int binlog_loader_load_ex(const char *subdir_name,
        struct sf_binlog_writer_info *writer,
        BinlogLoaderCallbacks *callbacks,
        const int buffer_count)
{
    struct {
        SFBinlogFilePosition holder;
        SFBinlogFilePosition *ptr;
    } position;

    if (writer != NULL) {
        position.holder.index = sf_binlog_get_start_index(writer);
        position.holder.offset = 0;
        position.ptr = &position.holder;
    } else {
        position.ptr = NULL;
    }

    return binlog_loader_load1(subdir_name, writer,
            position.ptr, callbacks, buffer_count);
}
