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

#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "sf/sf_func.h"
#include "../binlog/binlog_reader.h"
#include "../binlog/slice_binlog.h"
#include "../db/event_dealer.h"
#include "rebuild_binlog.h"
#include "db_remove.h"

typedef struct {
    BufferInfo buffer;
    int64_t binlog_sn;
    int64_t last_sn;
} DBRemoveContext;

static int parse_buffer(DBRemoveContext *ctx)
{
    const bool create_flag = true;
    string_t line;
    char *line_end;
    char *buff_end;
    RebuildBinlogRecord record;
    OBEntry *ob;
    char error_info[256];
    int64_t sn = 0;
    int result;

    buff_end = ctx->buffer.buff + ctx->buffer.length;
    line.str = ctx->buffer.buff;
    while (line.str < buff_end) {
        line_end = memchr(line.str, '\n', buff_end - line.str);
        if (line_end == NULL) {
            break;
        }

        ++line_end;
        line.len = line_end - line.str;
        if ((result=rebuild_binlog_record_unpack(&line,
                        &record, error_info)) != 0)
        {
            logError("file: "__FILE__", line: %d, "
                    "unpack binlog fail, error info: %s",
                    __LINE__, error_info);
            return result;
        }

        if ((ob=ob_index_get_ob_entry_ex(&g_ob_hashtable, &record.
                        bs_key.block, create_flag)) == NULL)
        {
            return ENOMEM;
        }

        sn = ob_index_generate_alone_sn();
        if ((result=change_notify_push_del_slice(sn, ob,
                        &record.bs_key.slice)) != 0)
        {
            return result;
        }

        line.str = line_end;
    }

    change_notify_signal_to_deal();
    while (event_dealer_get_last_data_version() < sn) {
        fc_sleep_ms(1);
    }

    return 0;
}

static int skip_lines(DBRemoveContext *ctx, ServerBinlogReader *reader)
{
    char *line_end;
    char *buff_end;
    char *p;
    int64_t sn;
    int remain;
    int result;

    sn = ctx->binlog_sn;
    buff_end = ctx->buffer.buff + ctx->buffer.length;
    p = ctx->buffer.buff;
    while (p < buff_end) {
        line_end = memchr(p, '\n', buff_end - p);
        if (line_end == NULL) {
            break;
        }

        ++line_end;
        if (++sn == ctx->last_sn) {
            remain = buff_end - line_end;
            if (remain > 0) {
                if (lseek(reader->fd, -1 * remain, SEEK_CUR) < 0) {
                    result = errno != 0 ? errno : EIO;
                    logError("file: "__FILE__", line: %d, "
                            "lseek file %s fail, error info: %s", __LINE__,
                            reader->filename, STRERROR(result));
                    return result;
                }
            }
            return 0;
        }

        p = line_end;
    }

    return EAGAIN;
}

static int find_position(DBRemoveContext *ctx, const char *subdir_name,
        const int write_index, SFBinlogFilePosition *pos)
{
    const bool reset_binlog_sn = false;
    int result;
    int64_t line_count;
    ServerBinlogReader reader;

    pos->index = 0;
    pos->offset = 0;
    ctx->last_sn = event_dealer_get_last_data_version();
    line_count = ctx->last_sn - ctx->binlog_sn;
    if (line_count <= 0) {
        return 0;
    }

    if ((result=binlog_reader_init1(&reader, subdir_name,
                    write_index, pos)) != 0)
    {
        return result;
    }

    while ((result=binlog_reader_integral_read(&reader, ctx->buffer.buff,
                    ctx->buffer.alloc_size, &ctx->buffer.length)) == 0 &&
            SF_G_CONTINUE_FLAG)
    {
        result = skip_lines(ctx, &reader);
        if (result == 0) {
            break;
        } else if (result != EAGAIN) {
            break;
        }
    }

    if (result == 0) {
        result = slice_binlog_set_sn_ex(ctx->last_sn, reset_binlog_sn);
    }
    *pos = reader.position;
    binlog_reader_destroy(&reader);
    return result;
}

int db_remove_slices(const char *subdir_name, const int write_index)
{
    int result;
    ServerBinlogReader reader;
    SFBinlogFilePosition pos;
    DBRemoveContext ctx;

    ctx.binlog_sn = FC_ATOMIC_GET(SLICE_BINLOG_SN);
    if ((result=fc_init_buffer(&ctx.buffer, 4 * 1024 * 1024)) != 0) {
        return result;
    }

    if ((result=find_position(&ctx, subdir_name, write_index, &pos)) != 0) {
        return result;
    }
    if ((result=binlog_reader_init1(&reader, subdir_name,
                    write_index, &pos)) != 0)
    {
        return result;
    }

    while ((result=binlog_reader_integral_read(&reader, ctx.buffer.buff,
                    ctx.buffer.alloc_size, &ctx.buffer.length)) == 0 &&
            SF_G_CONTINUE_FLAG)
    {
        if ((result=parse_buffer(&ctx)) != 0) {
            break;
        }
    }

    if (result == ENOENT) {
        result = 0;
    }
    fc_free_buffer(&ctx.buffer);
    binlog_reader_destroy(&reader);
    if (result != 0) {
        return result;
    }

    if ((result=slice_binlog_set_next_version()) != 0) {
        return result;
    }
    return slice_binlog_padding_one(BINLOG_SOURCE_REBUILD);
}
