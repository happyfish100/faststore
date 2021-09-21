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
#include "sf/sf_binlog_writer.h"
#include "../../common/fs_func.h"
#include "../server_global.h"
#include "../storage/storage_allocator.h"
#include "../storage/trunk_id_info.h"
#include "slice_loader.h"
#include "slice_binlog.h"

static SFBinlogWriterContext binlog_writer;

static int init_binlog_writer()
{
    int result;

    if ((result=sf_binlog_writer_init_by_version(&binlog_writer.writer,
                    DATA_PATH_STR, FS_SLICE_BINLOG_SUBDIR_NAME,
                    SLICE_BINLOG_SN + 1, BINLOG_BUFFER_SIZE, 10240)) != 0)
    {
        return result;
    }

    return sf_binlog_writer_init_thread(&binlog_writer.thread, "slice",
            &binlog_writer.writer, SF_BINLOG_THREAD_TYPE_ORDER_BY_VERSION,
            FS_SLICE_BINLOG_MAX_RECORD_SIZE);
}

struct sf_binlog_writer_info *slice_binlog_get_writer()
{
    return &binlog_writer.writer;
}

int slice_binlog_get_current_write_index()
{
    return sf_binlog_get_current_write_index(&binlog_writer.writer);
}

int slice_binlog_init()
{
    int result;

    if ((result=init_binlog_writer()) != 0) {
        return result;
    }

    return slice_loader_load(&binlog_writer.writer);
}

void slice_binlog_destroy()
{
    sf_binlog_writer_finish(&binlog_writer.writer);
}

int slice_binlog_log_add_slice(const OBSliceEntry *slice,
        const time_t current_time, const uint64_t sn,
        const uint64_t data_version, const int source)
{
    SFBinlogWriterBuffer *wbuffer;

    if ((wbuffer=sf_binlog_writer_alloc_buffer(&binlog_writer.thread)) == NULL) {
        return ENOMEM;
    }

    wbuffer->tag = data_version;
    SF_BINLOG_BUFFER_SET_VERSION(wbuffer, sn);
    wbuffer->bf.length = sprintf(wbuffer->bf.buff,
            "%"PRId64" %"PRId64" %c %c %"PRId64" %"PRId64" %d %d "
            "%d %"PRId64" %"PRId64" %"PRId64" %"PRId64"\n",
            (int64_t)current_time, data_version, source,
            slice->type == OB_SLICE_TYPE_FILE ?
            SLICE_BINLOG_OP_TYPE_WRITE_SLICE :
            SLICE_BINLOG_OP_TYPE_ALLOC_SLICE,
            slice->ob->bkey.oid, slice->ob->bkey.offset,
            slice->ssize.offset, slice->ssize.length,
            slice->space.store->index, slice->space.id_info.id,
            slice->space.id_info.subdir, slice->space.offset,
            slice->space.size);
    sf_push_to_binlog_write_queue(&binlog_writer.writer, wbuffer);
    return 0;
}

int slice_binlog_log_del_slice(const FSBlockSliceKeyInfo *bs_key,
        const time_t current_time, const uint64_t sn,
        const uint64_t data_version, const int source)
{
    SFBinlogWriterBuffer *wbuffer;

    if ((wbuffer=sf_binlog_writer_alloc_buffer(&binlog_writer.thread)) == NULL) {
        return ENOMEM;
    }

    wbuffer->tag = data_version;
    SF_BINLOG_BUFFER_SET_VERSION(wbuffer, sn);
    wbuffer->bf.length = sprintf(wbuffer->bf.buff,
            "%"PRId64" %"PRId64" %c %c %"PRId64" %"PRId64" %d %d\n",
            (int64_t)current_time, data_version, source,
            SLICE_BINLOG_OP_TYPE_DEL_SLICE, bs_key->block.oid,
            bs_key->block.offset, bs_key->slice.offset,
            bs_key->slice.length);
    sf_push_to_binlog_write_queue(&binlog_writer.writer, wbuffer);
    return 0;
}

int slice_binlog_log_del_block(const FSBlockKey *bkey,
        const time_t current_time, const uint64_t sn,
        const uint64_t data_version, const int source)
{
    SFBinlogWriterBuffer *wbuffer;

    if ((wbuffer=sf_binlog_writer_alloc_buffer(&binlog_writer.thread)) == NULL) {
        return ENOMEM;
    }

    wbuffer->tag = data_version;
    SF_BINLOG_BUFFER_SET_VERSION(wbuffer, sn);
    wbuffer->bf.length = sprintf(wbuffer->bf.buff,
            "%"PRId64" %"PRId64" %c %c %"PRId64" %"PRId64"\n",
            (int64_t)current_time, data_version, source,
            SLICE_BINLOG_OP_TYPE_DEL_BLOCK,
            bkey->oid, bkey->offset);
    sf_push_to_binlog_write_queue(&binlog_writer.writer, wbuffer);
    return 0;
}

void slice_binlog_writer_stat(FSBinlogWriterStat *stat)
{
    stat->total_count = binlog_writer.writer.fw.total_count;
    stat->next_version = binlog_writer.writer.version_ctx.next;
    stat->waiting_count = binlog_writer.writer.version_ctx.ring.waiting_count;
    stat->max_waitings = binlog_writer.writer.version_ctx.ring.max_waitings;
}
