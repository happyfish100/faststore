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


#ifndef _SLICE_BINLOG_H
#define _SLICE_BINLOG_H

#include "fastcommon/sched_thread.h"
#include "sf/sf_binlog_writer.h"
#include "binlog_types.h"
#include "../storage/object_block_index.h"

#define SLICE_BINLOG_OP_TYPE_WRITE_SLICE  BINLOG_OP_TYPE_WRITE_SLICE
#define SLICE_BINLOG_OP_TYPE_ALLOC_SLICE  BINLOG_OP_TYPE_ALLOC_SLICE
#define SLICE_BINLOG_OP_TYPE_DEL_SLICE    BINLOG_OP_TYPE_DEL_SLICE
#define SLICE_BINLOG_OP_TYPE_DEL_BLOCK    BINLOG_OP_TYPE_DEL_BLOCK

#ifdef __cplusplus
extern "C" {
#endif

    int slice_binlog_init();
    void slice_binlog_destroy();

    int slice_binlog_get_current_write_index();

    struct sf_binlog_writer_info *slice_binlog_get_writer();

    static inline const char *slice_binlog_get_filename(const
            int binlog_index, char *filename, const int size)
    {
        return sf_binlog_writer_get_filename(DATA_PATH_STR,
                FS_SLICE_BINLOG_SUBDIR_NAME, binlog_index,
                filename, size);
    }

    static inline int slice_binlog_log_to_buff(const OBSliceEntry *slice,
            const time_t current_time, const uint64_t data_version,
            const int source, char *buff)
    {
        return sprintf(buff, "%"PRId64" %"PRId64" %c %c %"PRId64" %"PRId64
                " %d %d %d %"PRId64" %"PRId64" %"PRId64" %"PRId64"\n",
                (int64_t)current_time, data_version, source,
                slice->type == OB_SLICE_TYPE_FILE ?
                SLICE_BINLOG_OP_TYPE_WRITE_SLICE :
                SLICE_BINLOG_OP_TYPE_ALLOC_SLICE,
                slice->ob->bkey.oid, slice->ob->bkey.offset,
                slice->ssize.offset, slice->ssize.length,
                slice->space.store->index, slice->space.id_info.id,
                slice->space.id_info.subdir, slice->space.offset,
                slice->space.size);
    }

    int slice_binlog_log_add_slice(const OBSliceEntry *slice,
            const time_t current_time, const uint64_t sn,
            const uint64_t data_version, const int source);

    int slice_binlog_log_del_slice(const FSBlockSliceKeyInfo *bs_key,
            const time_t current_time, const uint64_t sn,
            const uint64_t data_version, const int source);

    int slice_binlog_log_del_block(const FSBlockKey *bkey,
            const time_t current_time, const uint64_t sn,
            const uint64_t data_version, const int source);

    void slice_binlog_writer_stat(FSBinlogWriterStat *stat);

#ifdef __cplusplus
}
#endif

#endif
