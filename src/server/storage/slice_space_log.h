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


#ifndef _FS_SLICE_SPACE_LOG_H
#define _FS_SLICE_SPACE_LOG_H

#include "sf/sf_binlog_writer.h"
#include "../server_global.h"

#ifdef __cplusplus
extern "C" {
#endif

    int slice_space_log_init();
    void slice_space_log_destroy();

    static inline FSSliceSpaceLogRecord *slice_space_log_alloc_record()
    {
        FSSliceSpaceLogRecord *record;

        record = fast_mblock_alloc_object(&SLICE_SPACE_LOG_CTX.allocator);
        if (record != NULL) {
            record->sctx = NULL;
        }
        return record;
    }

    static inline FSSliceSpaceLogRecord *slice_space_log_alloc_init_record()
    {
        FSSliceSpaceLogRecord *record;

        record = fast_mblock_alloc_object(&SLICE_SPACE_LOG_CTX.allocator);
        if (record != NULL) {
            record->slice_head = NULL;
            record->space_chain.head = NULL;
            record->space_chain.tail = NULL;
        }
        return record;
    }

    static inline void slice_space_log_free_record(
            FSSliceSpaceLogRecord *record)
    {
        fast_mblock_free_object(&SLICE_SPACE_LOG_CTX.allocator, record);
    }

    static inline void slice_space_log_push(FSSliceSpaceLogRecord *record)
    {
        sorted_queue_push_silence(&SLICE_SPACE_LOG_CTX.queue, record);
    }

    void trunk_migrate_done_callback(const DATrunkFileInfo *trunk);

#ifdef __cplusplus
}
#endif

#endif
