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

#ifndef _FS_SERVER_COMMON_TYPES_H
#define _FS_SERVER_COMMON_TYPES_H

#include "fastcommon/fast_buffer.h"
#include "fs_types.h"

typedef struct fs_db_update_block_info {
    int64_t version;   //field version, NOT data version!
    FSBlockKey bkey;
    FastBuffer *buffer;
    struct fs_db_update_block_info *next;  //for queue
} FSDBUpdateBlockInfo;

typedef struct fs_db_update_block_array {
    FSDBUpdateBlockInfo *entries;
    int count;
    int alloc;
} FSDBUpdateBlockArray;

typedef struct fs_storage_engine_config {
    struct {
        short subdirs;
        short subdir_bits;
        int htable_capacity;
        int shared_lock_count;
    } block_segment;

    struct {
        int index_dump_interval;
        TimeInfo index_dump_base_time;
    } trunk;

    int64_t memory_limit;   //limit for block segment
    string_t path;   //data path
} FSStorageEngineConfig;

#endif
