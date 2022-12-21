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

typedef struct fs_db_update_field_info {
    int64_t version;   //field version, NOT data version!
    uint64_t inode;
    int64_t inc_alloc; //for dump namespaces
    int namespace_id;  //for dump namespaces
    mode_t mode;       //for dump namespaces
    DABinlogOpType op_type;
    int merge_count;
    int field_index;
    FastBuffer *buffer;
    void *args;   //dentry
    struct fs_db_update_field_info *next;  //for queue
} FSDBUpdateFieldInfo;

typedef struct fs_db_update_field_array {
    FSDBUpdateFieldInfo *entries;
    int count;
    int alloc;
} FSDBUpdateFieldArray;

typedef struct fs_storage_engine_config {
    struct {
        short subdirs;
        short subdir_bits;
        int htable_capacity;
        int shared_lock_count;
    } block_segment;
    int64_t memory_limit;   //limit for block segment
    string_t path;   //data path
} FSStorageEngineConfig;

#endif
