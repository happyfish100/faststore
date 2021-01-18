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

#ifndef _FS_TYPES_H
#define _FS_TYPES_H

#include "fastcommon/common_define.h"
#include "sf/sf_types.h"

#define FS_REPLICA_KEY_SIZE    8

#define FS_DEFAULT_BINLOG_BUFFER_SIZE (64 * 1024)

#define FS_SERVER_DEFAULT_CLUSTER_PORT  21013
#define FS_SERVER_DEFAULT_SERVICE_PORT  21014
#define FS_SERVER_DEFAULT_REPLICA_PORT  21015

#define FS_FILE_BLOCK_SIZE    (4 * 1024 * 1024)
#define FS_MAX_DATA_GROUPS_PER_SERVER   1024
#define FS_MAX_GROUP_SERVERS             128

//random seed to generate hash code for master election
#define FS_DATA_GROUP_MASTER_HC_SEED0   2020
#define FS_DATA_GROUP_MASTER_HC_SEED1   6024
#define FS_DATA_GROUP_MASTER_HC_SEED2   9035

#define FS_DS_STATUS_INIT       0
#define FS_DS_STATUS_REBUILDING 1
#define FS_DS_STATUS_OFFLINE    2
#define FS_DS_STATUS_RECOVERING 3
#define FS_DS_STATUS_ONLINE     4
#define FS_DS_STATUS_ACTIVE     5

#define FS_CLUSTER_STAT_FILTER_BY_GROUP           1
#define FS_CLUSTER_STAT_FILTER_BY_STATUS          2
#define FS_CLUSTER_STAT_FILTER_BY_IS_MASTER       4

#define FS_CLIENT_JOIN_FLAGS_IDEMPOTENCY_REQUEST    1

#define FS_FILE_BLOCK_ALIGN(offset) \
    (offset & (~(FS_FILE_BLOCK_SIZE - 1)))

#define FS_BLOCK_KEY_EQUAL(bkey1, bkey2) \
    ((bkey1).oid == (bkey2).oid && (bkey1).offset == (bkey2).offset)

#define FS_BLOCK_HASH_CODE_INDEX_DATA_GROUP  0
#define FS_BLOCK_HASH_CODE_INDEX_SERVER      1

#define FS_BLOCK_HASH_CODE(blk) (blk).hash_code

typedef struct fs_block_key {
    int64_t oid;    //object id
    int64_t offset; //aligned by block size
    uint32_t hash_code;
} FSBlockKey;

typedef struct fs_slice_size {
    int offset;  //offset within the block
    int length;  //slice length
} FSSliceSize;

typedef struct fs_block_slice_key_info {
    FSBlockKey block;
    FSSliceSize slice;
} FSBlockSliceKeyInfo;

typedef struct {
    int64_t total;
    int64_t success;
    int64_t ignore;
} FSCounterTripple;

typedef struct {
    char filter_by;
    char op_type;
    char status;
    char is_master;
    int data_group_id;
} FSClusterStatFilter;

typedef SFSpaceStat FSClusterSpaceStat;

#endif
