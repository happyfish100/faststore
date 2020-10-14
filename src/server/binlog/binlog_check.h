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

//binlog_check.h

#ifndef _BINLOG_CHECK_H_
#define _BINLOG_CHECK_H_

#include "binlog_types.h"

#define BINLOG_CHECK_RESULT_REPLICA_DIRTY  1
#define BINLOG_CHECK_RESULT_SLICE_DIRTY    2
#define BINLOG_CHECK_RESULT_ALL_DIRTY    \
    (BINLOG_CHECK_RESULT_REPLICA_DIRTY | BINLOG_CHECK_RESULT_SLICE_DIRTY)

typedef struct {
    int data_group_id;
    uint64_t data_version;
} BinlogDataGroupVersion;

typedef struct {
    int64_t alloc;
    int64_t count;
    BinlogDataGroupVersion *versions;
} BinlogDataGroupVersionArray;

typedef struct {
    struct {
        BinlogDataGroupVersionArray replica;
        BinlogDataGroupVersionArray slice;
    } version_arrays;

    struct {
        int base_dg_id;
        int dg_count;
        SFBinlogFilePosition *replicas;
        SFBinlogFilePosition slice;
    } positions;
} BinlogConsistencyContext;

#ifdef __cplusplus
extern "C" {
#endif

int binlog_compare_dg_version(const BinlogDataGroupVersion *p1,
        const BinlogDataGroupVersion *p2);

int binlog_consistency_init(BinlogConsistencyContext *ctx);

int binlog_consistency_check(BinlogConsistencyContext *ctx, int *flags);

void binlog_consistency_destroy(BinlogConsistencyContext *ctx);

#ifdef __cplusplus
}
#endif

#endif
