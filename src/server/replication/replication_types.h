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

//replication_types.h

#ifndef _REPLICATION_TYPES_H_
#define _REPLICATION_TYPES_H_

#include <time.h>
#include <limits.h>
#include <pthread.h>
#include "../server_types.h"

typedef struct replication_rpc_entry {
    struct fast_task_info *task;
    volatile short reffer_count;
    short body_offset;
    int body_length;
    struct replication_rpc_entry *nexts[0];  //for slave replications
} ReplicationRPCEntry;

typedef struct replication_rpc_result {
    FSReplication *replication;
    short err_no;
    int data_group_id;
    uint64_t data_version;
    struct replication_rpc_result *next;
} ReplicationRPCResult;

#endif
