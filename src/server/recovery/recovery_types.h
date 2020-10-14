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

//recovery_types.h

#ifndef _RECOVERY_TYPES_H_
#define _RECOVERY_TYPES_H_

#include <time.h>
#include <limits.h>
#include <pthread.h>
#include "fastcommon/shared_buffer.h"
#include "fastcommon/common_blocked_queue.h"
#include "../server_types.h"
#include "../binlog/binlog_reader.h"

#define RECOVERY_BINLOG_SUBDIR_NAME_FETCH   "fetch"
#define RECOVERY_BINLOG_SUBDIR_NAME_REPLAY  "replay"

typedef struct data_recovery_context {
    int64_t start_time;   //in ms
    FSClusterDataServerInfo *ds;
    int stage;
    int catch_up;
    bool is_online;
    struct {
        uint64_t last_data_version;
        FSBlockKey last_bkey;
    } fetch;
    FSServerContext *server_ctx;
    FSClusterDataServerInfo *master;
    void *arg;
} DataRecoveryContext;

#ifdef __cplusplus
extern "C" {
#endif

#ifdef __cplusplus
}
#endif

#endif
