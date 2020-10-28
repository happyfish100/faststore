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

//rpc_result_ring.h

#ifndef _RPC_RESULT_RING_H_
#define _RPC_RESULT_RING_H_

#include "../server_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int rpc_result_ring_check_init(FSReplicaRPCResultContext *ctx,
        const int alloc_size);

void rpc_result_ring_destroy(FSReplicaRPCResultContext *ctx);

int rpc_result_ring_add(FSReplicaRPCResultContext *ctx,
        const int data_group_id, const uint64_t data_version,
        struct fast_task_info *waiting_task);

int rpc_result_ring_remove(FSReplicaRPCResultContext *ctx,
        const int data_group_id, const uint64_t data_version);

void rpc_result_ring_clear_all(FSReplicaRPCResultContext *ctx);

void rpc_result_ring_clear_timeouts(FSReplicaRPCResultContext *ctx);

#ifdef __cplusplus
}
#endif

#endif
