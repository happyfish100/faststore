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

//replication_processor.h

#ifndef _REPLICATION_PROCESSOR_H_
#define _REPLICATION_PROCESSOR_H_

#include "replication_types.h"
#include "rpc_result_ring.h"

#ifdef __cplusplus
extern "C" {
#endif

int replication_alloc_connection_ptr_arrays(FSServerContext *server_context);

//replication server side
void replication_processor_bind_task(FSReplication *replication,
        struct fast_task_info *task);

//replication client side
int replication_processor_bind_thread(FSReplication *replication);

//replication server and client
int replication_processor_unbind(FSReplication *replication);

int replication_processor_process(FSServerContext *server_ctx);

void clean_connected_replications(FSServerContext *server_ctx);

static inline int replication_processors_deal_rpc_response(
        FSReplication *replication, const int data_group_id,
        const uint64_t data_version)
{
    if (__sync_add_and_fetch(&replication->stage, 0) ==
            FS_REPLICATION_STAGE_SYNCING)
    {
        return rpc_result_ring_remove(&replication->context.caller.
                rpc_result_ctx, data_group_id, data_version);
    } else {
        return 0;
    }
}

static inline bool replication_channel_is_ready(FSReplication *replication)
{
    return __sync_add_and_fetch(&replication->stage, 0) ==
        FS_REPLICATION_STAGE_SYNCING;
}

//for master only
static inline FSReplication *replication_channel_get(
        FSClusterDataServerInfo *slave)
{
    return slave->cs->repl_ptr_array.replications[slave->dg->id %
        slave->cs->repl_ptr_array.count];
}

static inline bool replication_channel_is_all_ready(
        FSClusterDataServerInfo *peer)
{
    FSReplication **repl;
    FSReplication **end;

    end = peer->cs->repl_ptr_array.replications +
        peer->cs->repl_ptr_array.count;
    for (repl=peer->cs->repl_ptr_array.replications; repl<end; repl++) {
        if (!replication_channel_is_ready(*repl)) {
            return false;
        }
    }

    return true;
}

static inline void set_replication_stage(FSReplication *
        replication, const int new_stage)
{
    int old_stage;

    while (1) {
        old_stage = __sync_add_and_fetch(&replication->stage, 0);
        if (new_stage == old_stage) {
            break;
        }
        if (__sync_bool_compare_and_swap(&replication->stage,
                    old_stage, new_stage))
        {
            __sync_add_and_fetch(&replication->version, 1);
            break;
        }
    }
}

#ifdef __cplusplus
}
#endif

#endif
