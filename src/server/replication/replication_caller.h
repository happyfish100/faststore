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

//replication_caller.h

#ifndef _REPLICATION_CALLER_H_
#define _REPLICATION_CALLER_H_

#include "replication_types.h"
#include "../data_thread.h"

#ifdef __cplusplus
extern "C" {
#endif

int replication_caller_init();
void replication_caller_destroy();

void replication_caller_release_rpc_entry(ReplicationRPCEntry *rpc);

int replication_caller_push_to_slave_queues(FSDataOperation *op);

#ifdef __cplusplus
}
#endif

#endif
