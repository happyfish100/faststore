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

//binlog_replay.h

#ifndef _BINLOG_REPLAY_H_
#define _BINLOG_REPLAY_H_

#include "recovery_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int binlog_replay_init();
void binlog_replay_destroy();

DataReplayTaskAllocatorInfo *binlog_replay_get_task_allocator();

void binlog_replay_release_task_allocator(DataReplayTaskAllocatorInfo *ai);

int data_recovery_replay_binlog(DataRecoveryContext *ctx);

int data_recovery_unlink_replay_binlog(DataRecoveryContext *ctx);

#ifdef __cplusplus
}
#endif

#endif
