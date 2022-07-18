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


#ifndef _REPLICATION_QUORUM_H
#define _REPLICATION_QUORUM_H

#include "fastcommon/pthread_func.h"
#include "../server_types.h"

#ifdef __cplusplus
extern "C" {
#endif

    int replication_quorum_init();
    int replication_quorum_start();
    void replication_quorum_destroy();

    int replication_quorum_add(FSReplicationQuorumContext *ctx,
            struct fast_task_info *task, const int64_t data_version,
            bool *finished);

    void replication_quorum_push_confirmed_version(
            FSReplicationQuorumContext *ctx,
            const int64_t data_version);

    void replication_quorum_deal_version_change(
            FSReplicationQuorumContext *ctx,
            const int64_t slave_confirmed_version);

    int replication_quorum_start_master_term(FSReplicationQuorumContext *ctx);

    int replication_quorum_end_master_term(FSReplicationQuorumContext *ctx);

#ifdef __cplusplus
}
#endif

#endif
