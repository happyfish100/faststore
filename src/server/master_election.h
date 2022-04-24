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

//master_election.h

#ifndef _MASTER_ELECTION_H_
#define _MASTER_ELECTION_H_

#include <time.h>
#include <errno.h>
#include "fastcommon/logger.h"
#include "server_types.h"
#include "server_group_info.h"

typedef struct fs_master_election_context {
    volatile int is_running;
    volatile int waiting_count;
    struct common_blocked_queue queue;
    struct common_blocked_queue delay_queue;
    pthread_mutex_t lock;
} FSMasterElectionContext;

#ifdef __cplusplus
extern "C" {
#endif

extern FSMasterElectionContext g_master_election_ctx;

int master_election_init();
void master_election_destroy();

static inline int master_election_lock()
{
    int result;
    if ((result=pthread_mutex_lock(&g_master_election_ctx.lock)) != 0) {
        logWarning("file: "__FILE__", line: %d, "
                "call pthread_mutex_lock fail, "
                "errno: %d, error info: %s",
                __LINE__, result, STRERROR(result));
    }
    return result;
}

static inline int master_election_unlock()
{
    int result;
    if ((result=pthread_mutex_unlock(&g_master_election_ctx.lock)) != 0) {
        logWarning("file: "__FILE__", line: %d, "
                "call pthread_mutex_unlock fail, "
                "errno: %d, error info: %s",
                __LINE__, result, STRERROR(result));
    }
    return result;
}

int master_election_queue_push(FSClusterDataGroupInfo *group);

bool master_election_set_master(FSClusterDataGroupInfo *group,
        FSClusterDataServerInfo *old_master,
        FSClusterDataServerInfo *new_master);

void master_election_unset_all_masters();

void master_election_deal_delay_queue();

#ifdef __cplusplus
}
#endif

#endif
