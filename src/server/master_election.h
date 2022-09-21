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

void master_election_queue_batch_push(struct common_blocked_chain *chain,
        const int dg_count);

static inline int master_election_queue_push(FSClusterDataGroupInfo *group)
{
    const int dg_count = 1;
    struct common_blocked_chain chain;

    if (__sync_bool_compare_and_swap(&group->election.in_queue, 0, 1)) {
        chain.head = chain.tail = common_blocked_queue_alloc_node(
                &g_master_election_ctx.queue);
        if (chain.head == NULL) {
            return ENOMEM;
        }

        chain.head->data = group;
        chain.tail->next = NULL;
        master_election_queue_batch_push(&chain, dg_count);
    }

    return 0;
}

static inline int master_election_add_to_chain(
        struct common_blocked_chain *chain, int *dg_count,
        FSClusterDataGroupInfo *group)
{
    struct common_blocked_node *node;

    if (__sync_bool_compare_and_swap(&group->election.in_queue, 0, 1)) {
        node = common_blocked_queue_alloc_node(&g_master_election_ctx.queue);
        if (node == NULL) {
            return ENOMEM;
        }

        node->data = group;
        if (chain->head == NULL) {
            chain->head = node;
        } else {
            chain->tail->next = node;
        }
        chain->tail = node;
        ++(*dg_count);
    }

    return 0;
}

bool master_election_set_master(FSClusterDataGroupInfo *group,
        FSClusterDataServerInfo *old_master,
        FSClusterDataServerInfo *new_master);

void master_election_unset_all_masters();

void master_election_deal_delay_queue();

#ifdef __cplusplus
}
#endif

#endif
