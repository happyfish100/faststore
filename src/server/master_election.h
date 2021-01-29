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

#ifdef __cplusplus
extern "C" {
#endif

int master_election_init();
void master_election_destroy();

int master_election_queue_push(FSClusterDataGroupInfo *group);

void master_election_unset_all_masters();

void master_election_deal_delay_queue();

#ifdef __cplusplus
}
#endif

#endif
