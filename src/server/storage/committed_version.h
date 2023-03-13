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


#ifndef _COMMITTED_VERSION_H
#define _COMMITTED_VERSION_H

#include "fastcommon/pthread_func.h"
#include "../server_types.h"

typedef struct {
    volatile int data_group_id;
    volatile int64_t data_version;
    volatile int64_t sn;
} FSVersionEntry;

typedef struct {
    volatile int count;
    volatile int waitings;
    FSVersionEntry *versions;
    volatile int64_t next_sn;
    pthread_lock_cond_pair_t lcp;
} FSCommittedVersionRing;

#ifdef __cplusplus
extern "C" {
#endif

    int committed_version_init(const int64_t sn);

    int committed_version_add(const int data_group_id,
            const int64_t data_version, const int64_t sn);

#ifdef __cplusplus
}
#endif

#endif
