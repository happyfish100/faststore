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

//binlog_fetch.h

#ifndef _BINLOG_FETCH_H_
#define _BINLOG_FETCH_H_

#include "recovery_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int data_recovery_fetch_binlog(DataRecoveryContext *ctx, int64_t *binlog_size);

int data_recovery_unlink_fetched_binlog(DataRecoveryContext *ctx);

const char *data_recovery_get_fetched_binlog_filename(
        DataRecoveryContext *ctx, char *full_filename, const int size);

static inline void data_recovery_notify_replication(FSClusterDataServerInfo *ds)
{
    PTHREAD_MUTEX_LOCK(&ds->replica.notify.lock);
    pthread_cond_signal(&ds->replica.notify.cond);
    PTHREAD_MUTEX_UNLOCK(&ds->replica.notify.lock);
}

#ifdef __cplusplus
}
#endif

#endif
