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

//data_recovery.h

#ifndef _DATA_RECOVERY_H_
#define _DATA_RECOVERY_H_

#include "recovery_types.h"
#include "../binlog/binlog_reader.h"

#define DATA_RECOVERY_CATCH_UP_DOING       0
#define DATA_RECOVERY_CATCH_UP_LAST_BATCH  1

#ifdef __cplusplus
extern "C" {
#endif

int data_recovery_init();
void data_recovery_destroy();

int data_recovery_start(FSClusterDataServerInfo *ds);

int data_recovery_unlink_sys_data(DataRecoveryContext *ctx);

static inline void data_recovery_get_subdir_name(DataRecoveryContext *ctx,
        const char *subdir, char *subdir_name)
{
    sprintf(subdir_name, "%s/%d/%s", FS_RECOVERY_BINLOG_SUBDIR_NAME,
            ctx->ds->dg->id, subdir);
}

FSClusterDataServerInfo *data_recovery_get_master(
        DataRecoveryContext *ctx, int *err_no);

#ifdef __cplusplus
}
#endif

#endif
