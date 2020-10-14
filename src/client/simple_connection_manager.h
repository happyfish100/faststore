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

#ifndef _FS_SIMPLE_CONNECTION_MANAGER_H
#define _FS_SIMPLE_CONNECTION_MANAGER_H

#include "client_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int fs_simple_connection_manager_init_ex(FSClientContext *client_ctx,
        FSConnectionManager *conn_manager, const int max_count_per_entry,
        const int max_idle_time);

static inline int fs_simple_connection_manager_init(
        FSClientContext *client_ctx, FSConnectionManager *conn_manager)
{
    const int max_count_per_entry = 0;
    const int max_idle_time = 1 * 3600;
    return fs_simple_connection_manager_init_ex(client_ctx,
            conn_manager, max_count_per_entry, max_idle_time);
}

void fs_simple_connection_manager_destroy(FSConnectionManager *conn_manager);

#ifdef __cplusplus
}
#endif

#endif
