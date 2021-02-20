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


#ifndef _FS_CLIENT_FUNC_H
#define _FS_CLIENT_FUNC_H

#include "fs_global.h"
#include "client_types.h"

#ifdef __cplusplus
extern "C" {
#endif

#define fs_client_load_from_file(filename) \
    fs_client_load_from_file_ex((&g_fs_client_vars.client_ctx), \
            filename, NULL)

#define fs_client_init(filename) \
    fs_client_init_ex((&g_fs_client_vars.client_ctx), filename, NULL, NULL)

#define fs_client_destroy() \
    fs_client_destroy_ex((&g_fs_client_vars.client_ctx))

#define fs_client_log_config(client_ctx) \
    fs_client_log_config_ex(client_ctx, NULL)


int fs_client_load_from_file_ex1(FSClientContext *client_ctx,
        IniFullContext *ini_ctx);

/**
* client initial from config file
* params:
*       client_ctx: the client context
*       config_filename: the client config filename
*       section_name: the section name, NULL or empty for global section
* return: 0 success, != 0 fail, return the error code
**/
static inline int fs_client_load_from_file_ex(FSClientContext *client_ctx,
        const char *config_filename, const char *section_name)
{
    IniFullContext ini_ctx;

    FAST_INI_SET_FULL_CTX(ini_ctx, config_filename, section_name);
    return fs_client_load_from_file_ex1(client_ctx, &ini_ctx);
}

int fs_client_init_ex1(FSClientContext *client_ctx, IniFullContext *ini_ctx,
        const SFConnectionManager *conn_manager);

static inline int fs_client_init_ex(FSClientContext *client_ctx,
        const char *config_filename, const char *section_name,
        const SFConnectionManager *conn_manager)
{
    IniFullContext ini_ctx;

    FAST_INI_SET_FULL_CTX(ini_ctx, config_filename, section_name);
    return fs_client_init_ex1(client_ctx, &ini_ctx, conn_manager);
}

/**
* client destroy function
* params:
*       client_ctx: tracker group
* return: none
**/
void fs_client_destroy_ex(FSClientContext *client_ctx);

int fs_alloc_group_servers(FSServerGroup *server_group,
        const int alloc_size);

void fs_client_log_config_ex(FSClientContext *client_ctx,
        const char *extra_config);

#ifdef __cplusplus
}
#endif

#endif
