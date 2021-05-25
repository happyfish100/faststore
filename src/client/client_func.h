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

#include "fastcfs/auth/fcfs_auth_client.h"
#include "fs_global.h"
#include "client_global.h"
#include "client_types.h"

#ifdef __cplusplus
extern "C" {
#endif

#define fs_client_load_from_file(filename) \
    fs_client_load_from_file_ex(&g_fs_client_vars.client_ctx, \
            &g_fcfs_auth_client_vars.client_ctx, filename, NULL)

#define fs_client_init(filename) \
    fs_client_init_ex(&g_fs_client_vars.client_ctx, \
            &g_fcfs_auth_client_vars.client_ctx,    \
            filename, NULL, NULL, true)

#define fs_client_init_with_auth_ex(filename, poolname, publish) \
    fs_client_init_with_auth_ex1(&g_fs_client_vars.client_ctx, \
            &g_fcfs_auth_client_vars.client_ctx, filename, \
            NULL, NULL, true, poolname, publish)

#define fs_client_destroy() \
    fs_client_destroy_ex((&g_fs_client_vars.client_ctx))

#define fs_client_log_config(client_ctx) \
    fs_client_log_config_ex(client_ctx, NULL, true)


int fs_client_load_from_file_ex1(FSClientContext *client_ctx,
        FCFSAuthClientContext *auth_ctx, IniFullContext *ini_ctx);

/**
* client initial from config file
* params:
*       client_ctx: the client context
*       config_filename: the client config filename
*       section_name: the section name, NULL or empty for global section
* return: 0 success, != 0 fail, return the error code
**/
static inline int fs_client_load_from_file_ex(FSClientContext *client_ctx,
        FCFSAuthClientContext *auth_ctx, const char *config_filename,
        const char *section_name)
{
    IniFullContext ini_ctx;

    FAST_INI_SET_FULL_CTX(ini_ctx, config_filename, section_name);
    return fs_client_load_from_file_ex1(client_ctx, auth_ctx, &ini_ctx);
}

int fs_client_init_ex1(FSClientContext *client_ctx,
        FCFSAuthClientContext *auth_ctx, IniFullContext *ini_ctx,
        const SFConnectionManager *cm, const bool bg_thread_enabled);

static inline int fs_client_init_ex(FSClientContext *client_ctx,
        FCFSAuthClientContext *auth_ctx, const char *config_filename,
        const char *section_name, const SFConnectionManager *cm,
        const bool bg_thread_enabled)
{
    IniFullContext ini_ctx;

    FAST_INI_SET_FULL_CTX(ini_ctx, config_filename, section_name);
    return fs_client_init_ex1(client_ctx, auth_ctx,
            &ini_ctx, cm, bg_thread_enabled);
}

static inline int fs_client_init_with_auth_ex1(FSClientContext *client_ctx,
        FCFSAuthClientContext *auth_ctx, const char *config_filename,
        const char *section_name, const SFConnectionManager *cm,
        const bool bg_thread_enabled, const string_t *poolname,
        const bool publish)
{
    int result;
    IniFullContext ini_ctx;

    FAST_INI_SET_FULL_CTX(ini_ctx, config_filename, section_name);
    if ((result=fs_client_init_ex1(client_ctx, auth_ctx,
                    &ini_ctx, cm, bg_thread_enabled)) != 0)
    {
        return result;
    }

    return fcfs_auth_client_session_create_ex(
            &client_ctx->auth, poolname, publish);
}


static inline int fs_client_init_with_auth(const char *config_filename,
        const bool publish)
{
    const string_t poolname = {NULL, 0};
    return fs_client_init_with_auth_ex(config_filename, &poolname, publish);
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
        const char *extra_config, const bool log_base_path);

#ifdef __cplusplus
}
#endif

#endif
