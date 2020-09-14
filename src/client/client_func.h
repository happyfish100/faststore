
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
        const FSConnectionManager *conn_manager);

static inline int fs_client_init_ex(FSClientContext *client_ctx,
        const char *config_filename, const char *section_name,
        const FSConnectionManager *conn_manager)
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

int fs_client_rpc_idempotency_reporter_start();

#ifdef __cplusplus
}
#endif

#endif
