
#ifndef _FS_CLIENT_FUNC_H
#define _FS_CLIENT_FUNC_H

#include "fs_global.h"
#include "client_types.h"

#ifdef __cplusplus
extern "C" {
#endif

#define fs_client_load_from_file(filename) \
    fs_client_load_from_file_ex((&g_client_global_vars.client_ctx), filename)

#define fs_client_init(filename) \
    fs_client_init_ex((&g_client_global_vars.client_ctx), filename, NULL)

#define fs_client_destroy() \
    fs_client_destroy_ex((&g_client_global_vars.client_ctx))

/**
* client initial from config file
* params:
*       client_ctx: the client context
*       conf_filename: the client config filename
* return: 0 success, !=0 fail, return the error code
**/
int fs_client_load_from_file_ex(FSClientContext *client_ctx,
        const char *conf_filename);

int fs_client_init_ex(FSClientContext *client_ctx,
        const char *conf_filename, const FSConnectionManager *conn_manager);

/**
* client destroy function
* params:
*       client_ctx: tracker group
* return: none
**/
void fs_client_destroy_ex(FSClientContext *client_ctx);


int fs_alloc_group_servers(FSServerGroup *server_group,
        const int alloc_size);

#ifdef __cplusplus
}
#endif

#endif
