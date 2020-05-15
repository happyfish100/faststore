#include <sys/stat.h>
#include <limits.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fs_api.h"

FSAPIContext g_fs_api_ctx;

int fs_api_init_ex1(FSAPIContext *ctx, FDIRClientContext *fdir,
        FSClientContext *fs, const char *ns, const char *fdir_config_file,
        const char *fs_config_file, const FDIRConnectionManager *
        fdir_conn_manager, const FSConnectionManager *fs_conn_manager)
{
    int result;

    if ((result=fdir_client_init_ex(fdir, fdir_config_file,
                    fdir_conn_manager)) != 0)
    {
        return result;
    }

    if ((result=fs_client_init_ex(fs, fs_config_file,
                    fs_conn_manager)) != 0)
    {
        return result;
    }

    fs_api_set_contexts_ex1(ctx, fdir, fs, ns);
    return 0;
}

int fs_api_init_ex2(FSAPIContext *ctx, FDIRClientContext *fdir,
        FSClientContext *fs, const char *ns, const char *fdir_config_file,
        const char *fs_config_file, const FDIRClientConnManagerType
        conn_manager_type, const FSConnectionManager *fs_conn_manager)
{
    int result;
    const int max_count_per_entry = 0;
    const int max_idle_time = 3600;

    if (conn_manager_type == conn_manager_type_simple) {
        result = fdir_client_simple_init_ex(fdir, fdir_config_file);
    } else if (conn_manager_type == conn_manager_type_pooled) {
        result = fdir_client_pooled_init_ex(fdir, fdir_config_file,
                max_count_per_entry, max_idle_time);
    } else {
        result =  EINVAL;
    }
    if (result != 0) {
        return result;
    }

    if ((result=fs_client_init_ex(fs, fs_config_file,
                    fs_conn_manager)) != 0)
    {
        return result;
    }

    fs_api_set_contexts_ex1(ctx, fdir, fs, ns);
    return 0;
}

void fs_api_destroy_ex(FSAPIContext *ctx)
{
    if (ctx->contexts.fdir != NULL) {
        fdir_client_destroy_ex(ctx->contexts.fdir);
        ctx->contexts.fdir = NULL;
    }

    if (ctx->contexts.fs != NULL) {
        fs_client_destroy_ex(ctx->contexts.fs);
        ctx->contexts.fs = NULL;
    }
}
