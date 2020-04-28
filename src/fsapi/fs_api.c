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
