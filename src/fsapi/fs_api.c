#include <sys/stat.h>
#include <limits.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fs_api.h"

FSAPIContext g_fs_api_ctx;

static int opendir_session_alloc_init(void *element, void *args)
{
    int result;
    FSAPIOpendirSession *session;

    session = (FSAPIOpendirSession *)element;
    if ((result=fdir_client_dentry_array_init(&session->array)) != 0) {
        return result;
    }

    if ((result=fast_buffer_init_ex(&session->buffer, 64 * 1024)) != 0) {
        return result;
    }
    return 0;
}

static int fs_api_common_init(FSAPIContext *ctx, FDIRClientContext *fdir,
        FSClientContext *fs, const char *ns, const bool need_lock)
{
    int result;

    if ((result=fast_mblock_init_ex2(&ctx->opendir_session_pool,
                    "opendir_session", sizeof(FSAPIOpendirSession),
                    64, opendir_session_alloc_init, NULL, need_lock,
                    NULL, NULL, NULL)) != 0)
    {
        return result;
    }

    fs_api_set_contexts_ex1(ctx, fdir, fs, ns);
    return 0;
}

int fs_api_init_ex1(FSAPIContext *ctx, FDIRClientContext *fdir,
        FSClientContext *fs, const char *ns, const char *fdir_config_file,
        const char *fs_config_file, const FDIRConnectionManager *
        fdir_conn_manager, const FSConnectionManager *fs_conn_manager,
        const bool need_lock)
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

    return fs_api_common_init(ctx, fdir, fs, ns, need_lock);
}

int fs_api_init_ex2(FSAPIContext *ctx, FDIRClientContext *fdir,
        FSClientContext *fs, const char *ns, const char *fdir_config_file,
        const char *fs_config_file, const FDIRClientConnManagerType
        conn_manager_type, const FSConnectionManager *fs_conn_manager,
        const bool need_lock)
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

    return fs_api_common_init(ctx, fdir, fs, ns, need_lock);
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
