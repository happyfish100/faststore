
#ifndef _FS_API_H
#define _FS_API_H

#include "fs_api_types.h"
#include "fs_api_file.h"

#ifdef __cplusplus
extern "C" {
#endif

#define fs_api_set_contexts(ns)  fs_api_set_contexts_ex(&g_fs_api_ctx, ns)

#define fs_api_init(ns, fdir_config_file, fs_config_file) \
    fs_api_init_ex(&g_fs_api_ctx, ns, fdir_config_file, fs_config_file)

#define fs_api_pooled_init(ns, fdir_config_file, fs_config_file) \
    fs_api_pooled_init_ex(&g_fs_api_ctx, ns, fdir_config_file, fs_config_file)

#define fs_api_destroy()  fs_api_destroy_ex(&g_fs_api_ctx)


    static inline void fs_api_set_contexts_ex1(FSAPIContext *ctx,
            FDIRClientContext *fdir, FSClientContext *fs, const char *ns)
    {
        ctx->contexts.fdir = fdir;
        ctx->contexts.fs = fs;

        ctx->ns.str = ctx->ns_holder;
        ctx->ns.len = snprintf(ctx->ns_holder,
                sizeof(ctx->ns_holder), "%s", ns);
    }

    static inline void fs_api_set_contexts_ex(FSAPIContext *ctx, const char *ns)
    {
        return fs_api_set_contexts_ex1(ctx, &g_fdir_client_vars.client_ctx,
                &g_fs_client_vars.client_ctx, ns);
    }

    int fs_api_init_ex1(FSAPIContext *ctx, FDIRClientContext *fdir,
            FSClientContext *fs, const char *ns, const char *fdir_config_file,
            const char *fs_config_file, const FDIRConnectionManager *
            fdir_conn_manager, const FSConnectionManager *fs_conn_manager,
            const bool need_lock);

    static inline int fs_api_init_ex(FSAPIContext *ctx, const char *ns,
            const char *fdir_config_file, const char *fs_config_file)
    {
        return fs_api_init_ex1(ctx, &g_fdir_client_vars.client_ctx,
                &g_fs_client_vars.client_ctx, ns, fdir_config_file,
                fs_config_file, NULL, NULL, false);
    }

    int fs_api_init_ex2(FSAPIContext *ctx, FDIRClientContext *fdir,
            FSClientContext *fs, const char *ns, const char *fdir_config_file,
            const char *fs_config_file, const FDIRClientConnManagerType
            conn_manager_type, const FSConnectionManager *fs_conn_manager,
            const bool need_lock);

    static inline int fs_api_pooled_init_ex(FSAPIContext *ctx, const char *ns,
            const char *fdir_config_file, const char *fs_config_file)
    {
        return fs_api_init_ex2(ctx, &g_fdir_client_vars.client_ctx,
                &g_fs_client_vars.client_ctx, ns, fdir_config_file,
                fs_config_file, conn_manager_type_pooled, NULL, true);
    }

    void fs_api_destroy_ex(FSAPIContext *ctx);

#ifdef __cplusplus
}
#endif

#endif
