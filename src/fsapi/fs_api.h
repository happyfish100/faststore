
#ifndef _FS_API_H
#define _FS_API_H

#include "fs_api_types.h"

#ifdef __cplusplus
extern "C" {
#endif

    extern FSAPIContext g_fs_api_ctx;

#define fs_api_set_contexts(ns)  fs_api_set_contexts_ex(&g_fs_api_ctx, ns)

#define fs_api_init(ns, fdir_config_file, fs_config_file) \
    fs_api_init_ex(&g_fs_api_ctx, ns, fdir_config_file, fs_config_file)

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
            fdir_conn_manager, const FSConnectionManager *fs_conn_manager);

    static inline int fs_api_init_ex(FSAPIContext *ctx, const char *ns,
            const char *fdir_config_file, const char *fs_config_file)
    {
        return fs_api_init_ex1(ctx, &g_fdir_client_vars.client_ctx,
                &g_fs_client_vars.client_ctx, ns, fdir_config_file,
                fs_config_file, NULL, NULL);
    }

    void fs_api_destroy_ex(FSAPIContext *ctx);

#ifdef __cplusplus
}
#endif

#endif
