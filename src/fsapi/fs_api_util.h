
#ifndef _FS_API_UTIL_H
#define _FS_API_UTIL_H

#include "fs_api_types.h"

#ifdef __cplusplus
extern "C" {
#endif
#define FSAPI_SET_PATH_FULLNAME(fullname, ctx, path_str) \
    fullname.ns = ctx->ns;    \
    FC_SET_STRING(fullname.path, path_str)

#define fsapi_lookup_inode(path, inode)  \
    fsapi_lookup_inode_ex(&g_fs_api_ctx, path, inode)

#define fsapi_stat_dentry_by_path(path, dentry)  \
    fsapi_stat_dentry_by_path_ex(&g_fs_api_ctx, path, dentry)

#define fsapi_stat_dentry_by_inode(inode, dentry)  \
    fsapi_stat_dentry_by_inode_ex(&g_fs_api_ctx, inode, dentry)

#define fsapi_stat_dentry_by_pname(parent_inode, name, dentry)  \
    fsapi_stat_dentry_by_pname_ex(&g_fs_api_ctx, parent_inode, name, dentry)

static inline int fsapi_lookup_inode_ex(FSAPIContext *ctx,
        const char *path, int64_t *inode)
{
    FDIRDEntryFullName fullname;
    FSAPI_SET_PATH_FULLNAME(fullname, ctx, path);
    logInfo("ns: %.*s, path: %s", fullname.ns.len, fullname.ns.str, path);
    return fdir_client_lookup_inode(ctx->contexts.fdir, &fullname, inode);
}

static inline int fsapi_stat_dentry_by_path_ex(FSAPIContext *ctx,
        const char *path, FDIRDEntryInfo *dentry)
{
    FDIRDEntryFullName fullname;
    FSAPI_SET_PATH_FULLNAME(fullname, ctx, path);
    return fdir_client_stat_dentry_by_path(ctx->contexts.fdir,
            &fullname, dentry);
}

static inline int fsapi_stat_dentry_by_inode_ex(FSAPIContext *ctx,
        const int64_t inode, FDIRDEntryInfo *dentry)
{
    return fdir_client_stat_dentry_by_inode(ctx->contexts.fdir,
            inode, dentry);
}

static inline int fsapi_stat_dentry_by_pname_ex(FSAPIContext *ctx,
        const int64_t parent_inode, const char *name, FDIRDEntryInfo *dentry)
{
    return fdir_client_stat_dentry_by_pname(ctx->contexts.fdir,
            parent_inode, name, dentry);
}

#ifdef __cplusplus
}
#endif

#endif
