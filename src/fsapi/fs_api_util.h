
#ifndef _FS_API_UTIL_H
#define _FS_API_UTIL_H

#include "fs_api_types.h"

#ifdef __cplusplus
extern "C" {
#endif
#define FSAPI_SET_PATH_FULLNAME(fullname, ctx, path_str) \
    fullname.ns = ctx->ns;    \
    FC_SET_STRING(fullname.path, (char *)path_str)

#define fsapi_lookup_inode(path, inode)  \
    fsapi_lookup_inode_ex(&g_fs_api_ctx, path, inode)

#define fsapi_stat_dentry_by_path(path, dentry)  \
    fsapi_stat_dentry_by_path_ex(&g_fs_api_ctx, path, dentry)

#define fsapi_stat_dentry_by_inode(inode, dentry)  \
    fsapi_stat_dentry_by_inode_ex(&g_fs_api_ctx, inode, dentry)

#define fsapi_stat_dentry_by_pname(parent_inode, name, dentry)  \
    fsapi_stat_dentry_by_pname_ex(&g_fs_api_ctx, parent_inode, name, dentry)

#define fsapi_create_dentry_by_pname(parent_inode, name, mode, dentry)  \
    fsapi_create_dentry_by_pname_ex(&g_fs_api_ctx, \
            parent_inode, name, mode, dentry)

#define fsapi_modify_dentry_stat(inode, attr, flags, dentry)  \
    fsapi_modify_dentry_stat_ex(&g_fs_api_ctx, inode, attr, flags, dentry)

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
        const int64_t parent_inode, const string_t *name,
        FDIRDEntryInfo *dentry)
{
    return fdir_client_stat_dentry_by_pname(ctx->contexts.fdir,
            parent_inode, name, dentry);
}

static inline int fsapi_create_dentry_by_pname_ex(FSAPIContext *ctx,
        const int64_t parent_inode, const string_t *name,
        const mode_t mode, FDIRDEntryInfo *dentry)
{
    return fdir_client_create_dentry_by_pname(ctx->contexts.fdir,
            &ctx->ns, parent_inode, name, mode, dentry);
}

static inline int fsapi_modify_dentry_stat_ex(FSAPIContext *ctx,
        const int64_t inode, const struct stat *attr, const int64_t flags,
        FDIRDEntryInfo *dentry)
{
    FDIRDEntryStatus stat;
    stat.mode = attr->st_mode;
    stat.gid = attr->st_gid;
    stat.uid = attr->st_uid;
    stat.atime = attr->st_atime;
    stat.ctime = attr->st_ctime;
    stat.mtime = attr->st_mtime;
    stat.size = attr->st_size;
    return fdir_client_modify_dentry_stat(ctx->contexts.fdir,
            &ctx->ns, inode, flags, &stat, dentry);
}

#ifdef __cplusplus
}
#endif

#endif
