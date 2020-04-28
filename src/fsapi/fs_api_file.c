#include <sys/stat.h>
#include <limits.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/connection_pool.h"
#include "fs_api_file.h"

static int deal_open_flags(FSAPIFileInfo *fi, FDIRDEntryFullName *fullname,
        const mode_t mode, int result)
{
    if ((result == 0) && (fi->dentry.stat.mode & S_IFREG) == 0) {
        return EISDIR;
    }

    if (!((fi->flags & O_WRONLY) || (fi->flags & O_RDWR))) {
        fi->offset = 0;
        return result;
    }

    if ((fi->flags & O_CREAT)) {
        if (result == 0) {
            if ((fi->flags & O_EXCL)) {
                return EEXIST;
            }
        } else if (result == ENOENT) {
            if ((result=fdir_client_create_dentry(fi->ctx->contexts.fdir,
                    fullname, mode | S_IFREG, &fi->dentry)) != 0)
            {
                if (result == EEXIST) {
                    if ((fi->flags & O_EXCL)) {
                        return EEXIST;
                    }
                    if ((result=fdir_client_stat_dentry(fi->ctx->contexts.
                                    fdir, fullname, &fi->dentry)) != 0) {
                        return result;
                    }

                    if ((fi->dentry.stat.mode & S_IFREG) == 0) {
                        return EISDIR;
                    }
                }
            }
        } else {
            return result;
        }
    } else if (result != 0) {
        return result;
    }

    if ((fi->flags & O_TRUNC)) {
        //TODO
    }

    if ((fi->flags & O_APPEND)) {
        fi->offset = fi->dentry.stat.size;
    } else {
        fi->offset = 0;
    }
    return 0;
}

int fsapi_open_ex(FSAPIContext *ctx, FSAPIFileInfo *fi,
            const char *path, const int flags, const mode_t mode)
{
    FDIRDEntryFullName fullname;
    int result;
    mode_t new_mode;

    if ((mode & S_IFMT)) {
        new_mode = (mode & (~S_IFMT))  | S_IFREG;
    } else {
        new_mode = mode | S_IFREG;
    }

    fi->ctx = ctx;
    fi->flags = flags;
    fullname.ns = ctx->ns;
    FC_SET_STRING(fullname.path, (char *)path);
    result = fdir_client_stat_dentry(ctx->contexts.fdir,
            &fullname, &fi->dentry);
    if ((result=deal_open_flags(fi, &fullname, new_mode, result)) != 0) {
        return result;
    }

    return 0;
}
