#include <sys/stat.h>
#include <limits.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/connection_pool.h"
#include "fs_api_file.h"

#define FS_API_MAGIC_NUMBER    1588076578

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
                    fullname, mode, &fi->dentry)) != 0)
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
                } else {
                    return result;
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

    fi->magic = FS_API_MAGIC_NUMBER;
    return 0;
}

int fsapi_close(FSAPIFileInfo *fi)
{
    fi->ctx = NULL;
    fi->magic = 0;
    return 0;
}

static inline void fsapi_set_block_key(const FSAPIFileInfo *fi, FSBlockKey *bkey)
{
    bkey->oid = fi->dentry.inode;
    bkey->offset = FS_FILE_BLOCK_ALIGN(fi->offset);
    fs_calc_block_hashcode(bkey);
}

static inline void fsapi_set_slice_size(const FSAPIFileInfo *fi,
        FSBlockSliceKeyInfo *bs_key, const int current_size)
{
    bs_key->slice.offset = fi->offset - bs_key->block.offset;
    if (bs_key->slice.offset + current_size <= FS_FILE_BLOCK_SIZE) {
        bs_key->slice.length = current_size;
    } else {
        bs_key->slice.length = FS_FILE_BLOCK_SIZE - bs_key->slice.offset;
    }
}

static inline void fsapi_next_block_slice_key(const FSAPIFileInfo *fi,
        FSBlockSliceKeyInfo *bs_key, const int current_size)
{
    bs_key->block.offset += FS_FILE_BLOCK_SIZE;
    fs_calc_block_hashcode(&bs_key->block);

    bs_key->slice.offset = 0;
    if (current_size <= FS_FILE_BLOCK_SIZE) {
        bs_key->slice.length = current_size;
    } else {
        bs_key->slice.length = FS_FILE_BLOCK_SIZE;
    }
}

int fsapi_write(FSAPIFileInfo *fi, const char *buff,
        const int size, int *written_bytes)
{
    FSBlockSliceKeyInfo bs_key;
    int result;
    int current_written;
    int remain;

    *written_bytes = 0;
    if (size == 0) {
        return 0;
    } else if (size < 0) {
        return EINVAL;
    }

    if (fi->magic != FS_API_MAGIC_NUMBER || !((fi->flags & O_WRONLY) ||
                (fi->flags & O_RDWR)))
    {
        return EBADF;
    }

    if ((fi->flags & O_APPEND)) {
        //TODO: fetch ans set fi->offset;
    }

    fsapi_set_block_key(fi, &bs_key.block);
    fsapi_set_slice_size(fi, &bs_key, size);
    while (1) {
        if ((result=fs_client_proto_slice_write(fi->ctx->contexts.fs,
                        &bs_key, buff + *written_bytes,
                        &current_written)) != 0)
        {
            if (current_written == 0) {
                break;
            }
        }

        if ((fi->flags & O_APPEND)) {
            //TODO: fetch ans set fi->offset;
        }

        fi->offset += current_written;
        *written_bytes += current_written;
        remain = size - *written_bytes;
        if (remain == 0) {
            break;
        }

        if (current_written == bs_key.slice.length) {  //fully completed
            fsapi_next_block_slice_key(fi, &bs_key, remain);
        } else {  //partially completed, try again the remain part
            fsapi_set_slice_size(fi, &bs_key, remain);
        }
    }

    return *written_bytes > 0 ? 0 : EIO;
}
