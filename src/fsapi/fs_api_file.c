#include <sys/stat.h>
#include <limits.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/sched_thread.h"
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

                    if ((result=fdir_client_stat_dentry_by_path(
                                    fi->ctx->contexts.fdir, fullname,
                                    &fi->dentry)) != 0)
                    {
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

    fi->write_notify.last_modified_time = fi->dentry.stat.mtime;
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
    result = fdir_client_stat_dentry_by_path(ctx->contexts.fdir,
            &fullname, &fi->dentry);
    if ((result=deal_open_flags(fi, &fullname, new_mode, result)) != 0) {
        return result;
    }

    fi->magic = FS_API_MAGIC_NUMBER;
    return 0;
}

int fsapi_close(FSAPIFileInfo *fi)
{
    if (fi->magic != FS_API_MAGIC_NUMBER) {
        return EBADF;
    }

    fi->ctx = NULL;
    fi->magic = 0;
    return 0;
}

static inline void fsapi_set_block_key(FSBlockKey *bkey,
        const int64_t oid, const int64_t offset)
{
    bkey->oid = oid;
    bkey->offset = FS_FILE_BLOCK_ALIGN(offset);
    fs_calc_block_hashcode(bkey);
}

static inline void fsapi_set_slice_size(FSBlockSliceKeyInfo *bs_key,
        const int64_t offset, const int current_size)
{
    bs_key->slice.offset = offset - bs_key->block.offset;
    if (bs_key->slice.offset + current_size <= FS_FILE_BLOCK_SIZE) {
        bs_key->slice.length = current_size;
    } else {
        bs_key->slice.length = FS_FILE_BLOCK_SIZE - bs_key->slice.offset;
    }
}

static inline void fsapi_set_block_slice(FSBlockSliceKeyInfo *bs_key,
        const int64_t oid, const int64_t offset, const int current_size)
{
    fsapi_set_block_key(&bs_key->block, oid, offset);
    fsapi_set_slice_size(bs_key, offset, current_size);
}

static inline void fsapi_next_block_slice_key(FSBlockSliceKeyInfo *bs_key,
        const int current_size)
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

static inline void print_block_slice_key(FSBlockSliceKeyInfo *bs_key)
{
    logInfo("block {oid: %"PRId64", offset: %"PRId64"}, "
            "slice {offset: %d, length: %d}", bs_key->block.oid,
            bs_key->block.offset, bs_key->slice.offset, bs_key->slice.length);
}

static int do_pwrite(FSAPIFileInfo *fi, const char *buff,
        const int size, const int64_t offset, int *written_bytes,
        const bool need_report_modified)
{
    FSBlockSliceKeyInfo bs_key;
    int64_t new_offset;
    int result;
    int current_written;
    int remain;

    *written_bytes = 0;
    new_offset = offset;
    fsapi_set_block_slice(&bs_key, fi->dentry.inode, offset, size);
    while (1) {
        print_block_slice_key(&bs_key);
        if ((result=fs_client_proto_slice_write(fi->ctx->contexts.fs,
                        &bs_key, buff + *written_bytes,
                        &current_written)) != 0)
        {
            if (current_written == 0) {
                break;
            }
        }

        new_offset += current_written;
        *written_bytes += current_written;
        remain = size - *written_bytes;
        if (remain <= 0) {
            break;
        }

        if (current_written == bs_key.slice.length) {  //fully completed
            fsapi_next_block_slice_key(&bs_key, remain);
        } else {  //partially completed, try again the remain part
            fsapi_set_slice_size(&bs_key, new_offset, remain);
        }
    }

    if (*written_bytes > 0) {
        bool report_modified;
        int64_t new_size;

        if (!need_report_modified) {
            return 0;
        }

        new_size = offset + *written_bytes;
        if (new_size > fi->dentry.stat.size) {
            report_modified = true;
        } else {
            int current_time;
            current_time = get_current_time();
            if (current_time > fi->write_notify.last_modified_time) {
                fi->write_notify.last_modified_time = current_time;
                report_modified = true;
            } else {
                report_modified = false;
            }
        }

        if (report_modified) {
            fdir_client_set_dentry_size(fi->ctx->contexts.fdir,
                    &fi->ctx->ns, fi->dentry.inode, new_size,
                    false, &fi->dentry);
        }
        return 0;
    } else {
        return EIO;
    }
}

int fsapi_pwrite(FSAPIFileInfo *fi, const char *buff,
        const int size, const int64_t offset, int *written_bytes)
{
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

    return do_pwrite(fi, buff, size, offset, written_bytes, true);
}

int fsapi_write(FSAPIFileInfo *fi, const char *buff,
        const int size, int *written_bytes)
{
    int result;
    bool need_report_modified;
    int64_t old_size;

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
        if ((result=fdir_client_dentry_sys_lock(fi->ctx->contexts.fdir,
                        fi->dentry.inode, 0, &old_size)) != 0)
        {
            return result;
        }

        fi->offset = old_size;
        need_report_modified = false;
    } else {
        old_size = 0;
        need_report_modified = true;
    }

    if ((result=do_pwrite(fi, buff, size, fi->offset,
                    written_bytes, need_report_modified)) == 0)
    {
        fi->offset += *written_bytes;
    }

    if ((fi->flags & O_APPEND)) {
        string_t *ns;
        if (fi->offset > old_size) {
            ns = &fi->ctx->ns;
        } else {
            ns = NULL;
        }
        logInfo("====pid: %d, old file size: %"PRId64", new file size: %"PRId64", "
                "written_bytes: %d", getpid(), old_size, fi->offset, *written_bytes);
        fdir_client_dentry_sys_unlock_ex(fi->ctx->contexts.fdir,
                ns, fi->dentry.inode, false, old_size, fi->offset);
    }

    return result;
}

int fsapi_pread(FSAPIFileInfo *fi, char *buff, const int size,
        const int64_t offset, int *read_bytes)
{
    FSBlockSliceKeyInfo bs_key;
    int64_t new_offset;
    int result;
    int current_read;
    int remain;

    *read_bytes = 0;
    if (size == 0) {
        return 0;
    } else if (size < 0) {
        return EINVAL;
    }

    if (fi->magic != FS_API_MAGIC_NUMBER || (fi->flags & O_WRONLY)) {
        return EBADF;
    }

    new_offset = offset;
    fsapi_set_block_slice(&bs_key, fi->dentry.inode, offset, size);
    while (1) {
        print_block_slice_key(&bs_key);
        if ((result=fs_client_proto_slice_read(fi->ctx->contexts.fs,
                        &bs_key, buff + *read_bytes,
                        &current_read)) != 0)
        {
            if (current_read == 0) {
                break;
            }
        }

        new_offset += current_read;
        *read_bytes += current_read;
        remain = size - *read_bytes;
        if (remain <= 0 || current_read < bs_key.slice.length) {
            break;
        }

        fsapi_next_block_slice_key(&bs_key, remain);
    }

    return *read_bytes > 0 ? 0 : EIO;
}

int fsapi_read(FSAPIFileInfo *fi, char *buff, const int size, int *read_bytes)
{
    int result;

    if ((result=fsapi_pread(fi, buff, size, fi->offset, read_bytes)) != 0) {
        return result;
    }

    fi->offset += *read_bytes;
    return 0;
}

static int do_truncate(FSAPIContext *ctx, const int64_t oid,
        const int64_t old_size, const int64_t new_size)
{
    FSBlockSliceKeyInfo bs_key;
    int64_t start_size;
    int64_t end_size;
    int64_t remain;
    bool is_delete;
    int result;

    if (new_size == old_size) {
        return 0;
    }
    if (new_size > old_size) {
        start_size = old_size;
        end_size = new_size;
        is_delete = false;
    } else {
        start_size = new_size;
        end_size = old_size;
        is_delete = true;
    }

    remain = end_size - start_size;
    fsapi_set_block_slice(&bs_key, oid, start_size, remain);
    while (1) {
        print_block_slice_key(&bs_key);
        if (is_delete) {
            if (bs_key.slice.length == FS_FILE_BLOCK_SIZE) {
                result = fs_client_proto_block_delete(ctx->contexts.fs,
                        &bs_key.block);
            } else {
                result = fs_client_proto_slice_delete(ctx->contexts.fs,
                        &bs_key);
            }
            if (!(result == 0 || result == ENOENT)) {
                break;
            }
        } else {
            if ((result=fs_client_proto_slice_truncate(ctx->contexts.fs,
                            &bs_key)) != 0)
            {
                break;
            }
        }

        remain -= bs_key.slice.length;
        if (remain <= 0) {
            break;
        }

        fsapi_next_block_slice_key(&bs_key, remain);
    }

    return result;
}

static int file_truncate(FSAPIContext *ctx, const int64_t oid,
        const int64_t new_size)
{
    int64_t old_size;
    int result;

    if (new_size < 0) {
        return EINVAL;
    }

    if ((result=fdir_client_dentry_sys_lock(ctx->contexts.fdir,
                    oid, 0, &old_size)) != 0)
    {
        return result;
    }
    result = do_truncate(ctx, oid, old_size, new_size);
    fdir_client_dentry_sys_unlock_ex(ctx->contexts.fdir, &ctx->ns,
            oid, true, old_size, new_size);

    return result;
}

int fsapi_ftruncate(FSAPIFileInfo *fi, const int64_t new_size)
{
    if (fi->magic != FS_API_MAGIC_NUMBER || !((fi->flags & O_WRONLY) ||
                (fi->flags & O_RDWR)))
    {
        return EBADF;
    }

    return file_truncate(fi->ctx, fi->dentry.inode, new_size);
}

int fsapi_truncate_ex(FSAPIContext *ctx, const char *path,
        const int64_t new_size)
{
    FDIRDEntryFullName fullname;
    FDIRDEntryInfo dentry;
    int result;

    fullname.ns = ctx->ns;
    FC_SET_STRING(fullname.path, (char *)path);
    if ((result=fdir_client_stat_dentry_by_path(ctx->contexts.fdir,
                    &fullname, &dentry)) != 0)
    {
        return result;
    }
    if ((dentry.stat.mode & S_IFREG) == 0) {
        return EISDIR;
    }

    return file_truncate(ctx, dentry.inode, new_size);
}
