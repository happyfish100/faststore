#include <sys/stat.h>
#include <limits.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/sched_thread.h"
#include "fs_api_file.h"

#define FS_API_MAGIC_NUMBER    1588076578

static int file_truncate(FSAPIContext *ctx, const int64_t oid,
        const int64_t new_size);

static int deal_open_flags(FSAPIFileInfo *fi, FDIRDEntryFullName *fullname,
        const mode_t mode, int result)
{
    if (result == 0) {
        if (S_ISDIR(fi->dentry.stat.mode)) {
            return EISDIR;
        }
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
                    if (S_ISDIR(fi->dentry.stat.mode)) {
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
        if (fi->dentry.stat.size > 0) {
            if ((result=file_truncate(fi->ctx, fi->dentry.inode, 0)) != 0) {
                return result;
            }

            fi->dentry.stat.size = 0;
        }
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
    fi->sessions.flock.mconn = NULL;
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

int fsapi_open_by_dentry_ex(FSAPIContext *ctx, FSAPIFileInfo *fi,
            const FDIRDEntryInfo *dentry, const int flags)
{
    int result;

    fi->dentry = *dentry;
    fi->ctx = ctx;
    fi->flags = flags;
    fi->sessions.flock.mconn = NULL;
    result = 0;
    if ((result=deal_open_flags(fi, NULL, 0755, result)) != 0) {
        return result;
    }

    fi->magic = FS_API_MAGIC_NUMBER;
    return 0;
}

int fsapi_open_by_inode_ex(FSAPIContext *ctx, FSAPIFileInfo *fi,
            const int64_t inode, const int flags)
{
    int result;

    if ((result=fdir_client_stat_dentry_by_inode(ctx->contexts.fdir,
                    inode, &fi->dentry)) != 0)
    {
        return result;
    }

    fi->ctx = ctx;
    fi->flags = flags;
    fi->sessions.flock.mconn = NULL;
    if ((result=deal_open_flags(fi, NULL, 0755, result)) != 0) {
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

    if (fi->sessions.flock.mconn != NULL) {
        /* force close connection to unlock */
        fdir_client_close_session(&fi->sessions.flock, true);
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

static inline void fsapi_next_block_key(FSBlockKey *bkey)
{
    bkey->offset += FS_FILE_BLOCK_SIZE;
    fs_calc_block_hashcode(bkey);
}

static inline void fsapi_next_block_slice_key(FSBlockSliceKeyInfo *bs_key,
        const int current_size)
{
    fsapi_next_block_key(&bs_key->block);

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

    logInfo("file: "__FILE__", line: %d, "
            "offset: %"PRId64", *written_bytes: %d, need_report_modified: %d",
            __LINE__, offset, *written_bytes, need_report_modified);

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
    int result;
    int current_read;
    int remain;
    int64_t current_offset;
    int64_t hole_bytes;
    int fill_bytes;

    *read_bytes = 0;
    if (size == 0) {
        return 0;
    } else if (size < 0) {
        return EINVAL;
    }

    if (fi->magic != FS_API_MAGIC_NUMBER || (fi->flags & O_WRONLY)) {
        return EBADF;
    }

    fsapi_set_block_slice(&bs_key, fi->dentry.inode, offset, size);
    while (1) {
        print_block_slice_key(&bs_key);
        if ((result=fs_client_proto_slice_read(fi->ctx->contexts.fs,
                        &bs_key, buff + *read_bytes,
                        &current_read)) != 0)
        {
            if (current_read == 0) {
                if (result != ENOENT) {
                    break;
                }
            }
        }

        /*
        logInfo("=====slice.length: %d, current_read: %d==",
                bs_key.slice.length, current_read);
         */
        while ((current_read < bs_key.slice.length) &&
                (result == 0 || result == ENOENT))
        {
            /* deal file hole caused by lseek */
            current_offset = offset + *read_bytes + current_read;
            if (current_offset == fi->dentry.stat.size) {
                break;
            }

            if (current_offset > fi->dentry.stat.size) {
                if ((result=fdir_client_stat_dentry_by_inode(fi->
                                ctx->contexts.fdir, fi->dentry.inode,
                                &fi->dentry)) != 0)
                {
                    break;
                }
            }

            hole_bytes = fi->dentry.stat.size - current_offset;
            if (hole_bytes > 0) {
                if (current_read + hole_bytes > (int64_t)bs_key.slice.length) {
                    fill_bytes = bs_key.slice.length - current_read;
                } else {
                    fill_bytes = hole_bytes;
                }

                memset(buff + *read_bytes + current_read, 0, fill_bytes);
                current_read += fill_bytes;

                logInfo("=====hole_bytes: %"PRId64", fill_bytes: %d==", hole_bytes, fill_bytes);
            }

            break;
        }

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
            if (result == ENOENT) {
                result = 0;
            } else if (result != 0) {
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
    string_t *ns;
    int result;
    int unlock_res;

    if (new_size < 0) {
        return EINVAL;
    }

    if ((result=fdir_client_dentry_sys_lock(ctx->contexts.fdir,
                    oid, 0, &old_size)) != 0)
    {
        return result;
    }
    if ((result=do_truncate(ctx, oid, old_size, new_size)) == 0) {
        ns = &ctx->ns;
    } else {
        ns = NULL;
    }
    unlock_res = fdir_client_dentry_sys_unlock_ex(ctx->contexts.fdir,
            ns, oid, true, old_size, new_size);

    return result == 0 ? unlock_res : result;
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

static int get_regular_file_inode(FSAPIContext *ctx, const char *path,
        int64_t *inode)
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
    if (S_ISDIR(dentry.stat.mode)) {
        return EISDIR;
    }

    *inode = dentry.inode;
    return 0;
}

int fsapi_truncate_ex(FSAPIContext *ctx, const char *path,
        const int64_t new_size)
{
    int result;
    int64_t inode;

    if ((result=get_regular_file_inode(ctx, path, &inode)) != 0) {
        return result;
    }
    return file_truncate(ctx, inode, new_size);
}

static int do_unlink(FSAPIContext *ctx, const int64_t oid,
        const int64_t file_size)
{
    FSBlockKey bkey;
    int64_t remain;
    int result;

    remain = file_size;
    fsapi_set_block_key(&bkey, oid, 0);
    while (1) {
        logInfo("block {oid: %"PRId64", offset: %"PRId64"}",
                bkey.oid, bkey.offset);

        result = fs_client_proto_block_delete(ctx->contexts.fs, &bkey);
        if (result == ENOENT) {
            result = 0;
        } else if (result != 0) {
            break;
        }

        remain -= FS_FILE_BLOCK_SIZE;
        if (remain <= 0) {
            break;
        }

        fsapi_next_block_key(&bkey);
    }

    return result;
}

int fsapi_unlink_ex(FSAPIContext *ctx, const char *path)
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

    if (S_ISDIR(dentry.stat.mode)) {
        return EISDIR;
    }

    if ((result=do_unlink(ctx, dentry.inode, dentry.stat.size)) == 0) {
        result = fdir_client_remove_dentry(ctx->contexts.fdir, &fullname);
    }

    return result;
}

#define calc_file_offset(fi, offset, whence, new_offset)  \
    calc_file_offset_ex(fi, offset, whence, false, new_offset)

static inline int calc_file_offset_ex(FSAPIFileInfo *fi,
        const int64_t offset, const int whence,
        const bool refresh_fsize, int64_t *new_offset)
{
    int result;

    switch (whence) {
        case SEEK_SET:
            if (offset < 0) {
                return EINVAL;
            }
            *new_offset = offset;
            break;
        case SEEK_CUR:
            *new_offset = fi->offset + offset;
            if (*new_offset < 0) {
                return EINVAL;
            }
            break;
        case SEEK_END:
            if (refresh_fsize) {
                if ((result=fdir_client_stat_dentry_by_inode(
                                fi->ctx->contexts.fdir,
                                fi->dentry.inode, &fi->dentry)) != 0)
                {
                    return result;
                }
            }

            *new_offset = fi->dentry.stat.size + offset;
            if (*new_offset < 0) {
                return EINVAL;
            }
            break;
        default:
            logError("file: "__FILE__", line: %d, "
                    "invalid whence: %d", __LINE__, whence);
            return EINVAL;
    }

    return 0;
}

int fsapi_lseek(FSAPIFileInfo *fi, const int64_t offset, const int whence)
{
    int64_t new_offset;
    int result;

    if (fi->magic != FS_API_MAGIC_NUMBER) {
        return EBADF;
    }

    if ((result=calc_file_offset_ex(fi, offset, whence,
                    true, &new_offset)) != 0)
    {
        return result;
    }

    fi->offset = new_offset;
    return 0;
}

static void fill_stat(const FDIRDEntryInfo *dentry, struct stat *buf)
{
    memset(buf, 0, sizeof(struct stat));
    buf->st_ino = dentry->inode;
    buf->st_mode = dentry->stat.mode;
    buf->st_size = dentry->stat.size;
    buf->st_mtime = dentry->stat.mtime;
    buf->st_ctime = dentry->stat.ctime;
}

int fsapi_fstat(FSAPIFileInfo *fi, struct stat *buf)
{
    int result;

    if (fi->magic != FS_API_MAGIC_NUMBER) {
        return EBADF;
    }

    if ((result=fdir_client_stat_dentry_by_inode(fi->ctx->contexts.
                    fdir, fi->dentry.inode, &fi->dentry)) != 0)
    {
        return result;
    }

    fill_stat(&fi->dentry, buf);
    return 0;
}

int fsapi_stat_ex(FSAPIContext *ctx, const char *path, struct stat *buf)
{
    int result;
    FDIRDEntryFullName fullname;
    FDIRDEntryInfo dentry;

    fullname.ns = ctx->ns;
    FC_SET_STRING(fullname.path, (char *)path);
    if ((result=fdir_client_stat_dentry_by_path(ctx->contexts.fdir,
                    &fullname, &dentry)) != 0)
    {
        return result;
    }

    fill_stat(&dentry, buf);
    return 0;
}

static inline int flock_lock(FSAPIFileInfo *fi, const int operation)
{
    int result;
    if ((result=fdir_client_init_session(fi->ctx->contexts.fdir,
                    &fi->sessions.flock)) != 0)
    {
        return result;
    }

    if ((result=fdir_client_flock_dentry(&fi->sessions.flock, operation,
                    fi->dentry.inode)) != 0)
    {
        fdir_client_close_session(&fi->sessions.flock, is_network_error(result));
    }

    return result;
}

static inline int flock_unlock(FSAPIFileInfo *fi, const int operation)
{
    int result;
    result = fdir_client_flock_dentry(&fi->sessions.flock, operation,
            fi->dentry.inode);
    fdir_client_close_session(&fi->sessions.flock, result != 0);
    return result;
}

int fsapi_flock(FSAPIFileInfo *fi, const int operation)
{
    if (fi->magic != FS_API_MAGIC_NUMBER) {
        return EBADF;
    }

    if ((operation & LOCK_UN)) {
        if (fi->sessions.flock.mconn == NULL) {
            return ENOENT;
        }
        return flock_unlock(fi, operation);
    } else if ((operation & LOCK_SH) || (operation & LOCK_EX)) {
        if (fi->sessions.flock.mconn != NULL) {
            logError("file: "__FILE__", line: %d, "
                    "flock on inode: %"PRId64" already exist",
                    __LINE__, fi->dentry.inode);
            return EEXIST;
        }
        return flock_lock(fi, operation);
    } else {
        return EINVAL;
    }
}

static inline int fcntl_lock(FSAPIFileInfo *fi, const int operation,
        const int64_t offset, const int64_t length,
        const int64_t owner_id, const pid_t pid)
{
    int result;
    if ((result=fdir_client_init_session(fi->ctx->contexts.fdir,
                    &fi->sessions.flock)) != 0)
    {
        return result;
    }

    if ((result=fdir_client_flock_dentry_ex2(&fi->sessions.flock, operation,
                    fi->dentry.inode, offset, length, owner_id, pid)) != 0)
    {
        fdir_client_close_session(&fi->sessions.flock, is_network_error(result));
    }

    return result;
}

static inline int fcntl_unlock(FSAPIFileInfo *fi, const int operation,
        const int64_t offset, const int64_t length,
        const int64_t owner_id, const pid_t pid)
{
    int result;
    result = fdir_client_flock_dentry_ex2(&fi->sessions.flock, operation,
            fi->dentry.inode, offset, length, owner_id, pid);
    fdir_client_close_session(&fi->sessions.flock, result != 0);
    return result;
}

static inline int fcntl_type_to_flock_op(const short type, int *operation)
{
    switch (type) {
        case F_RDLCK:
            *operation = LOCK_SH;
            break;
        case F_WRLCK:
            *operation = LOCK_EX;
            break;
        case F_UNLCK:
            *operation = LOCK_UN;
            break;
        default:
            return EINVAL;
    }

    return 0;
}

static inline int flock_op_to_fcntl_type(const int operation, short *type)
{
    switch (operation) {
        case LOCK_SH:
            *type = F_RDLCK;
            break;
        case LOCK_EX:
            *type = F_WRLCK;
            break;
        case LOCK_UN:
            *type = F_UNLCK;
            break;
        default:
            return EINVAL;
    }

    return 0;
}

int fsapi_setlk(FSAPIFileInfo *fi, const struct flock *lock,
        const int64_t owner_id)
{
    int operation;
    int result;
    int64_t offset;

    if (fi->magic != FS_API_MAGIC_NUMBER) {
        return EBADF;
    }

    if ((result=fcntl_type_to_flock_op(lock->l_type, &operation)) != 0) {
        return result;
    }

    if ((result=calc_file_offset(fi, lock->l_start,
                    lock->l_whence, &offset)) != 0)
    {
        return result;
    }

    if (operation == LOCK_UN) {
        if (fi->sessions.flock.mconn == NULL) {
            return ENOENT;
        }
        return fcntl_unlock(fi, operation, offset, lock->l_len,
                owner_id, lock->l_pid);
    } else {
        if (fi->sessions.flock.mconn != NULL) {
            logError("file: "__FILE__", line: %d, "
                    "flock on inode: %"PRId64" already exist",
                    __LINE__, fi->dentry.inode);
            return EEXIST;
        }
        return fcntl_lock(fi, operation, offset, lock->l_len,
                owner_id, lock->l_pid);
    }
}

int fsapi_getlk(FSAPIFileInfo *fi, struct flock *lock, int64_t *owner_id)
{
    int operation;
    int result;
    int64_t offset;
    int64_t length;

    if (fi->magic != FS_API_MAGIC_NUMBER) {
        return EBADF;
    }

    if ((result=fcntl_type_to_flock_op(lock->l_type, &operation)) != 0) {
        return result;
    }

    if ((result=calc_file_offset(fi, lock->l_start,
                    lock->l_whence, &offset)) != 0)
    {
        return result;
    }

    if (operation == LOCK_UN) {
        return EINVAL;
    }

    length = lock->l_len;
    if ((result=fdir_client_getlk_dentry(fi->ctx->contexts.fdir,
                    fi->dentry.inode, &operation, &offset, &length,
                    owner_id, &lock->l_pid)) == 0)
    {
        flock_op_to_fcntl_type(operation, &lock->l_type);
        lock->l_whence = SEEK_SET;
        lock->l_start = offset;
        lock->l_len = length;
    }

    return result;
}
