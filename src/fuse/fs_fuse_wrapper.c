#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include "fastcommon/common_define.h"
#include "fs_fuse_wrapper.h"

#define FS_ATTR_TIMEOUT  1.0
#define FS_ENTRY_TIMEOUT 1.0

static const char *hello_str = "Hello World!\n";
static const char *hello_name = "hello";

static void fill_stat(const FDIRDEntryInfo *dentry, struct stat *stat)
{
    stat->st_ino = dentry->inode;
    stat->st_mode = dentry->stat.mode;
    stat->st_size = dentry->stat.size;
    stat->st_mtime = dentry->stat.mtime;
    stat->st_ctime = dentry->stat.ctime;
    //stat->st_nlink = 2;
}

static inline int fs_convert_inode(const fuse_ino_t ino, int64_t *new_inode)
{
    if (ino == FUSE_ROOT_ID) {
        return fsapi_lookup_inode("/", new_inode);
    } else {
        *new_inode = ino;
        return 0;
    }
}

static void fs_do_getattr(fuse_req_t req, fuse_ino_t ino,
			     struct fuse_file_info *fi)
{
    int64_t new_inode;
    FDIRDEntryInfo dentry;
    struct stat stat;

    if (fs_convert_inode(ino, &new_inode) != 0) {
        fuse_reply_err(req, ENOENT);
        return;
    }

    fprintf(stderr, "file: "__FILE__", line: %d, func: %s, "
            "ino: %"PRId64", new_inode: %"PRId64", fi: %p\n",
            __LINE__, __FUNCTION__, ino, new_inode, fi);

    if (fsapi_stat_dentry_by_inode(new_inode, &dentry) == 0) {
        memset(&stat, 0, sizeof(stat));
        fill_stat(&dentry, &stat);
        fuse_reply_attr(req, &stat, FS_ATTR_TIMEOUT);
    } else {
        fuse_reply_err(req, ENOENT);
    }
}

static void fs_do_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    int result;
    int64_t parent_inode;
    FDIRDEntryInfo dentry;
    struct fuse_entry_param param;

    fprintf(stderr, "parent1: %"PRId64", name: %s\n", parent, name);

    if (fs_convert_inode(parent, &parent_inode) != 0) {
        fuse_reply_err(req, ENOENT);
        return;
    }
    fprintf(stderr, "parent2: %"PRId64", name: %s\n", parent_inode, name);

    if ((result=fsapi_stat_dentry_by_pname(parent_inode, name, &dentry)) != 0) {
        fuse_reply_err(req, ENOENT);
        return;
    }

    memset(&param, 0, sizeof(param));
    param.ino = dentry.inode;
    param.attr_timeout = FS_ATTR_TIMEOUT;
    param.entry_timeout = FS_ENTRY_TIMEOUT;
    fill_stat(&dentry, &param.attr);
    fuse_reply_entry(req, &param);
}

struct dirbuf {
	char *p;
	size_t size;
};

static void dirbuf_add(fuse_req_t req, struct dirbuf *b, const char *name,
		       fuse_ino_t ino)
{
	struct stat stbuf;
	size_t oldsize = b->size;
	b->size += fuse_add_direntry(req, NULL, 0, name, NULL, 0);
	b->p = (char *) realloc(b->p, b->size);
	memset(&stbuf, 0, sizeof(stbuf));
	stbuf.st_ino = ino;
	fuse_add_direntry(req, b->p + oldsize, b->size - oldsize, name, &stbuf,
			  b->size);
}

#define min(x, y) ((x) < (y) ? (x) : (y))

static int reply_buf_limited(fuse_req_t req, const char *buf, size_t bufsize,
			     off_t off, size_t maxsize)
{
	if (off < bufsize)
		return fuse_reply_buf(req, buf + off,
				      min(bufsize - off, maxsize));
	else
		return fuse_reply_buf(req, NULL, 0);
}

static void fs_do_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
			     off_t off, struct fuse_file_info *fi)
{
	(void) fi;

	if (ino != 1)
		fuse_reply_err(req, ENOTDIR);
	else {
		struct dirbuf b;

		memset(&b, 0, sizeof(b));
		dirbuf_add(req, &b, ".", 1);
		dirbuf_add(req, &b, "..", 1);
		dirbuf_add(req, &b, hello_name, 2);
		reply_buf_limited(req, b.p, b.size, off, size);
		free(b.p);
	}
}

static void fs_do_open(fuse_req_t req, fuse_ino_t ino,
			  struct fuse_file_info *fi)
{
	if (ino != 2)
		fuse_reply_err(req, EISDIR);
	else if ((fi->flags & O_ACCMODE) != O_RDONLY)
		fuse_reply_err(req, EACCES);
	else {
        fprintf(stderr, "file: "__FILE__", line: %d, func: %s, "
                "ino: %"PRId64", fh: %"PRId64"\n", __LINE__, __FUNCTION__,
                ino, fi->fh);
        fi->fh = 123456;
		fuse_reply_open(req, fi);
    }
}

static void fs_do_read(fuse_req_t req, fuse_ino_t ino, size_t size,
			  off_t off, struct fuse_file_info *fi)
{
	reply_buf_limited(req, hello_str, strlen(hello_str), off, size);
}

void fs_fuse_wrapper_get_ops(struct fuse_lowlevel_ops *ops)
{
	ops->lookup	= fs_do_lookup;
	ops->getattr	= fs_do_getattr;
	ops->readdir	= fs_do_readdir;
	ops->open	= fs_do_open;
	ops->read	= fs_do_read;
}
