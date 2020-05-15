
#ifndef _FS_FUSE_WRAPPER_H
#define _FS_FUSE_WRAPPER_H

#ifndef FUSE_USE_VERSION
#define FUSE_USE_VERSION 31
#endif

#include "fastcommon/logger.h"
#include "fsapi/fs_api.h"
#include "fsapi/fs_api_util.h"
#include "fuse3/fuse_lowlevel.h"

#ifdef __cplusplus
extern "C" {
#endif

	int fs_fuse_wrapper_init(struct fuse_lowlevel_ops *ops);

#ifdef __cplusplus
}
#endif

#endif
