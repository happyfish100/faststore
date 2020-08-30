
#ifndef _FS_FUSE_GLOBAL_H
#define _FS_FUSE_GLOBAL_H

#include "fastcommon/logger.h"

typedef enum {
    allow_none,
    allow_all,
    allow_root
} FUSEAllowOthersMode;

typedef struct {
    char *ns;
    char *mountpoint;
    bool singlethread;
    bool clone_fd;
    bool auto_unmount;
    int max_idle_threads;
    FUSEAllowOthersMode allow_others;
} FUSEGlobalVars;

#ifdef __cplusplus
extern "C" {
#endif

    extern FUSEGlobalVars g_fuse_global_vars;

	int fs_fuse_global_init(const char *config_filename);

#ifdef __cplusplus
}
#endif

#endif
