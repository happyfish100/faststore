
#ifndef _FS_FUSE_GLOBAL_H
#define _FS_FUSE_GLOBAL_H

#include "fastcommon/logger.h"

#define FS_FUSE_DEFAULT_ATTRIBUTE_TIMEOUT 1.0
#define FS_FUSE_DEFAULT_ENTRY_TIMEOUT     1.0

typedef enum {
    allow_none,
    allow_all,
    allow_root
} FUSEAllowOthersMode;

typedef enum {
    owner_type_caller,
    owner_type_fixed
} FUSEOwnerType;

typedef struct {
    char *ns;
    char *mountpoint;
    bool singlethread;
    bool clone_fd;
    bool auto_unmount;
    int max_idle_threads;
    double attribute_timeout;
    double entry_timeout;
    FUSEAllowOthersMode allow_others;
    struct {
        FUSEOwnerType type;
        uid_t uid;
        gid_t gid;
    } owner;
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
