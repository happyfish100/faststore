
#ifndef _FS_GLOBAL_H
#define _FS_GLOBAL_H

#include "fs_types.h"

typedef struct fs_global_vars {
    Version version;
} FSGlobalVars;

#ifdef __cplusplus
extern "C" {
#endif

    extern FSGlobalVars g_fs_global_vars;

#ifdef __cplusplus
}
#endif

#endif
