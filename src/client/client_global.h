
#ifndef _FS_CLIENT_GLOBAL_H
#define _FS_CLIENT_GLOBAL_H

#include "fs_global.h"
#include "client_types.h"

typedef struct client_global_vars {
    int connect_timeout;
    int network_timeout;
    char base_path[MAX_PATH_SIZE];

    FSClientContext client_ctx;
} FSClientGlobalVars;

#ifdef __cplusplus
extern "C" {
#endif

    extern FSClientGlobalVars g_client_global_vars;

#ifdef __cplusplus
}
#endif

#endif
