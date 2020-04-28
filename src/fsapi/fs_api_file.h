
#ifndef _FS_API_FILE_H
#define _FS_API_FILE_H

#include "fs_api_types.h"

#ifdef __cplusplus
extern "C" {
#endif

    int fsapi_open_ex(FSAPIContext *ctx, FSAPIFileInfo *fi,
            const char *path, const int flags);

    int fsapi_close(FSAPIFileInfo *fi);

#ifdef __cplusplus
}
#endif

#endif
