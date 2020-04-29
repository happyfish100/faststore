
#ifndef _FS_API_FILE_H
#define _FS_API_FILE_H

#include "fs_api_types.h"

#ifdef __cplusplus
extern "C" {
#endif

#define fsapi_open(fi, path, flags, mode) \
    fsapi_open_ex(&g_fs_api_ctx, fi, path, flags, mode)

    int fsapi_open_ex(FSAPIContext *ctx, FSAPIFileInfo *fi,
            const char *path, const int flags, const mode_t mode);

    int fsapi_close(FSAPIFileInfo *fi);

    int fsapi_pwrite(FSAPIFileInfo *fi, const char *buff,
            const int size, const int64_t offset, int *written_bytes);

    int fsapi_write(FSAPIFileInfo *fi, const char *buff,
            const int size, int *written_bytes);

    int fsapi_pread(FSAPIFileInfo *fi, char *buff, const int size,
            const int64_t offset, int *read_bytes);

    int fsapi_read(FSAPIFileInfo *fi, char *buff,
            const int size, int *read_bytes);

#ifdef __cplusplus
}
#endif

#endif
