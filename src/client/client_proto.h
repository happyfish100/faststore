
#ifndef _FS_CLIENT_PROTO_H
#define _FS_CLIENT_PROTO_H

#include "fastcommon/fast_mpool.h"
#include "fs_types.h"

#ifdef __cplusplus
extern "C" {
#endif

    int fs_client_proto_slice_write(FSClientContext *client_ctx,
            const FSBlockSliceKeyInfo *bs_key, char *buff, int *written);

#ifdef __cplusplus
}
#endif

#endif
