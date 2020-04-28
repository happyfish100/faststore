#include <sys/stat.h>
#include <limits.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/connection_pool.h"
#include "fs_api_file.h"

int fsapi_open_ex(FSAPIContext *ctx, FSAPIFileInfo *fi,
            const char *path, const int flags)
{
    fi->ctx = ctx;

    /*
    int mode;     //type and permission
    int64_t oid;     //object id
    int64_t offset;  //current offset
    */

    return 0;
}
