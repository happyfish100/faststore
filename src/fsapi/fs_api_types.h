#ifndef _FS_API_TYPES_H
#define _FS_API_TYPES_H

#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "fastcommon/fast_mblock.h"
#include "fastcommon/fast_buffer.h"
#include "fastdir/fdir_client.h"
#include "faststore/fs_client.h"

typedef struct fs_api_opendir_session {
    FDIRClientDentryArray array;
    FastBuffer buffer;
} FSAPIOpendirSession;

typedef struct fs_api_context {
    mode_t default_mode;
    string_t ns;  //namespace
    char ns_holder[NAME_MAX];
    struct {
        FDIRClientContext *fdir;
        FSClientContext *fs;
    } contexts;

    struct fast_mblock_man opendir_session_pool;
} FSAPIContext;

typedef struct fs_api_file_info {
    FSAPIContext *ctx;
    struct {
        FDIRClientSession flock;
        FSAPIOpendirSession *opendir;
    } sessions;
    FDIRDEntryInfo dentry;
    int flags;
    int magic;
    struct {
        int last_modified_time;
    } write_notify;
    int64_t offset;  //current offset
} FSAPIFileInfo;

#ifdef __cplusplus
extern "C" {
#endif

    extern FSAPIContext g_fs_api_ctx;

#ifdef __cplusplus
}
#endif

#endif
