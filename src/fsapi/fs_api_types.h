#ifndef _FS_API_TYPES_H
#define _FS_API_TYPES_H

#include <limits.h>
#include "fastdir/fdir_client.h"
#include "faststore/fs_client.h"

typedef struct fs_api_context {
    string_t ns;  //namespace
    char ns_holder[NAME_MAX];
    struct {
        FDIRClientContext *fdir;
        FSClientContext *fs;
    } contexts;
} FSAPIContext;

typedef struct fs_api_file_info {
    FSAPIContext *ctx;
    FDIRClientSession session;  //for flock
    FDIRDEntryInfo dentry;
    int flags;
    int magic;
    struct {
        int last_modified_time;
    } write_notify;
    int64_t offset;  //current offset
} FSAPIFileInfo;

#endif
