#ifndef _FS_TYPES_H
#define _FS_TYPES_H

#include "fastcommon/common_define.h"

#define FS_ERROR_INFO_SIZE   256
#define FS_REPLICA_KEY_SIZE    8

#define FS_NETWORK_TIMEOUT_DEFAULT    10
#define FS_CONNECT_TIMEOUT_DEFAULT     2

#define FS_DEFAULT_BINLOG_BUFFER_SIZE (64 * 1024)

#define FS_SERVER_DEFAULT_CLUSTER_PORT  21011
#define FS_SERVER_DEFAULT_SERVICE_PORT  21012

#define FS_FILE_SLICE_SIZE    (4 * 1024 * 1024)
#define FS_MAX_DATA_GROUPS_PER_SERVER   1024

#define FS_SERVER_STATUS_INIT       0
#define FS_SERVER_STATUS_BUILDING  10
#define FS_SERVER_STATUS_DUMPING   20
#define FS_SERVER_STATUS_OFFLINE   21
#define FS_SERVER_STATUS_SYNCING   22
#define FS_SERVER_STATUS_ACTIVE    23

typedef struct {
    int body_len;      //body length
    short flags;
    short status;
    unsigned char cmd; //command
} FSHeaderInfo;

typedef struct {
    FSHeaderInfo header;
    char *body;
} FSRequestInfo;

typedef struct {
    FSHeaderInfo header;
    struct {
        int length;
        char message[FS_ERROR_INFO_SIZE];
    } error;
} FSResponseInfo;

#endif
