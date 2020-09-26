
#ifndef _STORAGE_TYPES_H
#define _STORAGE_TYPES_H

#include "fastcommon/shared_buffer.h"
#include "../../common/fs_types.h"

#define FS_MAX_SPLIT_COUNT_PER_SPACE_ALLOC   2
#define FS_SLICE_SN_PARRAY_INIT_ALLOC_COUNT  4

struct fs_slice_op_context;

typedef void (*fs_slice_op_notify_func)(struct fs_slice_op_context *ctx);

struct ob_slice_entry;

typedef struct {
    struct ob_slice_entry *slice;
    uint64_t sn;     //for slice binlog
} FSSliceSNPair;

typedef struct {
    int count;
    int alloc;
    FSSliceSNPair *slice_sn_pairs;
} FSSliceSNPairArray;

struct fs_cluster_data_server_info;
typedef struct fs_slice_op_context {
    struct {
        fs_slice_op_notify_func func;
        void *arg;
    } notify;

    volatile short counter;
    short result;
    int done_bytes;

    struct {
        struct {
            bool log_replica;  //false for trunk reclaim
            bool immediately;  //false for master update
        } write_binlog;
        short source;      //for binlog write
        int data_group_id;
        uint64_t data_version;  //for replica binlog
        uint64_t sn;            //for slice binlog
        FSBlockSliceKeyInfo bs_key;
        struct fs_cluster_data_server_info *myself;
        char *body;
        int body_len;
    } info;

    struct {
        int space_changed;  //increase /decrease space in bytes for slice operate
        FSSliceSNPairArray sarray;
    } update;  //for slice update

} FSSliceOpContext;

typedef struct fs_slice_op_buffer_context {
    FSSliceOpContext op_ctx;
    SharedBuffer *buffer;
} FSSliceOpBufferContext;

#endif
