
#ifndef _STORAGE_TYPES_H
#define _STORAGE_TYPES_H

#include "fastcommon/shared_buffer.h"
#include "../../common/fs_types.h"

#define FS_MAX_SPLIT_COUNT_PER_SPACE_ALLOC  2

struct fs_slice_op_context;

typedef void (*fs_slice_op_notify_func)(struct fs_slice_op_context *ctx);

struct ob_slice_entry;
typedef struct {
    int count;
    struct ob_slice_entry *slices[FS_MAX_SPLIT_COUNT_PER_SPACE_ALLOC];
} FSSliceFixedArray;

struct fs_cluster_data_server_info;
typedef struct fs_slice_op_context {
    struct {
        fs_slice_op_notify_func func;
        void *arg;
    } notify;

    volatile int counter;
    int result;
    int done_bytes;

    struct {
        bool write_data_binlog;
        int data_group_id;
        uint64_t data_version;
        FSBlockSliceKeyInfo bs_key;
        struct fs_cluster_data_server_info *myself;
        char *body;
    } info;

    struct {
        int inc_alloc;  //increase alloc space in bytes for slice write
        FSSliceFixedArray sarray;
    } write;  //for slice write

} FSSliceOpContext;

typedef struct fs_slice_op_buffer_context {
    FSSliceOpContext op_ctx;
    SharedBuffer *buffer;
} FSSliceOpBufferContext;

#endif
