
#ifndef _STORAGE_TYPES_H
#define _STORAGE_TYPES_H

struct fs_slice_op_notify;

typedef void (*fs_slice_op_notify_func)(struct fs_slice_op_notify *notify);

typedef struct fs_slice_op_notify {
    struct {
        fs_slice_op_notify_func func;
        void *args;
    } notify;

    volatile int counter;
    int result;
    int done_bytes;
    int inc_alloc;   //increase alloc space in bytes for slice write
} FSSliceOpNotify;

#endif
