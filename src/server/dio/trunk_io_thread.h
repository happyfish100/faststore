
#ifndef _TRUNK_IO_THREAD_H
#define _TRUNK_IO_THREAD_H

#include "../../common/fs_types.h"
#include "../storage/storage_config.h"
#include "../storage/object_block_index.h"

#define FS_IO_TYPE_CREATE_TRUNK   'C'
#define FS_IO_TYPE_DELETE_TRUNK   'D'
#define FS_IO_TYPE_READ_SLICE     'R'
#define FS_IO_TYPE_WRITE_SLICE    'W'

struct trunk_io_buffer;

//Note: the record can NOT be persisted
typedef void (*trunk_io_notify_func)(struct trunk_io_buffer *record,
        const int result);

typedef struct trunk_io_buffer {
    int type;

    union {
        FSTrunkSpaceInfo space;  //for trunk op
        OBSliceEntry *slice;     //for slice op
    };

    string_t data;
    struct {
        trunk_io_notify_func func;
        void *args;
    } notify;
    struct trunk_io_buffer *next;
} TrunkIOBuffer;

#ifdef __cplusplus
extern "C" {
#endif

    int trunk_io_thread_init();
    void trunk_io_thread_terminate();

    int trunk_io_thread_push(const int type, const int path_index,
            const uint32_t hash_code, void *entry, char *buff,
            trunk_io_notify_func notify_func, void *notify_args);

    static inline int io_thread_push_trunk_op(const int type,
            const FSTrunkSpaceInfo *space, trunk_io_notify_func
            notify_func, void *notify_args)
    {
        return trunk_io_thread_push(type, space->store->index,
                space->id_info.id, (void *)space, NULL,
                notify_func, notify_args);
    }

    static inline int io_thread_push_slice_op(const int type,
            OBSliceEntry *slice, char *buff, trunk_io_notify_func
            notify_func, void *notify_args)
    {
        return trunk_io_thread_push(type, slice->space.store->index,
                slice->ob->bkey.hash_code, slice, buff,
                notify_func, notify_args);
    }

#ifdef __cplusplus
}
#endif

#endif
