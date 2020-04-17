
#ifndef _TRUNK_IO_THREAD_H
#define _TRUNK_IO_THREAD_H

#include "../../common/fs_types.h"
#include "../storage/storage_config.h"

#define FS_IO_TYPE_CREATE_TRUNK   'C'
#define FS_IO_TYPE_DELETE_TRUNK   'D'
#define FS_IO_TYPE_READ_SLICE     'R'
#define FS_IO_TYPE_WRITE_SLICE    'W'

struct trunk_io_buffer;

//Note: the record can NOT be persisted
typedef void (*trunk_io_notify_func)(struct trunk_io_buffer *record,
        const int result, void *args);

typedef struct trunk_io_buffer {
    int type;
    FSTrunkSpaceInfo space;
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

    int trunk_io_thread_push(const int type, const uint32_t hash_code,
            const FSTrunkSpaceInfo *space, string_t *data, trunk_io_notify_func
            notify_func, void *notify_args);

#ifdef __cplusplus
}
#endif

#endif
