
#ifndef _SLICE_BINLOG_H
#define _SLICE_BINLOG_H

#include "fastcommon/sched_thread.h"
#include "../storage/object_block_index.h"

#define SLICE_BINLOG_OP_TYPE_WRITE_SLICE  BINLOG_OP_TYPE_WRITE_SLICE
#define SLICE_BINLOG_OP_TYPE_ALLOC_SLICE  BINLOG_OP_TYPE_ALLOC_SLICE
#define SLICE_BINLOG_OP_TYPE_DEL_SLICE    BINLOG_OP_TYPE_DEL_SLICE
#define SLICE_BINLOG_OP_TYPE_DEL_BLOCK    BINLOG_OP_TYPE_DEL_BLOCK

#ifdef __cplusplus
extern "C" {
#endif

    int slice_binlog_init();
    void slice_binlog_destroy();

    int slice_binlog_get_current_write_index();

    struct binlog_writer_info *slice_binlog_get_writer();

    int slice_binlog_log_add_slice(const OBSliceEntry *slice,
            const time_t current_time, const uint64_t sn,
            const uint64_t data_version);

    int slice_binlog_log_del_slice(const FSBlockSliceKeyInfo *bs_key,
            const time_t current_time, const uint64_t sn,
            const uint64_t data_version);

    int slice_binlog_log_del_block(const FSBlockKey *bkey,
            const time_t current_time, const uint64_t sn,
            const uint64_t data_version);

#ifdef __cplusplus
}
#endif

#endif
