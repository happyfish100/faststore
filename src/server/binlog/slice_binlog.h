
#ifndef _SLICE_BINLOG_H
#define _SLICE_BINLOG_H

#include "../storage/object_block_index.h"

#ifdef __cplusplus
extern "C" {
#endif

    int slice_binlog_init();
    void slice_binlog_destroy();

    int slice_binlog_get_current_write_index();

    int slice_binlog_log_add_slice(const OBSliceEntry *slice,
            const uint64_t sn, const uint64_t data_version);

    int slice_binlog_log_del_slice(const FSBlockSliceKeyInfo *bs_key,
            const uint64_t sn, const uint64_t data_version);

    int slice_binlog_log_del_block(const FSBlockKey *bkey,
            const uint64_t sn, const uint64_t data_version);

#ifdef __cplusplus
}
#endif

#endif
