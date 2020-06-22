
#ifndef _SLICE_BINLOG_H
#define _SLICE_BINLOG_H

#include "../storage/object_block_index.h"

#ifdef __cplusplus
extern "C" {
#endif

    int slice_binlog_init();
    void slice_binlog_destroy();

    int slice_binlog_get_current_write_index();

    int slice_binlog_log_add_slice(const OBSliceEntry *slice);

    int slice_binlog_log_del_slice(const FSBlockSliceKeyInfo *bs_key);

    int slice_binlog_log_del_block(const FSBlockKey *bkey);

#ifdef __cplusplus
}
#endif

#endif
