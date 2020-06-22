
#ifndef _DATA_BINLOG_H
#define _DATA_BINLOG_H

#include "../storage/object_block_index.h"

#ifdef __cplusplus
extern "C" {
#endif

    int data_binlog_init();
    void data_binlog_destroy();

    int data_binlog_get_current_write_index();

    int data_binlog_log_write_slice(const int64_t data_version,
            const OBSliceEntry *slice);

    int data_binlog_log_del_slice(const int64_t data_version,
            const FSBlockSliceKeyInfo *bs_key);

    int data_binlog_log_del_block(const int64_t data_version,
            const FSBlockKey *bkey);

#ifdef __cplusplus
}
#endif

#endif
