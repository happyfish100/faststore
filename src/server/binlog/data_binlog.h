
#ifndef _DATA_BINLOG_H
#define _DATA_BINLOG_H

#include "../storage/object_block_index.h"

#define DATA_BINLOG_OP_TYPE_WRITE_SLICE  'w'
#define DATA_BINLOG_OP_TYPE_ALLOC_SLICE  'a'
#define DATA_BINLOG_OP_TYPE_DEL_SLICE    'd'
#define DATA_BINLOG_OP_TYPE_DEL_BLOCK    'D'

#ifdef __cplusplus
extern "C" {
#endif

    int data_binlog_init();
    void data_binlog_destroy();

    int data_binlog_get_current_write_index();

    int data_binlog_log_slice(const int data_group_id,
            const int64_t data_version, const FSBlockSliceKeyInfo *bs_key,
            const int op_type);

    int data_binlog_log_del_block(const int data_group_id,
            const int64_t data_version, const FSBlockKey *bkey);

    static inline int data_binlog_log_write_slice(const int data_group_id,
            const int64_t data_version, const FSBlockSliceKeyInfo *bs_key)
    {
        return data_binlog_log_slice(data_group_id, data_version,
                bs_key, DATA_BINLOG_OP_TYPE_WRITE_SLICE);
    }

    static inline int data_binlog_log_alloc_slice(const int data_group_id,
            const int64_t data_version, const FSBlockSliceKeyInfo *bs_key)
    {
        return data_binlog_log_slice(data_group_id, data_version,
                bs_key, DATA_BINLOG_OP_TYPE_ALLOC_SLICE);
    }

    static inline int data_binlog_log_del_slice(const int data_group_id,
            const int64_t data_version, const FSBlockSliceKeyInfo *bs_key)
    {
        return data_binlog_log_slice(data_group_id, data_version,
                bs_key, DATA_BINLOG_OP_TYPE_DEL_SLICE);
    }

#ifdef __cplusplus
}
#endif

#endif
