
#ifndef _REPLICA_BINLOG_H
#define _REPLICA_BINLOG_H

#include "../storage/object_block_index.h"

#define REPLICA_BINLOG_OP_TYPE_WRITE_SLICE  'w'
#define REPLICA_BINLOG_OP_TYPE_ALLOC_SLICE  'a'
#define REPLICA_BINLOG_OP_TYPE_DEL_SLICE    'd'
#define REPLICA_BINLOG_OP_TYPE_DEL_BLOCK    'D'

typedef struct replica_binlog_record {
    int op_type;
    FSBlockSliceKeyInfo bs_key;
    int64_t data_version;
} ReplicaBinlogRecord;

#ifdef __cplusplus
extern "C" {
#endif

    int replica_binlog_init();
    void replica_binlog_destroy();

    int replica_binlog_get_current_write_index(const int data_group_id);

    int replica_binlog_record_unpack(const string_t *line,
            ReplicaBinlogRecord *record, char *error_info);

    int replica_binlog_log_slice(const int data_group_id,
            const int64_t data_version, const FSBlockSliceKeyInfo *bs_key,
            const int op_type);

    int replica_binlog_log_del_block(const int data_group_id,
            const int64_t data_version, const FSBlockKey *bkey);

    static inline int replica_binlog_log_write_slice(const int data_group_id,
            const int64_t data_version, const FSBlockSliceKeyInfo *bs_key)
    {
        return replica_binlog_log_slice(data_group_id, data_version,
                bs_key, REPLICA_BINLOG_OP_TYPE_WRITE_SLICE);
    }

    static inline int replica_binlog_log_alloc_slice(const int data_group_id,
            const int64_t data_version, const FSBlockSliceKeyInfo *bs_key)
    {
        return replica_binlog_log_slice(data_group_id, data_version,
                bs_key, REPLICA_BINLOG_OP_TYPE_ALLOC_SLICE);
    }

    static inline int replica_binlog_log_del_slice(const int data_group_id,
            const int64_t data_version, const FSBlockSliceKeyInfo *bs_key)
    {
        return replica_binlog_log_slice(data_group_id, data_version,
                bs_key, REPLICA_BINLOG_OP_TYPE_DEL_SLICE);
    }

#ifdef __cplusplus
}
#endif

#endif
