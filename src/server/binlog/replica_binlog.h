
#ifndef _REPLICA_BINLOG_H
#define _REPLICA_BINLOG_H

#include "../storage/object_block_index.h"

#define REPLICA_BINLOG_OP_TYPE_WRITE_SLICE  'w'
#define REPLICA_BINLOG_OP_TYPE_ALLOC_SLICE  'a'
#define REPLICA_BINLOG_OP_TYPE_DEL_SLICE    'd'
#define REPLICA_BINLOG_OP_TYPE_DEL_BLOCK    'D'
#define REPLICA_BINLOG_OP_TYPE_NO_OP        'N'

struct binlog_writer_info;
struct server_binlog_reader;

typedef struct replica_binlog_record {
    int op_type;
    FSBlockSliceKeyInfo bs_key;
    int64_t data_version;
    struct replica_binlog_record *next;
} ReplicaBinlogRecord;

#ifdef __cplusplus
extern "C" {
#endif

    int replica_binlog_init();
    void replica_binlog_destroy();

    struct binlog_writer_info *replica_binlog_get_writer(
            const int data_group_id);

    int replica_binlog_get_current_write_index(const int data_group_id);

    int replica_binlog_get_last_record_ex(const char *filename,
            ReplicaBinlogRecord *record, FSBinlogFilePosition *position,
            int *record_len);

    static inline int replica_binlog_get_last_record(const char *filename,
            ReplicaBinlogRecord *record)
    {
        FSBinlogFilePosition position;
        int record_len;

        return replica_binlog_get_last_record_ex(filename,
                record, &position, &record_len);
    }

    static inline int replica_binlog_get_last_data_version_ex(
            const char *filename, uint64_t *data_version,
            FSBinlogFilePosition *position, int *record_len)
    {
        ReplicaBinlogRecord record;
        int result;

        if ((result=replica_binlog_get_last_record_ex(filename,
                        &record, position, record_len)) == 0)
        {
            *data_version = record.data_version;
        } else {
            *data_version = 0;
        }

        return result;
    }

    static inline int replica_binlog_get_last_data_version(
            const char *filename, uint64_t *data_version)
    {
        FSBinlogFilePosition position;
        int record_len;

        return replica_binlog_get_last_data_version_ex(filename,
                data_version, &position, &record_len);
    }

    int replica_binlog_record_unpack(const string_t *line,
            ReplicaBinlogRecord *record, char *error_info);

    int replica_binlog_log_slice(const int data_group_id,
            const int64_t data_version, const FSBlockSliceKeyInfo *bs_key,
            const int op_type);

    int replica_binlog_log_block(const int data_group_id,
            const int64_t data_version, const FSBlockKey *bkey,
            const int op_type);

    static inline int replica_binlog_log_del_block(const int data_group_id,
            const int64_t data_version, const FSBlockKey *bkey)
    {
        return replica_binlog_log_block(data_group_id, data_version, bkey,
                REPLICA_BINLOG_OP_TYPE_DEL_BLOCK);
    }

    static inline int replica_binlog_log_no_op(const int data_group_id,
            const int64_t data_version, const FSBlockKey *bkey)
    {
        return replica_binlog_log_block(data_group_id, data_version, bkey,
                REPLICA_BINLOG_OP_TYPE_NO_OP);
    }

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

    const char *replica_binlog_get_op_type_caption(const int op_type);

    int replica_binlog_reader_init(struct server_binlog_reader *reader,
            const int data_group_id, const uint64_t last_data_version);

    void replica_binlog_set_data_version(FSClusterDataServerInfo *myself,
            const uint64_t new_data_version);

#ifdef __cplusplus
}
#endif

#endif
