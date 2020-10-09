
#ifndef _FS_CLIENT_PROTO_H
#define _FS_CLIENT_PROTO_H

#include "fastcommon/fast_mpool.h"
#include "fs_types.h"
#include "fs_proto.h"
#include "client_types.h"

#ifdef __cplusplus
extern "C" {
#endif

    int fs_client_proto_slice_write(FSClientContext *client_ctx,
            ConnectionInfo *conn, const uint64_t req_id,
            const FSBlockSliceKeyInfo *bs_key, const char *data,
            int *inc_alloc);

    int fs_client_proto_slice_read(FSClientContext *client_ctx,
            ConnectionInfo *conn, const FSBlockSliceKeyInfo *bs_key,
            char *buff, int *read_bytes);

    int fs_client_proto_bs_operate(FSClientContext *client_ctx,
            ConnectionInfo *conn, const uint64_t req_id, const void *key,
            const int req_cmd, const int resp_cmd,
            const int enoent_log_level, int *inc_alloc);

    int fs_client_proto_block_delete(FSClientContext *client_ctx,
            ConnectionInfo *conn, const uint64_t req_id,
            const FSBlockKey *bkey, const int enoent_log_level,
            int *dec_alloc);

    int fs_client_proto_join_server(FSClientContext *client_ctx,
            ConnectionInfo *conn, FSConnectionParameters *conn_params);

    int fs_client_proto_get_master(FSClientContext *client_ctx,
            const int data_group_index, FSClientServerEntry *master);

    int fs_client_proto_get_readable_server(FSClientContext *client_ctx,
            const int data_group_index, FSClientServerEntry *server);

    int fs_client_proto_get_leader(FSClientContext *client_ctx,
            ConnectionInfo *conn, FSClientServerEntry *leader);

    int fs_client_proto_cluster_stat(FSClientContext *client_ctx,
            const ConnectionInfo *spec_conn, const int data_group_id,
            FSClientClusterStatEntry *stats, const int size, int *count);

    int fs_client_proto_cluster_space_stat(FSClientContext *client_ctx,
            ConnectionInfo *conn, FSClientServerSpaceStat *stat,
            const int size, int *count);

#ifdef __cplusplus
}
#endif

#endif
