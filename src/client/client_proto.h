
#ifndef _FS_CLIENT_PROTO_H
#define _FS_CLIENT_PROTO_H

#include "fastcommon/fast_mpool.h"
#include "fs_types.h"
#include "fs_proto.h"

typedef struct fs_client_cluster_stat_entry {
    int data_group_id;
    int server_id;
    bool is_master;
    char status;
    int64_t data_version;
} FSClientClusterStatEntry;

#ifdef __cplusplus
extern "C" {
#endif

    int fs_client_proto_slice_write(FSClientContext *client_ctx,
            const FSBlockSliceKeyInfo *bs_key, const char *buff,
            int *write_bytes, int *inc_alloc);

    int fs_client_proto_slice_read(FSClientContext *client_ctx,
            const FSBlockSliceKeyInfo *bs_key, char *buff, int *read_bytes);

    int fs_client_proto_slice_allocate(FSClientContext *client_ctx,
            const FSBlockSliceKeyInfo *bs_key, int *inc_alloc);

    int fs_client_proto_slice_delete(FSClientContext *client_ctx,
            const FSBlockSliceKeyInfo *bs_key, int *dec_alloc);

    int fs_client_proto_block_delete(FSClientContext *client_ctx,
            const FSBlockKey *bkey, int *dec_alloc);

    int fs_client_proto_join_server(FSClientContext *client_ctx,
            ConnectionInfo *conn, FSConnectionParameters *conn_params);

    int fs_client_proto_get_master(FSClientContext *client_ctx,
            const int data_group_index, FSClientServerEntry *master);

    int fs_client_proto_get_readable_server(FSClientContext *client_ctx,
            const int data_group_index, FSClientServerEntry *server);

    int fs_client_proto_cluster_stat(FSClientContext *client_ctx,
            const ConnectionInfo *spec_conn, const int data_group_id,
            FSClientClusterStatEntry *stats, const int size, int *count);

#ifdef __cplusplus
}
#endif

#endif
