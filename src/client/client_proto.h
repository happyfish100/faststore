/*
 * Copyright (c) 2020 YuQing <384681@qq.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */


#ifndef _FS_CLIENT_PROTO_H
#define _FS_CLIENT_PROTO_H

#include "fastcommon/fast_mpool.h"
#include "fs_types.h"
#include "fs_proto.h"
#include "client_types.h"

typedef struct fs_client_space_info {
    int64_t disk_avail;
    int64_t block_used_space;
    SFSpaceStat trunk;
} FSClientSpaceInfo;

typedef struct fs_client_service_stat {
    time_t up_time;
    int server_id;
    bool is_leader;
    bool auth_enabled;
    char padding[1];
     struct {
        bool enabled;
        int64_t current_version;
        int64_t version_delay;
        FSClientSpaceInfo space;
    } storage_engine;
    char version_holder[12];
    string_t version;

    struct {
        int current_count;
        int max_count;
    } connection;

    struct {
        int64_t current_version;
        FSBinlogWriterStat writer;
    } binlog;

    struct {
        FSServiceOBSliceStat ob;
        FSServiceOBSliceStat slice;
    } data;

    FSClientSpaceInfo space;

} FSClientServiceStat;

#ifdef __cplusplus
extern "C" {
#endif

#define fs_client_proto_slice_read(client_ctx, conn, bs_key, buff, read_bytes) \
        fs_client_proto_slice_read_ex(client_ctx,     \
            conn, 0, FS_SERVICE_PROTO_SLICE_READ_REQ, \
            FS_SERVICE_PROTO_SLICE_READ_RESP,  \
            bs_key, buff, read_bytes)

#define fs_client_proto_slice_readv(client_ctx, conn, \
        bs_key, iov, iovcnt, read_bytes) \
        fs_client_proto_slice_readv_ex(client_ctx,    \
            conn, 0, FS_SERVICE_PROTO_SLICE_READ_REQ, \
            FS_SERVICE_PROTO_SLICE_READ_RESP,  \
            bs_key, iov, iovcnt, read_bytes)

    static inline void fs_client_proto_pack_block_key(
            const FSBlockKey *bkey, FSProtoBlockKey *proto_bkey)
    {
        long2buff(bkey->oid, proto_bkey->oid);
        long2buff(bkey->offset, proto_bkey->offset);
    }

    static inline void fs_client_proto_pack_block_slice_key(
            const FSBlockSliceKeyInfo *bs_key,
            FSProtoBlockSlice *proto_bs)
    {
        fs_client_proto_pack_block_key(&bs_key->block, &proto_bs->bkey);
        int2buff(bs_key->slice.offset, proto_bs->slice_size.offset);
        int2buff(bs_key->slice.length, proto_bs->slice_size.length);
    }

    int fs_client_proto_slice_write(FSClientContext *client_ctx,
            ConnectionInfo *conn, const uint64_t req_id,
            const FSBlockSliceKeyInfo *bs_key, const char *data,
            int *inc_alloc);

    int fs_client_proto_slice_writev(FSClientContext *client_ctx,
            ConnectionInfo *conn, const uint64_t req_id,
            const FSBlockSliceKeyInfo *bs_key, const struct iovec *iov,
            const int iovcnt, int *inc_alloc);

    int fs_client_proto_slice_read_ex(FSClientContext *client_ctx,
            ConnectionInfo *conn, const int slave_id, const int req_cmd,
            const int resp_cmd, const FSBlockSliceKeyInfo *bs_key,
            char *buff, int *read_bytes);

    int fs_client_proto_slice_readv_ex(FSClientContext *client_ctx,
            ConnectionInfo *conn, const int slave_id, const int req_cmd,
            const int resp_cmd, const FSBlockSliceKeyInfo *bs_key,
            const struct iovec *iov, const int iovcnt, int *read_bytes);

    int fs_client_proto_bs_operate(FSClientContext *client_ctx,
            ConnectionInfo *conn, const uint64_t req_id, const void *key,
            const int req_cmd, const int resp_cmd,
            const int enoent_log_level, int *inc_alloc);

    int fs_client_proto_block_delete(FSClientContext *client_ctx,
            ConnectionInfo *conn, const uint64_t req_id,
            const FSBlockKey *bkey, const int enoent_log_level,
            int *dec_alloc);

    int fs_client_proto_join_server(FSClientContext *client_ctx,
            ConnectionInfo *conn, SFConnectionParameters *conn_params);

    int fs_client_proto_get_master(FSClientContext *client_ctx,
            const int data_group_index, FSClientServerEntry *master);

    int fs_client_proto_get_readable_server(FSClientContext *client_ctx,
            const int data_group_index, FSClientServerEntry *server);

    int fs_client_proto_cluster_stat(FSClientContext *client_ctx,
            const ConnectionInfo *spec_conn, const FSClusterStatFilter *filter,
            FSIdArray *gid_array, FSClientClusterStatEntryArray *cs_array);

    int fs_client_proto_server_group_space_stat(FSClientContext *client_ctx,
            ConnectionInfo *conn, FSClientServerSpaceStat *stats,
            const int size, int *count);

    int fs_client_proto_service_stat(FSClientContext *client_ctx,
            const ConnectionInfo *spec_conn, const int data_group_id,
            const bool include_block_space, FSClientServiceStat *stat);

#ifdef __cplusplus
}
#endif

#endif
