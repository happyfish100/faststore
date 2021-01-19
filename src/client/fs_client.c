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

#include <stdlib.h>
#include "fastcommon/fc_list.h"
#include "fastcommon/skiplist_set.h"
#include "sf/idempotency/client/client_channel.h"
#include "sf/idempotency/client/rpc_wrapper.h"
#include "client_global.h"
#include "fs_client.h"

int fs_unlink_file(FSClientContext *client_ctx, const int64_t oid,
        const int64_t file_size)
{
    FSBlockKey bkey;
    int64_t remain;
    int result;
    int dec_alloc;

    if (file_size == 0) {
        return 0;
    }

    remain = file_size;
    fs_set_block_key(&bkey, oid, 0);
    while (1) {
        /*
        logInfo("block {oid: %"PRId64", offset: %"PRId64"}",
                bkey.oid, bkey.offset);
                */

        result = fs_client_block_delete(client_ctx, &bkey, &dec_alloc);
        if (result == ENOENT) {
            result = 0;
        } else if (result != 0) {
            break;
        }

        remain -= FS_FILE_BLOCK_SIZE;
        if (remain <= 0) {
            break;
        }

        fs_next_block_key(&bkey);
    }

    return result;
}

static int stat_data_group_by_addresses(FSClientContext *client_ctx,
        const FSClusterStatFilter *filter, FCAddressPtrArray *addr_ptr_array,
        FSIdArray *gid_array, FSClientClusterStatEntryArray *cs_array)
{
    FCAddressInfo **addr;
    FCAddressInfo **end;
    int result;

    result = ENOENT;
    end = addr_ptr_array->addrs + addr_ptr_array->count;
    for (addr=addr_ptr_array->addrs; addr<end; addr++) {
        if ((result=fs_client_proto_cluster_stat(client_ctx, &(*addr)->conn,
                        filter, gid_array, cs_array)) == 0)
        {
            break;
        }
    }

    return result;
}

static int stat_data_group(FSClientContext *client_ctx,
        const int data_group_id, const FSClusterStatFilter *filter,
        FSIdArray *gid_array, FSClientClusterStatEntryArray *cs_array)
{
    FSServerGroup *server_group;
    FCServerInfo **server;
    int index;
    int i;
    int result;

    if ((server_group=fs_cluster_cfg_get_server_group(client_ctx->
                    cluster_cfg.ptr, data_group_id - 1)) == NULL)
    {
        return ENOENT;
    }

    result = ENOENT;
    for (i=0; i<server_group->server_array.count; i++) {
        index = (int)(((int64_t)server_group->server_array.count *
                    (int64_t)rand()) / (int64_t)RAND_MAX);
        server = server_group->server_array.servers + index;
        if ((result=stat_data_group_by_addresses(client_ctx, filter,
                        &FS_CFG_SERVICE_ADDRESS_ARRAY(client_ctx,
                            *server), gid_array, cs_array)) == 0)
        {
            break;
        }
    }

    return result;
}

int fs_cluster_stat(FSClientContext *client_ctx, const ConnectionInfo
        *spec_conn, const FSClusterStatFilter *filter,
        FSClientClusterStatEntry *stats, const int size, int *count)
{
#define FIXED_DATA_GROUP_SIZE  1024
    int data_group_count;
    int fixed_ids[FIXED_DATA_GROUP_SIZE];
    int fixed_gids[FIXED_DATA_GROUP_SIZE];
    int *ids;
    FSIdArray gid_array;
    FSClientClusterStatEntryArray cs_array;
    int *gid;
    int *gid_end;
    int i;
    int bytes;
    int result;

    data_group_count = FS_DATA_GROUP_COUNT(*client_ctx->cluster_cfg.ptr);
    if (data_group_count <= FIXED_DATA_GROUP_SIZE) {
        gid_array.ids = fixed_gids;
        gid_array.alloc = FIXED_DATA_GROUP_SIZE;
    } else {
        bytes = sizeof(int) * data_group_count;
        gid_array.ids = (int *)fc_malloc(bytes);
        if (gid_array.ids == NULL) {
            return ENOMEM;
        }
        gid_array.alloc = data_group_count;
    }

    cs_array.stats = stats;
    cs_array.size = size;
    if (spec_conn != NULL) {
        result = fs_client_proto_cluster_stat(client_ctx,
                spec_conn, filter, &gid_array, &cs_array);
    } else if ((filter->filter_by & FS_CLUSTER_STAT_FILTER_BY_GROUP)) {
        result = stat_data_group(client_ctx, filter->data_group_id,
                filter, &gid_array, &cs_array);
    } else {
        result = -1;
    }

    if (result >= 0) {
        if (result == 0) {
            *count = cs_array.count;
        } else {
            *count = 0;
        }

        if (gid_array.ids != fixed_gids) {
            free(gid_array.ids);
        }
        return result;
    }

    if (data_group_count <= FIXED_DATA_GROUP_SIZE) {
        ids = fixed_ids;
    } else {
        bytes = sizeof(int) * data_group_count;
        ids = (int *)fc_malloc(bytes);
        if (ids == NULL) {
            return ENOMEM;
        }
    }
    for (i=0; i<data_group_count; i++) {
        ids[i] = i + 1;
    }

    result = 0;
    *count = 0;
    for (i=0; i<data_group_count; i++) {
        if (ids[i] == 0) {
            continue;
        }

        cs_array.stats = stats + *count;
        cs_array.size = size - *count;
        if ((result=stat_data_group(client_ctx, i + 1, filter,
                        &gid_array, &cs_array)) != 0)
        {
            break;
        }

        gid_end = gid_array.ids + gid_array.count;
        for (gid=gid_array.ids; gid<gid_end; gid++) {
            ids[(*gid) - 1] = 0;
        }

        *count += cs_array.count;
    }

    if (ids != fixed_ids) {
        free(ids);
    }
    if (gid_array.ids != fixed_gids) {
        free(gid_array.ids);
    }

    return result;
}

int fs_client_slice_write(FSClientContext *client_ctx,
        const FSBlockSliceKeyInfo *bs_key, const char *data,
        int *write_bytes, int *inc_alloc)
{
    const FSConnectionParameters *connection_params;
    ConnectionInfo *conn;
    IdempotencyClientChannel *old_channel;
    FSBlockSliceKeyInfo new_key;
    int result;
    int conn_result;
    int remain;
    int bytes;
    int current_alloc;
    int i;
    uint64_t req_id;
    SFNetRetryIntervalContext net_retry_ctx;

    /*
    static int64_t total_time_used = 0;
    static int64_t conn_time_used = 0;
    int64_t start_time;
    int64_t time_used;
    
    start_time = get_current_time_us();
    */

    if ((conn=client_ctx->conn_manager.get_master_connection(client_ctx,
                    FS_CLIENT_DATA_GROUP_INDEX(client_ctx,
                        bs_key->block.hash_code), &result)) == NULL)
    {
        return SF_UNIX_ERRNO(result, EIO);
    }

    //conn_time_used += get_current_time_us() - start_time;

    connection_params = client_ctx->conn_manager.get_connection_params(
            client_ctx, conn);

    sf_init_net_retry_interval_context(&net_retry_ctx,
            &client_ctx->net_retry_cfg.interval_mm,
            &client_ctx->net_retry_cfg.network);

    *inc_alloc = *write_bytes = 0;
    new_key = *bs_key;
    remain = bs_key->slice.length;


    while (remain > 0) {
        if (remain <= connection_params->buffer_size) {
            bytes = remain;
        } else {
            bytes = connection_params->buffer_size;
        }
        new_key.slice.length = bytes;

        if (client_ctx->idempotency_enabled) {
            req_id = idempotency_client_channel_next_seq_id(
                    connection_params->channel);
        } else {
            req_id = 0;
        }

        old_channel = connection_params->channel;
        i = 0;
        while (1) {
            if (client_ctx->idempotency_enabled) {
                result = idempotency_client_channel_check_wait(
                        connection_params->channel);
            } else {
                result = 0;
            }

            if (result == 0) {
                if ((result=fs_client_proto_slice_write(client_ctx, conn,
                                req_id, &new_key, data + *write_bytes,
                                &current_alloc)) == 0)
                {
                    break;
                }
            }

            conn_result = result;
            if (result == SF_RETRIABLE_ERROR_CHANNEL_INVALID &&
                    client_ctx->idempotency_enabled)
            {
                if (idempotency_client_channel_check_wait(
                            connection_params->channel) == 0)
                {
                    if ((conn_result=sf_proto_rebind_idempotency_channel(
                                    conn, connection_params->channel->id,
                                    connection_params->channel->key,
                                    client_ctx->network_timeout)) == 0)
                    {
                        continue;
                    }
                }
            }

            SF_NET_RETRY_CHECK_AND_SLEEP(net_retry_ctx, client_ctx->
                    net_retry_cfg.network.times, ++i, result);

            /*
            logInfo("file: "__FILE__", line: %d, func: %s, "
                    "net retry result: %d, retry count: %d",
                    __LINE__, __FUNCTION__, result, i);
                    */

            SF_CLIENT_RELEASE_CONNECTION(client_ctx, conn, conn_result);
            if ((conn=client_ctx->conn_manager.get_master_connection(
                            client_ctx, FS_CLIENT_DATA_GROUP_INDEX(
                                client_ctx, bs_key->block.hash_code),
                            &result)) == NULL)
            {
                return SF_UNIX_ERRNO(result, EIO);
            }

            connection_params = client_ctx->conn_manager.
                get_connection_params(client_ctx, conn);
            if (connection_params->channel != old_channel) {
                break;
            }
        }

        /*
        logInfo("slice offset: %d, slice length: %d, current offset: %d, "
                "current length: %d, result: %d, current_alloc: %d",
                bs_key->slice.offset, bs_key->slice.length, new_key.slice.offset,
                new_key.slice.length, result, current_alloc);
                */

        if (connection_params->channel != old_channel) { //master changed
            sf_reset_net_retry_interval(&net_retry_ctx);
            continue;
        }

        if (client_ctx->idempotency_enabled &&
                !SF_IS_SERVER_RETRIABLE_ERROR(result))
        {
            idempotency_client_channel_push(
                    connection_params->channel, req_id);
        }

        if (result != 0) {
            break;
        }

        *inc_alloc += current_alloc;
        *write_bytes += bytes;
        remain -= bytes;

        if (remain == 0) {
            break;
        }

        new_key.slice.offset += bytes;
        sf_reset_net_retry_interval(&net_retry_ctx);
    }

    SF_CLIENT_RELEASE_CONNECTION(client_ctx, conn, result);

    /*
    time_used = get_current_time_us() - start_time;
    total_time_used += time_used;
    fprintf(stderr, "slice offset: %d, length: %d, time used: %"PRId64" us, "
            "total time used: %"PRId64" ms, conn_time_used: %"PRId64" ms\n",
            bs_key->slice.offset, bs_key->slice.length, time_used,
            total_time_used / 1000, conn_time_used / 1000);
            */

    return SF_UNIX_ERRNO(result, EIO);
}

int fs_client_slice_read_ex(FSClientContext *client_ctx,
        const int slave_id, const int req_cmd, const int resp_cmd,
        const FSBlockSliceKeyInfo *bs_key, char *buff, int *read_bytes)
{
    ConnectionInfo *conn;
    FSBlockSliceKeyInfo new_key;
    int result;
    int remain;
    int bytes;
    int i;
    SFNetRetryIntervalContext net_retry_ctx;

    if ((conn=client_ctx->conn_manager.get_readable_connection(client_ctx,
                    FS_CLIENT_DATA_GROUP_INDEX(client_ctx, bs_key->block.
                        hash_code), &result)) == NULL)
    {
        return SF_UNIX_ERRNO(result, EIO);
    }

    sf_init_net_retry_interval_context(&net_retry_ctx,
            &client_ctx->net_retry_cfg.interval_mm,
            &client_ctx->net_retry_cfg.network);

    *read_bytes = 0;
    new_key = *bs_key;
    remain = bs_key->slice.length;
    i = 0;
    while (remain > 0) {
        if ((result=fs_client_proto_slice_read_ex(client_ctx, conn,
                        slave_id, req_cmd, resp_cmd, &new_key,
                        buff + *read_bytes, &bytes)) == 0)
        {
            *read_bytes += bytes;
            break;
        }

        SF_NET_RETRY_CHECK_AND_SLEEP(net_retry_ctx, client_ctx->
                net_retry_cfg.network.times, ++i, result);

        /*
        logInfo("file: "__FILE__", line: %d, func: %s, "
                "net retry result: %d, retry count: %d",
                __LINE__, __FUNCTION__, result, i);
                */

        SF_CLIENT_RELEASE_CONNECTION(client_ctx, conn, result);
        if ((conn=client_ctx->conn_manager.get_readable_connection(client_ctx,
                        FS_CLIENT_DATA_GROUP_INDEX(client_ctx, bs_key->block.
                            hash_code), &result)) == NULL)
        {
            break;
        }

        *read_bytes += bytes;
        remain -= bytes;
        new_key.slice.offset += bytes;
        new_key.slice.length = remain;
    }

    if (conn != NULL) {
        SF_CLIENT_RELEASE_CONNECTION(client_ctx, conn, result);
    }

    if (result == 0) {
        return *read_bytes > 0 ? 0 : ENODATA;
    } else {
        return SF_UNIX_ERRNO(result, EIO);
    }
/*
    if (*read_bytes > 0) {
        return 0;
        //return result == ENODATA ? 0 : result;
    } else {
        return  ? ENODATA : result;
    }
    */
}

#define GET_MASTER_CONNECTION(client_ctx, arg1, result)        \
    client_ctx->conn_manager.get_master_connection(client_ctx, \
            arg1, result)

#define GET_LEADER_CONNECTION(client_ctx, arg1, result)        \
    client_ctx->conn_manager.get_leader_connection(client_ctx, \
            arg1, result)

int fs_client_bs_operate(FSClientContext *client_ctx,
        const void *key, const uint32_t hash_code,
        const int req_cmd, const int resp_cmd,
        const int enoent_log_level, int *inc_alloc)
{
    const FSConnectionParameters *connection_params;

    SF_CLIENT_IDEMPOTENCY_UPDATE_WRAPPER(client_ctx, GET_MASTER_CONNECTION,
            FS_CLIENT_DATA_GROUP_INDEX(client_ctx, hash_code),
            fs_client_proto_bs_operate, key, req_cmd, resp_cmd,
            enoent_log_level, inc_alloc);
}

int fs_client_server_group_space_stat(FSClientContext *client_ctx,
        FCServerInfo *server, FSClientServerSpaceStat *stats,
        const int size, int *count)
{
    SF_CLIENT_IDEMPOTENCY_QUERY_WRAPPER(client_ctx, GET_LEADER_CONNECTION,
            server, fs_client_proto_server_group_space_stat,
            stats, size, count);
}

static int cluster_space_stat(FSClientContext *client_ctx,
        SkiplistSet *sl, FSClusterSpaceStat *stat)
{
    struct {
        FSClientServerSpaceStat stats[FS_MAX_GROUP_SERVERS];
        int count;
    } stat_array;
    int result;
    FSClientServerSpaceStat *cur;
    FSClientServerSpaceStat *end;
    FCServerInfo *server;
    FCServerInfo target;
    FSClusterSpaceStat min_stat;

    stat->total = stat->avail = stat->used = 0;
    while ((server=(FCServerInfo *)skiplist_set_get_first(sl)) != NULL) {
        if (fs_client_server_group_space_stat(client_ctx, server,
                    stat_array.stats, FS_MAX_GROUP_SERVERS,
                    &stat_array.count) != 0)
        {
            if ((result=skiplist_set_delete(sl, server)) != 0) {
                logError("file: "__FILE__", line: %d, "
                        "remove server id: %d fail, result: %d",
                        __LINE__, server->id, result);
                return result;
            }
            continue;
        }

        min_stat.total = min_stat.avail = min_stat.used = 0;
        end = stat_array.stats + stat_array.count;
        for (cur=stat_array.stats; cur<end; cur++) {
            if ((cur->stat.total > 0) && ((min_stat.total == 0) ||
                        (cur->stat.used > min_stat.used)))
            {
                min_stat.total = cur->stat.total;
                min_stat.avail = cur->stat.avail;
                min_stat.used = cur->stat.used;
            }

            target.id = cur->server_id;
            if ((result=skiplist_set_delete(sl, &target)) != 0) {
                if (cur->server_id == server->id) {
                    logError("file: "__FILE__", line: %d, "
                            "remove server id: %d fail, result: %d",
                            __LINE__, cur->server_id, result);
                    return result;
                }
            }
        }

        /*
        logInfo("file: "__FILE__", line: %d, "
                "stat server id: %d, total: %"PRId64" MB, "
                "avail: %"PRId64" MB, used: %"PRId64" MB",
                __LINE__, server->id, min_stat.total / (1024 * 1024),
                min_stat.avail / (1024 * 1024), min_stat.used / (1024 * 1024));
                */

        stat->total += min_stat.total;
        stat->avail += min_stat.avail;
        stat->used += min_stat.used;
    }

    return 0;
}

static int compare_server(const void *p1, const void *p2)
{
    return (int)((FCServerInfo *)p1)->id - (int)((FCServerInfo *)p2)->id;
}

int fs_client_cluster_space_stat(FSClientContext *client_ctx,
        FSClusterSpaceStat *stat)
{
    const int min_alloc_elements_once = 2;
    int result;
    int level_count;
    SkiplistSet sl;
    const FCServerInfoPtrArray *parray;
    FCServerInfo **pp;
    FCServerInfo **end;

    parray = fs_cluster_cfg_get_used_servers(client_ctx->cluster_cfg.ptr);
    if (parray == NULL) {
        return ENOMEM;
    }

    level_count = skiplist_get_proper_level(parray->count);
    if ((result=skiplist_set_init_ex(&sl, level_count, compare_server,
                    NULL, min_alloc_elements_once)) != 0)
    {
        return result;
    }

    end = parray->servers + parray->count;
    for (pp=parray->servers; pp<end; pp++) {
        if ((result=skiplist_set_insert(&sl, *pp)) != 0) {
            break;
        }
    }

    if (result == 0) {
        result = cluster_space_stat(client_ctx, &sl, stat);
    }
    skiplist_set_destroy(&sl);
    return result;
}
