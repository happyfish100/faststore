#include "fs_client.h"

int fs_unlink_file(FSClientContext *client_ctx, const int64_t oid,
        const int64_t file_size)
{
    FSBlockKey bkey;
    int64_t remain;
    int result;
    int dec_alloc;

    remain = file_size;
    fs_set_block_key(&bkey, oid, 0);
    while (1) {
        logInfo("block {oid: %"PRId64", offset: %"PRId64"}",
                bkey.oid, bkey.offset);

        result = fs_client_proto_block_delete(client_ctx, &bkey, &dec_alloc);
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
        const int data_group_id, FCAddressPtrArray *addr_ptr_array,
        FSClientClusterStatEntry *stats, const int size, int *count)
{
    FCAddressInfo **addr;
    FCAddressInfo **end;
    int result;

    result = ENOENT;
    end = addr_ptr_array->addrs + addr_ptr_array->count;
    for (addr=addr_ptr_array->addrs; addr<end; addr++) {
        if ((result=fs_client_proto_cluster_stat(client_ctx, &(*addr)->conn,
                        data_group_id, stats, size, count)) == 0)
        {
            break;
        }
    }

    return result;
}

static int stat_data_group(FSClientContext *client_ctx,
        const int data_group_id, FSClientClusterStatEntry *stats,
        const int size, int *count)
{
    FSServerGroup *server_group;
    FCServerInfo **server;
    FCServerInfo **end;
    int result;

    if ((server_group=fs_cluster_cfg_get_server_group(&client_ctx->cluster_cfg,
                    data_group_id - 1)) == NULL)
    {
        return ENOENT;
    }

    result = ENOENT;
    end = server_group->server_array.servers + server_group->server_array.count;
    for (server=server_group->server_array.servers; server<end; server++) {
        if ((result=stat_data_group_by_addresses(client_ctx, data_group_id,
                        &FS_CFG_SERVICE_ADDRESS_ARRAY(client_ctx, *server),
                        stats, size, count)) == 0)
        {
            break;
        }
    }

    return result;
}

int fs_cluster_stat(FSClientContext *client_ctx,
        const int data_group_id, FSClientClusterStatEntry *stats,
        const int size, int *count)
{
    int data_group_count;
    int id;
    int n;
    int result;

    if (data_group_id > 0) {
        return stat_data_group(client_ctx, data_group_id, stats, size, count);
    }

    *count = 0;
    data_group_count = FS_DATA_GROUP_COUNT(client_ctx->cluster_cfg);
    for (id=1; id<=data_group_count; id++) {
        if ((result=stat_data_group(client_ctx, id, stats + *count,
                        size - *count, &n)) != 0)
        {
            return result;
        }

        *count += n;
    }

    return 0;
}
