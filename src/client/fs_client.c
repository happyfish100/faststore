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
        const int data_group_id, const int only_this_group,
        FSClientClusterStatEntry *stats, const int size, int *count)
{
    FSServerGroup *server_group;
    FCServerInfo **server;
    FCServerInfo **end;
    int new_group_id;
    int result;

    if ((server_group=fs_cluster_cfg_get_server_group(&client_ctx->cluster_cfg,
                    data_group_id - 1)) == NULL)
    {
        return ENOENT;
    }

    new_group_id = only_this_group ? data_group_id : 0;
    result = ENOENT;
    end = server_group->server_array.servers + server_group->server_array.count;
    for (server=server_group->server_array.servers; server<end; server++) {
        if ((result=stat_data_group_by_addresses(client_ctx, new_group_id,
                        &FS_CFG_SERVICE_ADDRESS_ARRAY(client_ctx, *server),
                        stats, size, count)) == 0)
        {
            break;
        }
    }

    return result;
}

int fs_cluster_stat(FSClientContext *client_ctx, const int data_group_id,
        FSClientClusterStatEntry *stats, const int size, int *count)
{
#define FIXED_DATA_GROUP_SIZE  1024
    int data_group_count;
    int fixed_ids[FIXED_DATA_GROUP_SIZE];
    int *ids;
    FSClientClusterStatEntry *stat;
    FSClientClusterStatEntry *end;
    int i;
    int n;
    int bytes;
    int result;

    if (data_group_id > 0) {
        return stat_data_group(client_ctx, data_group_id, true,
                stats, size, count);
    }

    data_group_count = FS_DATA_GROUP_COUNT(client_ctx->cluster_cfg);
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

    *count = 0;
    for (i=0; i<data_group_count; i++) {
        if (ids[i] == 0) {
            continue;
        }

        stat = stats + *count;
        if ((result=stat_data_group(client_ctx, i + 1, false,
                        stat, size - *count, &n)) != 0)
        {
            break;
        }

        end = stat + n;
        while (stat < end) {
            ids[stat->data_group_id - 1] = 0;
            stat++;
        }

        *count += n;
    }

    if (ids != fixed_ids) {
        free(ids);
    }

    return result;
}
