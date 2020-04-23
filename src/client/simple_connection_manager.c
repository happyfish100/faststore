#include <sys/stat.h>
#include <limits.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "client_global.h"
#include "client_func.h"
#include "client_proto.h"
#include "simple_connection_manager.h"

static ConnectionInfo *get_spec_connection(FSClientContext *client_ctx,
        const ConnectionInfo *target, int *err_no)
{
    return conn_pool_get_connection((ConnectionPool *)client_ctx->
            conn_manager.args, target, err_no);
}

static ConnectionInfo *make_connection(FSClientContext *client_ctx,
        FCAddressPtrArray *addr_array, int *err_no)
{
    FCAddressInfo **current;
    FCAddressInfo **addr;
    FCAddressInfo **end;
    ConnectionInfo *conn;

    if (addr_array->count <= 0) {
        *err_no = ENOENT;
        return NULL;
    }

    current = addr_array->addrs + addr_array->index;
    if ((conn=get_spec_connection(client_ctx, &(*current)->conn,
                    err_no)) != NULL)
    {
        return conn;
    }

    if (addr_array->count == 1) {
        return NULL;
    }

    end = addr_array->addrs + addr_array->count;
    for (addr=addr_array->addrs; addr<end; addr++) {
        if (addr == current) {
            continue;
        }

        if ((conn=get_spec_connection(client_ctx, &(*addr)->conn,
                        err_no)) != NULL)
        {
            addr_array->index = addr - addr_array->addrs;
            return conn;
        }
    }

    return NULL;
}

static ConnectionInfo *get_connection(FSClientContext *client_ctx,
        const uint32_t *hash_codes, int *err_no)
{
    FCServerInfoPtrArray *server_ptr_array;
    FCServerInfo *server;
    ConnectionInfo *conn;
    FCAddressPtrArray *addr_array;
    int data_group_index;
    int server_index;
    int i;

    data_group_index = hash_codes[FS_BLOCK_HASH_CODE_INDEX_DATA_GROUP] %
        FS_DATA_GROUP_COUNT(client_ctx->cluster_cfg);

    server_ptr_array = &client_ctx->cluster_cfg.data_groups.mappings
        [data_group_index].server_group->server_array;
    server_index = hash_codes[FS_BLOCK_HASH_CODE_INDEX_SERVER] %
        server_ptr_array->count;
    server = server_ptr_array->servers[server_index];

    addr_array = &FS_CFG_SERVICE_ADDRESS_ARRAY(client_ctx, server);
    if ((conn=make_connection(client_ctx, addr_array, err_no)) != NULL) {
        return conn;
    }
    
    if (server_ptr_array->count > 1) {
        for (i=0; i<server_ptr_array->count; i++) {
            if (i == server_index) {
                continue;
            }

            addr_array = &FS_CFG_SERVICE_ADDRESS_ARRAY(client_ctx,
                    server_ptr_array->servers[i]);
            if ((conn=make_connection(client_ctx, addr_array,
                            err_no)) != NULL)
            {
                return conn;
            }
        }
    }

    logError("file: "__FILE__", line: %d, "
            "data group index: %d, get_connection fail, "
            "configured server count: %d", __LINE__,
            data_group_index, server_ptr_array->count);
    return NULL;
}

static void release_connection(FSClientContext *client_ctx,
        ConnectionInfo *conn)
{
    conn_pool_close_connection_ex((ConnectionPool *)client_ctx->
            conn_manager.args, conn, false);
}

static void close_connection(FSClientContext *client_ctx,
        ConnectionInfo *conn)
{
    conn_pool_close_connection_ex((ConnectionPool *)client_ctx->
            conn_manager.args, conn, true);
}

int fs_simple_connection_manager_init_ex(FSConnectionManager *conn_manager,
        const int max_count_per_entry, const int max_idle_time)
{
    ConnectionPool *cp;
    int result;

    cp = (ConnectionPool *)malloc(sizeof(ConnectionPool));
    if (cp == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__,
                (int)sizeof(ConnectionPool));
        return ENOMEM;
    }

    if ((result=conn_pool_init(cp, g_client_global_vars.connect_timeout,
                    max_count_per_entry, max_idle_time)) != 0)
    {
        return result;
    }

    conn_manager->args = cp;
    conn_manager->get_connection = get_connection;
    conn_manager->get_spec_connection = get_spec_connection;
    conn_manager->release_connection = release_connection;
    conn_manager->close_connection = close_connection;
    return 0;
}

void fs_simple_connection_manager_destroy(FSConnectionManager *conn_manager)
{
    ConnectionPool *cp;

    if (conn_manager->args != NULL) {
        cp = (ConnectionPool *)conn_manager->args;
        conn_pool_destroy(cp);
        free(cp);
        conn_manager->args = NULL;
    }
}
