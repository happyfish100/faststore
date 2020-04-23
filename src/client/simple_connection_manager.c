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

static ConnectionInfo *get_connection(FSClientContext *client_ctx,
        const uint32_t *hash_codes, int *err_no)
{
    /*
    int index;
    int i;
    ConnectionInfo *server;
    ConnectionInfo *conn;

    index = rand() % client_ctx->server_group.count;
    server = client_ctx->server_group.servers + index;
    if ((conn=get_spec_connection(client_ctx, server->ip_addr,
                    server->port, err_no)) != NULL)
    {
        return conn;
    }

    i = (index + 1) % client_ctx->server_group.count;
    while (i != index) {
        server = client_ctx->server_group.servers + i;
        if ((conn=get_spec_connection(client_ctx, server->ip_addr,
                        server->port, err_no)) != NULL)
        {
            return conn;
        }

        i = (i + 1) % client_ctx->server_group.count;
    }

    logError("file: "__FILE__", line: %d, "
            "get_connection fail, configured server count: %d",
            __LINE__, client_ctx->server_group.count);
            */
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
