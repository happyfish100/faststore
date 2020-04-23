
#include <sys/stat.h>
#include "fastcommon/ini_file_reader.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fs_cluster_cfg.h"
#include "client_global.h"
#include "client_func.h"

static int fs_client_do_init_ex(FSClientContext *client_ctx,
        const char *conf_filename, IniContext *iniContext)
{
    char *pBasePath;
    int result;

    pBasePath = iniGetStrValue(NULL, "base_path", iniContext);
    if (pBasePath == NULL) {
        strcpy(g_client_global_vars.base_path, "/tmp");
    } else {
        snprintf(g_client_global_vars.base_path,
                sizeof(g_client_global_vars.base_path),
                "%s", pBasePath);
        chopPath(g_client_global_vars.base_path);
        if (!fileExists(g_client_global_vars.base_path)) {
            logError("file: "__FILE__", line: %d, "
                "\"%s\" can't be accessed, error info: %s",
                __LINE__, g_client_global_vars.base_path,
                STRERROR(errno));
            return errno != 0 ? errno : ENOENT;
        }
        if (!isDir(g_client_global_vars.base_path)) {
            logError("file: "__FILE__", line: %d, "
                "\"%s\" is not a directory!",
                __LINE__, g_client_global_vars.base_path);
            return ENOTDIR;
        }
    }

    g_client_global_vars.connect_timeout = iniGetIntValue(NULL,
            "connect_timeout", iniContext, DEFAULT_CONNECT_TIMEOUT);
    if (g_client_global_vars.connect_timeout <= 0) {
        g_client_global_vars.connect_timeout = DEFAULT_CONNECT_TIMEOUT;
    }

    g_client_global_vars.network_timeout = iniGetIntValue(NULL,
            "network_timeout", iniContext, DEFAULT_NETWORK_TIMEOUT);
    if (g_client_global_vars.network_timeout <= 0) {
        g_client_global_vars.network_timeout = DEFAULT_NETWORK_TIMEOUT;
    }

    if ((result=fs_cluster_cfg_load_from_ini(&client_ctx->cluster_cfg,
                    iniContext, conf_filename)) != 0)
    {
        return result;
    }

#ifdef DEBUG_FLAG
    logDebug("FastStore v%d.%02d, "
            "base_path: %s, "
            "connect_timeout: %d, "
            "network_timeout: %d, "
            "server group count: %d, "
            "data group count: %d",
            g_fs_global_vars.version.major,
            g_fs_global_vars.version.minor,
            g_client_global_vars.base_path,
            g_client_global_vars.connect_timeout,
            g_client_global_vars.network_timeout,
            FS_SERVER_GROUP_COUNT(client_ctx->cluster_cfg),
            FS_DATA_GROUP_COUNT(client_ctx->cluster_cfg));
#endif

    fs_cluster_cfg_to_log(&client_ctx->cluster_cfg);
    return 0;
}

int fs_client_load_from_file_ex(FSClientContext *client_ctx,
        const char *conf_filename)
{
    IniContext iniContext;
    int result;

    if ((result=iniLoadFromFile(conf_filename, &iniContext)) != 0) {
        logError("file: "__FILE__", line: %d, "
            "load conf file \"%s\" fail, ret code: %d",
            __LINE__, conf_filename, result);
        return result;
    }

    result = fs_client_do_init_ex(client_ctx, conf_filename,
                &iniContext);
    iniFreeContext(&iniContext);

    if (result == 0) {
        client_ctx->inited = true;
    }
    return result;
}

int fs_client_init_ex(FSClientContext *client_ctx,
        const char *conf_filename, const FSConnectionManager *conn_manager)
{
    int result;
    if ((result=fs_client_load_from_file_ex(
                    client_ctx, conf_filename)) != 0)
    {
        return result;
    }

    if (conn_manager == NULL) {
        /*
        if ((result=fs_simple_connection_manager_init(
                        &client_ctx->conn_manager)) != 0)
        {
            return result;
        }
        */
        client_ctx->is_simple_conn_mananger = true;
    } else if (conn_manager != &client_ctx->conn_manager) {
        client_ctx->conn_manager = *conn_manager;
        client_ctx->is_simple_conn_mananger = false;
    } else {
        client_ctx->is_simple_conn_mananger = false;
    }

    srand(time(NULL));
    return 0;
}

void fs_client_destroy_ex(FSClientContext *client_ctx)
{
    if (!client_ctx->inited) {
        return;
    }

    /*
    if (client_ctx->is_simple_conn_mananger) {
        fs_simple_connection_manager_destroy(&client_ctx->conn_manager);
    }
    */
    memset(client_ctx, 0, sizeof(FSClientContext));
}
