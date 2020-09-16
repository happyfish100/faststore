
#include <sys/stat.h>
#include "fastcommon/ini_file_reader.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "sf/sf_global.h"
#include "sf/sf_service.h"
#include "sf/sf_nio.h"
#include "sf/idempotency/client/client_channel.h"
#include "sf/idempotency/client/receipt_handler.h"
#include "fs_func.h"
#include "fs_cluster_cfg.h"
#include "client_global.h"
#include "simple_connection_manager.h"
#include "client_func.h"

static int fs_client_do_init_ex(FSClientContext *client_ctx,
        IniFullContext *ini_ctx)
{
    char *pBasePath;
    char net_retry_output[256];
    int result;

    pBasePath = iniGetStrValue(NULL, "base_path", ini_ctx->context);
    if (pBasePath == NULL) {
        strcpy(g_fs_client_vars.base_path, "/tmp");
    } else {
        snprintf(g_fs_client_vars.base_path,
                sizeof(g_fs_client_vars.base_path),
                "%s", pBasePath);
        chopPath(g_fs_client_vars.base_path);
        if (!fileExists(g_fs_client_vars.base_path)) {
            logError("file: "__FILE__", line: %d, "
                "\"%s\" can't be accessed, error info: %s",
                __LINE__, g_fs_client_vars.base_path,
                STRERROR(errno));
            return errno != 0 ? errno : ENOENT;
        }
        if (!isDir(g_fs_client_vars.base_path)) {
            logError("file: "__FILE__", line: %d, "
                "\"%s\" is not a directory!",
                __LINE__, g_fs_client_vars.base_path);
            return ENOTDIR;
        }
    }

    client_ctx->connect_timeout = iniGetIntValueEx(
            ini_ctx->section_name, "connect_timeout",
            ini_ctx->context, DEFAULT_CONNECT_TIMEOUT, true);
    if (client_ctx->connect_timeout <= 0) {
        client_ctx->connect_timeout = DEFAULT_CONNECT_TIMEOUT;
    }

    client_ctx->network_timeout = iniGetIntValueEx(
            ini_ctx->section_name, "network_timeout",
            ini_ctx->context, DEFAULT_NETWORK_TIMEOUT, true);
    if (client_ctx->network_timeout <= 0) {
        client_ctx->network_timeout = DEFAULT_NETWORK_TIMEOUT;
    }

    sf_load_read_rule_config(&client_ctx->read_rule, ini_ctx);

    if ((result=fs_cluster_cfg_load_from_ini_ex1(client_ctx->
                    cluster_cfg.ptr, ini_ctx)) != 0)
    {
        return result;
    }

    if ((result=sf_load_net_retry_config(&client_ctx->
                    net_retry_cfg, ini_ctx)) != 0)
    {
        return result;
    }

    sf_net_retry_config_to_string(&client_ctx->net_retry_cfg,
            net_retry_output, sizeof(net_retry_output));

    logDebug("FastStore v%d.%02d, "
            "base_path: %s, "
            "connect_timeout: %d, "
            "network_timeout: %d, "
            "read_rule: %s, %s, "
            "server group count: %d, "
            "data group count: %d",
            g_fs_global_vars.version.major,
            g_fs_global_vars.version.minor,
            g_fs_client_vars.base_path,
            client_ctx->connect_timeout,
            client_ctx->network_timeout,
            sf_get_read_rule_caption(client_ctx->read_rule),
            net_retry_output,
            FS_SERVER_GROUP_COUNT(*client_ctx->cluster_cfg.ptr),
            FS_DATA_GROUP_COUNT(*client_ctx->cluster_cfg.ptr));

    fs_cluster_cfg_to_log(client_ctx->cluster_cfg.ptr);
    return 0;
}

int fs_client_load_from_file_ex1(FSClientContext *client_ctx,
        IniFullContext *ini_ctx)
{
    IniContext iniContext;
    int result;

    if (ini_ctx->context == NULL) {
        if ((result=iniLoadFromFile(ini_ctx->filename, &iniContext)) != 0) {
            logError("file: "__FILE__", line: %d, "
                    "load conf file \"%s\" fail, ret code: %d",
                    __LINE__, ini_ctx->filename, result);
            return result;
        }
        ini_ctx->context = &iniContext;
    }

    result = fs_client_do_init_ex(client_ctx, ini_ctx);

    if (ini_ctx->context == &iniContext) {
        iniFreeContext(&iniContext);
        ini_ctx->context = NULL;
    }

    if (result == 0) {
        client_ctx->inited = true;
    }
    return result;
}

int fs_client_init_ex1(FSClientContext *client_ctx, IniFullContext *ini_ctx,
        const FSConnectionManager *conn_manager)
{
    int result;

    client_ctx->cluster_cfg.ptr = &client_ctx->cluster_cfg.holder;
    if ((result=fs_client_load_from_file_ex1(client_ctx, ini_ctx)) != 0) {
        return result;
    }

    if (conn_manager == NULL) {
        if ((result=fs_simple_connection_manager_init(client_ctx,
                        &client_ctx->conn_manager)) != 0)
        {
            return result;
        }
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

    if (client_ctx->is_simple_conn_mananger) {
        fs_simple_connection_manager_destroy(&client_ctx->conn_manager);
    }
    memset(client_ctx, 0, sizeof(FSClientContext));
}

int fs_client_rpc_idempotency_reporter_start()
{
    int result;

    if ((result=receipt_handler_init()) != 0) {
        return result;
    }

    sf_enable_thread_notify(true);
    sf_set_remove_from_ready_list(false);
    fc_sleep_ms(100);

    return 0;
}
