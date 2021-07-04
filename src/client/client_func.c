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


#include <sys/stat.h>
#include "fastcommon/ini_file_reader.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fs_func.h"
#include "fs_cluster_cfg.h"
#include "client_global.h"
#include "simple_connection_manager.h"
#include "client_func.h"

static int fs_client_do_init_ex(FSClientContext *client_ctx,
        IniFullContext *ini_ctx)
{
    char *pBasePath;
    char cluster_full_filename[PATH_MAX];
    int result;

    pBasePath = iniGetStrValue(NULL, "base_path", ini_ctx->context);
    if (pBasePath == NULL) {
        strcpy(g_fs_client_vars.base_path, "/tmp");
    } else {
        snprintf(g_fs_client_vars.base_path,
                sizeof(g_fs_client_vars.base_path),
                "%s", pBasePath);
        chopPath(g_fs_client_vars.base_path);
    }

    client_ctx->common_cfg.connect_timeout = iniGetIntValueEx(
            ini_ctx->section_name, "connect_timeout",
            ini_ctx->context, SF_DEFAULT_CONNECT_TIMEOUT, true);
    if (client_ctx->common_cfg.connect_timeout <= 0) {
        client_ctx->common_cfg.connect_timeout = SF_DEFAULT_CONNECT_TIMEOUT;
    }

    client_ctx->common_cfg.network_timeout = iniGetIntValueEx(
            ini_ctx->section_name, "network_timeout",
            ini_ctx->context, SF_DEFAULT_NETWORK_TIMEOUT, true);
    if (client_ctx->common_cfg.network_timeout <= 0) {
        client_ctx->common_cfg.network_timeout = SF_DEFAULT_NETWORK_TIMEOUT;
    }

    sf_load_read_rule_config(&client_ctx->common_cfg.read_rule, ini_ctx);

    if ((result=fs_cluster_cfg_load_from_ini_ex1(client_ctx->
                    cluster_cfg.ptr, ini_ctx,cluster_full_filename,
                    sizeof(cluster_full_filename))) != 0)
    {
        return result;
    }

    if ((result=sf_load_net_retry_config(&client_ctx->
                    common_cfg.net_retry_cfg, ini_ctx)) != 0)
    {
        return result;
    }

    if ((result=fcfs_auth_load_config(&client_ctx->auth,
                    cluster_full_filename)) != 0)
    {
        return result;
    }

    return 0;
}

void fs_client_log_config_ex(FSClientContext *client_ctx,
        const char *extra_config, const bool log_base_path)
{
    char base_path_output[PATH_MAX];
    char net_retry_output[256];

    if (log_base_path) {
        snprintf(base_path_output, sizeof(base_path_output),
                "base_path: %s, ", g_fs_client_vars.base_path);
    } else {
        *base_path_output = '\0';
    }

    sf_net_retry_config_to_string(&client_ctx->common_cfg.net_retry_cfg,
            net_retry_output, sizeof(net_retry_output));
    logInfo("FastStore v%d.%d.%d, %s"
            "connect_timeout: %d, "
            "network_timeout: %d, "
            "read_rule: %s, %s, "
            "server group count: %d, "
            "data group count: %d%s%s",
            g_fs_global_vars.version.major,
            g_fs_global_vars.version.minor,
            g_fs_global_vars.version.patch,
            base_path_output,
            client_ctx->common_cfg.connect_timeout,
            client_ctx->common_cfg.network_timeout,
            sf_get_read_rule_caption(client_ctx->common_cfg.read_rule),
            net_retry_output,
            FS_SERVER_GROUP_COUNT(*client_ctx->cluster_cfg.ptr),
            FS_DATA_GROUP_COUNT(*client_ctx->cluster_cfg.ptr),
            extra_config != NULL ? ", " : "",
            extra_config != NULL ? extra_config : "");

    //fs_cluster_cfg_to_log(client_ctx->cluster_cfg.ptr);
}

int fs_client_load_from_file_ex1(FSClientContext *client_ctx,
        FCFSAuthClientContext *auth_ctx, IniFullContext *ini_ctx)
{
    IniContext iniContext;
    int result;

    client_ctx->auth.ctx = auth_ctx;
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
        client_ctx->cluster_cfg.group_index = client_ctx->
            cluster_cfg.ptr->service_group_index;
    }
    return result;
}

int fs_client_init_ex1(FSClientContext *client_ctx,
        FCFSAuthClientContext *auth_ctx, IniFullContext *ini_ctx,
        const SFConnectionManager *cm, const bool bg_thread_enabled)
{
    int result;

    client_ctx->cluster_cfg.ptr = &client_ctx->cluster_cfg.holder;
    if ((result=fs_client_load_from_file_ex1(client_ctx,
                    auth_ctx, ini_ctx)) != 0)
    {
        return result;
    }

    if (cm == NULL) {
        if ((result=fs_simple_connection_manager_init(client_ctx,
                        &client_ctx->cm, bg_thread_enabled)) != 0)
        {
            return result;
        }
        client_ctx->is_simple_conn_mananger = true;
    } else if (cm != &client_ctx->cm) {
        client_ctx->cm = *cm;
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
        fs_simple_connection_manager_destroy(&client_ctx->cm);
    }
    memset(client_ctx, 0, sizeof(FSClientContext));
}
