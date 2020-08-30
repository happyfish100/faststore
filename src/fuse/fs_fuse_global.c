#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include "fastcommon/common_define.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "fsapi/fs_api.h"
#include "fs_fuse_wrapper.h"
#include "fs_fuse_global.h"

#define INI_FUSE_SECTION_NAME  "FUSE"

FUSEGlobalVars g_fuse_global_vars;

static int load_fuse_config(IniFullContext *ini_ctx)
{
    string_t mountpoint;
    string_t ns;
    char *allow_others;

    ns.str = iniGetStrValue(FS_API_DEFAULT_FASTDIR_SECTION_NAME,
            "namespace", ini_ctx->context);
    if (ns.str == NULL || *ns.str == '\0') {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, section: %s, item: namespace "
                "not exist or is empty", __LINE__, ini_ctx->filename,
                FS_API_DEFAULT_FASTDIR_SECTION_NAME);
        return ENOENT;
    }

    ini_ctx->section_name = INI_FUSE_SECTION_NAME;
    mountpoint.str = iniGetStrValue(ini_ctx->section_name,
            "mountpoint", ini_ctx->context);
    if (mountpoint.str == NULL || *mountpoint.str == '\0') {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, section: %s, item: mountpoint "
                "not exist or is empty", __LINE__, ini_ctx->filename,
                ini_ctx->section_name);
        return ENOENT;
    }

    ns.len = strlen(ns.str);
    mountpoint.len = strlen(mountpoint.str);
    g_fuse_global_vars.ns = fc_malloc(ns.len + mountpoint.len + 2);
    if (g_fuse_global_vars.ns == NULL) {
        return ENOMEM;
    }
    memcpy(g_fuse_global_vars.ns, ns.str, ns.len + 1);
    g_fuse_global_vars.mountpoint = g_fuse_global_vars.ns + ns.len + 1;
    memcpy(g_fuse_global_vars.mountpoint, mountpoint.str, mountpoint.len + 1);

    g_fuse_global_vars.max_idle_threads = iniGetIntValue(ini_ctx->
            section_name, "max_idle_threads", ini_ctx->context, 10);

    g_fuse_global_vars.singlethread = iniGetBoolValue(ini_ctx->
            section_name, "singlethread", ini_ctx->context, false);

    g_fuse_global_vars.clone_fd = iniGetBoolValue(ini_ctx->
            section_name, "clone_fd", ini_ctx->context, false);

    g_fuse_global_vars.auto_unmount = iniGetBoolValue(ini_ctx->
            section_name, "auto_unmount", ini_ctx->context, true);

    allow_others = iniGetStrValue(ini_ctx->section_name,
            "allow_others", ini_ctx->context);
    if (allow_others == NULL || *allow_others == '\0') {
        g_fuse_global_vars.allow_others = allow_none;
    } else if (strcasecmp(allow_others, "all") == 0) {
        g_fuse_global_vars.allow_others = allow_all;
    } else if (strcasecmp(allow_others, "root") == 0) {
        g_fuse_global_vars.allow_others = allow_root;
    } else {
        g_fuse_global_vars.allow_others = allow_none;
    }
    return 0;
}

int fs_fuse_global_init(const char *config_filename)
{
    const bool load_network_params = false;
    int result;
    IniContext iniContext;
    IniFullContext ini_ctx;

    if ((result=iniLoadFromFile(config_filename, &iniContext)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load conf file \"%s\" fail, ret code: %d",
                __LINE__, config_filename, result);
        return result;
    }

    FAST_INI_SET_FULL_CTX_EX(ini_ctx, config_filename,
            FS_API_DEFAULT_FASTDIR_SECTION_NAME, &iniContext);
    do {
        if ((result=sf_load_global_config_ex("fs_fused", config_filename,
                        &iniContext, load_network_params)) != 0)
        {
            break;
        }

        if ((result=load_fuse_config(&ini_ctx)) != 0) {
            break;
        }

        if ((result=fs_api_pooled_init1(g_fuse_global_vars.ns,
                        &ini_ctx)) != 0)
        {
            break;
        }
    } while (0);

    iniFreeContext(&iniContext);
    if (result != 0) {
        return result;
    }

    logInfo("FUSE library version %s", fuse_pkgversion());
    return 0;
}
