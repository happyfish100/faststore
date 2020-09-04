#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include "fastcommon/common_define.h"

#ifdef OS_LINUX
#include <sys/vfs.h>
#include <sys/statfs.h>
#include <linux/magic.h>

#define unmount umount2

#elif defined(OS_FREEBSD)
#endif

#include <sys/param.h>
#include <sys/mount.h>
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "fsapi/fs_api.h"
#include "fs_fuse_wrapper.h"
#include "fs_fuse_global.h"

#ifndef FUSE_SUPER_MAGIC
#define FUSE_SUPER_MAGIC 0x65735546
#endif

#define INI_FUSE_SECTION_NAME  "FUSE"

FUSEGlobalVars g_fuse_global_vars;

static int load_fuse_mountpoint(IniFullContext *ini_ctx, string_t *mountpoint)
{
    struct statfs buf;
    int result;

    mountpoint->str = iniGetStrValue(ini_ctx->section_name,
            "mountpoint", ini_ctx->context);
    if (mountpoint->str == NULL || *mountpoint->str == '\0') {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, section: %s, item: mountpoint "
                "not exist or is empty", __LINE__, ini_ctx->filename,
                ini_ctx->section_name);
        return ENOENT;
    }
    if (!fileExists(mountpoint->str)) {
        result = errno != 0 ? errno : ENOENT;
        if (result == ENOTCONN) {
            if (unmount(mountpoint->str, 0) == 0) {
                result = 0;
            } else if (errno == EPERM) {
                logError("file: "__FILE__", line: %d, "
                        "unmount %s fail, you should run "
                        "\"sudo umount %s\" manually", __LINE__,
                        mountpoint->str,mountpoint->str);
            }
        }

        if (result != 0) {
            logError("file: "__FILE__", line: %d, "
                    "mountpoint: %s can't be accessed, "
                    "errno: %d, error info: %s",
                    __LINE__, mountpoint->str,
                    result, STRERROR(result));
            return result;
        }
    }
    if (!isDir(mountpoint->str)) {
        logError("file: "__FILE__", line: %d, "
                "mountpoint: %s is not a directory!",
                __LINE__, mountpoint->str);
        return ENOTDIR;
    }

    if (statfs(mountpoint->str, &buf) != 0) {
        logError("file: "__FILE__", line: %d, "
                "statfs mountpoint: %s fail, error info: %s",
                __LINE__, mountpoint->str, STRERROR(errno));
        return errno != 0 ? errno : ENOENT;
    }

    if ((buf.f_type & FUSE_SUPER_MAGIC) == FUSE_SUPER_MAGIC) {
        logError("file: "__FILE__", line: %d, "
                "mountpoint: %s already mounted by FUSE",
                __LINE__, mountpoint->str);
        return EEXIST;
    }

    mountpoint->len = strlen(mountpoint->str);
    return 0;
}

static int load_fuse_config(IniFullContext *ini_ctx)
{
    string_t mountpoint;
    string_t ns;
    char *allow_others;
    int result;

    ns.str = iniGetStrValue(FS_API_DEFAULT_FASTDIR_SECTION_NAME,
            "namespace", ini_ctx->context);
    if (ns.str == NULL || *ns.str == '\0') {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, section: %s, item: namespace "
                "not exist or is empty", __LINE__, ini_ctx->filename,
                FS_API_DEFAULT_FASTDIR_SECTION_NAME);
        return ENOENT;
    }
    ns.len = strlen(ns.str);

    ini_ctx->section_name = INI_FUSE_SECTION_NAME;
    if ((result=load_fuse_mountpoint(ini_ctx, &mountpoint)) != 0) {
        return result;
    }

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

static const char *get_allow_others_caption(
        const FUSEAllowOthersMode allow_others)
{
    switch (allow_others) {
        case allow_all:
            return "all";
        case allow_root:
            return "root";
        default:
            return "";
    }
}

int fs_fuse_global_init(const char *config_filename)
{
    const bool load_network_params = false;
    int result;
    string_t base_path;
    string_t mountpoint;
    IniContext iniContext;
    IniFullContext ini_ctx;
    int64_t inode;

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

        FC_SET_STRING(base_path, SF_G_BASE_PATH);
        FC_SET_STRING(mountpoint, g_fuse_global_vars.mountpoint);
        if (fc_path_contains(&base_path, &mountpoint, &result)) {
            logError("file: "__FILE__", line: %d, "
                    "config file: %s, base path: %s contains mountpoint: %s, "
                    "this case is not allowed", __LINE__, config_filename,
                    SF_G_BASE_PATH, g_fuse_global_vars.mountpoint);
            result = EINVAL;
            break;
        } else if (result != 0) {
            logError("file: "__FILE__", line: %d, "
                    "config file: %s, base path: %s or mountpoint: %s "
                    "is invalid", __LINE__, config_filename,
                    SF_G_BASE_PATH, g_fuse_global_vars.mountpoint);
            break;
        }

        if ((result=fs_api_pooled_init1(g_fuse_global_vars.ns,
                        &ini_ctx)) != 0)
        {
            break;
        }

        if ((result=fsapi_lookup_inode("/", &inode)) != 0) {
            if (result == ENOENT) {
                FDIRDEntryFullName fullname;
                FDIRDEntryInfo dentry;

                FC_SET_STRING(fullname.ns, g_fuse_global_vars.ns);
                FC_SET_STRING(fullname.path, "/");
                result = fdir_client_create_dentry(g_fs_api_ctx.contexts.fdir,
                        &fullname, 0775 | S_IFDIR, &dentry);
            }
        }

    } while (0);

    iniFreeContext(&iniContext);
    if (result != 0) {
        return result;
    }

    logInfo("FastStore V%d.%02d, FUSE library version %s, "
            "FastDIR namespace: %s, FUSE mountpoint: %s, "
            "singlethread: %d, clone_fd: %d, max_idle_threads: %d, "
            "allow_others: %s, auto_unmount: %d",
            g_fs_global_vars.version.major,
            g_fs_global_vars.version.minor,
            fuse_pkgversion(), g_fuse_global_vars.ns,
            g_fuse_global_vars.mountpoint, g_fuse_global_vars.singlethread,
            g_fuse_global_vars.clone_fd, g_fuse_global_vars.max_idle_threads,
            get_allow_others_caption(g_fuse_global_vars.allow_others),
            g_fuse_global_vars.auto_unmount);
    return 0;
}
