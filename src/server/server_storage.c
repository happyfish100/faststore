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

#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "diskallocator/store_path_index.h"
#include "binlog/slice_binlog.h"
#include "server_global.h"
#include "server_storage.h"

static int set_data_rebuild_path_index()
{
    int result;
    int child_count;
    char rebuild_path[PATH_MAX];
    DAStorePathEntry *pentry;

    if (DATA_REBUILD_PATH_STR == NULL) {
        DATA_REBUILD_PATH_INDEX = -1;
        return 0;
    }

    if ((result=fc_remove_redundant_slashes2(DATA_REBUILD_PATH_STR,
                    rebuild_path, sizeof(rebuild_path))) != 0)
    {
        DATA_REBUILD_PATH_INDEX = -1;
        return result;
    }

    if ((pentry=da_store_path_index_get(&DA_CTX, rebuild_path)) == NULL) {
        logError("file: "__FILE__", line: %d, "
                "data rebuild path: %s not exist in storage.conf",
                __LINE__, rebuild_path);
        DATA_REBUILD_PATH_INDEX = -1;
        return ENOENT;
    }

    child_count = fc_get_path_child_count(rebuild_path);
    if (child_count < 0) {
        DATA_REBUILD_PATH_INDEX = -1;
        return errno != 0 ? errno : EPERM;
    }

    if (child_count > 1) {
        logError("file: "__FILE__", line: %d, "
                "data rebuild path: %s not empty, child count: %d",
                __LINE__, rebuild_path, child_count);
        DATA_REBUILD_PATH_INDEX = -1;
        return ENOTEMPTY;
    }

    DATA_REBUILD_PATH_INDEX = pentry->index;
    return 0;
}

static bool fs_slice_load_done()
{
    return SLICE_LOAD_DONE;
}

static int storage_init()
{
    const bool have_extra_field = true;
    const bool destroy_store_path_index = false;
    const bool migrate_path_mark_filename = true;
    int result;

    if ((result=da_global_init(CLUSTER_MY_SERVER_ID)) != 0) {
        return result;
    }

    if ((result=da_load_config_ex(&DA_CTX, "[faststore]", FILE_BLOCK_SIZE,
                    &DATA_CFG, STORAGE_FILENAME, have_extra_field,
                    destroy_store_path_index, migrate_path_mark_filename)) != 0)
    {
        return result;
    }

    if ((result=set_data_rebuild_path_index()) != 0) {
        return result;
    }
    da_store_path_index_destroy(&DA_CTX);

    if ((result=da_init_start_ex(&DA_CTX, fs_slice_load_done,
                    slice_migrate_done_callback,
                    trunk_migrate_done_callback,
                    slice_binlog_cached_slice_write_done,
                    DATA_REBUILD_PATH_INDEX)) != 0)
    {
        return result;
    }

    return 0;
}

static int slice_storage_engine_init()
{
    int result;

    if ((result=block_serializer_init()) != 0) {
        return result;
    }

    if ((result=change_notify_init()) != 0) {
        return result;
    }

    if ((result=STORAGE_ENGINE_START_API()) != 0) {
        return result;
    }

    if ((result=event_dealer_init()) != 0) {
        return result;
    }

    return 0;
}

int server_storage_init()
{
    int result;

    if ((result=ob_index_init()) != 0) {
        return result;
    }

    if ((result=storage_init()) != 0) {
        return result;
    }

    if (STORAGE_ENABLED) {
        if ((result=slice_storage_engine_init()) != 0) {
            return result;
        }
    }

    return 0;
}

void server_storage_destroy()
{
}
