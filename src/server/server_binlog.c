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

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <fcntl.h>
#include <pthread.h>
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "server_global.h"
#include "server_binlog.h"
#include "binlog/binlog_check.h"
#include "binlog/binlog_repair.h"
#include "rebuild/store_path_rebuild.h"
#include "replication/replication_quorum.h"
#include "storage/slice_space_log.h"

static int do_binlog_check()
{
    int flags;
    int result;
    BinlogConsistencyContext ctx;

    if ((result=binlog_consistency_repair_finish()) != 0) {
        return result;
    }

    if (LOCAL_BINLOG_CHECK_LAST_SECONDS <= 0) {
        return 0;
    }

    if ((result=binlog_consistency_init(&ctx)) != 0) {
        return result;
    }

    if ((result=binlog_consistency_check(&ctx, &flags)) == 0) {
        if ((flags & BINLOG_CHECK_RESULT_REPLICA_DIRTY)) {
            result = binlog_consistency_repair_replica(&ctx);
        }
        if (result == 0 && (flags & BINLOG_CHECK_RESULT_SLICE_DIRTY)) {
            result = binlog_consistency_repair_slice(&ctx);
        }

        if (result == 0) {
            logInfo("binlog_consistency_check from_timestamp: %d, "
                    "replica count: %"PRId64", slice count: %"PRId64", "
                    "flags: %d", (int)ctx.from_timestamp,
                    ctx.version_arrays.replica.count,
                    ctx.version_arrays.slice.count, flags);
        }
    }

    binlog_consistency_destroy(&ctx);
    return result;
}

#ifdef FS_DUMP_SLICE_FOR_DEBUG
static int dump_slice_index()
{
    int result;
    int64_t total_slice_count;
    int64_t start_time_ms;
    int64_t end_time_ms;
    char time_buff[32];
    char filepath[PATH_MAX];
    char filename[PATH_MAX];

    logInfo("file: "__FILE__", line: %d, "
            "dump slices to file for debug ...", __LINE__);

    start_time_ms = get_current_time_ms();
    snprintf(filepath, sizeof(filepath), "%s/dump", DATA_PATH_STR);
    if ((result=fc_check_mkdir(filepath, 0755)) != 0) {
        return result;
    }

    snprintf(filename, sizeof(filename), "%s/slice.index", filepath);
    if ((result=ob_index_dump_slice_index_to_file(filename,
                    &total_slice_count)) == 0)
    {
        end_time_ms = get_current_time_ms();
        long_to_comma_str(end_time_ms - start_time_ms, time_buff);
        logInfo("file: "__FILE__", line: %d, "
                "dump %"PRId64" slices to file %s, time used: %s ms",
                __LINE__, total_slice_count, filename, time_buff);
    }

    return result;
}
#endif

int server_binlog_init()
{
    int result;

    if ((result=slice_binlog_init()) != 0) {
        return result;
    }

    if ((result=replica_binlog_init()) != 0) {
        return result;
    }

    if ((result=slice_dedup_redo()) != 0) {
        return result;
    }

    if ((result=slice_binlog_migrate_redo()) != 0) {
        return result;
    }

    if ((result=slice_binlog_get_last_sn_from_file()) != 0) {
        return result;
    }

    if ((result=slice_space_log_init()) != 0) {
        return result;
    }

    if ((result=committed_version_init(SLICE_BINLOG_SN)) != 0) {
        return result;
    }

    if ((result=store_path_rebuild_redo_step1()) != 0) {
        return result;
    }

    if ((result=migrate_clean_redo()) != 0) {
        return result;
    }

    if ((result=replication_quorum_init()) != 0) {
        return result;
    }

    if ((result=do_binlog_check()) != 0) {
        return result;
    }

    if ((result=slice_binlog_load()) != 0) {
        return result;
    }

#ifdef FS_DUMP_SLICE_FOR_DEBUG
    if ((result=dump_slice_index()) != 0) {
        return result;
    }
#endif

    return 0;
}

void server_binlog_destroy()
{
    slice_binlog_destroy();
    replica_binlog_destroy();
}
