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

#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fc_atomic.h"
#include "../../common/fs_func.h"
#include "binlog_func.h"
#include "replica_binlog.h"
#include "replica_clean.h"

static int check_last_binlog(const int data_group_id, const int last_index)
{
    int result;
    char binlog_filename[PATH_MAX];
    struct stat stbuf;
    time_t first_timestamp;
    time_t last_timestamp;

    replica_binlog_get_filename(data_group_id, last_index,
            binlog_filename, sizeof(binlog_filename));
    if ((result=binlog_get_first_timestamp(binlog_filename,
                    &first_timestamp)) != 0)
    {
        return result;
    }
    if ((result=binlog_get_last_timestamp(binlog_filename,
                    &last_timestamp)) != 0)
    {
        return result;
    }

    if ((last_timestamp - first_timestamp) < 4 *
            (LOCAL_BINLOG_CHECK_LAST_SECONDS + 1))
    {
        return ECANCELED;
    }

    if (stat(binlog_filename, &stbuf) != 0) {
        return (errno != 0 ? errno : EPERM);
    }

    if (stbuf.st_size < 4 * (FS_REPLICA_BINLOG_MAX_RECORD_SIZE *
                (SLAVE_BINLOG_CHECK_LAST_ROWS + 1)))
    {
        return ECANCELED;
    }

    return (stbuf.st_size >= g_sf_global_vars.net_buffer_cfg.
            max_buff_size ? 0 : ECANCELED);
}

static int remove_old_binlogs(const int data_group_id,
        const time_t before_time, int *remove_count)
{
    int result;
    int start_index;
    int last_index;
    int binlog_index;
    time_t last_timestamp;
    char binlog_filename[PATH_MAX];

    *remove_count = 0;
    if ((result=replica_binlog_get_binlog_indexes(data_group_id,
                    &start_index, &last_index)) != 0)
    {
        return result;
    }

    /*
    logInfo("data_group_id: %d, binlog start_index: %d, last_index: %d",
            data_group_id, start_index, last_index);
            */

    for (binlog_index=start_index; binlog_index<last_index; binlog_index++) {
        replica_binlog_get_filename(data_group_id, binlog_index,
                binlog_filename, sizeof(binlog_filename));
        if ((result=binlog_get_last_timestamp(binlog_filename,
                        &last_timestamp)) != 0)
        {
            return result;
        }

        if (last_timestamp >= before_time) {
            break;
        }

        if (binlog_index + 1 == last_index && check_last_binlog(
                    data_group_id, last_index) != 0)
        {
            break;
        }

        if ((result=replica_binlog_set_binlog_start_index(
                        data_group_id, binlog_index + 1)) != 0)
        {
            return result;
        }
        if ((result=fc_delete_file_ex(binlog_filename,
                        "replica binlog")) != 0)
        {
            return result;
        }

        (*remove_count)++;
    }

    return 0;
}

static int clean_binlogs(int *total_remove_count)
{
    FSIdArray *id_array;
    time_t before_time;
    struct tm tm;
    int data_group_id;
    int remove_count;
    int i;
    int result;

    result = ENOENT;
    *total_remove_count = 0;
    if ((id_array=fs_cluster_cfg_get_my_data_group_ids(&CLUSTER_CONFIG_CTX,
                    CLUSTER_MYSELF_PTR->server->id)) == NULL)
    {
        return result;
    }

    before_time = time(NULL) - REPLICA_KEEP_DAYS * 86400;
    localtime_r(&before_time, &tm);
    tm.tm_hour = 0;
    tm.tm_min = 0;
    tm.tm_sec = 0;
    before_time = mktime(&tm);

    for (i=0; i<id_array->count; i++) {
        data_group_id = id_array->ids[i];
        if ((result=remove_old_binlogs(data_group_id,
                        before_time, &remove_count)) != 0)
        {
            break;
        }

        *total_remove_count += remove_count;
    }

    return result;
}

static int replica_clean_func(void *args)
{
    static volatile bool clean_in_progress = false;
    int result;
    int total_remove_count;
    int64_t start_time_ms;
    int64_t time_used;
    char time_buff[32];

    if (!clean_in_progress) {
        clean_in_progress = true;

        start_time_ms = get_current_time_ms();
        logInfo("file: "__FILE__", line: %d, "
                "clean replica binlogs ...", __LINE__);

        result = clean_binlogs(&total_remove_count);
        time_used = get_current_time_ms() - start_time_ms;
        logInfo("file: "__FILE__", line: %d, "
                "clean replica binlogs %s, remove binlog count: %d, "
                "time used: %s ms", __LINE__, (result == 0 ? "success" :
                    "fail"), total_remove_count,
                long_to_comma_str(time_used, time_buff));

        clean_in_progress = false;
    }

    return 0;
}

int replica_clean_add_schedule()
{
    ScheduleArray scheduleArray;
    ScheduleEntry scheduleEntry;

    if (REPLICA_KEEP_DAYS <= 0) {
        return 0;
    }

    INIT_SCHEDULE_ENTRY_EX1(scheduleEntry, sched_generate_next_id(),
            REPLICA_DELETE_TIME, 86400, replica_clean_func, NULL, true);
    scheduleArray.entries = &scheduleEntry;
    scheduleArray.count = 1;
    return sched_add_entries(&scheduleArray);
}
