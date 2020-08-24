#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include "fastcommon/logger.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "binlog_func.h"
#include "binlog_reader.h"
#include "slice_binlog.h"
#include "replica_binlog.h"
#include "binlog_check.h"

static int get_last_timestamp(const char *subdir_name,
        const int binlog_index, time_t *timestamp)
{
    char filename[PATH_MAX];
    int result;

    binlog_reader_get_filename(subdir_name, binlog_index,
            filename, sizeof(filename));
    if ((result=binlog_get_last_timestamp(filename, timestamp)) == ENOENT) {
        *timestamp = 0;
        result = 0;
    }

    return result; 
}

static int get_replica_last_timestamp(time_t *last_timestamp)
{
    FSIdArray *id_array;
    int data_group_id;
    int last_index;
    int i;
    int result;
    time_t timestamp;
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];

    if ((id_array=fs_cluster_cfg_get_my_data_group_ids(&CLUSTER_CONFIG_CTX,
                    CLUSTER_MYSELF_PTR->server->id)) == NULL)
    {
        return ENOENT;
    }

    for (i=0; i<id_array->count; i++) {
        data_group_id = id_array->ids[i];
        sprintf(subdir_name, "%s/%d", FS_REPLICA_BINLOG_SUBDIR_NAME,
                data_group_id);

        last_index = replica_binlog_get_current_write_index(data_group_id);
        if ((result=get_last_timestamp(subdir_name,
                        last_index, &timestamp)) != 0)
        {
            return result;
        }

        if (timestamp > *last_timestamp) {
            *last_timestamp = timestamp;
        }
    }

    return 0;
}

static int binlog_check_get_last_timestamp(time_t *last_timestamp)
{
    int last_index;
    int result;
    time_t timestamp;

    *last_timestamp = 0;

    last_index = slice_binlog_get_current_write_index();
    if ((result=get_last_timestamp(FS_SLICE_BINLOG_SUBDIR_NAME,
                    last_index, &timestamp)) != 0)
    {
        return result;
    }

    if (timestamp > *last_timestamp) {
        *last_timestamp = timestamp;
    }

    logInfo("last_timestamp1: %"PRId64, (int64_t)*last_timestamp);
    return get_replica_last_timestamp(last_timestamp);
}

/*
        struct binlog_writer_info *replica_binlog_get_writer(
            const int data_group_id);

            int binlog_reader_init_ex(ServerBinlogReader *reader, const char *subdir_name,
        const char *fname_suffix, struct binlog_writer_info *writer,
        const FSBinlogFilePosition *pos);
 */


int binlog_consistency_check(int *flags)
{
    int result;
    time_t last_timestamp;

    *flags = 0;
    if ((result=binlog_check_get_last_timestamp(&last_timestamp)) != 0) {
        return result;
    }

    logInfo("last_timestamp2: %"PRId64, (int64_t)last_timestamp);
    if (last_timestamp == 0) {
        return 0;
    }

    return 0;
}
