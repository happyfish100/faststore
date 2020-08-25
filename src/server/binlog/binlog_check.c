#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include "fastcommon/logger.h"
#include "fastcommon/shared_func.h"
#include "sf/sf_global.h"
#include "../../common/fs_func.h"
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

int binlog_consistency_init(BinlogConsistencyContext *ctx)
{
    FSIdArray *id_array;
    int min_dg_id;
    int max_dg_id;
    int bytes;

    memset(ctx, 0, sizeof(*ctx));
    if ((id_array=fs_cluster_cfg_get_my_data_group_ids(&CLUSTER_CONFIG_CTX,
                    CLUSTER_MYSELF_PTR->server->id)) == NULL)
    {
        return ENOENT;
    }

    min_dg_id = fs_cluster_cfg_get_min_data_group_id(id_array);
    max_dg_id = fs_cluster_cfg_get_max_data_group_id(id_array);
    ctx->positions.dg_count = max_dg_id - min_dg_id + 1;
    bytes = sizeof(FSBinlogFilePosition) * ctx->positions.dg_count;
    ctx->positions.replicas = fc_malloc(bytes);
    if (ctx->positions.replicas == NULL) {
        return ENOMEM;
    }
    memset(ctx->positions.replicas, 0, bytes);
    ctx->positions.base_dg_id = min_dg_id;
    return 0;
}

void binlog_consistency_destroy(BinlogConsistencyContext *ctx)
{
    if (ctx->version_arrays.replica.versions != NULL) {
        free(ctx->version_arrays.replica.versions);
        ctx->version_arrays.replica.versions = NULL;
    }

    if (ctx->version_arrays.slice.versions != NULL) {
        free(ctx->version_arrays.slice.versions);
        ctx->version_arrays.slice.versions = NULL;
    }

    if (ctx->positions.replicas != NULL) {
        free(ctx->positions.replicas);
        ctx->positions.replicas = NULL;
    }
}

static int check_alloc_version_array(BinlogDataGroupVersionArray *array)
{
    BinlogDataGroupVersion *versions;
    int64_t new_alloc;
    int64_t bytes;

    if (array->alloc > array->count) {
        return 0;
    }

    new_alloc = (array->alloc > 0) ? 2 * array->alloc : 8192;
    bytes = sizeof(BinlogDataGroupVersion) * new_alloc;
    versions = (BinlogDataGroupVersion *)fc_malloc(bytes);
    if (versions == NULL) {
        return ENOMEM;
    }

    if (array->versions != NULL) {
        if (array->count > 0) {
            memcpy(versions, array->versions, array->count *
                    sizeof(BinlogDataGroupVersion));
        }
        free(array->versions);
    }

    array->alloc = new_alloc;
    array->versions = versions;
    return 0;
}

static int binlog_parse_buffer(ServerBinlogReader *reader, const int length,
        const int obj_filed_skip, BinlogDataGroupVersionArray *varray)
{
    int result;
    string_t line;
    char *buff;
    char *line_start;
    char *buff_end;
    char *line_end;
    BinlogDataGroupVersion *dg_version;
    BinlogCommonFields fields;
    char error_info[256];

    result = 0;
    buff = reader->binlog_buffer.buff;
    line_start = buff;
    buff_end = buff + length;
    while (line_start < buff_end) {
        line_end = (char *)memchr(line_start, '\n', buff_end - line_start);
        if (line_end == NULL) {
            result = EINVAL;
            sprintf(error_info, "expect line end char (\\n)");
            break;
        }

        line.str = line_start;
        line.len = line_end - line_start;
        if ((result=binlog_unpack_common_fields_ex(&line, obj_filed_skip,
                        &fields, error_info)) != 0)
        {
            break;
        }

        fs_calc_block_hashcode(&fields.bkey);
        if ((result=check_alloc_version_array(varray)) != 0) {
            break;
        }

        if (!BINLOG_IS_INTERNAL_RECORD(fields.op_type, fields.data_version)) {
            dg_version = varray->versions + varray->count++;
            dg_version->data_version = fields.data_version;
            dg_version->data_group_id = FS_DATA_GROUP_ID(fields.bkey);
        }

        line_start = line_end + 1;
    }

    if (result != 0) {
        int64_t file_offset;
        int64_t line_count;
        int remain_bytes;

        remain_bytes = length - (line_start - buff);
        file_offset = reader->position.offset - remain_bytes;
        fc_get_file_line_count_ex(reader->filename,
                file_offset, &line_count);
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", %s",
                __LINE__, reader->filename, line_count, error_info);
    }
    return result;
}

static int do_load_data_versions(const char *subdir_name,
        const int obj_filed_skip,
        struct binlog_writer_info *writer, const time_t from_timestamp,
        FSBinlogFilePosition *pos, BinlogDataGroupVersionArray *varray)
{
    int result;
    int read_bytes;
    ServerBinlogReader reader;

    if ((result=binlog_get_position_by_timestamp(subdir_name,
                    writer, from_timestamp, pos)) != 0)
    {
        return result;
    }

    if ((result=binlog_reader_init(&reader, subdir_name, writer, pos)) != 0) {
        return result;
    }

    {
        int64_t line_count;

        fc_get_file_line_count_ex(reader.filename,
                reader.position.offset, &line_count);

        logInfo("head -n %"PRId64" %s, index: %d, offset: %"PRId64,
                line_count + 1, reader.filename, pos->index, pos->offset); 
    }

    while ((result=binlog_reader_integral_read(&reader,
                    reader.binlog_buffer.buff,
                    reader.binlog_buffer.size,
                    &read_bytes)) == 0)
    {
        if ((result=binlog_parse_buffer(&reader, read_bytes,
                        obj_filed_skip, varray)) != 0)
        {
            break;
        }
    }

    if (result == ENOENT) {
        result = 0;
    }
    logInfo("subdir_name: %s, index: %d, offset: %"PRId64", count: %"PRId64,
            subdir_name, pos->index, pos->offset, varray->count);

    binlog_reader_destroy(&reader);
    return result;
}

static int compare_dg_version(const BinlogDataGroupVersion *p1,
        const BinlogDataGroupVersion *p2)
{
    int64_t sub;

    sub = p1->data_group_id - p2->data_group_id;
    if (sub < 0) {
        return -1;
    } else if (sub > 0) {
        return 1;
    }

    sub = p1->data_version - p2->data_version;
    if (sub < 0) {
        return -1;
    } else if (sub == 0) {
        return 0;
    } else {
        return 1;
    }
}

static inline void sort_version_array(BinlogDataGroupVersionArray *varray)
{
    if (varray->count > 1) {
        qsort(varray->versions, varray->count,
                sizeof(BinlogDataGroupVersion),
                (int (*)(const void *, const void *))
                compare_dg_version);
    }
}

static int binlog_load_data_versions(BinlogConsistencyContext *ctx,
        const time_t from_timestamp)
{
    int result;
    int data_group_id;
    int index;
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];
    FSBinlogFilePosition *replica;
    FSBinlogFilePosition *end;

    end = ctx->positions.replicas + ctx->positions.dg_count;
    for (replica=ctx->positions.replicas; replica<end; replica++) {
        index = replica - ctx->positions.replicas;
        data_group_id = ctx->positions.base_dg_id + index;
        sprintf(subdir_name, "%s/%d", FS_REPLICA_BINLOG_SUBDIR_NAME,
                data_group_id);
        if ((result=do_load_data_versions(subdir_name, 0,
                        replica_binlog_get_writer(data_group_id),
                        from_timestamp, replica,
                        &ctx->version_arrays.replica)) != 0)
        {
            return result;
        }
    }

    if ((result=do_load_data_versions(FS_SLICE_BINLOG_SUBDIR_NAME, 1,
                    slice_binlog_get_writer(), from_timestamp, &ctx->
                    positions.slice, &ctx->version_arrays.slice)) != 0)
    {
        return result;
    }

    sort_version_array(&ctx->version_arrays.replica);
    sort_version_array(&ctx->version_arrays.slice);
    return 0;
}

static int binlog_check(BinlogConsistencyContext *ctx,
        const time_t from_timestamp, int *flags)
{
    int result;
    int compr;
    BinlogDataGroupVersion *rv;
    BinlogDataGroupVersion *rend;
    BinlogDataGroupVersion *sv;
    BinlogDataGroupVersion *send;

    if ((result=binlog_load_data_versions(ctx, from_timestamp)) != 0) {
        return result;
    }

    rend = ctx->version_arrays.replica.versions +
        ctx->version_arrays.replica.count;
    send = ctx->version_arrays.slice.versions +
        ctx->version_arrays.slice.count;
    rv = ctx->version_arrays.replica.versions;
    sv = ctx->version_arrays.slice.versions;
    while (rv < rend) {
        if (sv == send) {
            break;
        }

        compr = compare_dg_version(rv, sv);
        if (compr < 0) {
            *flags |= BINLOG_CHECK_RESULT_REPLICA_DIRTY;
            if (*flags == BINLOG_CHECK_RESULT_ALL_DIRTY) {
                return 0;
            }
            rv++;
        } else if (compr == 0) {
            rv++;
            sv++;
        } else {
            *flags |= BINLOG_CHECK_RESULT_SLICE_DIRTY;
            if (*flags == BINLOG_CHECK_RESULT_ALL_DIRTY) {
                return 0;
            }
            sv++;
        }
    }

    if (rv < rend) {
        *flags |= BINLOG_CHECK_RESULT_REPLICA_DIRTY;
    } else if (sv < send) {
        *flags |= BINLOG_CHECK_RESULT_SLICE_DIRTY;
    }
    return 0;
}

int binlog_consistency_check(BinlogConsistencyContext *ctx, int *flags)
{
    int result;
    time_t last_timestamp;
    time_t from_timestamp;

    *flags = 0;
    if ((result=binlog_check_get_last_timestamp(&last_timestamp)) != 0) {
        return result;
    }

    if (last_timestamp <= 0) {
        return 0;
    }

    from_timestamp = last_timestamp - BINLOG_CHECK_LAST_SECONDS + 1;

    logInfo("last_timestamp2: %"PRId64", from_timestamp: %"PRId64,
            (int64_t)last_timestamp, (int64_t)from_timestamp);
    return binlog_check(ctx, from_timestamp, flags);
}
