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
    char *p;

    if ((id_array=fs_cluster_cfg_get_my_data_group_ids(&CLUSTER_CONFIG_CTX,
                    CLUSTER_MYSELF_PTR->server->id)) == NULL)
    {
        return ENOENT;
    }

    for (i=0; i<id_array->count; i++) {
        data_group_id = id_array->ids[i];
        p = subdir_name;
        memcpy(p, FS_REPLICA_BINLOG_SUBDIR_NAME_STR,
                FS_REPLICA_BINLOG_SUBDIR_NAME_LEN);
        p += FS_REPLICA_BINLOG_SUBDIR_NAME_LEN;
        *p++ = '/';
        p += fc_itoa(data_group_id, p);
        *p = '\0';
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
    if ((result=get_last_timestamp(FS_SLICE_BINLOG_SUBDIR_NAME_STR,
                    last_index, &timestamp)) != 0)
    {
        return result;
    }

    if (timestamp > *last_timestamp) {
        *last_timestamp = timestamp;
    }

    return get_replica_last_timestamp(last_timestamp);
}

int binlog_consistency_init(BinlogConsistencyContext *ctx)
{
    FSIdArray *id_array;
    int bytes;
    int *id;
    ReplicaBinlogFilePosition *replica;
    ReplicaBinlogFilePosition *end;

    memset(ctx, 0, sizeof(*ctx));
    if ((id_array=fs_cluster_cfg_get_my_data_group_ids(&CLUSTER_CONFIG_CTX,
                    CLUSTER_MYSELF_PTR->server->id)) == NULL)
    {
        return ENOENT;
    }

    ctx->positions.dg_count = id_array->count;
    bytes = sizeof(ReplicaBinlogFilePosition) * ctx->positions.dg_count;
    ctx->positions.replicas = fc_malloc(bytes);
    if (ctx->positions.replicas == NULL) {
        return ENOMEM;
    }
    memset(ctx->positions.replicas, 0, bytes);

    end = ctx->positions.replicas + ctx->positions.dg_count;
    for (replica=ctx->positions.replicas, id=id_array->ids;
            replica<end; replica++, id++)
    {
        replica->data_group_id = *id;
    }

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

static int binlog_parse_buffer(ServerBinlogReader *reader,
        const int length, const time_t from_timestamp,
        binlog_unpack_common_fields_func unpack_common_fields,
        BinlogDataGroupVersionArray *varray)
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

    *error_info = '\0';
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
        line.len = ++line_end - line_start;
        if ((result=unpack_common_fields(&line, &fields, error_info)) != 0) {
            break;
        }

        if ((fields.timestamp >= from_timestamp) &&
                FS_BINLOG_CHECKED_BY_SOURCE(fields.source))
        {
            CALC_BLOCK_HASHCODE(&fields.bkey);
            if ((result=check_alloc_version_array(varray)) != 0) {
                sprintf(error_info, "out of memory");
                break;
            }

            dg_version = varray->versions + varray->count++;
            dg_version->data_version = fields.data_version;
            dg_version->data_group_id = FS_DATA_GROUP_ID(fields.bkey);
        }

        line_start = line_end;
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
        SFBinlogWriterInfo *writer, const time_t from_timestamp,
        SFBinlogFilePosition *pos, binlog_unpack_common_fields_func
        unpack_common_fields, BinlogDataGroupVersionArray *varray)
{
    int result;
    int read_bytes;
    ServerBinlogReader reader;

    if ((result=binlog_get_position_by_timestamp(subdir_name,
                    writer, from_timestamp - 1, pos)) != 0)
    {
        return result;
    }

    if ((result=binlog_reader_init(&reader, subdir_name, writer, pos)) != 0) {
        return result;
    }

    while ((result=binlog_reader_integral_read(&reader,
                    reader.binlog_buffer.buff,
                    reader.binlog_buffer.size,
                    &read_bytes)) == 0)
    {
        if ((result=binlog_parse_buffer(&reader, read_bytes, from_timestamp,
                        unpack_common_fields, varray)) != 0)
        {
            break;
        }
    }

    if (result == ENOENT) {
        result = 0;
    }

    binlog_reader_destroy(&reader);
    return result;
}

int binlog_compare_dg_version(const BinlogDataGroupVersion *p1,
        const BinlogDataGroupVersion *p2)
{
    int64_t sub;

    sub = (int)p1->data_group_id - (int)p2->data_group_id;
    if (sub < 0) {
        return -1;
    } else if (sub > 0) {
        return 1;
    }

    return fc_compare_int64(p1->data_version, p2->data_version);
}

static inline void sort_version_array(BinlogDataGroupVersionArray *varray)
{
    if (varray->count > 1) {
        qsort(varray->versions, varray->count,
                sizeof(BinlogDataGroupVersion),
                (int (*)(const void *, const void *))
                binlog_compare_dg_version);
    }
}

static int binlog_load_data_versions(BinlogConsistencyContext *ctx)
{
    int result;
    char subdir_name[FS_BINLOG_SUBDIR_NAME_SIZE];
    ReplicaBinlogFilePosition *replica;
    ReplicaBinlogFilePosition *end;
    char *p;

    end = ctx->positions.replicas + ctx->positions.dg_count;
    for (replica=ctx->positions.replicas; replica<end; replica++) {
        p = subdir_name;
        memcpy(p, FS_REPLICA_BINLOG_SUBDIR_NAME_STR,
                FS_REPLICA_BINLOG_SUBDIR_NAME_LEN);
        p += FS_REPLICA_BINLOG_SUBDIR_NAME_LEN;
        *p++ = '/';
        p += fc_itoa(replica->data_group_id, p);
        *p = '\0';
        if ((result=do_load_data_versions(subdir_name,
                        replica_binlog_get_writer(replica->data_group_id),
                        ctx->from_timestamp, &replica->position,
                        binlog_unpack_replica_common_fields,
                        &ctx->version_arrays.replica)) != 0)
        {
            return result;
        }
    }

    if ((result=do_load_data_versions(FS_SLICE_BINLOG_SUBDIR_NAME_STR,
                    slice_binlog_get_writer(), ctx->from_timestamp,
                    &ctx->positions.slice,
                    binlog_unpack_slice_common_fields,
                    &ctx->version_arrays.slice)) != 0)
    {
        return result;
    }

    sort_version_array(&ctx->version_arrays.replica);
    sort_version_array(&ctx->version_arrays.slice);
    return 0;
}

#define SET_BINLOG_CHECK_FLAGS(f) \
    do { \
        if ((*flags & f) == 0) {  \
            *flags |= f;  \
            if (*flags == BINLOG_CHECK_RESULT_ALL_DIRTY) {  \
                return 0;  \
            }  \
        }  \
    } while (0)

static int binlog_check(BinlogConsistencyContext *ctx, int *flags)
{
    int result;
    int compr;
    BinlogDataGroupVersion *rv;
    BinlogDataGroupVersion *rend;
    BinlogDataGroupVersion *sv;
    BinlogDataGroupVersion *send;
    BinlogDataGroupVersion *current;

    if ((result=binlog_load_data_versions(ctx)) != 0) {
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

        compr = binlog_compare_dg_version(rv, sv);
        if (compr < 0) {
            SET_BINLOG_CHECK_FLAGS(BINLOG_CHECK_RESULT_REPLICA_DIRTY);
            rv++;
        } else if (compr == 0) {
            current = rv;
            do {
                rv++;
            } while (rv < rend && binlog_compare_dg_version(rv, current) == 0);

            do {
                sv++;
            } while (sv < send && binlog_compare_dg_version(sv, current) == 0);
        } else {
            SET_BINLOG_CHECK_FLAGS(BINLOG_CHECK_RESULT_SLICE_DIRTY);
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

    *flags = 0;
    if ((result=binlog_check_get_last_timestamp(&last_timestamp)) != 0) {
        return result;
    }

    if (last_timestamp <= 0) {
        ctx->from_timestamp = 0;
        return 0;
    }

    ctx->from_timestamp = last_timestamp - LOCAL_BINLOG_CHECK_LAST_SECONDS + 1;
    return binlog_check(ctx, flags);
}
