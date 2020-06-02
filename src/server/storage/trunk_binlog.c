#include <limits.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "../dio/trunk_io_thread.h"
#include "../binlog/binlog_writer.h"
#include "../binlog/binlog_loader.h"
#include "storage_allocator.h"
#include "trunk_id_info.h"
#include "trunk_binlog.h"

#define TRUNK_BINLOG_MAX_RECORD_SIZE   128
#define TRUNK_BINLOG_SUBDIR_NAME      "trunk"

static BinlogWriterContext binlog_writer = {NULL, NULL, 0, 0};

#define TRUNK_GET_FILENAME_LINE_COUNT(r, binlog_filename, \
        line_str, line_count) \
        BINLOG_GET_FILENAME_LINE_COUNT(r, TRUNK_BINLOG_SUBDIR_NAME, \
        binlog_filename, line_str, line_count)

#define TRUNK_PARSE_INT_EX(var, caption, index, endchr, min_val) \
    BINLOG_PARSE_INT_EX(TRUNK_BINLOG_SUBDIR_NAME, var, caption,  \
            index, endchr, min_val)

#define TRUNK_PARSE_INT(var, index, endchr, min_val)  \
    BINLOG_PARSE_INT_EX(TRUNK_BINLOG_SUBDIR_NAME, var, #var, \
            index, endchr, min_val)

static int trunk_parse_line(BinlogReadThreadResult *r,
        string_t *line, char *line_end)
{
#define MAX_FIELD_COUNT     8
#define EXPECT_FIELD_COUNT  6
#define FIELD_INDEX_TIMESTAMP   0
#define FIELD_INDEX_OP_TYPE     1
#define FIELD_INDEX_PATH_INDEX  2
#define FIELD_INDEX_TRUNK_ID    3
#define FIELD_INDEX_SUBDIR      4
#define FIELD_INDEX_TRUNK_SIZE  5

    int result;
    int count;
    int64_t line_count;
    string_t cols[MAX_FIELD_COUNT];
    char binlog_filename[PATH_MAX];
    char *endptr;
    char op_type;
    int path_index;
    FSTrunkIdInfo id_info;
    int64_t trunk_size;

    count = split_string_ex(line, ' ', cols,
            MAX_FIELD_COUNT, false);
    if (count < EXPECT_FIELD_COUNT) {
        TRUNK_GET_FILENAME_LINE_COUNT(r, binlog_filename,
                line->str, line_count);
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", "
                "field count: %d < %d", __LINE__,
                binlog_filename, line_count,
                count, EXPECT_FIELD_COUNT);
        return EINVAL;
    }

    op_type = cols[FIELD_INDEX_OP_TYPE].str[0];
    TRUNK_PARSE_INT(path_index, FIELD_INDEX_PATH_INDEX, ' ', 0);
    if (path_index > STORAGE_CFG.max_store_path_index) {
        TRUNK_GET_FILENAME_LINE_COUNT(r, binlog_filename,
                line->str, line_count);
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", "
                "invalid path_index: %d > max_store_path_index: %d",
                __LINE__, binlog_filename, line_count,
                path_index, STORAGE_CFG.max_store_path_index);
        return EINVAL;
    }

    TRUNK_PARSE_INT_EX(id_info.id, "trunk_id", FIELD_INDEX_TRUNK_ID, ' ', 1);
    TRUNK_PARSE_INT_EX(id_info.subdir, "subdir", FIELD_INDEX_SUBDIR, ' ', 1);
    TRUNK_PARSE_INT(trunk_size, FIELD_INDEX_TRUNK_SIZE,
            '\n', FS_TRUNK_FILE_MIN_SIZE);
    if (trunk_size > FS_TRUNK_FILE_MAX_SIZE) {
        TRUNK_GET_FILENAME_LINE_COUNT(r, binlog_filename,
                line->str, line_count);
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", "
                "invalid trunk size: %"PRId64, __LINE__,
                binlog_filename, line_count, trunk_size);
        return EINVAL;
    }

    if (op_type == FS_IO_TYPE_CREATE_TRUNK) {
       if ((result=storage_allocator_add_trunk(path_index,
                       &id_info, trunk_size)) != 0)
       {
           return result;
       }
    } else if (op_type == FS_IO_TYPE_DELETE_TRUNK) {
        if ((result=storage_allocator_delete_trunk(path_index,
                        &id_info)) != 0)
        {
            return result;
        }
    } else {
        TRUNK_GET_FILENAME_LINE_COUNT(r, binlog_filename,
                line->str, line_count);
        logError("file: "__FILE__", line: %d, "
                "binlog file %s, line no: %"PRId64", "
                "invalid op_type: %c (0x%02x)", __LINE__,
                binlog_filename, line_count,
                op_type, (unsigned char)op_type);
        return EINVAL;
    }

    return 0;
}

int trunk_binlog_get_current_write_index()
{
    return binlog_get_current_write_index(&binlog_writer);
}

static int init_binlog_writer()
{
    return binlog_writer_init(&binlog_writer, TRUNK_BINLOG_SUBDIR_NAME,
            TRUNK_BINLOG_MAX_RECORD_SIZE);
}

int trunk_binlog_init()
{
    int result;
    if ((result=init_binlog_writer()) != 0) {
        return result;
    }

    return binlog_loader_load(TRUNK_BINLOG_SUBDIR_NAME,
                    trunk_binlog_get_current_write_index,
                    trunk_parse_line);
}

void trunk_binlog_destroy()
{
    binlog_writer_finish(&binlog_writer);
}

int trunk_binlog_write(const char op_type, const int path_index,
        const FSTrunkIdInfo *id_info, const int64_t file_size)
{
    BinlogWriterBuffer *wbuffer;

    if ((wbuffer=binlog_writer_alloc_buffer(&binlog_writer)) == NULL) {
        return ENOMEM;
    }

    wbuffer->bf.length = sprintf(wbuffer->bf.buff,
            "%d %c %d %"PRId64" %"PRId64" %"PRId64"\n",
            (int)g_current_time, op_type, path_index, id_info->id,
            id_info->subdir, file_size);
    push_to_binlog_write_queue(&binlog_writer, wbuffer);
    return 0;
}
