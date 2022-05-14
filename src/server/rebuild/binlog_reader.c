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
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "sf/sf_func.h"
#include "../../common/fs_func.h"
#include "../binlog/slice_binlog.h"
#include "../storage/slice_op.h"
#include "../server_global.h"
#include "rebuild_binlog.h"
#include "binlog_reader.h"

#define BINLOG_POSITION_FILENAME  "position.mark"

#define BINLOG_POSITION_ITEM_NAME_INDEX   "index"
#define BINLOG_POSITION_ITEM_NAME_OFFSET  "offset"

static const char *get_position_filename(const char *subdir_name,
        char *filename, const int size)
{
    snprintf(filename, size, "%s/%s/%s", DATA_PATH_STR,
            subdir_name, BINLOG_POSITION_FILENAME);
    return filename;
}

int rebuild_binlog_reader_save_position(ServerBinlogReader *reader)
{
    char filename[PATH_MAX];
    char buff[256];
    int result;
    int len;

    get_position_filename(reader->subdir_name,
            filename, sizeof(filename));
    len = sprintf(buff, "%s=%d\n"
            "%s=%"PRId64"\n",
            BINLOG_POSITION_ITEM_NAME_INDEX, reader->position.index,
            BINLOG_POSITION_ITEM_NAME_OFFSET, reader->position.offset);
    if ((result=safeWriteToFile(filename, buff, len)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "write to file \"%s\" fail, errno: %d, error info: %s",
                __LINE__, filename, result, STRERROR(result));
    }

    return result;
}

static int get_position_from_file(const char *subdir_name,
        SFBinlogFilePosition *position)
{
    char filename[PATH_MAX];
    IniContext ini_context;
    int result;

    get_position_filename(subdir_name, filename, sizeof(filename));
    if (access(filename, F_OK) != 0) {
        result =  errno != 0 ? errno : EPERM;
        if (result == ENOENT) {
            position->index = 0;
            position->offset = 0;
            return 0;
        } else {
            logError("file: "__FILE__", line: %d, "
                    "access file %s fail, errno: %d, error info: %s",
                    __LINE__, filename, result, STRERROR(result));
            return result;
        }
    }

    if ((result=iniLoadFromFile(filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load from file \"%s\" fail, error code: %d",
                __LINE__, filename, result);
        return result;
    }

    position->index = iniGetIntValue(NULL,
            BINLOG_POSITION_ITEM_NAME_INDEX,
            &ini_context, 0);
    position->offset = iniGetInt64Value(NULL,
            BINLOG_POSITION_ITEM_NAME_OFFSET,
            &ini_context, 0);

    iniFreeContext(&ini_context);
    return 0;
}

int rebuild_binlog_reader_init(ServerBinlogReader *reader,
        const char *subdir_name)
{
    int result;
    int write_index;
    SFBinlogFilePosition position;

    if ((result=sf_binlog_writer_get_binlog_last_index(DATA_PATH_STR,
                    subdir_name, &write_index)) != 0)
    {
        return result;
    }

    if ((result=get_position_from_file(subdir_name, &position)) != 0) {
        return result;
    }

    return binlog_reader_init1(reader, subdir_name,
            write_index, &position);
}

int rebuild_binlog_reader_unlink_subdir(const char *subdir_name)
{
    int result;
    int write_index;
    int binlog_index;
    char filepath[PATH_MAX];
    char filename[PATH_MAX];

    sf_binlog_writer_get_filepath(DATA_PATH_STR, subdir_name,
            filepath, sizeof(filepath));
    if (access(filepath, F_OK) != 0) {
        if (errno == ENOENT) {
            return 0;
        }
        return errno != 0 ? errno : EPERM;
    }

    if ((result=sf_binlog_writer_get_binlog_last_index(DATA_PATH_STR,
                    subdir_name, &write_index)) != 0)
    {
        return result == ENOENT ? 0 : result;
    }

    for (binlog_index=0; binlog_index<=write_index; binlog_index++) {
        sf_binlog_writer_get_filename(DATA_PATH_STR, subdir_name,
                binlog_index, filename, sizeof(filename));
        if ((result=fc_delete_file_ex(filename, "binlog")) != 0) {
            return result;
        }
    }

    get_position_filename(subdir_name, filename, sizeof(filename));
    if ((result=fc_delete_file_ex(filename, "position mark")) != 0) {
        return result;
    }

    sf_binlog_writer_get_index_filename(DATA_PATH_STR,
            subdir_name, filename, sizeof(filename));
    if ((result=fc_delete_file_ex(filename, "binlog index")) != 0) {
        return result;
    }

    return rebuild_binlog_reader_rmdir(filepath);
}
