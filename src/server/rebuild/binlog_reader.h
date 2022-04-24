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


#ifndef _FS_REBUILD_BINLOG_READER_H
#define _FS_REBUILD_BINLOG_READER_H

#include "../server_types.h"

#ifdef __cplusplus
extern "C" {
#endif

    int rebuild_binlog_reader_init(ServerBinlogReader *reader,
            const char *subdir_name);

    int rebuild_binlog_reader_save_position(ServerBinlogReader *reader);

    static inline int rebuild_binlog_reader_rmdir(const char *filepath)
    {
        int result;

        if (rmdir(filepath) < 0) {
            result = errno != 0 ? errno : EPERM;
            if (result != ENOTEMPTY) {
                logError("file: "__FILE__", line: %d, "
                        "rmdir %s fail, errno: %d, error info: %s",
                        __LINE__, filepath, result, STRERROR(result));
                return result;
            }
        }

        return 0;
    }

    int rebuild_binlog_reader_unlink_subdir(const char *subdir_name);

#ifdef __cplusplus
}
#endif

#endif
