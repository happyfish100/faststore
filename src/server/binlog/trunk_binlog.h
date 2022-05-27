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


#ifndef _TRUNK_BINLOG_H
#define _TRUNK_BINLOG_H

#include "sf/sf_binlog_writer.h"
#include "../../common/fs_types.h"
#include "../storage/storage_config.h"
#include "../server_global.h"

#ifdef __cplusplus
extern "C" {
#endif

    int trunk_binlog_init();
    void trunk_binlog_destroy();

    static inline const char *trunk_binlog_get_filepath(
            char *filepath, const int size)
    {
        return sf_binlog_writer_get_filepath(DATA_PATH_STR,
                FS_TRUNK_BINLOG_SUBDIR_NAME, filepath, size);
    }

    static inline const char *trunk_binlog_get_filename(const
            int binlog_index, char *filename, const int size)
    {
        return sf_binlog_writer_get_filename(DATA_PATH_STR,
                FS_TRUNK_BINLOG_SUBDIR_NAME, binlog_index,
                filename, size);
    }

    int trunk_binlog_get_current_write_index();

    int trunk_binlog_set_binlog_write_index(const int binlog_index);

    static inline int trunk_binlog_log_to_buff(const char op_type,
            const int path_index, const FSTrunkIdInfo *id_info,
            const int64_t file_size, char *buff)
    {
        return sprintf(buff, "%d %c %d %"PRId64" %"PRId64" %"PRId64"\n",
                (int)g_current_time, op_type, path_index, id_info->id,
                id_info->subdir, file_size);
    }

    int trunk_binlog_write(const char op_type, const int path_index,
            const FSTrunkIdInfo *id_info, const int64_t file_size);

#ifdef __cplusplus
}
#endif

#endif
