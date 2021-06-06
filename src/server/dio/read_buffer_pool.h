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


#ifndef _READ_BUFFER_POOL_H
#define _READ_BUFFER_POOL_H

#include "fastcommon/fc_list.h"
#include "../../common/fs_types.h"

typedef struct aligned_read_buffer {
    char *buff;  //aligned by device block size
    int offset;  //data offset
    int length;  //data length
    int read_bytes;
    int size;
    struct {
        short path;
        short allocator;
    } indexes;
    time_t last_access_time;
    struct fc_list_head dlink;  //for freelist
} AlignedReadBuffer;

#ifdef __cplusplus
extern "C" {
#endif

    int read_buffer_pool_init(const int path_count,
            const SFMemoryWatermark *watermark);

    int read_buffer_pool_start(const int max_idle_time,
            const int reclaim_interval);

    int read_buffer_pool_create(const short path_index, const int block_size);

    AlignedReadBuffer *read_buffer_pool_alloc(
            const short path_index, const int size);

    void read_buffer_pool_free(AlignedReadBuffer *buffer);

#ifdef __cplusplus
}
#endif

#endif
