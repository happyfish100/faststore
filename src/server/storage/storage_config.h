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


#ifndef _STORAGE_CONFIG_H
#define _STORAGE_CONFIG_H

#include "../../common/fs_types.h"
#include "../server_types.h"

typedef struct {
    volatile int64_t total;
    volatile int64_t avail;  //current available space
    volatile int64_t used;
    int64_t last_used;      //for avail allocator check
} FSTrunkSpaceStat;

typedef struct {
#ifdef OS_LINUX
    int block_size;
#endif
    FSStorePath store;
    int write_thread_count;
    int read_thread_count;
    int prealloc_trunks;
    int read_io_depth;
    bool read_direct_io;
    int fsync_every_n_writes;
    struct {
        int64_t value;
        double ratio;
    } reserved_space;

    struct {
        int64_t value;
        double ratio;
        int trunk_count;  //calculate by: value / trunk_file_size
    } prealloc_space;

    struct {
        int64_t total;
        int64_t avail;  //current available space
        volatile time_t last_stat_time;
        double used_ratio;
    } space_stat;  //for disk space

    FSTrunkSpaceStat trunk_stat;  //for trunk space
} FSStoragePathInfo;

typedef struct {
    FSStoragePathInfo *paths;
    int count;
} FSStoragePathArray;

typedef struct {
    FSStoragePathInfo **paths;
    int count;
} FSStoragePathPtrArray;

typedef struct {
    FSStoragePathArray store_path;
    FSStoragePathArray write_cache;
    FSStoragePathPtrArray paths_by_index;
    int max_store_path_index;  //the max of FSStorePath->index from dat file

    struct {
        double on_usage;  //usage ratio
        TimeInfo start_time;
        TimeInfo end_time;
    } write_cache_to_hd;

    int write_threads_per_path;
    int read_threads_per_path;
    int io_depth_per_read_thread;
    bool read_direct_io;
    int fsync_every_n_writes;
    double reserved_space_per_disk;
    int max_trunk_files_per_subdir;
    int64_t trunk_file_size;
    int discard_remain_space_size;
    int trunk_prealloc_threads;
    int fd_cache_capacity_per_read_thread;
    struct {
        int shared_lock_count;
        int shared_allocator_count;
        int64_t hashtable_capacity;
    } object_block;
    double reclaim_trunks_on_path_usage;
    double never_reclaim_on_trunk_usage;

    struct {
        double ratio_per_path;
        TimeInfo start_time;
        TimeInfo end_time;
    } prealloc_space;

#ifdef OS_LINUX
    struct {
        struct {
            int64_t value;
            double ratio;
        } memory_watermark_low;

        struct {
            int64_t value;
            double ratio;
        } memory_watermark_high;

        int max_idle_time;
        int reclaim_interval;
    } aio_read_buffer;
#endif

} FSStorageConfig;

#ifdef __cplusplus
extern "C" {
#endif

    int storage_config_load(FSStorageConfig *storage_cfg,
            const char *storage_filename);

    int storage_config_calc_path_avail_space(FSStoragePathInfo *path_info);

    void storage_config_stat_path_spaces(FSClusterServerSpaceStat *ss);

    void storage_config_to_log(FSStorageConfig *storage_cfg);

    static inline int storage_config_path_count(FSStorageConfig *storage_cfg)
    {
        return storage_cfg->store_path.count + storage_cfg->write_cache.count;
    }

#ifdef __cplusplus
}
#endif

#endif
