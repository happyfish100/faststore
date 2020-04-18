
#ifndef _STORAGE_CONFIG_H
#define _STORAGE_CONFIG_H

#include "../../common/fs_types.h"
#include "../server_types.h"

typedef struct {
    FSStorePath store;
    int write_thread_count;
    int read_thread_count;
    int prealloc_trunks;
    struct {
        int64_t value;
        double ratio;
    } reserved_space;
    int64_t avail_space;  //current available space
    struct {
        volatile int64_t total_bytes;
        volatile int64_t used_bytes;
    } trunk_stat;
} FSStoragePathInfo;

typedef struct {
    FSStoragePathInfo *paths;
    int count;
} FSStoragePathArray;

typedef struct {
    FSStoragePathArray store_path;
    FSStoragePathArray write_cache;
    int max_store_path_index;  //the max of FSStorePath->index from dat file

    struct {
        double on_usage;  //usage ratio
        TimeInfo start_time;
        TimeInfo end_time;
    } write_cache_to_hd;

    int write_threads_per_disk;
    int read_threads_per_disk;
    double reserved_space_per_disk;
    int max_trunk_files_per_subdir;
    int64_t trunk_file_size;
    int discard_remain_space_size;
    int prealloc_trunks_per_writer;
    int prealloc_trunk_threads;
    int fd_cache_capacity_per_read_thread;
    double reclaim_trunks_on_usage;
} FSStorageConfig;

#ifdef __cplusplus
extern "C" {
#endif

    int storage_config_load(FSStorageConfig *storage_cfg,
            const char *storage_filename);

    int storage_config_calc_path_spaces(FSStoragePathInfo *path_info);

    void storage_config_to_log(FSStorageConfig *storage_cfg);

#ifdef __cplusplus
}
#endif

#endif
