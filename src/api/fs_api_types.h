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

#ifndef _FS_API_TYPES_H
#define _FS_API_TYPES_H

#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "fastcommon/common_define.h"
#include "fastcommon/fc_list.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/locked_timer.h"
#include "faststore/client/fs_client.h"

#define FS_API_COMBINED_WRITER_STAGE_NONE        0
#define FS_API_COMBINED_WRITER_STAGE_MERGING     1
#define FS_API_COMBINED_WRITER_STAGE_PROCESSING  2
#define FS_API_COMBINED_WRITER_STAGE_CLEANUP     3

#define FS_NOT_COMBINED_REASON_SLICE_SIZE         1001
#define FS_NOT_COMBINED_REASON_REACH_BUFF_SIZE    1002
#define FS_NOT_COMBINED_REASON_LAST_MERGED_SLICES 1003
#define FS_NOT_COMBINED_REASON_SLICE_POSITION     1004
#define FS_NOT_COMBINED_REASON_WAITING_TIMEOUT    1005
#define FS_NOT_COMBINED_REASON_DIFFERENT_OID      1099

struct fs_api_block_entry;
struct fs_wcombine_otid_entry;
struct fs_api_waiting_task;
struct fs_api_slice_entry;
struct fs_api_allocator_context;
struct fs_api_write_done_callback_arg;
struct fs_api_context;

#define FS_API_FETCH_SLICE_OTID(slice) \
    (FSWCombineOTIDEntry *)__sync_add_and_fetch(&slice->otid, 0);

#define FS_API_FETCH_SLICE_BLOCK(slice) \
    (FSAPIBlockEntry *)__sync_add_and_fetch(&slice->block, 0);

#define FS_API_CALC_TIMEOUT_BY_SUCCESSIVE(op_ctx, successive_count)  \
    (successive_count * op_ctx->api_ctx->write_combine.min_wait_time_ms)

typedef void (*fs_api_write_done_callback)(struct
        fs_api_write_done_callback_arg *callback_arg);

typedef struct fs_api_waiting_task_slice_pair {
    volatile struct fs_api_waiting_task *task;
    struct fast_mblock_man *allocator; //for free
    struct fc_list_head dlink;         //for waiting task
    struct fs_api_waiting_task_slice_pair *next; //for slice entry
} FSAPIWaitingTaskSlicePair;

typedef struct fs_api_waiting_task {
    pthread_lock_cond_pair_t lcp;  //for notify
    struct {
        FSAPIWaitingTaskSlicePair fixed_pair; //for only one writer
        struct fc_list_head head;   //element: FSAPIWaitingTaskSlicePair
    } waitings;
    struct fast_mblock_man *allocator;  //for free
} FSAPIWaitingTask;

typedef struct fs_api_write_done_callback_arg {
    const FSBlockSliceKeyInfo *bs_key;
    struct fast_mblock_man *allocator;  //for free
    int write_bytes;
    int inc_alloc;
    char extra_data[0];
} FSAPIWriteDoneCallbackArg;

typedef struct fs_api_slice_entry {
    LockedTimerEntry timer;  //must be the first
    volatile struct fs_wcombine_otid_entry *otid;
    volatile struct fs_api_block_entry *block;
    volatile int64_t version;
    int stage;
    int merged_slices;
    int64_t start_time;
    FSBlockSliceKeyInfo bs_key;
    char *buff;
    struct {
        FSAPIWaitingTaskSlicePair *head; //use lock of block sharding
    } waitings;
    struct fs_api_context *api_ctx;
    FSAPIWriteDoneCallbackArg *done_callback_arg;  //write done callback arg
    struct fs_api_allocator_context *allocator_ctx; //for free, set by fast_mblock
    struct fc_list_head dlink;          //for block entry
    struct fs_api_slice_entry *next;    //for combine handler queue
} FSAPISliceEntry;

typedef struct fs_api_operation_context {
    uint64_t tid;  //thread id
    uint64_t bid;  //file block id
    char op_type;  //operation type for debug
    FSBlockSliceKeyInfo bs_key;
    struct fs_api_allocator_context *allocator_ctx;
    struct fs_api_context *api_ctx;
} FSAPIOperationContext;

typedef struct fs_api_write_buffer {
    const char *buff;
    void *extra_data;  //for write done callback
    bool combined;
    short reason;       //not combine reason
} FSAPIWriteBuffer;

typedef struct fs_api_context {
    struct {
        int shared_allocator_count;
        int hashtable_sharding_count;
        int64_t hashtable_total_capacity;
    } common;

    struct {
        volatile bool enabled;
        int buffer_size;
        int min_wait_time_ms;
        int max_wait_time_ms;
        int skip_combine_on_slice_size;
        int skip_combine_on_last_merged_slices;
        int timer_shared_lock_count;
        int thread_pool_max_threads;
        int thread_pool_min_idle_count;
        int thread_pool_max_idle_time;
    } write_combine;

    struct {
        volatile bool enabled;
        int cache_ttl_ms;
        int min_buffer_size;
        int max_buffer_size;
        int skip_preread_on_slice_size;
    } read_ahead;

    FSClientContext *fs;
    struct {
        fs_api_write_done_callback func;
        int arg_extra_size;
    } write_done_callback;
} FSAPIContext;

#ifdef __cplusplus
extern "C" {
#endif

    extern FSAPIContext g_fs_api_ctx;

    static inline const char *fs_api_get_combine_stage(const int stage)
    {
        switch (stage) {
            case FS_API_COMBINED_WRITER_STAGE_NONE:
                return "none";
            case FS_API_COMBINED_WRITER_STAGE_MERGING:
                return "merging";
            case FS_API_COMBINED_WRITER_STAGE_PROCESSING:
                return "processing";
            case FS_API_COMBINED_WRITER_STAGE_CLEANUP:
                return "cleanup";
            default:
                return "unkown";
        }
    }

#ifdef __cplusplus
}
#endif

#endif
