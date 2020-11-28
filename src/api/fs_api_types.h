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

struct fs_api_block_entry;
struct fs_api_otid_entry;
struct fs_api_waiting_task;
struct fs_api_slice_entry;
struct fs_api_allocator_context;
struct fs_api_context;

#define FS_API_FETCH_SLICE_OTID(slice) \
    (FSAPIOTIDEntry *)__sync_add_and_fetch(&slice->otid, 0);

#define FS_API_FETCH_SLICE_BLOCK(slice) \
    (FSAPIBlockEntry *)__sync_add_and_fetch(&slice->block, 0);

#define FS_API_CALC_TIMEOUT_BY_SUCCESSIVE(op_ctx, successive_count)  \
    (successive_count * op_ctx->api_ctx->write_combine.min_wait_time_ms)

typedef struct fs_api_waiting_task_slice_pair {
    volatile struct fs_api_waiting_task *task;
    struct fs_api_slice_entry *slice;
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

typedef struct fs_api_slice_entry {
    LockedTimerEntry timer;  //must be the first
    volatile struct fs_api_otid_entry *otid;
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
    struct fs_api_allocator_context *allocator_ctx; //for free, set by fast_mblock
    struct fc_list_head dlink;          //for block entry
    struct fs_api_slice_entry *next;    //for combine handler queue
} FSAPISliceEntry;

typedef struct fs_api_operation_context {
    uint64_t tid;  //thread id
    uint64_t bid;  //file block id
    FSBlockSliceKeyInfo bs_key;
    struct fs_api_allocator_context *allocator_ctx;
    struct fs_api_context *api_ctx;
} FSAPIOperationContext;

typedef struct fs_api_insert_slice_context {
    FSAPIOperationContext *op_ctx;
    const char *buff;
    bool *combined;
    struct {
        int successive_count;
        struct fs_api_otid_entry *entry;
        FSAPISliceEntry *old_slice;
    } otid;
    FSAPISliceEntry *slice;   //new created slice
    FSAPIWaitingTask *waiting_task;
} FSAPIInsertSliceContext;

typedef struct fs_api_context {
    struct {
        volatile bool enabled;
        int min_wait_time_ms;
        int max_wait_time_ms;
        int skip_combine_on_slice_size;
        int skip_combine_on_last_merged_slices;
    } write_combine;
    FSClientContext *fs;
} FSAPIContext;

#ifdef __cplusplus
extern "C" {
#endif

    extern FSAPIContext g_fs_api_ctx;

#ifdef __cplusplus
}
#endif

#endif
