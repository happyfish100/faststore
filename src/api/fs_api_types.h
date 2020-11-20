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
#include "fastcommon/fc_list.h"
#include "fastcommon/pthread_func.h"
#include "faststore/client/fs_client.h"

struct fs_api_waiting_task;
struct fs_api_combined_writer;

typedef struct fs_api_waiting_task_writer_pair {
    struct fs_api_waiting_task *task;
    struct fs_api_combined_writer *writer;
    struct fast_mblock_man *allocator;   //for free
    struct fc_list_head dlink;
} FSAPIWaitingTaskWriterPair;

typedef struct fs_api_waiting_task {
    pthread_lock_cond_pair_t lcp;  //for notify
    struct {
        FSAPIWaitingTaskWriterPair fixed_pair; //for only one writer
        struct fc_list_head head;   //element: FSAPIWaitingTaskWriterPair
    } waitings;
    struct fast_mblock_man *allocator;   //for free
    struct fs_api_waiting_task *next; //for waiting list in FSAPICombinedWriter
} FSAPIWaitingTask;

typedef struct fs_api_combined_writer {
    int64_t oid;    //object id
    int64_t offset;
    int length;    //data length
    int size;      //allocate size
    char *buff;
    pthread_mutex_t lock;
    struct {
        FSAPIWaitingTaskWriterPair *head;
    } waitings;
    struct fast_mblock_man *allocator;   //for free
} FSAPICombinedWriter;

typedef struct fs_api_context {
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
