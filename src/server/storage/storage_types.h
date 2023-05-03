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


#ifndef _STORAGE_TYPES_H
#define _STORAGE_TYPES_H

#include "fastcommon/fc_list.h"
#include "fastcommon/shared_buffer.h"
#include "fastcommon/uniq_skiplist.h"
#include "fastcommon/fast_mblock.h"
#include "fastcommon/sorted_queue.h"
#include "sf/sf_shared_mbuffer.h"
#include "diskallocator/storage_types.h"
#include "../../common/fs_types.h"

#define FS_MAX_SPLIT_COUNT_PER_SPACE_ALLOC   2
#define FS_SLICE_SN_PARRAY_INIT_ALLOC_COUNT  4

struct ob_slice_entry;
struct fs_data_operation;
struct fs_slice_op_context;
struct fs_trunk_allocator;

typedef void (*fs_data_op_notify_func)(struct fs_data_operation *op);
typedef void (*fs_rw_done_callback_func)(
        struct fs_slice_op_context *op_ctx, void *arg);

typedef struct {
    struct ob_slice_entry *slice;
    uint64_t sn;     //for slice binlog
    int64_t version; //for write in order
    DATrunkFileInfo *trunk;
} FSSliceSNPair;

typedef struct {
    int count;
    int alloc;
    FSSliceSNPair *slice_sn_pairs;
} FSSliceSNPairArray;

typedef struct ob_db_args {
    bool locked;
    volatile short ref_count;
    UniqSkiplist *slices;  //the element is OBSliceEntry
    struct fc_list_head dlink; //for storage engine LRU
} OBDBArgs;

typedef struct ob_entry {
    FSBlockKey bkey;
    UniqSkiplist *slices;  //the element is OBSliceEntry
    struct ob_entry *next; //for hashtable
    struct fast_mblock_man *allocator; //for free
    OBDBArgs db_args[0];    //for storage engine, since V3.8
} OBEntry;

typedef struct {
    volatile int64_t count;
    int64_t capacity;
    OBEntry **buckets;
    bool need_reclaim;
} OBHashtable;

typedef struct ob_slice_entry {
    int64_t data_version;
    OBEntry *ob;
    DASliceType type;    //in file, write cache or memory as fallocate
    volatile int ref_count;
    FSSliceSize ssize;
    DATrunkSpaceInfo space;
    struct {
        SFSharedMBuffer *mbuffer;
        char *buff;
    } cache; //for write cache only
    struct fc_queue_info *space_chain;
    struct fast_mblock_man *allocator; //for free
} OBSliceEntry;

typedef struct ob_slice_ptr_array {
    int64_t alloc;
    int64_t count;
    OBSliceEntry **slices;
} OBSlicePtrArray;

typedef struct ob_slice_read_buffer_pair {
    OBSliceEntry *slice;
    DATrunkReadBuffer rb;
} OBSliceReadBufferPair;

typedef struct ob_slice_read_buffer_array {
    int64_t alloc;
    int64_t count;
    OBSliceReadBufferPair *pairs;
} OBSliceReadBufferArray;

#ifdef OS_LINUX
typedef enum {
    fs_buffer_type_direct,  /* char *buff */
    fs_buffer_type_array    /* aligned_read_buffer **array */
} FSIOBufferType;
#endif

struct fs_cluster_data_server_info;
struct fs_data_thread_context;
typedef struct fs_slice_op_context {
    fs_data_op_notify_func notify_func;  //for data thread
    fs_rw_done_callback_func rw_done_callback; //for caller (data or nio thread)
    void *arg;  //for signal data thread or nio task
    volatile short counter;
    short result;
    volatile int done_bytes;

    struct {
        bool deal_done;    //for continue deal check
        bool set_dv_done;
        bool is_update;
        struct {
            bool log_replica;   //false for trunk reclaim and data rebuild
        } write_binlog;
        bool write_to_cache;
        char source;            //for binlog write
        struct {
            unsigned char count;
            uint64_t last;
        } sn;  //for slice binlog
        int data_group_id;
        int body_len;
        uint64_t data_version;  //for replica binlog
        FSBlockSliceKeyInfo bs_key;
        struct fs_cluster_data_server_info *myself;
#ifdef OS_LINUX
        FSIOBufferType buffer_type;
#endif
        char *body;
        char *buff;  //read or write buffer
    } info;

    struct {
        int timestamp;      //for log to binlog
        int space_changed;  //increase /decrease space in bytes for slice operate
        FSSliceSNPairArray sarray;
        struct fc_queue_info space_chain;
    } update;  //for slice update

    SFSharedMBuffer *mbuffer;  //for slice write
    OBSliceReadBufferArray slice_rbuffer_array;  //for slice read
    iovec_array_t iovec_array;

#ifdef OS_LINUX
    AIOBufferPtrArray aio_buffer_parray;
#endif

} FSSliceOpContext;

typedef struct fs_slice_op_buffer_context {
    FSSliceOpContext op_ctx;
} FSSliceOpBufferContext;

typedef struct fs_slice_blocked_op_context {
    FSSliceOpContext op_ctx;
    int buffer_size;
    struct {
        bool finished;
        pthread_lock_cond_pair_t lcp; //for notify
    } notify;
} FSSliceBlockedOpContext;

typedef struct fs_binlog_write_file_buffer_pair {
    SafeWriteFileInfo fi;
    FastBuffer buffer;
    int record_count;
} FSBinlogWriteFileBufferPair;

typedef struct fs_slice_space_log_record {
    int64_t last_sn;
    SFBinlogWriterBuffer *slice_head;
    struct fc_queue_info space_chain;  //element: DATrunkSpaceLogRecord
    SFSynchronizeContext *sctx;
    struct fs_slice_space_log_record *next;
} FSSliceSpaceLogRecord;

typedef struct fs_slice_space_log_context {
    int record_count;
    FSBinlogWriteFileBufferPair slice_redo;
    FSBinlogWriteFileBufferPair space_redo;
    struct fast_mblock_man allocator;  //element: FSSliceSpaceLogRecord
    struct sorted_queue queue;
} FSSliceSpaceLogContext;

#endif
