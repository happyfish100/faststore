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


#ifndef _FS_CHANGE_NOTIFY_H
#define _FS_CHANGE_NOTIFY_H

#include "../server_types.h"

typedef enum fs_change_entry_type {
    fs_change_entry_type_block = 'b',
    fs_change_entry_type_slice = 's'
} FSChangeEntryType;

typedef struct fs_change_notify_event {
    int64_t sn;
    OBEntry *ob;
    FSChangeEntryType entry_type;
    DABinlogOpType op_type;

    union {
        struct {
            OBSliceType type;
            FSSliceSize ssize;
            FSTrunkSpaceInfo space;
        } slice;  //for slice add

        FSSliceSize ssize;    //for slice delete
    };
    struct fs_change_notify_event *next; //for queue
} FSChangeNotifyEvent;

typedef struct fs_change_notify_event_ptr_array {
    FSChangeNotifyEvent **events;
    int count;
    int alloc;
} FSChangeNotifyEventPtrArray;

#ifdef __cplusplus
extern "C" {
#endif

    int change_notify_init();
    void change_notify_destroy();

    int change_notify_push_add_slice(const int64_t sn, OBSliceEntry *slice);

    int change_notify_push_del_slice(const int64_t sn,
            OBEntry *ob, const FSSliceSize *ssize);

    int change_notify_push_del_block(const int64_t sn, OBEntry *ob);

    void change_notify_load_done_signal();

#ifdef __cplusplus
}
#endif

#endif
