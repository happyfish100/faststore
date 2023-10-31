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

//common_handler.h

#ifndef FS_COMMON_HANDLER_H
#define FS_COMMON_HANDLER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fastcommon/fast_task_queue.h"
#include "server_types.h"

#ifdef __cplusplus

extern "C" {
#endif

int common_handler_init();

int handler_check_config_signs(struct fast_task_info *task,
        const int server_id, const bool auth_enabled,
        FSProtoConfigSigns *config_signs);

static inline void release_pending_send_buffer(FSPendingSendBuffer *buffer)
{
    struct fast_task_info *task;

    task = buffer->task;
    fc_list_del_init(&buffer->dlink);
    fast_mblock_free_object(&PENDING_SEND_ALLOCATOR, buffer);
    TASK_PENDING_SEND_COUNT--;
}

static inline void remove_from_pending_send_queue(struct fc_list_head *head,
        struct fast_task_info *task)
{
    FSPendingSendBuffer *buffer;
    FSPendingSendBuffer *tmp;

    fc_list_for_each_entry_safe(buffer, tmp, head, dlink) {
        if (buffer->task == task) {
            release_pending_send_buffer(buffer);
        }
    }
}

static inline void process_pending_send_queue(struct fc_list_head *head)
{
    FSPendingSendBuffer *buffer;
    FSPendingSendBuffer *tmp;
    struct fast_task_info *task;

    fc_list_for_each_entry_safe(buffer, tmp, head, dlink) {
        task = buffer->task;
        if (task->canceled || !sf_nio_task_send_done(task)) {
            continue;
        }

        memcpy(task->send.ptr->data, buffer->data, buffer->length);
        task->send.ptr->length = buffer->length;
        release_pending_send_buffer(buffer);
        sf_send_add_event(task);
    }
}

static inline void push_to_pending_send_queue(struct fc_list_head *head,
        struct fast_task_info *task, FSPendingSendBuffer *pb)
{
    FSProtoHeader *header;

    pb->task = task;
    header = (FSProtoHeader *)pb->data;
    short2buff(0, header->status);
    header->cmd = RESPONSE.header.cmd;
    int2buff(RESPONSE.header.body_len, header->body_len);
    pb->length = sizeof(FSProtoHeader) + RESPONSE.header.body_len;
    fc_list_add_tail(&pb->dlink, head);
    TASK_PENDING_SEND_COUNT++;
}

#ifdef __cplusplus
}
#endif

#endif
