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

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <pthread.h>
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/ioevent_loop.h"
#include "sf/sf_global.h"
#include "sf/sf_nio.h"
#include "../../common/fs_proto.h"
#include "../server_global.h"
#include "../server_group_info.h"
#include "../storage/slice_op.h"
#include "replication_processor.h"
#include "rpc_result_ring.h"
#include "replication_callee.h"

int replication_callee_init()
{
    return 0;
}

void replication_callee_destroy()
{
}

void replication_callee_terminate()
{
}

static int slice_op_buffer_ctx_init(void *element, void *args)
{
    FSSliceSNPairArray *slice_sn_parray;
    slice_sn_parray = &((FSSliceOpBufferContext *)
            element)->op_ctx.update.sarray;
    return fs_slice_array_init(slice_sn_parray);
}

int replication_callee_init_allocator(FSServerContext *server_context)
{
    int result;
    int element_size;

    element_size = sizeof(FSSliceOpBufferContext);
    if ((result=fast_mblock_init_ex1(&server_context->replica.
                    op_ctx_allocator, "slice_op_ctx", element_size,
                    1024, 0, slice_op_buffer_ctx_init, NULL, true)) != 0)
    {
        return result;
    }

    if ((result=shared_buffer_init(&server_context->replica.shared_buffer_ctx,
                    64, g_sf_global_vars.min_buff_size)) != 0)
    {
        return result;
    }

    return 0;
}

int replication_callee_push_to_rpc_result_queue(FSReplication *replication,
        const int data_group_id, const uint64_t data_version, const int err_no)
{
    ReplicationRPCResult *r;
    bool notify;

    r = (ReplicationRPCResult *)fast_mblock_alloc_object(
            &replication->context.callee.result_allocator);
    if (r == NULL) {
        return ENOMEM;
    }

    r->data_group_id = data_group_id;
    r->data_version = data_version;
    r->err_no = err_no;
    fc_queue_push_ex(&replication->context.callee.done_queue, r, &notify);
    if (notify) {
        ioevent_notify_thread(replication->task->thread_data);
    }

    return 0;
}

int replication_callee_deal_rpc_result_queue(FSReplication *replication)
{
    struct fc_queue_info qinfo;
    bool notify;
    struct fast_task_info *task;
    ReplicationRPCResult *r;
    ReplicationRPCResult *deleted;
    char *p;
    int count;

    task = replication->task;
    if (!(task->offset == 0 && task->length == 0)) {
        return 0;
    }

    fc_queue_try_pop_to_queue(&replication->
            context.callee.done_queue, &qinfo);
    if (qinfo.head == NULL) {
        return 0;
    }

    count = 0;
    r = qinfo.head;
    p = task->data + sizeof(FSProtoHeader) +
        sizeof(FSProtoReplicaRPCRespBodyHeader);
    do {
        if ((p - task->data) + sizeof(FSProtoReplicaRPCRespBodyPart) >
                task->size)
        {
            qinfo.head = r;
            fc_queue_push_queue_to_head_ex(&replication->context.
                    callee.done_queue, &qinfo, &notify);
            break;
        }

        int2buff(r->data_group_id, ((FSProtoReplicaRPCRespBodyPart *)
                    p)->data_group_id);
        long2buff(r->data_version, ((FSProtoReplicaRPCRespBodyPart *)
                    p)->data_version);
        short2buff(r->err_no, ((FSProtoReplicaRPCRespBodyPart *)p)->
                err_no);
        p += sizeof(FSProtoReplicaRPCRespBodyPart);

        ++count;

        deleted = r;
        r = r->next;

        fast_mblock_free_object(&replication->context.
                callee.result_allocator, deleted);
    } while (r != NULL);

    int2buff(count, ((FSProtoReplicaRPCRespBodyHeader *)
                (task->data + sizeof(FSProtoHeader)))->count);

    task->length = p - task->data;
    SF_PROTO_SET_HEADER((FSProtoHeader *)task->data,
            FS_REPLICA_PROTO_RPC_RESP, task->length - sizeof(FSProtoHeader));
    sf_send_add_event(task);
    return 0;
}
