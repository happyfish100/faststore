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

    return 0;
}
