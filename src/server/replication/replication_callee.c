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
#include "../server_global.h"
#include "../server_group_info.h"
#include "replication_processor.h"
#include "rpc_result_ring.h"
#include "replication_callee.h"

typedef struct {
} ReplicationCommonContext;

static ReplicationCommonContext repl_ctx;

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

int replication_callee_push_to_rpc_result_queue(FSReplication *replication,
        const uint64_t data_version, const int err_no)
{
    ReplicationRPCResult *r;
    bool notify;

    if (replication == NULL) {
        return ENOENT;
    }

    r = (ReplicationRPCResult *)fast_mblock_alloc_object(
            &replication->context.callee.result_allocator);
    if (r == NULL) {
        return ENOMEM;
    }

    r->data_version = data_version;
    r->err_no = err_no;
    fc_queue_push_ex(&replication->context.callee.done_queue, r, &notify);
    if (notify) {
        iovent_notify_thread(replication->task->thread_data);
    }

    return 0;
}
