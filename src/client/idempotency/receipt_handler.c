//receipt_handler.c

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/ioevent_loop.h"
#include "sf/sf_util.h"
#include "sf/sf_func.h"
#include "sf/sf_nio.h"
#include "sf/sf_global.h"
#include "sf/sf_service.h"
#include "fs_proto.h"
#include "fs_func.h"
#include "receipt_handler.h"

static int receipt_init_task(struct fast_task_info *task)
{
    task->connect_timeout = SF_G_CONNECT_TIMEOUT; //for client side
    task->network_timeout = SF_G_NETWORK_TIMEOUT;
    return 0;
}

static int receipt_recv_timeout_callback(struct fast_task_info *task)
{
    //TODO
    if (1) {
        logError("file: "__FILE__", line: %d, "
                "server %s:%d, sock: %d, recv timeout",
                __LINE__, task->server_ip, task->port, task->event.fd);
        return ETIMEDOUT;
    }

    return 0;
}

static void receipt_task_finish_cleanup(struct fast_task_info *task)
{
    IdempotencyClientChannel *channel;
    channel = (IdempotencyClientChannel *)task->arg;
    __sync_bool_compare_and_swap(&channel->in_ioevent, 1, 0);
}

static int receipt_deal_task(struct fast_task_info *task)
{
    int result;

    result = 0;
    switch (((FSProtoHeader *)task->data)->cmd) {
        case FS_PROTO_ACK:
            //result = handler_deal_actvie_test(task);
            break;
        case FS_SERVICE_PROTO_SETUP_CHANNEL_RESP:
            //result = receipt_deal_setup_channel(task);
            break;
       case FS_SERVICE_PROTO_REPORT_REQ_RECEIPT_RESP:
            //result = receipt_deal_report_req_receipt(task);
            break;
        default:
            /*
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "unkown cmd: %d", ((FSProtoHeader *)task->data)->cmd);
                    */
            result = -EINVAL;
            break;
    }

    return result;
}

void *receipt_alloc_thread_extra_data(const int thread_index)
{
    return NULL;
}

int receipt_handler_init()
{
    return sf_service_init_ex2(&g_sf_context, NULL, NULL, NULL,
            fs_proto_set_body_length, receipt_deal_task,
            receipt_task_finish_cleanup, receipt_recv_timeout_callback,
            1000, sizeof(FSProtoHeader), 0, receipt_init_task);
}

int receipt_handler_destroy()
{
    return 0;
}
