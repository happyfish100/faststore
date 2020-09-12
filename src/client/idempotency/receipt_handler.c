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
    if (SF_NIO_TASK_STAGE_FETCH(task) == SF_NIO_STAGE_CONNECT) {
        logError("file: "__FILE__", line: %d, "
                "connect to server %s:%d timeout",
                __LINE__, task->server_ip, task->port);
        return ETIMEDOUT;
    }

    return 0;
}

static void receipt_task_finish_cleanup(struct fast_task_info *task)
{
    IdempotencyClientChannel *channel;
    channel = (IdempotencyClientChannel *)task->arg;

    if (task->event.fd >= 0) {
        sf_task_detach_thread(task);
        close(task->event.fd);
        task->event.fd = -1;
    }

    __sync_bool_compare_and_swap(&channel->in_ioevent, 1, 0);
}

static int setup_channel_request(struct fast_task_info *task)
{
    IdempotencyClientChannel *channel;
    FSProtoHeader *header;
    FSProtoSetupChannelReq *req;

    channel = (IdempotencyClientChannel *)task->arg;
    header = (FSProtoHeader *)task->data;
    req = (FSProtoSetupChannelReq *)(header + 1);
    int2buff(channel->id, req->channel_id);
    int2buff(channel->key, req->key);

    FS_PROTO_SET_HEADER(header, FS_SERVICE_PROTO_SETUP_CHANNEL_REQ,
            sizeof(FSProtoSetupChannelReq));
    task->length = sizeof(FSProtoHeader) + sizeof(FSProtoSetupChannelReq);
    return sf_send_add_event(task);
}

static int check_report_req_receipt(struct fast_task_info *task,
        int *count)
{
    IdempotencyClientChannel *channel;
    FSProtoHeader *header;
    FSProtoReportReqReceiptHeader *rheader;
    FSProtoReportReqReceiptBody *rbody;
    FSProtoReportReqReceiptBody *rstart;
    IdempotencyClientReceipt *last;
    IdempotencyClientReceipt *receipt;
    char *buff_end;

    if (task->length > 0) {
        *count = 0;
        logWarning("file: "__FILE__", line: %d, "
                "server %s:%d, task length: %d != 0, skip check "
                "and report receipt request!", __LINE__,
                task->server_ip, task->port, task->length);
        return 0;
    }

    channel = (IdempotencyClientChannel *)task->arg;
    if (channel->waiting_resp_qinfo.head != NULL) {
        *count = 0;
        return 0;
    }

    fc_queue_pop_to_queue(&channel->queue, &channel->waiting_resp_qinfo);
    if (channel->waiting_resp_qinfo.head == NULL) {
        *count = 0;
        return 0;
    }

    header = (FSProtoHeader *)task->data;
    rheader = (FSProtoReportReqReceiptHeader *)(header + 1);
    rbody = rstart = (FSProtoReportReqReceiptBody *)(rheader + 1);
    buff_end = task->data + task->size;
    last = NULL;
    receipt = channel->waiting_resp_qinfo.head;
    do {
        //check buffer remain space
        if (buff_end - (char *)rbody < sizeof(FSProtoReportReqReceiptBody)) {
            break;
        }

        long2buff(receipt->req_id, rbody->req_id);
        rbody++;

        last = receipt;
        receipt = receipt->next;
    } while (receipt != NULL);

    if (receipt != NULL) {  //repush to queue
        struct fc_queue_info qinfo;
        bool notify;

        qinfo.head = receipt;
        qinfo.tail = channel->waiting_resp_qinfo.tail;
        fc_queue_push_queue_to_head_ex(&channel->queue, &qinfo, &notify);

        last->next = NULL;
        channel->waiting_resp_qinfo.tail = last;
    }

    *count = rbody - rstart;
    int2buff(*count, rheader->count);
    task->length = (char *)rbody - task->data;
    int2buff(task->length - sizeof(FSProtoHeader), header->body_len);
    header->cmd = FS_SERVICE_PROTO_REPORT_REQ_RECEIPT_REQ;
    return sf_send_add_event(task);
}

static int report_req_receipt_request(struct fast_task_info *task)
{
    int result;
    int count;

    if ((result=check_report_req_receipt(task, &count)) != 0) {
        return result;
    }

    if (count == 0) {
        result = sf_set_read_event(task);
    }
    return result;
}

static inline int receipt_expect_body_length(struct fast_task_info *task,
        const int expect_body_len)
{
    if ((int)(task->length - sizeof(FSProtoHeader)) != expect_body_len) {
        logError("file: "__FILE__", line: %d, "
                "server %s:%d, response body length: %d != %d",
                __LINE__, task->server_ip, task->port, (int)(task->length -
                    sizeof(FSProtoHeader)), expect_body_len);
        return EINVAL;
    }

    return 0;
}

static int deal_setup_channel_response(struct fast_task_info *task)
{
    int result;
    FSProtoSetupChannelResp *resp;
    IdempotencyClientChannel *channel;

    if ((result=receipt_expect_body_length(task,
                    sizeof(FSProtoSetupChannelResp))) != 0)
    {
        return result;
    }

    channel = (IdempotencyClientChannel *)task->arg;
    resp = (FSProtoSetupChannelResp *)(task->data + sizeof(FSProtoHeader));
    channel->id = buff2int(resp->channel_id);
    channel->key = buff2int(resp->key);

    if (channel->waiting_resp_qinfo.head != NULL) {
        bool notify;
        fc_queue_push_queue_to_head_ex(&channel->queue,
                &channel->waiting_resp_qinfo, &notify);
        channel->waiting_resp_qinfo.head = NULL;
        channel->waiting_resp_qinfo.tail = NULL;
    }

    return 0;
}

static inline int deal_report_req_receipt_response(struct fast_task_info *task)
{
    int result;
    IdempotencyClientChannel *channel;
    IdempotencyClientReceipt *current;
    IdempotencyClientReceipt *deleted;

    if ((result=receipt_expect_body_length(task, 0)) != 0) {
        return result;
    }

    channel = (IdempotencyClientChannel *)task->arg;
    if (channel->waiting_resp_qinfo.head == NULL) {
        logWarning("file: "__FILE__", line: %d, "
                "response from server %s:%d, unexpect cmd: "
                "REPORT_REQ_RECEIPT_RESP", __LINE__,
                task->server_ip, task->port);
        return 0;
    }

    current = channel->waiting_resp_qinfo.head;
    do {
        deleted = current;
        current = current->next;

        fast_mblock_free_object(&channel->receipt_allocator, deleted);
    } while (current != NULL);

    channel->waiting_resp_qinfo.head = NULL;
    channel->waiting_resp_qinfo.tail = NULL;
    return 0;
}

static int receipt_deal_task(struct fast_task_info *task)
{
    int result;
    int stage;

    do {
        stage = SF_NIO_TASK_STAGE_FETCH(task);
        if (stage == SF_NIO_STAGE_HANDSHAKE) {
            result = setup_channel_request(task);
            break;
        } else if (stage == SF_NIO_STAGE_CONTINUE) {
            result = report_req_receipt_request(task);
            break;
        }

        result = buff2short(((FSProtoHeader *)task->data)->status);
        if (result != 0) {
            logError("file: "__FILE__", line: %d, "
                    "response from server %s:%d, cmd: %d (%s), status: %d",
                    __LINE__, task->server_ip, task->port,
                    ((FSProtoHeader *)task->data)->cmd,
                    fs_get_cmd_caption(((FSProtoHeader *)task->data)->cmd),
                    result);
            break;
        }

        switch (((FSProtoHeader *)task->data)->cmd) {
            case FS_SERVICE_PROTO_SETUP_CHANNEL_RESP:
                result = deal_setup_channel_response(task);
                break;
            case FS_SERVICE_PROTO_REPORT_REQ_RECEIPT_RESP:
                result = deal_report_req_receipt_response(task);
                break;
            default:
                logError("file: "__FILE__", line: %d, "
                        "response from server %s:%d, unexpect cmd: %d (%s)",
                        __LINE__, task->server_ip, task->port,
                        ((FSProtoHeader *)task->data)->cmd,
                        fs_get_cmd_caption(((FSProtoHeader *)task->data)->cmd));
                result = EINVAL;
                break;
        }

        if (result == 0) {
            task->offset = task->length = 0;
            result = report_req_receipt_request(task);
        }
    } while (0);

    return result > 0 ? -1 * result : result;
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
