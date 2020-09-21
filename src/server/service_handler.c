//service_handler.c

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
#include "sf/sf_configs.h"
#include "common/fs_proto.h"
#include "common/fs_func.h"
#include "binlog/replica_binlog.h"
#include "replication/replication_common.h"
#include "sf/idempotency/server/server_channel.h"
#include "sf/idempotency/server/server_handler.h"
#include "server_global.h"
#include "server_func.h"
#include "server_group_info.h"
#include "server_storage.h"
#include "common_handler.h"
#include "data_update_handler.h"
#include "service_handler.h"

int service_handler_init()
{
    return idempotency_channel_init(SF_IDEMPOTENCY_MAX_CHANNEL_ID,
            SF_IDEMPOTENCY_DEFAULT_REQUEST_HINT_CAPACITY,
            SF_IDEMPOTENCY_DEFAULT_CHANNEL_RESERVE_INTERVAL,
            SF_IDEMPOTENCY_DEFAULT_CHANNEL_SHARED_LOCK_COUNT);
}

int service_handler_destroy()
{
    return 0;
}

void service_task_finish_cleanup(struct fast_task_info *task)
{
    switch (SERVER_TASK_TYPE) {
        case SF_SERVER_TASK_TYPE_CHANNEL_HOLDER:
        case SF_SERVER_TASK_TYPE_CHANNEL_USER:
            if (IDEMPOTENCY_CHANNEL != NULL) {
                idempotency_channel_release(IDEMPOTENCY_CHANNEL,
                        SERVER_TASK_TYPE == SF_SERVER_TASK_TYPE_CHANNEL_HOLDER);
                IDEMPOTENCY_CHANNEL = NULL;
            } else {
                logError("file: "__FILE__", line: %d, "
                        "mistake happen! task: %p, SERVER_TASK_TYPE: %d, "
                        "IDEMPOTENCY_CHANNEL is NULL", __LINE__, task,
                        SERVER_TASK_TYPE);
            }
            SERVER_TASK_TYPE = SF_SERVER_TASK_TYPE_NONE;
            break;
        default:
            break;
    }

    if (IDEMPOTENCY_CHANNEL != NULL) {
        logError("file: "__FILE__", line: %d, "
                "mistake happen! task: %p, SERVER_TASK_TYPE: %d, "
                "IDEMPOTENCY_CHANNEL: %p != NULL", __LINE__, task,
                SERVER_TASK_TYPE, IDEMPOTENCY_CHANNEL);
    }

    ((FSServerTaskArg *)task->arg)->task_version =
        __sync_add_and_fetch(&NEXT_TASK_VERSION, 1);
    sf_task_finish_clean_up(task);
}

static int service_deal_client_join(struct fast_task_info *task)
{
    int result;
    int data_group_count;
    int file_block_size;
    uint32_t channel_id;
    int key;
    int flags;
    FSProtoClientJoinReq *req;
    FSProtoClientJoinResp *join_resp;

    if ((result=server_expect_body_length(task,
                    sizeof(FSProtoClientJoinReq))) != 0)
    {
        return result;
    }

    req = (FSProtoClientJoinReq *)REQUEST.body;
    data_group_count = buff2int(req->data_group_count);
    file_block_size = buff2int(req->file_block_size);
    flags = buff2int(req->flags);
    channel_id = buff2int(req->idempotency.channel_id);
    key = buff2int(req->idempotency.key);
    if (data_group_count != FS_DATA_GROUP_COUNT(CLUSTER_CONFIG_CTX)) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "client data group count: %d != mine: %d",
                data_group_count, FS_DATA_GROUP_COUNT(CLUSTER_CONFIG_CTX));
        return EINVAL;
    }
    if (file_block_size != FS_FILE_BLOCK_SIZE) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "file block size: %d != mine: %d",
                file_block_size, FS_FILE_BLOCK_SIZE);
        return EINVAL;
    }

    if ((flags & FS_CLIENT_JOIN_FLAGS_IDEMPOTENCY_REQUEST) != 0) {
        if (IDEMPOTENCY_CHANNEL != NULL) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "channel already exist, the channel id: %d",
                    IDEMPOTENCY_CHANNEL->id);
            return EEXIST;
        }

        IDEMPOTENCY_CHANNEL = idempotency_channel_find_and_hold(
                channel_id, key, &result);
        if (IDEMPOTENCY_CHANNEL == NULL) {
            RESPONSE.error.length = sprintf(RESPONSE.error.message,
                    "find channel fail, channel id: %d, result: %d",
                    channel_id, result);
            return SF_RETRIABLE_ERROR_NO_CHANNEL;
        }

        SERVER_TASK_TYPE = SF_SERVER_TASK_TYPE_CHANNEL_USER;
    }

    join_resp = (FSProtoClientJoinResp *)REQUEST.body;
    int2buff(g_sf_global_vars.min_buff_size - (sizeof(FSProtoHeader) +
            4 * sizeof(FSProtoSliceWriteReqHeader) +
            sizeof(FSProtoReplicaRPCReqBodyPart)),
            join_resp->buffer_size);
    RESPONSE.header.body_len = sizeof(FSProtoClientJoinResp);
    RESPONSE.header.cmd = FS_SERVICE_PROTO_CLIENT_JOIN_RESP;
    TASK_ARG->context.response_done = true;
    return 0;
}

static int service_deal_service_stat(struct fast_task_info *task)
{
    int result;
    FSProtoServiceStatResp *stat_resp;

    if ((result=server_expect_body_length(task, 0)) != 0) {
        return result;
    }

    stat_resp = (FSProtoServiceStatResp *)REQUEST.body;
    stat_resp->is_leader  = CLUSTER_MYSELF_PTR == CLUSTER_LEADER_PTR ? 1 : 0;
    int2buff(CLUSTER_MYSELF_PTR->server->id, stat_resp->server_id);
    int2buff(SF_G_CONN_CURRENT_COUNT, stat_resp->connection.current_count);
    int2buff(SF_G_CONN_MAX_COUNT, stat_resp->connection.max_count);

    RESPONSE.header.body_len = sizeof(FSProtoServiceStatResp);
    RESPONSE.header.cmd = FS_SERVICE_PROTO_SERVICE_STAT_RESP;
    TASK_ARG->context.response_done = true;

    return 0;
}

static void slice_read_done_notify(FSSliceOpContext *notify)
{
    struct fast_task_info *task;

    task = (struct fast_task_info *)notify->notify.arg;
    if (notify->result != 0) {
        RESPONSE.error.length = snprintf(RESPONSE.error.message,
                sizeof(RESPONSE.error.message),
                "%s", STRERROR(notify->result));

        logError("file: "__FILE__", line: %d, "
                "client ip: %s, read slice fail, "
                "oid: %"PRId64", block offset: %"PRId64", "
                "slice offset: %d, length: %d, "
                "errno: %d, error info: %s",
                __LINE__, task->client_ip,
                OP_CTX_INFO.bs_key.block.oid, OP_CTX_INFO.bs_key.block.offset,
                OP_CTX_INFO.bs_key.slice.offset, OP_CTX_INFO.bs_key.slice.length,
                notify->result, STRERROR(notify->result));
        TASK_ARG->context.log_level = LOG_NOTHING;
    } else {
        RESPONSE.header.cmd = FS_SERVICE_PROTO_SLICE_READ_RESP;
        RESPONSE.header.body_len = notify->done_bytes;
        TASK_ARG->context.response_done = true;
    }

    RESPONSE_STATUS = notify->result;
    sf_nio_notify(task, SF_NIO_STAGE_CONTINUE);
}

static int service_deal_slice_read(struct fast_task_info *task)
{
    int result;
    FSProtoSliceReadReqHeader *req_header;
    char *buff;

    RESPONSE.header.cmd = FS_SERVICE_PROTO_SLICE_READ_RESP;
    if ((result=server_expect_body_length(task,
                    sizeof(FSProtoSliceReadReqHeader))) != 0)
    {
        return result;
    }

    req_header = (FSProtoSliceReadReqHeader *)REQUEST.body;
    if ((result=du_handler_parse_check_readable_block_slice(
                    task, &req_header->bs)) != 0)
    {
        return result;
    }

    if (OP_CTX_INFO.bs_key.slice.length > task->size - sizeof(FSProtoHeader)) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "read slice length: %d > task buffer size: %d",
                OP_CTX_INFO.bs_key.slice.length, (int)(
                    task->size - sizeof(FSProtoHeader)));
        return EOVERFLOW;
    }

    buff = REQUEST.body;
    OP_CTX_NOTIFY.func = slice_read_done_notify;
    OP_CTX_NOTIFY.arg = task;
    result = fs_slice_read_ex(&SLICE_OP_CTX, buff,
            SERVER_CTX->slice_ptr_array);
    if (result != 0) {
        du_handler_set_slice_op_error_msg(task, &SLICE_OP_CTX, "read", result);
        return result;
    }
    
    return TASK_STATUS_CONTINUE;
}

static int service_deal_get_master(struct fast_task_info *task)
{
    int result;
    int data_group_id;
    FSClusterDataGroupInfo *group;
    FSProtoGetServerResp *resp;
    FSClusterDataServerInfo *master;
    const FCAddressInfo *addr;

    if ((result=server_expect_body_length(task, 4)) != 0) {
        return result;
    }

    data_group_id = buff2int(REQUEST.body);
    if ((group=fs_get_data_group(data_group_id)) == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data_group_id: %d not exist", data_group_id);
        return ENOENT;
    }
    master = (FSClusterDataServerInfo *)__sync_fetch_and_add(&group->master, 0);
    if (master == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "the master NOT exist");
        return SF_RETRIABLE_ERROR_NO_SERVER;
    }

    resp = (FSProtoGetServerResp *)REQUEST.body;
    addr = fc_server_get_address_by_peer(&SERVICE_GROUP_ADDRESS_ARRAY(
                master->cs->server), task->client_ip);

    int2buff(master->cs->server->id, resp->server_id);
    snprintf(resp->ip_addr, sizeof(resp->ip_addr), "%s",
            addr->conn.ip_addr);
    short2buff(addr->conn.port, resp->port);

    RESPONSE.header.body_len = sizeof(FSProtoGetServerResp);
    RESPONSE.header.cmd = FS_SERVICE_PROTO_GET_MASTER_RESP;
    TASK_ARG->context.response_done = true;
    return 0;
}

static FSClusterDataServerInfo *get_readable_server(
        FSClusterDataGroupInfo *group, const SFDataReadRule read_rule)
{
    int index;
    int acc_index;
    int active_count;
    int i;
    FSClusterDataServerInfo *ds;
    FSClusterDataServerInfo *send;

    if (group->data_server_array.count == 1) {
        return (FSClusterDataServerInfo *)__sync_fetch_and_add(
                &group->master, 0);
    }

    index = rand() % group->data_server_array.count;
    if (__sync_add_and_fetch(&group->data_server_array.servers[index].
                status, 0) == FS_SERVER_STATUS_ACTIVE)
    {
        if (read_rule != sf_data_read_rule_slave_first ||
                !__sync_add_and_fetch(&group->data_server_array.
                    servers[index].is_master, 0))
        {
            return group->data_server_array.servers + index;
        }
    }

    acc_index = 0;
    send = group->data_server_array.servers + group->data_server_array.count;
    for (i=0; i<group->data_server_array.count; i++) {
        active_count = 0;
        for (ds=group->data_server_array.servers; ds<send; ds++) {
            if (__sync_add_and_fetch(&ds->status, 0) !=
                    FS_SERVER_STATUS_ACTIVE)
            {
                continue;
            }

            active_count++;
            if (read_rule == sf_data_read_rule_slave_first &&
                    __sync_add_and_fetch(&ds->is_master, 0)) {
                continue;
            }

            if (acc_index++ == index) {
                return ds;
            }
        }

        if (active_count == 0) {
            return NULL;
        } else if (active_count == 1) {
            break;
        }
    }

    return (FSClusterDataServerInfo *)__sync_fetch_and_add(
            &group->master, 0);
}

static int service_deal_get_readable_server(struct fast_task_info *task)
{
    int result;
    int data_group_id;
    SFDataReadRule read_rule;
    FSClusterDataGroupInfo *group;
    FSClusterDataServerInfo *ds;
    FSProtoGetReadableServerReq *req;
    FSProtoGetServerResp *resp;
    const FCAddressInfo *addr;

    if ((result=server_expect_body_length(task,
                    sizeof(FSProtoGetReadableServerReq))) != 0)
    {
        return result;
    }

    req = (FSProtoGetReadableServerReq *)REQUEST.body;
    data_group_id = buff2int(req->data_group_id);
    read_rule = req->read_rule;
    if ((group=fs_get_data_group(data_group_id)) == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "data_group_id: %d not exist", data_group_id);
        return ENOENT;
    }

    if (read_rule == sf_data_read_rule_master_only) {
        ds = (FSClusterDataServerInfo *)__sync_fetch_and_add(
                &group->master, 0);
    } else {
        ds = get_readable_server(group, read_rule);
    }

    if (ds == NULL) {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "no active server, read rule: %d", read_rule);
        return SF_RETRIABLE_ERROR_NO_SERVER;
    }

    resp = (FSProtoGetServerResp *)REQUEST.body;
    addr = fc_server_get_address_by_peer(&SERVICE_GROUP_ADDRESS_ARRAY(
                ds->cs->server), task->client_ip);

    int2buff(ds->cs->server->id, resp->server_id);
    snprintf(resp->ip_addr, sizeof(resp->ip_addr), "%s",
            addr->conn.ip_addr);
    short2buff(addr->conn.port, resp->port);

    RESPONSE.header.body_len = sizeof(FSProtoGetServerResp);
    RESPONSE.header.cmd = FS_SERVICE_PROTO_GET_READABLE_SERVER_RESP;
    TASK_ARG->context.response_done = true;
    return 0;
}

static int service_deal_cluster_stat(struct fast_task_info *task)
{
    int result;
    int data_group_id;
    FSProtoClusterStatRespBodyHeader *body_header;
    FSProtoClusterStatRespBodyPart *part_start;
    FSProtoClusterStatRespBodyPart *body_part;
    FSClusterDataGroupInfo *group;
    FSClusterDataGroupInfo *gend;
    FSClusterDataServerInfo *ds;
    FSClusterDataServerInfo *dend;
    const FCAddressInfo *addr;

    if ((result=server_check_max_body_length(task, 4)) != 0) {
        return result;
    }

    if (REQUEST.header.body_len == 0) {
        group = CLUSTER_DATA_RGOUP_ARRAY.groups;
        gend = CLUSTER_DATA_RGOUP_ARRAY.groups +
            CLUSTER_DATA_RGOUP_ARRAY.count;
    } else if (REQUEST.header.body_len == 4) {
        data_group_id = buff2int(REQUEST.body);
        if ((group=fs_get_data_group(data_group_id)) == NULL) {
            return ENOENT;
        }
        gend = group + 1;
    } else {
        RESPONSE.error.length = sprintf(RESPONSE.error.message,
                "invalid request body length: %d != 0 or 4",
                REQUEST.header.body_len);
        return EINVAL;
    }

    body_header = (FSProtoClusterStatRespBodyHeader *)REQUEST.body;
    part_start = (FSProtoClusterStatRespBodyPart *)(REQUEST.body +
            sizeof(FSProtoClusterStatRespBodyHeader));
    body_part = part_start;

    for (; group<gend; group++) {
        dend = group->data_server_array.servers +
            group->data_server_array.count;
        for (ds=group->data_server_array.servers; ds<dend;
                ds++, body_part++)
        {

            addr = fc_server_get_address_by_peer(&SERVICE_GROUP_ADDRESS_ARRAY(
                        ds->cs->server), task->client_ip);

            int2buff(ds->dg->id, body_part->data_group_id);
            int2buff(ds->cs->server->id, body_part->server_id);
            snprintf(body_part->ip_addr, sizeof(body_part->ip_addr),
                    "%s", addr->conn.ip_addr);
            short2buff(addr->conn.port, body_part->port);
            body_part->is_preseted = ds->is_preseted;
            body_part->is_master = __sync_add_and_fetch(&ds->is_master, 0);
            body_part->status = __sync_add_and_fetch(&ds->status, 0);
            long2buff(ds->replica.data_version, body_part->data_version);
        }
    }

    int2buff(body_part - part_start, body_header->count);
    RESPONSE.header.body_len = (char *)body_part - REQUEST.body;
    RESPONSE.header.cmd = FS_SERVICE_PROTO_CLUSTER_STAT_RESP;
    TASK_ARG->context.response_done = true;
    return 0;
}

static int service_update_prepare_and_check(struct fast_task_info *task,
        bool *deal_done)
{
    if (SERVER_TASK_TYPE == SF_SERVER_TASK_TYPE_CHANNEL_USER &&
            IDEMPOTENCY_CHANNEL != NULL)
    {
        IdempotencyRequest *request;
        int result;

        request = sf_server_update_prepare_and_check(
                task, &SERVER_CTX->service.request_allocator,
                IDEMPOTENCY_CHANNEL, &RESPONSE, &result);
        if (request != NULL) {
            if (result != 0) {
                if (result == EEXIST) { //found
                    result = request->output.result;
                    du_handler_fill_slice_update_response(task,
                            ((FSUpdateOutput *)request->output.
                             response)->inc_alloc);
                }

                fast_mblock_free_object(request->allocator, request);
                *deal_done = true;
                return result;
            }
        } else {
            *deal_done = true;
            return result;
        }

        IDEMPOTENCY_REQUEST = request;
        OP_CTX_INFO.body = REQUEST.body +
            sizeof(SFProtoIdempotencyAdditionalHeader);
        OP_CTX_INFO.body_len = REQUEST.header.body_len -
            sizeof(SFProtoIdempotencyAdditionalHeader);
    } else {
        OP_CTX_INFO.body = REQUEST.body;
        OP_CTX_INFO.body_len = REQUEST.header.body_len;
    }

    TASK_CTX.which_side = FS_WHICH_SIDE_MASTER;
    OP_CTX_INFO.data_version = 0;
    SLICE_OP_CTX.write.inc_alloc = 0;

    *deal_done = false;
    return 0;
}

static inline int service_deal_slice_write(struct fast_task_info *task)
{
    int result;
    bool deal_done;

    result = service_update_prepare_and_check(task, &deal_done);
    if (result != 0 || deal_done) {
        return result;
    }

    if ((result=du_handler_deal_slice_write(task, &SLICE_OP_CTX)) !=
            TASK_STATUS_CONTINUE)
    {
        du_handler_idempotency_request_finish(task, result);
    }
    return result;
}

static inline int service_deal_slice_allocate(struct fast_task_info *task)
{
    int result;
    bool deal_done;

    result = service_update_prepare_and_check(task, &deal_done);
    if (result != 0 || deal_done) {
        return result;
    }

    if ((result=du_handler_deal_slice_allocate(task, &SLICE_OP_CTX)) !=
            TASK_STATUS_CONTINUE)
    {
        du_handler_idempotency_request_finish(task, result);
    }
    return result;
}

static inline int service_deal_slice_delete(struct fast_task_info *task)
{
    int result;
    bool deal_done;

    result = service_update_prepare_and_check(task, &deal_done);
    if (result != 0 || deal_done) {
        return result;
    }

    if ((result=du_handler_deal_slice_delete(task, &SLICE_OP_CTX)) !=
            TASK_STATUS_CONTINUE)
    {
        du_handler_idempotency_request_finish(task, result);
    }
    return result;
}

static inline int service_deal_block_delete(struct fast_task_info *task)
{
    int result;
    bool deal_done;

    result = service_update_prepare_and_check(task, &deal_done);
    if (result != 0 || deal_done) {
        return result;
    }

    if ((result=du_handler_deal_block_delete(task, &SLICE_OP_CTX)) !=
            TASK_STATUS_CONTINUE)
    {
        du_handler_idempotency_request_finish(task, result);
    }
    return result;
}

int service_deal_task(struct fast_task_info *task)
{
    int result;
    int stage;

    stage = SF_NIO_TASK_STAGE_FETCH(task);

    /*
    logInfo("file: "__FILE__", line: %d, "
            "nio stage: %d, SF_NIO_STAGE_CONTINUE: %d", __LINE__,
            stage, SF_NIO_STAGE_CONTINUE);
            */

    if (stage == SF_NIO_STAGE_CONTINUE) {
        sf_nio_swap_stage(task, stage, SF_NIO_STAGE_SEND);
        if (TASK_ARG->context.deal_func != NULL) {
            result = TASK_ARG->context.deal_func(task);
        } else {
            result = RESPONSE_STATUS;
            if (result == TASK_STATUS_CONTINUE) {
                logError("file: "__FILE__", line: %d, "
                        "unexpect status: %d", __LINE__, result);
                result = EBUSY;
            }
        }
    } else {
        handler_init_task_context(task);

        switch (REQUEST.header.cmd) {
            case SF_PROTO_ACTIVE_TEST_REQ:
                RESPONSE.header.cmd = SF_PROTO_ACTIVE_TEST_RESP;
                result = sf_proto_deal_actvie_test(task, &REQUEST, &RESPONSE);
                break;
            case FS_SERVICE_PROTO_CLIENT_JOIN_REQ:
                result = service_deal_client_join(task);
                break;
            case FS_SERVICE_PROTO_SERVICE_STAT_REQ:
                result = service_deal_service_stat(task);
                break;
            case FS_SERVICE_PROTO_SLICE_WRITE_REQ:
                result = service_deal_slice_write(task);
                break;
            case FS_SERVICE_PROTO_SLICE_ALLOCATE_REQ:
                result = service_deal_slice_allocate(task);
                break;
            case FS_SERVICE_PROTO_SLICE_DELETE_REQ:
                result = service_deal_slice_delete(task);
                break;
            case FS_SERVICE_PROTO_BLOCK_DELETE_REQ:
                result = service_deal_block_delete(task);
                break;
            case FS_SERVICE_PROTO_SLICE_READ_REQ:
                result = service_deal_slice_read(task);
                break;
            case FS_SERVICE_PROTO_GET_MASTER_REQ:
                result = service_deal_get_master(task);
                break;
            case FS_SERVICE_PROTO_GET_READABLE_SERVER_REQ:
                result = service_deal_get_readable_server(task);
                break;
            case FS_SERVICE_PROTO_CLUSTER_STAT_REQ:
                result = service_deal_cluster_stat(task);
                break;
            case SF_SERVICE_PROTO_SETUP_CHANNEL_REQ:
                if ((result=sf_server_deal_setup_channel(task,
                                &SERVER_TASK_TYPE, &IDEMPOTENCY_CHANNEL,
                                &RESPONSE)) == 0)
                {
                    TASK_ARG->context.response_done = true;
                }
                break;
            case SF_SERVICE_PROTO_CLOSE_CHANNEL_REQ:
                result = sf_server_deal_close_channel(task,
                        &SERVER_TASK_TYPE, &IDEMPOTENCY_CHANNEL, &RESPONSE);
                break;
            case SF_SERVICE_PROTO_REPORT_REQ_RECEIPT_REQ:
                result = sf_server_deal_report_req_receipt(task,
                        SERVER_TASK_TYPE, IDEMPOTENCY_CHANNEL, &RESPONSE);
                break;
            default:
                RESPONSE.error.length = sprintf(
                        RESPONSE.error.message,
                        "unkown cmd: %d", REQUEST.header.cmd);
                result = -EINVAL;
                break;
        }
    }

    if (result == TASK_STATUS_CONTINUE) {
        return 0;
    } else {
        RESPONSE_STATUS = result;
        return handler_deal_task_done(task);
    }
}

void *service_alloc_thread_extra_data(const int thread_index)
{
    FSServerContext *server_context;
    int element_size;

    if ((server_context=du_handler_alloc_server_context()) == NULL) {
        return NULL;
    }

    element_size = sizeof(IdempotencyRequest) + sizeof(FSUpdateOutput);
    if (fast_mblock_init_ex1(&server_context->service.request_allocator,
                "idempotency_request", element_size, 1024, 0,
                idempotency_request_alloc_init,
                &server_context->service.request_allocator, true) != 0)
    {
        return NULL;
    }
    return server_context;
}
