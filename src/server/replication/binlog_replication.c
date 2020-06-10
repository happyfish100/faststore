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
#include "fastcommon/sched_thread.h"
#include "fastcommon/ioevent_loop.h"
#include "sf/sf_global.h"
#include "sf/sf_nio.h"
#include "../../common/fs_proto.h"
#include "../server_global.h"
#include "../server_group_info.h"
#include "../binlog/binlog_reader.h"
#include "push_result_ring.h"
#include "binlog_replication.h"

static void replication_queue_discard_all(FSReplication *replication);

static int check_alloc_ptr_array(FSReplicationPtrArray *array)
{
    int bytes;

    if (array->replications != NULL) {
        return 0;
    }

    bytes = sizeof(FSReplication *) * CLUSTER_SERVER_ARRAY.count;
    array->replications = (FSReplication **)malloc(bytes);
    if (array->replications == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(array->replications, 0, bytes);
    return 0;
}

static void add_to_replication_ptr_array(FSReplicationPtrArray *
        array, FSReplication *replication)
{
    array->replications[array->count++] = replication;
}

static int remove_from_replication_ptr_array(FSReplicationPtrArray *
        array, FSReplication *replication)
{
    int i;

    for (i=0; i<array->count; i++) {
        if (array->replications[i] == replication) {
            break;
        }
    }

    if (i == array->count) {
        logError("file: "__FILE__", line: %d, "
                "can't found replication slave id: %d",
                __LINE__, replication->peer->server->id);
        return ENOENT;
    }

    for (i=i+1; i<array->count; i++) {
        array->replications[i - 1] = array->replications[i];
    }
    array->count--;
    return 0;
}

static inline void set_replication_stage(FSReplication *
        replication, const int stage)
{
    switch (stage) {
        case FS_REPLICATION_STAGE_INITED:
            /*
            if (replication->peer->status == FS_SERVER_STATUS_SYNCING ||
                    replication->peer->status == FS_SERVER_STATUS_ACTIVE)
            {
                cluster_info_set_status(replication->peer,
                        FS_SERVER_STATUS_OFFLINE);
            }
            */
            break;

        case FS_REPLICATION_STAGE_SYNC_FROM_DISK:
            /*
            if (replication->peer->status == FS_SERVER_STATUS_INIT) {
                cluster_info_set_status(replication->peer,
                        FS_SERVER_STATUS_BUILDING);
            } else if (replication->peer->status !=
                    FS_SERVER_STATUS_BUILDING)
            {
                cluster_info_set_status(replication->peer,
                        FS_SERVER_STATUS_SYNCING);
            }
            */
            break;

        case FS_REPLICATION_STAGE_SYNC_FROM_QUEUE:
            /*
            cluster_info_set_status(replication->peer,
                    FS_SERVER_STATUS_ACTIVE);
                    */
            break;

        default:
            break;
    }
    replication->stage = stage;
}

int binlog_replication_bind_thread(FSReplication *replication)
{
    int result;
    int alloc_size;
    struct fast_task_info *task;
    FSServerContext *server_ctx;

    task = free_queue_pop();
    if (task == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc task buff failed, you should "
                "increase the parameter: max_connections",
                __LINE__);
        return ENOMEM;
    }

    alloc_size = 4 * task->size / FS_REPLICA_BINLOG_MAX_RECORD_SIZE;
    if ((result=push_result_ring_check_init(&replication->
                    context.push_result_ctx, alloc_size)) != 0)
    {
        return result;
    }

    task->canceled = false;
    task->ctx = &CLUSTER_SF_CTX;
    task->event.fd = -1;
    task->thread_data = CLUSTER_SF_CTX.thread_data +
        replication->thread_index % CLUSTER_SF_CTX.work_threads;

    set_replication_stage(replication, FS_REPLICATION_STAGE_INITED);
    replication->context.last_data_versions.by_disk.previous = 0;
    replication->context.last_data_versions.by_disk.current = 0;
    replication->context.last_data_versions.by_queue = 0;
    replication->context.last_data_versions.by_resp = 0;

    replication->context.sync_by_disk_stat.start_time_ms = 0;
    replication->context.sync_by_disk_stat.binlog_size = 0;
    replication->context.sync_by_disk_stat.record_count = 0;

    CLUSTER_TASK_TYPE = FS_CLUSTER_TASK_TYPE_REPLICA_MASTER;
    CLUSTER_REPLICA = replication;
    replication->connection_info.conn.sock = -1;
    replication->task = task;

    server_ctx = (FSServerContext *)task->thread_data->arg;
    if ((result=check_alloc_ptr_array(&server_ctx->
                    cluster.connectings)) != 0)
    {
        return result;
    }
    if ((result=check_alloc_ptr_array(&server_ctx->
                    cluster.connected)) != 0)
    {
        return result;
    }

    add_to_replication_ptr_array(&server_ctx->
            cluster.connectings, replication);
    return 0;
}

int binlog_replication_rebind_thread(FSReplication *replication)
{
    int result;
    FSServerContext *server_ctx;

    server_ctx = (FSServerContext *)replication->task->thread_data->arg;
    if ((result=remove_from_replication_ptr_array(&server_ctx->
                cluster.connected, replication)) == 0)
    {
        replication_queue_discard_all(replication);
        push_result_ring_clear_all(&replication->context.push_result_ctx);
        /*
        if (MYSELF_IS_MASTER) {
            result = binlog_replication_bind_thread(replication);
        }
        */
    }

    return result;
}

static int async_connect_server(ConnectionInfo *conn)
{
    int result;
    int domain;
    sockaddr_convert_t convert;

    if (conn->socket_domain == AF_INET || conn->socket_domain == AF_INET6) {
        domain = conn->socket_domain;
    } else {
        domain = is_ipv6_addr(conn->ip_addr) ? AF_INET6 : AF_INET;
    }
    conn->sock = socket(domain, SOCK_STREAM, 0);
    if(conn->sock < 0) {
        logError("file: "__FILE__", line: %d, "
                "socket create fail, errno: %d, "
                "error info: %s", __LINE__, errno, STRERROR(errno));
        return errno != 0 ? errno : EPERM;
    }

    SET_SOCKOPT_NOSIGPIPE(conn->sock);
    if ((result=tcpsetnonblockopt(conn->sock)) != 0) {
        close(conn->sock);
        conn->sock = -1;
        return result;
    }

    if ((result=setsockaddrbyip(conn->ip_addr, conn->port, &convert)) != 0) {
        return result;
    }

    if (connect(conn->sock, &convert.sa.addr, convert.len) < 0) {
        result = errno != 0 ? errno : EINPROGRESS;
        if (result != EINPROGRESS) {
            logError("file: "__FILE__", line: %d, "
                    "connect to %s:%d fail, errno: %d, error info: %s",
                    __LINE__, conn->ip_addr, conn->port,
                    result, STRERROR(result));
            close(conn->sock);
            conn->sock = -1;
        }
        return result;
    }

    return 0;
}

static void calc_next_connect_time(FSReplication *replication)
{
    int interval;

    switch (replication->connection_info.fail_count) {
        case 0:
            interval = 1;
            break;
        case 1:
            interval = 2;
            break;
        case 2:
            interval = 4;
            break;
        case 3:
            interval = 8;
            break;
        case 4:
            interval = 16;
            break;
        default:
            interval = 32;
            break;
    }

    replication->connection_info.next_connect_time = g_current_time + interval;
}

static int check_and_make_replica_connection(FSReplication *replication)
{
    int result;
    int polled;
    socklen_t len;
    struct pollfd pollfds;

    if (replication->connection_info.conn.sock < 0) {
        FCAddressPtrArray *addr_array;
        FCAddressInfo *addr;

        if (replication->connection_info.next_connect_time > g_current_time) {
            return EAGAIN;
        }

        addr_array = &CLUSTER_GROUP_ADDRESS_ARRAY(replication->peer->server);
        addr = addr_array->addrs[addr_array->index++];
        if (addr_array->index >= addr_array->count) {
            addr_array->index = 0;
        }

        replication->connection_info.start_time = g_current_time;
        replication->connection_info.conn = addr->conn;
        calc_next_connect_time(replication);
        if ((result=async_connect_server(&replication->
                        connection_info.conn)) == 0)
        {
            return 0;
        }
        if (result != EINPROGRESS) {
            return result;
        }
    }

    pollfds.fd = replication->connection_info.conn.sock;
    pollfds.events = POLLIN | POLLOUT;
    polled = poll(&pollfds, 1, 0);
    if (polled == 0) {  //timeout
        if (g_current_time - replication->connection_info.start_time >
                SF_G_CONNECT_TIMEOUT)
        {
            result = ETIMEDOUT;
        } else {
            result = EINPROGRESS;
        }
    } else if (polled < 0) {   //error
        result = errno != 0 ? errno : EIO;
    } else {
        len = sizeof(result);
        if (getsockopt(replication->connection_info.conn.sock, SOL_SOCKET,
                    SO_ERROR, &result, &len) < 0)
        {
            result = errno != 0 ? errno : EACCES;
        }
    }

    if (!(result == 0 || result == EINPROGRESS)) {
        close(replication->connection_info.conn.sock);
        replication->connection_info.conn.sock = -1;
    }
    return result;
}

static int send_join_slave_package(FSReplication *replication)
{
	int result;
	FSProtoHeader *header;
    FSProtoJoinServerReq *req;
	char out_buff[sizeof(FSProtoHeader) + sizeof(FSProtoJoinServerReq)];

    header = (FSProtoHeader *)out_buff;
    FS_PROTO_SET_HEADER(header, FS_REPLICA_PROTO_JOIN_SERVER_REQ,
            sizeof(out_buff) - sizeof(FSProtoHeader));

    req = (FSProtoJoinServerReq *)(out_buff + sizeof(FSProtoHeader));
    int2buff(CLUSTER_MY_SERVER_ID, req->server_id);
    int2buff(replication->task->size, req->buffer_size);
    int2buff(REPLICA_CHANNELS_BETWEEN_TWO_SERVERS,
            req->replica_channels_between_two_servers);
    memcpy(req->config_signs.servers, SERVERS_CONFIG_SIGN_BUF,
            SERVERS_CONFIG_SIGN_LEN);
    memcpy(req->config_signs.cluster, CLUSTER_CONFIG_SIGN_BUF,
            CLUSTER_CONFIG_SIGN_LEN);
    if ((result=tcpsenddata_nb(replication->connection_info.conn.sock,
                    out_buff, sizeof(out_buff), SF_G_NETWORK_TIMEOUT)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "send data to server %s:%d fail, "
                "errno: %d, error info: %s", __LINE__,
                replication->connection_info.conn.ip_addr,
                replication->connection_info.conn.port,
                result, STRERROR(result));
        close(replication->connection_info.conn.sock);
        replication->connection_info.conn.sock = -1;
    }

    return result;
}

static void decrease_task_waiting_rpc_count(ServerBinlogRecordBuffer *rb)
{
    if (rb->task_version != __sync_add_and_fetch(&((FSServerTaskArg *)
                    rb->task->arg)->task_version, 0))
    {
        logWarning("file: "__FILE__", line: %d, "
                "task %p already cleanup", __LINE__, rb->task);
        return;
    }

    if (__sync_sub_and_fetch(&((FSServerTaskArg *)rb->task->arg)->
                context.service.waiting_rpc_count, 1) == 0)
    {
        sf_nio_notify(rb->task, SF_NIO_STAGE_CONTINUE);
    }
}

static void discard_queue(FSReplication *replication,
        ServerBinlogRecordBuffer *head, ServerBinlogRecordBuffer *tail)
{
    ServerBinlogRecordBuffer *rb;
    while (head != tail) {
        rb = head;
        head = head->nexts[replication->peer->link_index];

        replication->context.last_data_versions.by_queue = rb->data_version;
        decrease_task_waiting_rpc_count(rb);
        rb->release_func(rb);
    }
}

static void replication_queue_discard_all(FSReplication *replication)
{
    ServerBinlogRecordBuffer *head;

    PTHREAD_MUTEX_LOCK(&replication->context.queue.lock);
    head = replication->context.queue.head;
    if (replication->context.queue.head != NULL) {
        replication->context.queue.head = replication->context.queue.tail = NULL;
    }
    PTHREAD_MUTEX_UNLOCK(&replication->context.queue.lock);

    if (head != NULL) {
        discard_queue(replication, head, NULL);
    }
}

static void replication_queue_discard_synced(FSReplication *replication)
{
    ServerBinlogRecordBuffer *head;
    ServerBinlogRecordBuffer *tail;

    PTHREAD_MUTEX_LOCK(&replication->context.queue.lock);
    do {
        head = replication->context.queue.head;
        if (head == NULL) {
            tail = NULL;
            break;
        }

        tail = head;
        while ((tail != NULL) && (tail->data_version <=
                    replication->context.last_data_versions.by_disk.current))
        {
            tail = tail->nexts[replication->peer->link_index];
        }

        replication->context.queue.head = tail;
        if (tail == NULL) {
            replication->context.queue.tail = NULL;
        }
    } while (0);
    PTHREAD_MUTEX_UNLOCK(&replication->context.queue.lock);

    if (head != NULL) {
        discard_queue(replication, head, tail);
    }
}

static int deal_connecting_replication(FSReplication *replication)
{
    int result;

    result = check_and_make_replica_connection(replication);
    if (result == 0) {
        result = send_join_slave_package(replication);
    }

    return result;
}

static int deal_replication_connectings(FSServerContext *server_ctx)
{
#define SUCCESS_ARRAY_ELEMENT_MAX  64

    int result;
    int i;
    char prompt[128];
    struct {
        int count;
        FSReplication *replications[SUCCESS_ARRAY_ELEMENT_MAX];
    } success_array;
    FSReplication *replication;

    if (server_ctx->cluster.connectings.count == 0) {
        return 0;
    }

    success_array.count = 0;
    for (i=0; i<server_ctx->cluster.connectings.count; i++) {
        replication = server_ctx->cluster.connectings.replications[i];
        result = deal_connecting_replication(replication);
        if (result == 0) {
            if (success_array.count < SUCCESS_ARRAY_ELEMENT_MAX) {
                success_array.replications[success_array.count++] = replication;
            }
            set_replication_stage(replication,
                    FS_REPLICATION_STAGE_WAITING_JOIN_RESP);
        } else if (result == EINPROGRESS) {
            set_replication_stage(replication,
                    FS_REPLICATION_STAGE_CONNECTING);
        } else if (result != EAGAIN) {
            if (result != replication->connection_info.last_errno
                    || replication->connection_info.fail_count % 100 == 0)
            {
                replication->connection_info.last_errno = result;
                logError("file: "__FILE__", line: %d, "
                        "%dth connect to %s:%d fail, time used: %ds, "
                        "errno: %d, error info: %s", __LINE__,
                        replication->connection_info.fail_count + 1,
                        replication->connection_info.conn.ip_addr,
                        replication->connection_info.conn.port, (int)
                        (g_current_time - replication->connection_info.start_time),
                        result, STRERROR(result));
            }
            replication->connection_info.fail_count++;
        }
    }

    for (i=0; i<success_array.count; i++) {
        replication = success_array.replications[i];

        if (replication->connection_info.fail_count > 0) {
            sprintf(prompt, " after %d retries",
                    replication->connection_info.fail_count);
        } else {
            *prompt = '\0';
        }
        logInfo("file: "__FILE__", line: %d, "
                "connect to slave %s:%d successfully%s.", __LINE__,
                replication->connection_info.conn.ip_addr,
                replication->connection_info.conn.port, prompt);

        if (remove_from_replication_ptr_array(&server_ctx->
                    cluster.connectings, replication) == 0)
        {
            /*
            replication->peer->last_data_version = -1;
            replication->peer->binlog_pos_hint.index = -1;
            replication->peer->binlog_pos_hint.offset = -1;
            */

            add_to_replication_ptr_array(&server_ctx->
                    cluster.connected, replication);
        }

        replication->connection_info.fail_count = 0;
        replication->task->event.fd = replication->
            connection_info.conn.sock;
        snprintf(replication->task->client_ip,
                sizeof(replication->task->client_ip), "%s",
                replication->connection_info.conn.ip_addr);
        replication->task->port = replication->connection_info.conn.port;
        sf_nio_notify(replication->task, SF_NIO_STAGE_INIT);
    }
    return 0;
}

void clean_connected_replications(FSServerContext *server_ctx)
{
    FSReplication *replication;
    int i;

    for (i=0; i<server_ctx->cluster.connected.count; i++) {
        replication = server_ctx->cluster.connected.replications[i];
        iovent_add_to_deleted_list(replication->task);
    }
}

static void repush_to_replication_queue(FSReplication *replication,
        ServerBinlogRecordBuffer *head, ServerBinlogRecordBuffer *tail)
{
    PTHREAD_MUTEX_LOCK(&replication->context.queue.lock);
    tail->nexts[replication->peer->link_index] = replication->context.queue.head;
    replication->context.queue.head = head;
    if (replication->context.queue.tail == NULL) {
        replication->context.queue.tail = tail;
    }
    PTHREAD_MUTEX_UNLOCK(&replication->context.queue.lock);
}

static int sync_binlog_from_queue(FSReplication *replication)
{
    ServerBinlogRecordBuffer *rb;
    ServerBinlogRecordBuffer *head;
    ServerBinlogRecordBuffer *tail;
    FSProtoPushBinlogReqBodyHeader *body_header;
    uint64_t last_data_version;
    int body_len;
    int result;

    PTHREAD_MUTEX_LOCK(&replication->context.queue.lock);
    head = replication->context.queue.head;
    tail = replication->context.queue.tail;
    if (replication->context.queue.head != NULL) {
        replication->context.queue.head = replication->context.queue.tail = NULL;
    }
    PTHREAD_MUTEX_UNLOCK(&replication->context.queue.lock);

    if (head == NULL) {
        return 0;
    }

    last_data_version = 0;
    replication->task->length = sizeof(FSProtoHeader) +
        sizeof(FSProtoPushBinlogReqBodyHeader);
    while (head != NULL) {
        rb = head;
        if (rb->task_version != __sync_add_and_fetch(&((FSServerTaskArg *)
                        rb->task->arg)->task_version, 0))
        {
            logWarning("file: "__FILE__", line: %d, "
                    "task %p already cleanup", __LINE__, rb->task);
        } else {

            //TODO
            /*
            if (replication->task->length + rb->buffer.length >
                    replication->task->size)
            {
                break;
            }

            last_data_version = rb->data_version;
            replication->context.last_data_versions.by_queue = rb->data_version;
            memcpy(replication->task->data + replication->task->length,
                    rb->buffer.data, rb->buffer.length);
            replication->task->length += rb->buffer.length;
            */

            //logInfo("call push_result_ring_add data_version: %"PRId64, rb->data_version);
            if ((result=push_result_ring_add(&replication->context.
                            push_result_ctx, rb->data_version,
                            rb->task, rb->task_version)) != 0)
            {
                return result;
            }
        }

        head = head->nexts[replication->peer->link_index];
        rb->release_func(rb);
    }

    body_header = (FSProtoPushBinlogReqBodyHeader *)
        (replication->task->data + sizeof(FSProtoHeader));
    body_len = replication->task->length - sizeof(FSProtoHeader);
    int2buff(body_len - sizeof(FSProtoPushBinlogReqBodyHeader),
            body_header->binlog_length);
    long2buff(last_data_version, body_header->last_data_version);

    FS_PROTO_SET_HEADER((FSProtoHeader *)replication->task->data,
            FS_REPLICA_PROTO_PUSH_BINLOG_REQ, body_len);
    sf_send_add_event(replication->task);

    if (head != NULL) {
        repush_to_replication_queue(replication, head, tail);
    }
    return 0;
}

int binlog_replications_check_response_data_version(
        FSReplication *replication,
        const int64_t data_version)
{
    if (data_version > replication->context.last_data_versions.by_resp) {
        replication->context.last_data_versions.by_resp = data_version;
    }

    if (replication->stage == FS_REPLICATION_STAGE_SYNC_FROM_QUEUE) {
        return push_result_ring_remove(&replication->context.
                push_result_ctx, data_version);
    } else if (replication->stage == FS_REPLICATION_STAGE_SYNC_FROM_DISK) {
        replication->context.sync_by_disk_stat.record_count++;
    }

    return 0;
}

static int deal_connected_replication(FSReplication *replication)
{
    int result;

    //logInfo("replication stage: %d", replication->stage);

    if (replication->stage != FS_REPLICATION_STAGE_SYNC_FROM_QUEUE) {
        replication_queue_discard_all(replication);
    }

    if (replication->stage == FS_REPLICATION_STAGE_WAITING_JOIN_RESP) {
        /*
        if (replication->peer->last_data_version < 0 ||
                replication->peer->binlog_pos_hint.index < 0 ||
                replication->peer->binlog_pos_hint.offset < 0)
        {
            return 0;
        }

        if ((result=start_binlog_read_thread(replication)) == 0) {
            set_replication_stage(replication,
                    FS_REPLICATION_STAGE_SYNC_FROM_DISK);
            replication->context.sync_by_disk_stat.start_time_ms =
                get_current_time_ms();
        }
        */
        result = 0;
        return result;
    }

    if (!(replication->task->offset == 0 && replication->task->length == 0)) {
        return 0;
    }

    if (replication->stage == FS_REPLICATION_STAGE_SYNC_FROM_DISK) {
        if (replication->context.last_data_versions.by_resp >=
                replication->context.last_data_versions.by_disk.previous) //flow control
        {
            //return sync_binlog_from_disk(replication);
        }
    } else if (replication->stage == FS_REPLICATION_STAGE_SYNC_FROM_QUEUE) {
        push_result_ring_clear_timeouts(&replication->context.push_result_ctx);
        return sync_binlog_from_queue(replication);
    }

    return 0;
}

static int deal_replication_connected(FSServerContext *server_ctx)
{
    FSReplication *replication;
    int i;

    /*
    static int count = 0;

    if (++count % 10000 == 0) {
        logInfo("server_ctx %p, connected.count: %d", server_ctx, server_ctx->cluster.connected.count);
    }
    */

    if (server_ctx->cluster.connected.count == 0) {
        return 0;
    }

    for (i=0; i<server_ctx->cluster.connected.count; i++) {
        replication = server_ctx->cluster.connected.replications[i];
        if (deal_connected_replication(replication) != 0) {
            iovent_add_to_deleted_list(replication->task);
        }
    }

    return 0;
}

int binlog_replication_process(FSServerContext *server_ctx)
{
    int result;

    /*
    static int count = 0;

    if (++count % 10000 == 0) {
        logInfo("file: "__FILE__", line: %d, count: %d, g_current_time: %d",
                __LINE__, count, (int)g_current_time);
    }
    */

    if ((result=deal_replication_connectings(server_ctx)) != 0) {
        return result;
    }

    return deal_replication_connected(server_ctx);
}
