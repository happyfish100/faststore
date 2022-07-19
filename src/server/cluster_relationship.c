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

#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/fast_buffer.h"
#include "sf/sf_global.h"
#include "fastcfs/vote/fcfs_vote_client.h"
#include "common/fs_proto.h"
#include "replication/replication_quorum.h"
#include "server_global.h"
#include "server_recovery.h"
#include "master_election.h"
#include "cluster_topology.h"
#include "cluster_relationship.h"

#define ALIGN_TIME(interval) (((interval) / 60) * 60)

#define NEED_REQUEST_VOTE_NODE(active_count) \
    SF_ELECTION_QUORUM_NEED_REQUEST_VOTE_NODE(LEADER_ELECTION_QUORUM, \
            VOTE_NODE_ENABLED, CLUSTER_SERVER_ARRAY.count, active_count)

#define NEED_CHECK_VOTE_NODE() \
    SF_ELECTION_QUORUM_NEED_CHECK_VOTE_NODE(LEADER_ELECTION_QUORUM, \
            VOTE_NODE_ENABLED, CLUSTER_SERVER_ARRAY.count)

typedef struct fs_data_server_status {
    char is_master;
    char status;
    int master_dealing_count;
    int64_t data_version;
} FSDataServerStatus;

typedef struct fs_cluster_server_status {
    FSClusterServerInfo *cs;
    char is_leader;
    char leader_hint;
    char force_election;
    int last_heartbeat_time;
    int server_id;
    int up_time;
    int last_shutdown_time;
    int64_t version;
} FSClusterServerStatus;

typedef struct fs_my_data_group_info {
    int data_group_id;
    FSClusterDataServerInfo *ds;
} FSMyDataGroupInfo;

typedef struct fs_my_data_group_array {
    FSMyDataGroupInfo *groups;
    int count;
} FSMyDataGroupArray;

typedef struct fs_cluster_server_detect_entry {
    FSClusterServerInfo *cs;
    int next_time;
} FSClusterServerDetectEntry;

typedef struct fs_cluster_server_detect_array {
    FSClusterServerDetectEntry *entries;
    int count;
    int alloc;
    pthread_mutex_t lock;
} FSClusterServerDetectArray;

typedef struct fs_cluster_relationship_context {
    struct {
        int retry_count;
        int64_t start_time_ms;
        FSClusterServerInfo *next_leader;
    } leader_election;

    FSMyDataGroupArray my_data_group_array;
    FSClusterServerDetectArray inactive_server_array;
    time_t last_stat_time;
    volatile int immediate_report;
    FastBuffer buffer;
    ConnectionInfo vote_connection;
} FSClusterRelationshipContext;

#define MY_DATA_GROUP_ARRAY relationship_ctx.my_data_group_array
#define INACTIVE_SERVER_ARRAY relationship_ctx.inactive_server_array
#define IMMEDIATE_REPORT relationship_ctx.immediate_report
#define NETWORK_BUFFER relationship_ctx.buffer
#define LEADER_ELECTION relationship_ctx.leader_election
#define VOTE_CONNECTION relationship_ctx.vote_connection

static FSClusterRelationshipContext relationship_ctx = {
    {0, 0, NULL}, {NULL, 0}, {NULL, 0}, 0, 0
};

#define SET_SERVER_DETECT_ENTRY(entry, server) \
    do {  \
        entry->cs = server;    \
        entry->next_time = g_current_time + 1; \
    } while (0)

static inline void proto_unpack_server_status(
        FSProtoGetServerStatusResp *resp,
        FSClusterServerStatus *server_status)
{
    server_status->is_leader = resp->is_leader;
    server_status->leader_hint = resp->leader_hint;
    server_status->force_election = resp->force_election;
    server_status->server_id = buff2int(resp->server_id);
    server_status->up_time = buff2int(resp->up_time);
    server_status->last_heartbeat_time = buff2int(resp->last_heartbeat_time);
    server_status->last_shutdown_time = buff2int(resp->last_shutdown_time);
    server_status->version = buff2long(resp->version);
}

static int proto_get_server_status(ConnectionInfo *conn,
        const int network_timeout, FSClusterServerStatus *server_status)
{
    int result;
    FSProtoHeader *header;
    FSProtoGetServerStatusReq *req;
    SFGetServerStatusRequest status_request;
    SFResponseInfo response;
    char out_buff[sizeof(FSProtoHeader) + sizeof(FSProtoGetServerStatusReq)];
    FSProtoGetServerStatusResp resp;

    header = (FSProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, FS_CLUSTER_PROTO_GET_SERVER_STATUS_REQ,
            sizeof(out_buff) - sizeof(FSProtoHeader));

    req = (FSProtoGetServerStatusReq *)(out_buff + sizeof(FSProtoHeader));
    status_request.server_id = CLUSTER_MY_SERVER_ID;
    status_request.is_leader = (CLUSTER_MYSELF_PTR == CLUSTER_LEADER_ATOM_PTR);
    status_request.servers_sign = SERVERS_CONFIG_SIGN_BUF;
    status_request.cluster_sign = CLUSTER_CONFIG_SIGN_BUF;
    sf_proto_get_server_status_pack(&status_request, req);
    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff,
                    sizeof(out_buff), &response, network_timeout,
                    FS_CLUSTER_PROTO_GET_SERVER_STATUS_RESP, (char *)&resp,
                    sizeof(FSProtoGetServerStatusResp))) != 0)
    {
        if (result != EOPNOTSUPP) {
            fs_log_network_error(&response, conn, result);
        }
        return result;
    }

    proto_unpack_server_status(&resp, server_status);
    return 0;
}

static int proto_join_leader(FSClusterServerInfo *leader,
        ConnectionInfo *conn, const int network_timeout)
{
	int result;
	FSProtoHeader *header;
    FSProtoJoinLeaderReq *req;
    FSProtoJoinLeaderResp resp;
    SFResponseInfo response;
	char out_buff[sizeof(FSProtoHeader) + sizeof(FSProtoJoinLeaderReq)];

    header = (FSProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, FS_CLUSTER_PROTO_JOIN_LEADER_REQ,
            sizeof(out_buff) - sizeof(FSProtoHeader));

    req = (FSProtoJoinLeaderReq *)(header + 1);
    int2buff(CLUSTER_MY_SERVER_ID, req->server_id);
    long2buff(CLUSTER_MYSELF_PTR->key, req->key);
    memcpy(req->config_signs.cluster, CLUSTER_CONFIG_SIGN_BUF,
            SF_CLUSTER_CONFIG_SIGN_LEN);
    memcpy(req->config_signs.servers, SERVERS_CONFIG_SIGN_BUF,
            SF_CLUSTER_CONFIG_SIGN_LEN);
    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff,
                    sizeof(out_buff), &response, network_timeout,
                    FS_CLUSTER_PROTO_JOIN_LEADER_RESP,
                    (char *)&resp, sizeof(resp))) == 0)
    {
        leader->leader_version = buff2long(resp.leader_version);
    } else {
        if (result != EOPNOTSUPP) {
            fs_log_network_error(&response, conn, result);
        }
    }

    return result;
}

static int proto_unset_master(FSClusterDataServerInfo *master,
        ConnectionInfo *conn, const int network_timeout)
{
	int result;
	FSProtoHeader *header;
    FSProtoUnsetMasterReq *req;
    SFResponseInfo response;
	char out_buff[sizeof(FSProtoHeader) + sizeof(FSProtoUnsetMasterReq)];

    header = (FSProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, FS_CLUSTER_PROTO_UNSET_MASTER_REQ,
            sizeof(out_buff) - sizeof(FSProtoHeader));

    req = (FSProtoUnsetMasterReq *)(header + 1);
    int2buff(CLUSTER_MY_SERVER_ID, req->leader_id);
    int2buff(master->dg->id, req->data_group_id);
    long2buff(master->cs->key, req->key);
    response.error.length = 0;
    if ((result=sf_send_and_recv_none_body_response(conn, out_buff,
                    sizeof(out_buff), &response, network_timeout,
                    FS_CLUSTER_PROTO_UNSET_MASTER_RESP)) != 0)
    {
        fs_log_network_error(&response, conn, result);
    }

    return result;
}

static int proto_get_ds_status(FSClusterDataServerInfo *ds,
        ConnectionInfo *conn, const int network_timeout,
        FSDataServerStatus *ds_status)
{
    int result;
    FSProtoHeader *header;
    FSProtoGetDSStatusReq *req;
    FSProtoGetDSStatusResp resp;
    SFResponseInfo response;
    char out_buff[sizeof(FSProtoHeader) + sizeof(FSProtoGetDSStatusReq)];

    header = (FSProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, FS_CLUSTER_PROTO_GET_DS_STATUS_REQ,
            sizeof(out_buff) - sizeof(FSProtoHeader));

    req = (FSProtoGetDSStatusReq *)(header + 1);
    int2buff(ds->dg->id, req->data_group_id);
    response.error.length = 0;
    if ((result=sf_send_and_recv_response(conn, out_buff,
                    sizeof(out_buff), &response, network_timeout,
                    FS_CLUSTER_PROTO_GET_DS_STATUS_RESP,
                    (char *)&resp, sizeof(resp))) == 0)
    {
        ds_status->is_master = resp.is_master;
        ds_status->status = resp.status;
        ds_status->master_dealing_count = buff2int(
                resp.master_dealing_count);
        ds_status->data_version = buff2long(resp.data_version);
    } else {
        fs_log_network_error(&response, conn, result);
    }

    return result;
}

static int cluster_get_ds_status(FSClusterDataServerInfo *ds,
        FSDataServerStatus *ds_status)
{
    const int connect_timeout = 2;
    const int network_timeout = 3;
    ConnectionInfo conn;
    int result;

    if (ds->cs == CLUSTER_MYSELF_PTR) {
        ds_status->is_master = FC_ATOMIC_GET(ds->is_master);
        ds_status->status = FC_ATOMIC_GET(ds->status);
        ds_status->master_dealing_count = FC_ATOMIC_GET(
                ds->master_dealing_count);
        ds_status->data_version = FC_ATOMIC_GET(ds->data.current_version);
        return 0;
    }

    if ((result=fc_server_make_connection(&CLUSTER_GROUP_ADDRESS_ARRAY(
                        ds->cs->server), &conn, "fstore",
                    connect_timeout)) != 0)
    {
        return result;
    }

    result = proto_get_ds_status(ds, &conn, network_timeout, ds_status);
    conn_pool_disconnect_server(&conn);
    return result;
}

static int cluster_unset_master(FSClusterDataServerInfo *master)
{
    const int connect_timeout = 2;
    const int network_timeout = 3;
    ConnectionInfo conn;
    int result;

    if (master->cs == CLUSTER_MYSELF_PTR) {
        __sync_bool_compare_and_swap(&master->dg->master_swapping, 0, 1);
        return 0;
    }

    if ((result=fc_server_make_connection(&CLUSTER_GROUP_ADDRESS_ARRAY(
                        master->cs->server), &conn, "fstore",
                    connect_timeout)) != 0)
    {
        return result;
    }

    result = proto_unset_master(master, &conn, network_timeout);
    conn_pool_disconnect_server(&conn);
    return result;
}

static void pack_changed_data_versions(int *count, const bool report_all)
{
    FSMyDataGroupInfo *group;
    FSMyDataGroupInfo *end;
    FSProtoPingLeaderReqBodyPart *body_part;
    struct {
        int64_t current;
        int64_t confirmed;
    } data_versions;

    *count = 0;
    body_part = (FSProtoPingLeaderReqBodyPart *)(NETWORK_BUFFER.data +
            NETWORK_BUFFER.length);
    end = MY_DATA_GROUP_ARRAY.groups + MY_DATA_GROUP_ARRAY.count;
    for (group=MY_DATA_GROUP_ARRAY.groups; group<end; group++) {
        data_versions.confirmed = FC_ATOMIC_GET(
                group->ds->data.confirmed_version);
        if (REPLICA_QUORUM_ROLLBACK_DONE) {
            data_versions.current = FC_ATOMIC_GET(
                    group->ds->data.current_version);
        } else {
            data_versions.current = data_versions.confirmed;
        }
        if (report_all || group->ds->last_report_versions.current !=
                data_versions.current || group->ds->last_report_versions.
                confirmed != data_versions.confirmed)
        {
            group->ds->last_report_versions.current = data_versions.current;
            group->ds->last_report_versions.confirmed = data_versions.confirmed;
            int2buff(group->data_group_id, body_part->data_group_id);
            long2buff(data_versions.current, body_part->data_versions.current);
            long2buff(data_versions.confirmed, body_part->
                    data_versions.confirmed);
            body_part->status = FC_ATOMIC_GET(group->ds->status);
            body_part++;
            ++(*count);
        }
    }

    NETWORK_BUFFER.length = (char *)body_part - NETWORK_BUFFER.data;
}

static void leader_deal_data_version_changes()
{
    FSMyDataGroupInfo *group;
    FSMyDataGroupInfo *end;
    struct {
        int64_t current;
        int64_t confirmed;
    } data_versions;
    int count;

    count = 0;
    end = MY_DATA_GROUP_ARRAY.groups + MY_DATA_GROUP_ARRAY.count;
    for (group=MY_DATA_GROUP_ARRAY.groups; group<end; group++) {
        data_versions.current = FC_ATOMIC_GET(
                group->ds->data.current_version);
        data_versions.confirmed = FC_ATOMIC_GET(
                group->ds->data.confirmed_version);
        if (group->ds->last_report_versions.current != data_versions.
                current || group->ds->last_report_versions.
                confirmed != data_versions.confirmed)
        {
            group->ds->last_report_versions.current = data_versions.current;
            group->ds->last_report_versions.confirmed = data_versions.confirmed;
            cluster_topology_data_server_chg_notify(group->ds,
                    FS_EVENT_SOURCE_SELF_REPORT,
                    FS_EVENT_TYPE_DV_CHANGE, false);
            ++count;
        }
    }

    if (count > 0) {
        logDebug("file: "__FILE__", line: %d, "
                "leader data versions change count: %d",
                __LINE__, count);
    }
}

static int cluster_cmp_server_status(const void *p1, const void *p2)
{
    FSClusterServerStatus *status1;
    FSClusterServerStatus *status2;
    int restart_interval1;
    int restart_interval2;
    int64_t sub;

    status1 = (FSClusterServerStatus *)p1;
    status2 = (FSClusterServerStatus *)p2;
    sub = (int)status1->is_leader - (int)status2->is_leader;
    if (sub != 0) {
        return sub;
    }

    if ((sub=fc_compare_int64(status1->version, status2->version)) != 0) {
        return sub;
    }

    sub = status1->last_heartbeat_time - status2->last_heartbeat_time;
    if (!(sub >= -3 && sub <= 3)) {
        return sub;
    }

    sub = (int)status1->leader_hint - (int)status2->leader_hint;
    if (sub != 0) {
        return sub;
    }

    sub = (int)status1->force_election - (int)status2->force_election;
    if (sub != 0) {
        return sub;
    }

    sub = ALIGN_TIME(status2->up_time) - ALIGN_TIME(status1->up_time);
    if (sub != 0) {
        return sub;
    }

    restart_interval1 = status1->up_time - status1->last_shutdown_time;
    restart_interval2 = status2->up_time - status2->last_shutdown_time;
    sub = ALIGN_TIME(restart_interval2) - ALIGN_TIME(restart_interval1);
    if (sub != 0) {
        return sub;
    }

    return (int)status1->server_id - (int)status2->server_id;
}

#define cluster_get_server_status(server_status) \
    cluster_get_server_status_ex(server_status, true)

static int cluster_get_server_status_ex(FSClusterServerStatus *server_status,
        const bool log_connect_error)
{
    const int connect_timeout = 2;
    const int network_timeout = 2;
    ConnectionInfo conn;
    int result;

    if (server_status->cs == CLUSTER_MYSELF_PTR) {
        server_status->is_leader = (CLUSTER_MYSELF_PTR ==
                CLUSTER_LEADER_ATOM_PTR ? 1 : 0);
        server_status->leader_hint = (MYSELF_IS_LEADER ? 1 : 0);
        server_status->force_election = (FORCE_LEADER_ELECTION ? 1 : 0);
        server_status->up_time = g_sf_global_vars.up_time;
        server_status->last_heartbeat_time = CLUSTER_LAST_HEARTBEAT_TIME;
        server_status->last_shutdown_time = CLUSTER_LAST_SHUTDOWN_TIME;
        server_status->server_id = CLUSTER_MY_SERVER_ID;
        server_status->version = __sync_add_and_fetch(
                &CLUSTER_CURRENT_VERSION, 0);
        return 0;
    } else {
        if ((result=fc_server_make_connection_ex(&CLUSTER_GROUP_ADDRESS_ARRAY(
                            server_status->cs->server), &conn, "fstore",
                        connect_timeout, NULL, log_connect_error)) != 0)
        {
            return result;
        }

        result = proto_get_server_status(&conn,
                network_timeout, server_status);
        conn_pool_disconnect_server(&conn);
        return result;
    }
}

static inline void fill_join_request(FCFSVoteClientJoinRequest
        *join_request, const bool persistent)
{
    join_request->server_id = CLUSTER_MY_SERVER_ID;
    join_request->is_leader = (CLUSTER_MYSELF_PTR ==
            CLUSTER_LEADER_ATOM_PTR ? 1 : 0);
    join_request->group_id = CLUSTER_SERVER_GROUP_ID;
    join_request->response_size = sizeof(FSProtoGetServerStatusResp);
    join_request->service_id = FCFS_VOTE_SERVICE_ID_FSTORE;
    join_request->persistent = persistent;
}

static int get_vote_server_status(FSClusterServerStatus *server_status)
{
    FCFSVoteClientJoinRequest join_request;
    SFGetServerStatusRequest status_request;
    FSProtoGetServerStatusResp resp;
    int result;

    if (VOTE_CONNECTION.sock >= 0) {
        status_request.servers_sign = SERVERS_CONFIG_SIGN_BUF;
        status_request.cluster_sign = CLUSTER_CONFIG_SIGN_BUF;
        status_request.server_id = CLUSTER_MY_SERVER_ID;
        status_request.is_leader = (CLUSTER_MYSELF_PTR ==
                CLUSTER_LEADER_ATOM_PTR ? 1 : 0);
        result = vote_client_proto_get_vote(&VOTE_CONNECTION,
                &status_request, (char *)&resp, sizeof(resp));
        if (result != 0) {
            vote_client_proto_close_connection(&VOTE_CONNECTION);
        }
    } else {
        fill_join_request(&join_request, false);
        result = fcfs_vote_client_get_vote(&join_request,
                SERVERS_CONFIG_SIGN_BUF, CLUSTER_CONFIG_SIGN_BUF,
                (char *)&resp, sizeof(resp));
    }

    if (result == 0) {
        proto_unpack_server_status(&resp, server_status);
    }
    return result;
}

static int notify_vote_next_leader(FSClusterServerStatus *server_status,
        const unsigned char vote_req_cmd)
{
    FCFSVoteClientJoinRequest join_request;

    fill_join_request(&join_request, false);
    return fcfs_vote_client_notify_next_leader(&join_request, vote_req_cmd);
}

static int cluster_get_leader(FSClusterServerStatus *server_status,
        const bool log_connect_error, int *success_count, int *active_count)
{
#define STATUS_ARRAY_FIXED_COUNT  8
	FSClusterServerInfo *server;
	FSClusterServerInfo *end;
	FSClusterServerStatus *current_status;
	FSClusterServerStatus *cs_status;
	FSClusterServerStatus status_array[STATUS_ARRAY_FIXED_COUNT];
	int result;
	int r;
	int i;

	memset(server_status, 0, sizeof(FSClusterServerStatus));
    if (CLUSTER_SERVER_ARRAY.count <= STATUS_ARRAY_FIXED_COUNT) {
        cs_status = status_array;
    } else {
        int bytes;
        bytes = sizeof(FSClusterServerStatus) * CLUSTER_SERVER_ARRAY.count;
        cs_status = (FSClusterServerStatus *)fc_malloc(bytes);
        if (cs_status == NULL) {
            *success_count = *active_count = 0;
            return ENOMEM;
        }
    }

	current_status = cs_status;
	result = 0;
	end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
	for (server=CLUSTER_SERVER_ARRAY.servers; server<end; server++) {
		current_status->cs = server;
        r = cluster_get_server_status_ex(current_status, log_connect_error);
		if (r == 0) {
			current_status++;
		} else if (r != ENOENT) {
			result = r;
		}
	}

    *success_count = *active_count = current_status - cs_status;
    if (*active_count == 0) {
        logError("file: "__FILE__", line: %d, "
                "get server status fail, server count: %d",
                __LINE__, CLUSTER_SERVER_ARRAY.count);
        return result == 0 ? ENOENT : result;
    }

    if (NEED_REQUEST_VOTE_NODE(*success_count)) {
        current_status->cs = NULL;
        if (get_vote_server_status(current_status) == 0) {
            ++(*success_count);
        }
    }

    qsort(cs_status, *success_count,
            sizeof(FSClusterServerStatus),
            cluster_cmp_server_status);

    for (i=0; i<*success_count; i++) {
        int restart_interval;

        if (cs_status[i].cs == NULL) {
            logDebug("file: "__FILE__", line: %d, "
                    "%d. status from vote server", __LINE__, i + 1);
        } else {
            restart_interval = cs_status[i].up_time -
                cs_status[i].last_shutdown_time;
            logDebug("file: "__FILE__", line: %d, "
                    "%d. server_id: %d, ip addr %s:%u, version: %"PRId64", "
                    "is_leader: %d, leader_hint: %d, last_heartbeat_time: %d, "
                    "up_time: %d, restart interval: %d", __LINE__,
                    i + 1, cs_status[i].server_id,
                    CLUSTER_GROUP_ADDRESS_FIRST_IP(cs_status[i].cs->server),
                    CLUSTER_GROUP_ADDRESS_FIRST_PORT(cs_status[i].cs->server),
                    cs_status[i].version, cs_status[i].is_leader,
                    cs_status[i].leader_hint, cs_status[i].last_heartbeat_time,
                    ALIGN_TIME(cs_status[i].up_time),
                    ALIGN_TIME(restart_interval));
        }
    }

	memcpy(server_status, cs_status + (*success_count - 1),
			sizeof(FSClusterServerStatus));
    if (cs_status != status_array) {
        free(cs_status);
    }
	return 0;
}

static int do_notify_leader_changed(FSClusterServerInfo *cs,
		FSClusterServerInfo *leader, const char cmd, bool *bConnectFail)
{
    char out_buff[sizeof(FSProtoHeader) + 4];
    ConnectionInfo conn;
    FSProtoHeader *header;
    SFResponseInfo response;
    int result;

    if ((result=fc_server_make_connection(&CLUSTER_GROUP_ADDRESS_ARRAY(
                        cs->server), &conn, "fstore",
                    SF_G_CONNECT_TIMEOUT)) != 0)
    {
        *bConnectFail = true;
        return result;
    }
    *bConnectFail = false;

    header = (FSProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, cmd, sizeof(out_buff) -
            sizeof(FSProtoHeader));
    int2buff(leader->server->id, out_buff + sizeof(FSProtoHeader));
    response.error.length = 0;
    if ((result=sf_send_and_recv_none_body_response(&conn, out_buff,
                    sizeof(out_buff), &response, SF_G_NETWORK_TIMEOUT,
                    SF_PROTO_ACK)) != 0)
    {
        if (result != EOPNOTSUPP) {
            fs_log_network_error(&response, &conn, result);
        }
    }

    conn_pool_disconnect_server(&conn);
    return result;
}

static int report_ds_status_to_leader(FSClusterDataServerInfo *ds,
        const int status)
{
    FSClusterServerInfo *leader;
    FSProtoHeader *header;
    FSProtoReportDSStatusReq *req;
    char out_buff[sizeof(FSProtoHeader) + sizeof(FSProtoReportDSStatusReq)];
    ConnectionInfo conn;
    SFResponseInfo response;
    int result;

    leader = CLUSTER_LEADER_ATOM_PTR;
    if (leader == NULL) {
        return ENOENT;
    }

    if ((result=fc_server_make_connection(&CLUSTER_GROUP_ADDRESS_ARRAY(
                        leader->server), &conn, "fstore",
                    SF_G_CONNECT_TIMEOUT)) != 0)
    {
        return result;
    }

    header = (FSProtoHeader *)out_buff;
    req = (FSProtoReportDSStatusReq *)(header + 1);
    SF_PROTO_SET_HEADER(header, FS_CLUSTER_PROTO_REPORT_DS_STATUS_REQ,
            sizeof(out_buff) - sizeof(FSProtoHeader));
    int2buff(CLUSTER_MY_SERVER_ID, req->my_server_id);
    int2buff(ds->cs->server->id, req->ds_server_id);
    int2buff(ds->dg->id, req->data_group_id);
    req->status = status;
    response.error.length = 0;
    if ((result=sf_send_and_recv_none_body_response(&conn, out_buff,
                    sizeof(out_buff), &response, SF_G_NETWORK_TIMEOUT,
                    FS_CLUSTER_PROTO_REPORT_DS_STATUS_RESP)) != 0)
    {
        if (result != EOPNOTSUPP) {
            fs_log_network_error(&response, &conn, result);
        }
    }

    conn_pool_disconnect_server(&conn);
    return result;
}

int cluster_relationship_pre_set_leader(FSClusterServerInfo *leader)
{
    FSClusterServerInfo *next_leader;

    next_leader = LEADER_ELECTION.next_leader;
    if (next_leader == NULL) {
        LEADER_ELECTION.next_leader = leader;
    } else if (next_leader != leader) {
        logError("file: "__FILE__", line: %d, "
                "try to set next leader id: %d, "
                "but next leader: %d already exist",
                __LINE__, leader->server->id, next_leader->server->id);
        LEADER_ELECTION.next_leader = NULL;
        return EEXIST;
    }

    return 0;
}

static void unset_all_data_groups()
{
    FSClusterDataGroupInfo *group;
    FSClusterDataGroupInfo *gend;
    FSClusterDataServerInfo *ds;
    FSClusterDataServerInfo *send;
    FSClusterDataServerInfo *master;

    gend = CLUSTER_DATA_RGOUP_ARRAY.groups + CLUSTER_DATA_RGOUP_ARRAY.count;
    for (group=CLUSTER_DATA_RGOUP_ARRAY.groups; group<gend; group++) {
        master = (FSClusterDataServerInfo *)FC_ATOMIC_GET(group->master);
        if (master != NULL) {
            __sync_bool_compare_and_swap(&group->master, master, NULL);
        }

        send = group->data_server_array.servers +
            group->data_server_array.count;
        for (ds=group->data_server_array.servers; ds<send; ds++) {
            if (FC_ATOMIC_GET(ds->is_master)) {
                __sync_bool_compare_and_swap(&ds->is_master, 1, 0);
            }

            if (FC_ATOMIC_GET(ds->status) == FS_DS_STATUS_ACTIVE) {
                __sync_bool_compare_and_swap(&ds->status,
                        FS_DS_STATUS_ACTIVE,
                        FS_DS_STATUS_OFFLINE);
            }
        }
    }
}

static inline bool cluster_unset_leader(FSClusterServerInfo *leader)
{
    if (__sync_bool_compare_and_swap(&CLUSTER_LEADER_PTR,
                leader, NULL))
    {
        leader->is_leader = false;
        unset_all_data_groups();
        return true;
    } else {
        return false;
    }
}

static int do_check_brainsplit(FSClusterServerInfo *cs)
{
    int result;
    const bool log_connect_error = false;
    FSClusterServerStatus server_status;

    server_status.cs = cs;
    if ((result=cluster_get_server_status_ex(&server_status,
                    log_connect_error)) != 0)
    {
        return result;
    }

    if (server_status.is_leader) {
        logWarning("file: "__FILE__", line: %d, "
                "two leaders occurs, anonther leader id: %d, ip %s:%u, "
                "trigger re-select leader ...", __LINE__, cs->server->id,
                CLUSTER_GROUP_ADDRESS_FIRST_IP(cs->server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(cs->server));
        cluster_relationship_trigger_reselect_leader();
        return EEXIST;
    }

    return 0;
}

static int cluster_check_brainsplit(int *inactive_count)
{
    FSClusterServerDetectEntry *entry;
    FSClusterServerDetectEntry *end;
    int result;

    end = INACTIVE_SERVER_ARRAY.entries + *inactive_count;
    for (entry=INACTIVE_SERVER_ARRAY.entries; entry<end; entry++) {
        if (entry >= INACTIVE_SERVER_ARRAY.entries +
                INACTIVE_SERVER_ARRAY.count)
        {
            break;
        }
        if (entry->next_time > g_current_time) {
            continue;
        }

        result = do_check_brainsplit(entry->cs);
        if (result == EEXIST) {  //brain-split occurs
            return result;
        }

        if (result == 0 || result == EOPNOTSUPP) {
            --(*inactive_count);
            cluster_relationship_swap_server_status(entry->cs,
                    FS_SERVER_STATUS_OFFLINE, FS_SERVER_STATUS_ONLINE);
        }
        entry->next_time = g_current_time + 1;
    }

    return 0;
}

static void init_inactive_server_array()
{
    FSClusterServerInfo *cs;
    FSClusterServerInfo *end;
    FSClusterServerDetectEntry *entry;

    PTHREAD_MUTEX_LOCK(&INACTIVE_SERVER_ARRAY.lock);
    entry = INACTIVE_SERVER_ARRAY.entries;
    end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (cs=CLUSTER_SERVER_ARRAY.servers; cs<end; cs++) {
        if (cs != CLUSTER_MYSELF_PTR) {
            SET_SERVER_DETECT_ENTRY(entry, cs);
            entry++;
        }
    }

    INACTIVE_SERVER_ARRAY.count = entry - INACTIVE_SERVER_ARRAY.entries;
    PTHREAD_MUTEX_UNLOCK(&INACTIVE_SERVER_ARRAY.lock);
}

static void cluster_relationship_deactivate_all_servers()
{
    FSClusterServerInfo *cs;
    FSClusterServerInfo *end;

    end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (cs=CLUSTER_SERVER_ARRAY.servers; cs<end; cs++) {
        if (cs != CLUSTER_MYSELF_PTR) {
            cluster_relationship_set_server_status(cs,
                    FS_SERVER_STATUS_OFFLINE);
        }
    }
}

static void cluster_relationship_clear_old_leader(
        FSClusterServerInfo *new_leader)
{
    FSClusterServerInfo *cs;
    FSClusterServerInfo *end;

    end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (cs=CLUSTER_SERVER_ARRAY.servers; cs<end; cs++) {
        if (cs->is_leader && (cs != new_leader)) {
            cs->is_leader = false;
        }
    }
}

static int cluster_relationship_set_leader(FSClusterServerInfo *new_leader)
{
    FSClusterServerInfo *old_leader;
    int64_t time_used;
    char prefix_prompt[64];
    char time_buff[32];
    char affix_prompt[128];

    old_leader = CLUSTER_LEADER_ATOM_PTR;
    new_leader->is_leader = true;
    if (CLUSTER_MYSELF_PTR == new_leader) {
        if (new_leader != old_leader) {
            cluster_relationship_deactivate_all_servers();
            cluster_relationship_set_server_status(CLUSTER_MYSELF_PTR,
                    FS_SERVER_STATUS_ACTIVE);
            CLUSTER_MYSELF_PTR->last_ping_time = g_current_time + 1;
            CLUSTER_MYSELF_PTR->leader_version = __sync_add_and_fetch(
                    &CLUSTER_CURRENT_VERSION, 1);

            init_inactive_server_array();
            cluster_topology_offline_all_data_servers(new_leader);
        }
        strcpy(prefix_prompt, "i am the new leader,");
    } else {
        if (MYSELF_IS_LEADER) {
            MYSELF_IS_LEADER = false;
        }
        strcpy(prefix_prompt, "the leader server");
    }

    if (new_leader != old_leader) {
        __sync_bool_compare_and_swap(&CLUSTER_LEADER_PTR,
                old_leader, new_leader);

        //clear other server's is_leader field
        cluster_relationship_clear_old_leader(new_leader);

        if (LEADER_ELECTION.retry_count > 0) {
            time_used = get_current_time_ms() - LEADER_ELECTION.start_time_ms;
            long_to_comma_str(time_used, time_buff);
            sprintf(affix_prompt, ", retry count: %d, time used: %s ms",
                    LEADER_ELECTION.retry_count, time_buff);
        } else {
            *affix_prompt = '\0';
        }
        logInfo("file: "__FILE__", line: %d, "
                "%s id: %d, ip %s:%u%s", __LINE__,
                prefix_prompt, new_leader->server->id,
                CLUSTER_GROUP_ADDRESS_FIRST_IP(new_leader->server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(new_leader->server),
                affix_prompt);

        if (CLUSTER_MYSELF_PTR == CLUSTER_LEADER_ATOM_PTR) {  //i am leader
            master_election_unset_all_masters();
        }
    }

    LEADER_ELECTION.start_time_ms = 0;
    LEADER_ELECTION.retry_count = 0;
    return 0;
}

int cluster_relationship_commit_leader(FSClusterServerInfo *leader)
{
    FSClusterServerInfo *next_leader;
    int result;

    next_leader = LEADER_ELECTION.next_leader;
    if (next_leader == NULL) {
        logError("file: "__FILE__", line: %d, "
                "next leader is NULL", __LINE__);
        return EBUSY;
    }
    if (next_leader != leader) {
        logError("file: "__FILE__", line: %d, "
                "next leader server id: %d != expected server id: %d",
                __LINE__, next_leader->server->id, leader->server->id);
        LEADER_ELECTION.next_leader = NULL;
        return EBUSY;
    }

    result = cluster_relationship_set_leader(leader);
    LEADER_ELECTION.next_leader = NULL;
    return result;
}

void cluster_relationship_trigger_reselect_leader()
{
    FSClusterServerInfo *leader;

    leader = CLUSTER_LEADER_ATOM_PTR;
    if (CLUSTER_MYSELF_PTR != leader) {
        return;
    }

    if (cluster_unset_leader(leader)) {
        if (NEED_CHECK_VOTE_NODE()) {
            vote_client_proto_close_connection(&VOTE_CONNECTION);
        }
    }
}

static int cluster_pre_set_next_leader(FSClusterServerInfo *cs,
        FSClusterServerStatus *server_status, bool *bConnectFail)
{
    FSClusterServerInfo *leader;
    leader = server_status->cs;
    if (cs == CLUSTER_MYSELF_PTR) {
        return cluster_relationship_pre_set_leader(leader);
    } else {
        return do_notify_leader_changed(cs, leader,
                FS_CLUSTER_PROTO_PRE_SET_NEXT_LEADER, bConnectFail);
    }
}

static int cluster_commit_next_leader(FSClusterServerInfo *cs,
        FSClusterServerStatus *server_status, bool *bConnectFail)
{
    FSClusterServerInfo *leader;

    leader = server_status->cs;
    if (cs == CLUSTER_MYSELF_PTR) {
        return cluster_relationship_commit_leader(leader);
    } else {
        return do_notify_leader_changed(cs, leader,
                FS_CLUSTER_PROTO_COMMIT_NEXT_LEADER, bConnectFail);
    }
}

typedef int (*cluster_notify_next_leader_func)(FSClusterServerInfo *cs,
        FSClusterServerStatus *server_status, bool *bConnectFail);

static int notify_next_leader(cluster_notify_next_leader_func notify_func,
        FSClusterServerStatus *server_status, const unsigned
        char vote_req_cmd, int *success_count)
{
	FSClusterServerInfo *server;
	FSClusterServerInfo *send;
	int result;
	bool bConnectFail;

	result = ENOENT;
	*success_count = 0;
	send = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
	for (server=CLUSTER_SERVER_ARRAY.servers; server<send; server++) {
		if ((result=notify_func(server, server_status, &bConnectFail)) != 0) {
            if (!bConnectFail && result != EOPNOTSUPP) {
                return result;
            }
		} else {
            ++(*success_count);
		}
	}

    if (NEED_CHECK_VOTE_NODE()) {
        result = notify_vote_next_leader(server_status, vote_req_cmd);
        if (result == 0) {
            if (*success_count < CLUSTER_SERVER_ARRAY.count) {
                ++(*success_count);
            }
        } else if (result == SF_CLUSTER_ERROR_LEADER_INCONSISTENT) {
            return -1 * result;
        }
    }

    if (!sf_election_quorum_check(LEADER_ELECTION_QUORUM,
                    VOTE_NODE_ENABLED, CLUSTER_SERVER_ARRAY.count,
                    *success_count))
    {
        return EAGAIN;
    }

	return 0;
}

static inline int cluster_notify_leader_changed(
        FSClusterServerStatus *server_status)
{
    int result;
    int success_count;
    const char *caption;

    if ((result=notify_next_leader(cluster_pre_set_next_leader, server_status,
                    FCFS_VOTE_SERVICE_PROTO_PRE_SET_NEXT_LEADER,
                    &success_count)) != 0)
    {
        return result;
    }

    if ((result=notify_next_leader(cluster_commit_next_leader, server_status,
                    FCFS_VOTE_SERVICE_PROTO_COMMIT_NEXT_LEADER,
                    &success_count)) != 0)
    {
        if (result == SF_CLUSTER_ERROR_LEADER_INCONSISTENT ||
                result == -SF_CLUSTER_ERROR_LEADER_INCONSISTENT)
        {
            if (result == SF_CLUSTER_ERROR_LEADER_INCONSISTENT) {
                caption = "other server";
            } else {
                caption = "the vote node";
                result = SF_CLUSTER_ERROR_LEADER_INCONSISTENT;
            }
            logWarning("file: "__FILE__", line: %d, "
                    "trigger re-select leader because leader "
                    "inconsistent with %s", __LINE__, caption);
        } else {
            logWarning("file: "__FILE__", line: %d, "
                    "trigger re-select leader because alive server "
                    "count: %d < half of total server count: %d ...",
                    __LINE__, success_count, CLUSTER_SERVER_ARRAY.count);
        }
        cluster_relationship_trigger_reselect_leader();
    }

    return result;
}

static int cluster_select_leader()
{
	int result;
    int success_count;
    int active_count;
    int i;
    int max_sleep_secs;
    int sleep_secs;
    int remain_time;
    bool need_log;
    bool force_sleep;
    time_t start_time;
    time_t last_log_time;
    char prompt[512];
	FSClusterServerStatus server_status;
    FSClusterServerInfo *next_leader;

	logInfo("file: "__FILE__", line: %d, "
		"selecting leader...", __LINE__);

    if (LEADER_ELECTION.start_time_ms == 0) {
        LEADER_ELECTION.start_time_ms = get_current_time_ms();
        LEADER_ELECTION.retry_count = 1;
    } else {
        LEADER_ELECTION.retry_count++;
    }

    start_time = g_current_time;
    last_log_time = 0;
    sleep_secs = 10;
    max_sleep_secs = 1;
    i = 0;
    while (CLUSTER_LEADER_ATOM_PTR == NULL) {
        if (sleep_secs > 1) {
            need_log = true;
            last_log_time = g_current_time;
        } else if (g_current_time - last_log_time > 8) {
            need_log = ((i + 1) % 10 == 0);
            if (need_log) {
                last_log_time = g_current_time;
            }
        } else {
            need_log = false;
        }

        if ((result=cluster_get_leader(&server_status, need_log,
                        &success_count, &active_count)) != 0)
        {
            return result;
        }

        ++i;
        if (!sf_election_quorum_check(LEADER_ELECTION_QUORUM,
                    VOTE_NODE_ENABLED, CLUSTER_SERVER_ARRAY.count,
                    success_count) && !FORCE_LEADER_ELECTION)
        {
            sleep_secs = 1;
            if (need_log) {
                logWarning("file: "__FILE__", line: %d, "
                        "round %dth select leader fail because alive server "
                        "count: %d < half of total server count: %d, "
                        "try again after %d seconds.", __LINE__, i,
                        success_count, CLUSTER_SERVER_ARRAY.count,
                        sleep_secs);
            }
            sleep(sleep_secs);
            continue;
        }

        if ((active_count == CLUSTER_SERVER_ARRAY.count) ||
                (active_count >= 2 && server_status.is_leader) ||
                (start_time - server_status.last_heartbeat_time <=
                 LEADER_ELECTION_LOST_TIMEOUT + 1))
        {
            break;
        }

        if ((server_status.up_time - server_status.last_shutdown_time >
                    3600) && (server_status.last_heartbeat_time == 0) &&
                !FORCE_LEADER_ELECTION)
        {
            sprintf(prompt, "the candidate leader server id: %d, "
                    "does not match the selection rule because it's "
                    "restart interval: %d exceeds 3600, "
                    "you must start ALL servers in the first time, "
                    "or remove the deprecated server(s) from the "
                    "config file, or execute fs_serverd with option --%s",
                    server_status.cs->server->id, (int)(server_status.
                        up_time - server_status.last_shutdown_time),
                    FS_FORCE_ELECTION_LONG_OPTION_STR);
            force_sleep = true;
        } else {
            if (g_current_time - start_time > LEADER_ELECTION_MAX_WAIT_TIME) {
                break;
            }

            if (FORCE_LEADER_ELECTION) {
                sprintf(prompt, "force_leader_election: %d",
                        FORCE_LEADER_ELECTION);
            } else {
                *prompt = '\0';
            }

            force_sleep = false;
        }

        remain_time = LEADER_ELECTION_MAX_WAIT_TIME -
            (g_current_time - start_time);
        if (remain_time > 0) {
            sleep_secs = FC_MIN(remain_time, max_sleep_secs);
        } else {
            if (force_sleep) {
                sleep_secs = max_sleep_secs;
            } else {
                sleep_secs = 1;
            }
        }

        if (need_log) {
            logWarning("file: "__FILE__", line: %d, "
                    "round %dth select leader, alive server count: %d "
                    "< server count: %d, %s. try again after %d seconds.",
                    __LINE__, i, active_count, CLUSTER_SERVER_ARRAY.count,
                    prompt, sleep_secs);
        }
        sleep(sleep_secs);
        if (max_sleep_secs < 32) {
            max_sleep_secs *= 2;
        }
    }

    next_leader = CLUSTER_LEADER_ATOM_PTR;
    if (next_leader != NULL) {
        logInfo("file: "__FILE__", line: %d, "
                "abort election because the leader exists, "
                "leader id: %d, ip %s:%u, election time used: %ds",
                __LINE__, next_leader->server->id,
                CLUSTER_GROUP_ADDRESS_FIRST_IP(next_leader->server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(next_leader->server),
                (int)(g_current_time - start_time));
        return 0;
    }

    next_leader = server_status.cs;
    if (CLUSTER_MYSELF_PTR == next_leader) {
        if ((result=cluster_notify_leader_changed(
                        &server_status)) != 0)
        {
            return result;
        }
    } else {
        if (server_status.is_leader) {
            cluster_relationship_set_leader(next_leader);
        } else if (CLUSTER_LEADER_ATOM_PTR == NULL) {
            logInfo("file: "__FILE__", line: %d, "
                    "election time used: %ds, waiting for the candidate "
                    "leader server id: %d, ip %s:%u notify ...", __LINE__,
                    (int)(g_current_time - start_time), next_leader->server->id,
                    CLUSTER_GROUP_ADDRESS_FIRST_IP(next_leader->server),
                    CLUSTER_GROUP_ADDRESS_FIRST_PORT(next_leader->server));
            return ENOENT;
        }
    }

    CLUSTER_MYSELF_PTR->key = g_current_time | ((int64_t)(rand() +
                CLUSTER_MY_SERVER_ID) << 32);
	return 0;
}

static void unset_master_rollback(FSClusterDataGroupInfo *group,
        FSClusterDataServerInfo *old_master,
        FSClusterDataServerInfo *new_master)
{
    bool need_rollback;

    master_election_lock();
    need_rollback = (FC_ATOMIC_GET(group->master) == old_master);
    if (need_rollback) {
        /* rollback */
        __sync_bool_compare_and_swap(&old_master->is_master, 0, 1);
    }
    master_election_unlock();

    if (need_rollback) {
        cluster_topology_data_server_chg_notify(old_master,
                FS_EVENT_SOURCE_CS_LEADER,
                FS_EVENT_TYPE_MASTER_CHANGE, true);

        /* downgrade status for try again */
        if (FC_ATOMIC_GET(new_master->status) == FS_DS_STATUS_ACTIVE) {
            cluster_relationship_set_ds_status_ex(new_master,
                    FS_DS_STATUS_ACTIVE, FS_DS_STATUS_OFFLINE);
            cluster_topology_data_server_chg_notify(new_master,
                    FS_EVENT_SOURCE_CS_LEADER,
                    FS_EVENT_TYPE_STATUS_CHANGE, true);
        }
    }
}

static int check_swap_master(FSClusterDataServerInfo *old_master,
        FSClusterDataServerInfo *new_master)
{
    int result;
    int i;
    int sleep_ms;
    FSDataServerStatus old_status;
    FSDataServerStatus new_status;

    if ((result=cluster_unset_master(old_master)) != 0) {
        return result;
    }

    sleep_ms = 10;
    for (i=0; i<5; i++) {
        if (i > 0) {
            sleep_ms *= 2;
        }
        fc_sleep_ms(sleep_ms);
        if ((result=cluster_get_ds_status(old_master, &old_status)) != 0) {
            return result;
        }

        if (old_status.master_dealing_count == 0) {
            break;
        }
    }

    /* try again to avoid race case */
    if (old_status.master_dealing_count == 0) {
        fc_sleep_ms(10);
        if ((result=cluster_get_ds_status(old_master, &old_status)) != 0) {
            return result;
        }
    }

    if (old_status.master_dealing_count != 0) {
        logWarning("file: "__FILE__", line: %d, "
                "data group id: %d, old master server id: %d 's "
                "dealing count: %d != 0!", __LINE__,
                old_master->dg->id, old_master->cs->server->id,
                old_status.master_dealing_count);
        return EBUSY;
    }

    if ((result=cluster_get_ds_status(new_master, &new_status)) != 0) {
        return result;
    }

    if (new_status.data_version != old_status.data_version) {
        logWarning("file: "__FILE__", line: %d, "
                "data group id: %d, new master server id: %d 's "
                "data_version: %"PRId64" != old master server id: %d 's "
                "data_version: %"PRId64"!", __LINE__, new_master->dg->id,
                new_master->cs->server->id, new_status.data_version,
                old_master->cs->server->id, old_status.data_version);
        return EBUSY;
    }

    return 0;
}

static bool cluster_resume_master(FSClusterDataGroupInfo *group,
        FSClusterDataServerInfo *old_master,
        FSClusterDataServerInfo *new_master)
{
    int64_t start_time_us;
    int64_t time_used;
    char time_buff[32];

    if (FC_ATOMIC_GET(group->master) != old_master) {
        return false;
    }
    if (FC_ATOMIC_GET(new_master->cs->status) != FS_SERVER_STATUS_ACTIVE) {
        return false;
    }

    start_time_us = get_current_time_us();
    logDebug("file: "__FILE__", line: %d, "
            "data_group_id: %d, try to resume master from server id "
            "%d to %d ...", __LINE__, group->id, old_master->
            cs->server->id, new_master->cs->server->id);

    __sync_bool_compare_and_swap(&old_master->is_master, 1, 0);
    if (check_swap_master(old_master, new_master) != 0) {
        unset_master_rollback(group, old_master, new_master);
        return false;
    }

    if (!master_election_set_master(group, old_master, new_master)) {
        if (FC_ATOMIC_GET(group->master) == NULL) {
            if (!master_election_set_master(group, NULL, new_master)) {
                return false;
            }
        } else {
            unset_master_rollback(group, old_master, new_master);
            return false;
        }
    }

    time_used = get_current_time_us() - start_time_us;
    logInfo("file: "__FILE__", line: %d, "
            "data_group_id: %d, resume master from server id %d to %d "
            "successfully, time used: %s us", __LINE__, group->id,
            old_master->cs->server->id, new_master->cs->server->id,
            long_to_comma_str(time_used, time_buff));

    cluster_topology_data_server_chg_notify(new_master,
            FS_EVENT_SOURCE_CS_LEADER,
            FS_EVENT_TYPE_MASTER_CHANGE, true);
    return true;
}

static void calc_data_group_alive_count(FSClusterDataGroupInfo *group)
{
    FSClusterDataServerInfo **ds;
    FSClusterDataServerInfo **end;
    int active_count;

    active_count = 1;
    end = group->slave_ds_array.servers + group->slave_ds_array.count;
    for (ds=group->slave_ds_array.servers; ds<end; ds++) {
        if (FC_ATOMIC_GET((*ds)->status) == FS_DS_STATUS_ACTIVE) {
            active_count++;
        }
    }

    FC_ATOMIC_SET(group->active_count, active_count);
    /*
    logInfo("data group id: %d, server count: %d, active count: %d",
            group->id, group->ds_ptr_array.count, active_count);
            */
}

static void cluster_relationship_on_status_change(
        FSClusterDataServerInfo *ds,
        const int old_status, const int new_status)
{
    FSClusterDataServerInfo *master;

    master = (FSClusterDataServerInfo *)FC_ATOMIC_GET(ds->dg->master);
    if (master == NULL) {
        return;
    }

    if (CLUSTER_MYSELF_PTR == CLUSTER_LEADER_ATOM_PTR) {  //i am leader
        if (RESUME_MASTER_ROLE && (ds != master) && ds->is_preseted &&
                (new_status == FS_DS_STATUS_ACTIVE))
        {
            cluster_resume_master(ds->dg, master, ds);
            master = (FSClusterDataServerInfo *)FC_ATOMIC_GET(ds->dg->master);
            if (master == NULL) {
                return;
            }
        }
    }

    if (master->cs == CLUSTER_MYSELF_PTR) {  //i am master
        calc_data_group_alive_count(ds->dg);
    }

    if (ds->cs == CLUSTER_MYSELF_PTR) {  //myself
        if ((new_status == FS_DS_STATUS_INIT ||
                    new_status == FS_DS_STATUS_OFFLINE) &&
                (master->cs != CLUSTER_MYSELF_PTR))
        {
            recovery_thread_push_to_queue(ds);
        }
    }
}

bool cluster_relationship_set_ds_status_ex(FSClusterDataServerInfo *ds,
        const int old_status, const int new_status)
{
    if (new_status == old_status) {
        return false;
    }

    if (__sync_bool_compare_and_swap(&ds->status, old_status, new_status)) {
        cluster_relationship_on_status_change(ds, old_status, new_status);
        return true;
    } else {
        return false;
    }
}

int cluster_relationship_set_ds_status_and_dv(FSClusterDataServerInfo *ds,
        const int status, const FSClusterDataVersionPair *data_versions)
{
    int flags;
    struct {
        uint64_t current;
        uint64_t confirmed;
    } old_dvs;

    if (cluster_relationship_set_ds_status(ds, status)) {
        flags = FS_EVENT_TYPE_STATUS_CHANGE;
    } else {
        flags = 0;
    }

    old_dvs.current = FC_ATOMIC_GET(ds->data.current_version);
    if (data_versions->current != old_dvs.current) {
        FC_ATOMIC_CAS(ds->data.current_version, old_dvs.
                current, data_versions->current);
        flags |= FS_EVENT_TYPE_CURRENT_DV_CHANGE;
    }

    old_dvs.confirmed = FC_ATOMIC_GET(ds->data.confirmed_version);
    if (data_versions->confirmed != old_dvs.confirmed) {
        FC_ATOMIC_CAS(ds->data.confirmed_version, old_dvs.
                confirmed, data_versions->confirmed);
        flags |= FS_EVENT_TYPE_CONFIRMED_DV_CHANGE;
    }

    return flags;
}

void cluster_relationship_trigger_report_ds_status(FSClusterDataServerInfo *ds)
{
    ds->last_report_versions.current = -1;
    ds->last_report_versions.confirmed = -1;
    __sync_bool_compare_and_swap(&IMMEDIATE_REPORT, 0, 1);
}

int cluster_relationship_report_ds_status(FSClusterDataServerInfo *ds,
        const int old_status, const int new_status, const int source)
{
    int result;
    if (CLUSTER_MYSELF_PTR == CLUSTER_LEADER_ATOM_PTR) {  //i am leader
        bool notify_self;

        if (new_status != old_status) {
            __sync_bool_compare_and_swap(&ds->status,
                    old_status, new_status);
        }
        notify_self = (ds->cs != CLUSTER_MYSELF_PTR);
        cluster_topology_data_server_chg_notify(ds, source,
                FS_EVENT_TYPE_STATUS_CHANGE, notify_self);
        result = 0;
    } else {   //follower
        result = report_ds_status_to_leader(ds, new_status);
        if (ds->cs == CLUSTER_MYSELF_PTR) {
            //trigger report to the leader again for rare case
            cluster_relationship_trigger_report_ds_status(ds);
        }
    }

    return result;
}

bool cluster_relationship_swap_report_ds_status(FSClusterDataServerInfo *ds,
        const int old_status, const int new_status, const int source)
{
    bool changed;
    changed = cluster_relationship_set_ds_status_ex(
            ds, old_status, new_status);
    if (!changed) {
        return changed;
    }

    cluster_relationship_report_ds_status(ds,
            old_status, new_status, source);
    return changed;
}

void cluster_relationship_on_master_change(FSClusterDataGroupInfo *group,
        FSClusterDataServerInfo *old_master,
        FSClusterDataServerInfo *new_master)
{
    FSClusterDataServerInfo *ds;
    int old_status;
    int new_status;

    if (new_master == old_master) {
        return;
    }

    if (group->myself == NULL) {
        return;
    }

    old_status = FC_ATOMIC_GET(group->myself->status);
    if (group->myself == new_master) {
        new_status = FS_DS_STATUS_ACTIVE;
        ds = (old_status != FS_DS_STATUS_ACTIVE ? group->myself : NULL);
        if (__sync_bool_compare_and_swap(&group->is_my_term, 0, 1)) {
            replication_quorum_start_master_term(&group->repl_quorum_ctx);
        }
    } else {
        new_status = FS_DS_STATUS_OFFLINE;
        if (group->myself == old_master) {
            ds = (old_status == FS_DS_STATUS_ACTIVE ? group->myself : NULL);
        } else {
            ds = NULL;
        }

        if (group->myself == old_master || group->myself ==
                FC_ATOMIC_GET(group->old_master))
        {
            if (FC_ATOMIC_GET(group->master_swapping)) {
                __sync_bool_compare_and_swap(&group->master_swapping, 1, 0);
            }

            if (new_master != NULL) {
                __sync_bool_compare_and_swap(&group->is_my_term, 1, 0);
                replication_quorum_end_master_term(&group->repl_quorum_ctx);
            }
        }
    }

    if (new_master != NULL) {
        FC_ATOMIC_SET(group->old_master, new_master);
    }

    if (ds != NULL) {
        cluster_relationship_swap_report_ds_status(ds, old_status,
                new_status, FS_EVENT_SOURCE_SELF_REPORT);
    }

    if (new_master != NULL && group->myself != new_master) {
        recovery_thread_push_to_queue(group->myself);
    }
}

static void cluster_process_push_entry(FSClusterDataServerInfo *ds,
        const FSProtoPushDataServerStatusBodyPart *body_part)
{
    FSClusterDataServerInfo *old_master;
    FSClusterDataServerInfo *new_master;
    int old_status;
    int is_master;
    int64_t old_confirmed_version;
    int64_t new_confirmed_version;

    is_master = FC_ATOMIC_GET(ds->is_master);
    if (ds->cs == CLUSTER_MYSELF_PTR) {  //myself
        /* accept ACTIVE => OFFLINE only */
        if (body_part->status == FS_DS_STATUS_OFFLINE) {
            old_status = __sync_add_and_fetch(&ds->status, 0);
            if ((old_status == FS_DS_STATUS_ACTIVE) &&
                    (!body_part->is_master))
            {
                cluster_relationship_set_ds_status_ex(ds,
                        old_status, FS_DS_STATUS_OFFLINE);
            }
        }
    } else {
        cluster_relationship_set_ds_status(ds, body_part->status);
        ds->data.current_version = buff2long(body_part->data_versions.current);
        new_confirmed_version = buff2long(body_part->data_versions.confirmed);
        old_confirmed_version = FC_ATOMIC_GET(ds->data.confirmed_version);
        if (new_confirmed_version != old_confirmed_version) {
            FC_ATOMIC_CAS(ds->data.confirmed_version,
                    old_confirmed_version,
                    new_confirmed_version);
            if (REPLICA_QUORUM_NEED_MAJORITY && ds->dg->myself ==
                    FC_ATOMIC_GET(ds->dg->master))
            {
                replication_quorum_deal_version_change(&ds->dg->
                        repl_quorum_ctx, new_confirmed_version);
            }
        }
    }

    if (is_master == body_part->is_master) { //master NOT changed
        return;
    }

    old_master = (FSClusterDataServerInfo *)FC_ATOMIC_GET(ds->dg->master);
    __sync_bool_compare_and_swap(&ds->is_master,
            is_master, body_part->is_master);
    if (FC_ATOMIC_GET(ds->is_master)) {
        new_master = ds;
        if (new_master != old_master) {
            if (old_master != NULL) {
                __sync_bool_compare_and_swap(&old_master->is_master, 1, 0);
            }
            __sync_bool_compare_and_swap(&ds->dg->master,
                    old_master, new_master);
            logDebug("data_group_id: %d, set master server_id: %d",
                    ds->dg->id, ds->cs->server->id);
        }
    } else if (old_master == ds) {
        new_master = NULL;
        __sync_bool_compare_and_swap(&ds->dg->master,
                old_master, new_master);
        logDebug("data_group_id: %d, unset master server_id: %d",
                ds->dg->id, ds->cs->server->id);
    } else {
        return;
    }

    cluster_relationship_on_master_change(ds->dg, old_master, new_master);
}

static int cluster_process_leader_push(SFResponseInfo *response,
        char *body_buff, const int body_len)
{
    FSProtoPushDataServerStatusHeader *body_header;
    FSProtoPushDataServerStatusBodyPart *body_part;
    FSProtoPushDataServerStatusBodyPart *body_end;
    FSClusterDataServerInfo *ds;
    int data_server_count;
    int calc_size;
    int data_group_id;
    int server_id;

    body_header = (FSProtoPushDataServerStatusHeader *)body_buff;
    data_server_count = buff2int(body_header->data_server_count);
    calc_size = sizeof(FSProtoPushDataServerStatusHeader) +
        data_server_count * sizeof(FSProtoPushDataServerStatusBodyPart);
    if (calc_size != body_len) {
        response->error.length = sprintf(response->error.message,
                "response body length: %d != calculate size: %d, "
                "data server count: %d", body_len, calc_size,
                data_server_count);
        return EINVAL;
    }

    /*
    logDebug("file: "__FILE__", line: %d, func: %s, "
            "recv push from leader: %d, data_server_count: %d",
            __LINE__, __FUNCTION__, CLUSTER_LEADER_ATOM_PTR->server->id,
            data_server_count);
            */

    body_part = (FSProtoPushDataServerStatusBodyPart *)(body_header + 1);
    body_end = body_part + data_server_count;
    for (; body_part < body_end; body_part++) {
        data_group_id = buff2int(body_part->data_group_id);
        server_id = buff2int(body_part->server_id);
        if ((ds=fs_get_data_server(data_group_id, server_id)) != NULL) {
            cluster_process_push_entry(ds, body_part);
        }
    }

    CLUSTER_CURRENT_VERSION = buff2long(body_header->current_version);
    return 0;
}

static int cluster_recv_from_leader(ConnectionInfo *conn,
        SFResponseInfo *response, const int timeout_ms,
        const bool ignore_timeout)
{
    FSProtoHeader header_proto;
    int recv_bytes;
    int status;
    int result;
    int body_len;

    if ((result=tcprecvdata_nb_ms(conn->sock, &header_proto,
                    sizeof(FSProtoHeader), timeout_ms, &recv_bytes)) != 0)
    {
        if (result == ETIMEDOUT) {
            if (recv_bytes == 0 && ignore_timeout) {
                return 0;
            }
        }
        response->error.length = sprintf(response->error.message,
                "recv data fail, recv bytes: %d, "
                "errno: %d, error info: %s",
                recv_bytes, result, STRERROR(result));
        return result;
    }

    if (!SF_PROTO_CHECK_MAGIC(header_proto.magic)) {
        response->error.length = sprintf(response->error.message,
                "magic "SF_PROTO_MAGIC_FORMAT" is invalid, expect: "
                SF_PROTO_MAGIC_FORMAT, SF_PROTO_MAGIC_PARAMS(
                    header_proto.magic), SF_PROTO_MAGIC_EXPECT_PARAMS);
        return EINVAL;
    }

    body_len = buff2int(header_proto.body_len);
    if (body_len < 0 || body_len > g_sf_global_vars.max_buff_size) {
        response->error.length = sprintf(response->error.message,
                "invalid body length: %d < 0 or > %d",
                body_len, g_sf_global_vars.max_buff_size);
        return EINVAL;
    }
    if ((result=fast_buffer_check_capacity(
                    &NETWORK_BUFFER, body_len)) != 0)
    {
        return EINVAL;
    }

    if (body_len > 0) {
        if ((result=tcprecvdata_nb_ms(conn->sock, NETWORK_BUFFER.data,
                        body_len, timeout_ms, &recv_bytes)) != 0)
        {
            response->error.length = sprintf(response->error.message,
                    "recv data fail, recv bytes: %d, "
                    "errno: %d, error info: %s",
                    recv_bytes, result, STRERROR(result));
            return result;
        }
    }

    status = buff2short(header_proto.status);
    if (status != 0) {
        if (body_len > 0) {
            if (body_len >= sizeof(response->error.message)) {
                response->error.length = sizeof(response->error.message) - 1;
            } else {
                response->error.length = body_len;
            }

            memcpy(response->error.message, NETWORK_BUFFER.data,
                    response->error.length);
            *(response->error.message + response->error.length) = '\0';
        }

        return status;
    }

    if (header_proto.cmd == FS_CLUSTER_PROTO_PING_LEADER_RESP ||
            header_proto.cmd == FS_CLUSTER_PROTO_REPORT_DISK_SPACE_RESP)
    {
        return 0;
    } else if (header_proto.cmd == FS_CLUSTER_PROTO_PUSH_DATA_SERVER_STATUS) {
        return cluster_process_leader_push(response,
                NETWORK_BUFFER.data, body_len);
    } else {
        response->error.length = sprintf(response->error.message,
                "unexpect cmd: %d (%s)", header_proto.cmd,
                fs_get_cmd_caption(header_proto.cmd));
        return EINVAL;
    }
}

static int proto_ping_leader_ex(FSClusterServerInfo *leader,
        ConnectionInfo *conn, const unsigned char cmd,
        const int network_timeout, const bool report_all)
{
    FSProtoHeader *header;
    SFResponseInfo response;
    FSProtoPingLeaderReqHeader *req_header;
    int data_group_count;
    int result;
    int log_level;

    header = (FSProtoHeader *)NETWORK_BUFFER.data;
    req_header = (FSProtoPingLeaderReqHeader *)(header + 1);
    NETWORK_BUFFER.length = sizeof(FSProtoHeader) +
        sizeof(FSProtoPingLeaderReqHeader);
    pack_changed_data_versions(&data_group_count, report_all);

    if (data_group_count > 0) {
        logDebug("file: "__FILE__", line: %d, "
                "ping leader %s:%u, report_all: %d, "
                "report data_group_count: %d", __LINE__,
                conn->ip_addr, conn->port, report_all, data_group_count);
    }

    long2buff(leader->leader_version, req_header->leader_version);
    int2buff(data_group_count, req_header->data_group_count);
    SF_PROTO_SET_HEADER(header, cmd, NETWORK_BUFFER.length -
        sizeof(FSProtoHeader));

    response.error.length = 0;
    if ((result=tcpsenddata_nb(conn->sock, NETWORK_BUFFER.data,
                    NETWORK_BUFFER.length, network_timeout)) == 0)
    {
        result = cluster_recv_from_leader(conn, &response,
                1000 * network_timeout, false);
    }

    if (result != 0 && result != EOPNOTSUPP) {
        log_level = (result == SF_CLUSTER_ERROR_LEADER_VERSION_INCONSISTENT
                ? LOG_WARNING : LOG_ERR);
        fs_log_network_error_ex(&response, conn, result, log_level);
    }

    return result;
}

#define proto_activate_server(leader, conn, network_timeout) \
    proto_ping_leader_ex(leader, conn, FS_CLUSTER_PROTO_ACTIVATE_SERVER, \
            network_timeout, true)

#define proto_ping_leader(leader, conn, network_timeout) \
    proto_ping_leader_ex(leader, conn, FS_CLUSTER_PROTO_PING_LEADER_REQ, \
            network_timeout, false)

static int proto_report_disk_space(ConnectionInfo *conn,
        const FSClusterServerSpaceStat *stat)
{
    FSProtoHeader *header;
    SFResponseInfo response;
    char out_buff[sizeof(FSProtoHeader) + sizeof(FSProtoReportDiskSpaceReq)];
    FSProtoReportDiskSpaceReq *req;
    int result;

    header = (FSProtoHeader *)out_buff;
    req = (FSProtoReportDiskSpaceReq *)(header + 1);
    SF_PROTO_SET_HEADER(header, FS_CLUSTER_PROTO_REPORT_DISK_SPACE_REQ,
            sizeof(FSProtoReportDiskSpaceReq));
    long2buff(stat->total, req->total);
    long2buff(stat->avail, req->avail);
    long2buff(stat->used, req->used);

    response.error.length = 0;
    if ((result=tcpsenddata_nb(conn->sock, out_buff, sizeof(out_buff),
                    SF_G_NETWORK_TIMEOUT)) == 0)
    {
        result = cluster_recv_from_leader(conn, &response,
                1000 * SF_G_NETWORK_TIMEOUT, false);
    }

    if (result != 0 && result != EOPNOTSUPP) {
        fs_log_network_error(&response, conn, result);
    }

    return result;
}

static int cluster_try_recv_push_data(FSClusterServerInfo *leader,
        ConnectionInfo *conn)
{
    const int network_timeout = 2;
    int result;
    int start_time;
    int timeout_ms;
    SFResponseInfo response;

    start_time = g_current_time;
    timeout_ms = 100;
    response.error.length = 0;
    do {
        if ((result=cluster_recv_from_leader(conn, &response,
                        timeout_ms, true)) != 0)
        {
            fs_log_network_error(&response, conn, result);
            return result;
        }

        if (__sync_bool_compare_and_swap(&IMMEDIATE_REPORT, 1, 0)) {
            if ((result=proto_ping_leader(leader, conn,
                            network_timeout)) != 0)
            {
                return result;
            }
        }
    } while (start_time == g_current_time);

    return 0;
}

static inline int vote_node_active_check()
{
    int result;
    FCFSVoteClientJoinRequest join_request;

    if (VOTE_CONNECTION.sock < 0) {
        fill_join_request(&join_request, true);
        if ((result=fcfs_vote_client_join(&VOTE_CONNECTION,
                        &join_request)) != 0)
        {
            return result;
        }
    }

    if ((result=vote_client_proto_active_check(&VOTE_CONNECTION)) != 0) {
        vote_client_proto_close_connection(&VOTE_CONNECTION);
    }

    return result;
}

static int leader_check()
{
    int result;
    int active_count;
    int inactive_count;
    int vote_node_active;

    sleep(1);
    if (g_current_time - relationship_ctx.last_stat_time >= 10) {
        relationship_ctx.last_stat_time = g_current_time;
        storage_config_stat_path_spaces(&CLUSTER_MYSELF_PTR->space_stat);
    }

    if (NEED_CHECK_VOTE_NODE()) {
        if ((result=vote_node_active_check()) == 0) {
            vote_node_active = 1;
        } else {
            if (result == SF_CLUSTER_ERROR_LEADER_INCONSISTENT) {
                logWarning("file: "__FILE__", line: %d, "
                        "trigger re-select leader because leader "
                        "inconsistent with the vote node", __LINE__);
                cluster_relationship_trigger_reselect_leader();
                return EBUSY;
            }
            vote_node_active = 0;
        }
    } else {
        vote_node_active = 0;
    }

    PTHREAD_MUTEX_LOCK(&INACTIVE_SERVER_ARRAY.lock);
    inactive_count = INACTIVE_SERVER_ARRAY.count;
    PTHREAD_MUTEX_UNLOCK(&INACTIVE_SERVER_ARRAY.lock);
    if (inactive_count > 0) {
        if ((result=cluster_check_brainsplit(&inactive_count)) != 0) {
            return result;
        }

        active_count = (CLUSTER_SERVER_ARRAY.count -
                inactive_count) + vote_node_active;
        if (!sf_election_quorum_check(LEADER_ELECTION_QUORUM,
                    VOTE_NODE_ENABLED, CLUSTER_SERVER_ARRAY.count,
                    active_count))
        {
            logWarning("file: "__FILE__", line: %d, "
                    "trigger re-select leader because alive server "
                    "count: %d < half of total server count: %d ...",
                    __LINE__, active_count, CLUSTER_SERVER_ARRAY.count);
            cluster_relationship_trigger_reselect_leader();
            return EBUSY;
        }
    }

    leader_deal_data_version_changes();
    master_election_deal_delay_queue();
    return 0;  //do not need ping myself
}

static int follower_ping(FSClusterServerInfo *leader,
        ConnectionInfo *conn, const int timeout)
{
    int connect_timeout;
    int network_timeout;
    int result;

    network_timeout = FC_MIN(SF_G_NETWORK_TIMEOUT, timeout);
    if (conn->sock < 0) {
        connect_timeout = FC_MIN(SF_G_CONNECT_TIMEOUT, timeout);
        if ((result=fc_server_make_connection(&CLUSTER_GROUP_ADDRESS_ARRAY(
                            leader->server), conn, "fstore",
                        connect_timeout)) != 0)
        {
            return result;
        }

        if ((result=proto_join_leader(leader, conn, network_timeout)) != 0) {
            conn_pool_disconnect_server(conn);
            return result;
        }

        if ((result=proto_activate_server(leader, conn,
                        network_timeout)) != 0)
        {
            conn_pool_disconnect_server(conn);
            return result;
        }
    }

    if ((result=cluster_try_recv_push_data(leader, conn)) != 0) {
        conn_pool_disconnect_server(conn);
        return result;
    }

    result = proto_ping_leader(leader, conn, network_timeout);
    if (result == 0) {
        if (g_current_time - relationship_ctx.last_stat_time >= 10) {
            relationship_ctx.last_stat_time = g_current_time;
            storage_config_stat_path_spaces(&CLUSTER_MYSELF_PTR->space_stat);
            result = proto_report_disk_space(conn,
                    &CLUSTER_MYSELF_PTR->space_stat);
        }
    } else {
        conn_pool_disconnect_server(conn);
    }

    return result;
}

static inline int cluster_ping_leader(FSClusterServerInfo *leader,
        ConnectionInfo *conn, const int timeout, bool *is_ping)
{
    if (CLUSTER_MYSELF_PTR == CLUSTER_LEADER_ATOM_PTR) {
        *is_ping = false;
        return leader_check();
    } else {
        *is_ping = true;
        return follower_ping(leader, conn, timeout);
    }
}

static void *cluster_thread_entrance(void *arg)
{
#define MAX_SLEEP_SECONDS  10

    int result;
    int fail_count;
    int sleep_seconds;
    int ping_remain_time;
    bool is_ping;
    time_t ping_start_time;
    FSClusterServerInfo *leader;
    ConnectionInfo mconn;  //leader connection

#ifdef OS_LINUX
    prctl(PR_SET_NAME, "relationship");
#endif

    memset(&mconn, 0, sizeof(mconn));
    mconn.sock = -1;
    storage_config_stat_path_spaces(&CLUSTER_MYSELF_PTR->space_stat);

    fail_count = 0;
    sleep_seconds = 1;
    ping_start_time = g_current_time;
    while (SF_G_CONTINUE_FLAG) {
        leader = CLUSTER_LEADER_ATOM_PTR;
        if (leader == NULL) {
            if (cluster_select_leader() != 0) {
                sleep_seconds = 1 + (int)((double)rand()
                        * (double)MAX_SLEEP_SECONDS / RAND_MAX);
            } else {
                if (mconn.sock >= 0) {
                    conn_pool_disconnect_server(&mconn);
                }
                ping_start_time = g_current_time;
                sleep_seconds = 1;
            }
        } else {
            ping_remain_time = LEADER_ELECTION_LOST_TIMEOUT -
                (g_current_time - ping_start_time);
            if (ping_remain_time < 2) {
                ping_remain_time = 2;
            }
            if ((result=cluster_ping_leader(leader, &mconn,
                            ping_remain_time, &is_ping)) == 0)
            {
                fail_count = 0;
                ping_start_time = g_current_time;
                CLUSTER_LAST_HEARTBEAT_TIME = g_current_time;
                sleep_seconds = 0;
            } else if (is_ping) {
                ++fail_count;
                logError("file: "__FILE__", line: %d, "
                        "%dth ping leader id: %d, ip %s:%u fail",
                        __LINE__, fail_count, leader->server->id,
                        CLUSTER_GROUP_ADDRESS_FIRST_IP(leader->server),
                        CLUSTER_GROUP_ADDRESS_FIRST_PORT(leader->server));

                if (g_current_time - ping_start_time >
                        LEADER_ELECTION_LOST_TIMEOUT)
                {
                    if (fail_count > 1) {
                        cluster_unset_leader(leader);
                        fail_count = 0;
                    }
                    sleep_seconds = 0;
                } else {
                    sleep_seconds = 1;
                }
            } else {
                sleep_seconds = 0;  //leader check fail
            }
        }

        if (sleep_seconds > 0) {
            sleep(sleep_seconds);
        }
    }

    return NULL;
}

static int init_my_data_group_array()
{
    FSIdArray *id_array;
    FSClusterDataGroupInfo *data_group;
    FSClusterDataServerInfo *ds;
    FSClusterDataServerInfo *end;
    int bytes;
    int group_id;
    int group_index;
    int i;

    id_array = fs_cluster_cfg_get_my_data_group_ids(&CLUSTER_CONFIG_CTX,
            CLUSTER_MYSELF_PTR->server->id);

    bytes = sizeof(FSMyDataGroupInfo) * id_array->count;
    MY_DATA_GROUP_ARRAY.groups = (FSMyDataGroupInfo *)fc_malloc(bytes);
    if (MY_DATA_GROUP_ARRAY.groups == NULL) {
        return ENOMEM;
    }

    for (i=0; i<id_array->count; i++) {
        group_id = id_array->ids[i];
        group_index = group_id - CLUSTER_DATA_RGOUP_ARRAY.base_id;
        data_group = CLUSTER_DATA_RGOUP_ARRAY.groups + group_index;
        end = data_group->data_server_array.servers +
            data_group->data_server_array.count;
        for (ds=data_group->data_server_array.servers; ds<end; ds++) {
            if (ds->cs == CLUSTER_MYSELF_PTR) {
                MY_DATA_GROUP_ARRAY.groups[i].data_group_id = group_id;
                MY_DATA_GROUP_ARRAY.groups[i].ds = ds;
                break;
            }
        }
        if (ds == end) {
            logError("file: "__FILE__", line: %d, "
                    "data group id: %d, NOT found me, my server id: %d",
                    __LINE__, group_id, CLUSTER_MYSELF_PTR->server->id);
            return ENOENT;
        }
    }
    MY_DATA_GROUP_ARRAY.count = id_array->count;

    return 0;
}

int cluster_relationship_init()
{
    int result;
    int bytes;
    int init_size;

    VOTE_CONNECTION.sock = -1;
    bytes = sizeof(FSClusterServerDetectEntry) * CLUSTER_SERVER_ARRAY.count;
    INACTIVE_SERVER_ARRAY.entries = fc_malloc(bytes);
    if (INACTIVE_SERVER_ARRAY.entries == NULL) {
        return ENOMEM;
    }
    if ((result=init_pthread_lock(&INACTIVE_SERVER_ARRAY.lock)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "init_pthread_lock fail, errno: %d, error info: %s",
                __LINE__, result, STRERROR(result));
        return result;
    }
    INACTIVE_SERVER_ARRAY.alloc = CLUSTER_SERVER_ARRAY.count;

    if ((result=init_my_data_group_array()) != 0) {
        return result;
    }

    bytes = sizeof(FSProtoHeader) + sizeof(FSProtoPingLeaderReqHeader) +
        sizeof(FSProtoPingLeaderReqBodyPart) * MY_DATA_GROUP_ARRAY.count;
    init_size = FC_MAX(bytes, 4 * 1024);
    if ((result=fast_buffer_init_ex(&NETWORK_BUFFER, init_size)) != 0) {
        return result;
    }

    if ((result=master_election_init()) != 0) {
        return result;
    }

    return 0;
}

int cluster_relationship_start()
{
    pthread_t tid;
    return fc_create_thread(&tid, cluster_thread_entrance, NULL,
            SF_G_THREAD_STACK_SIZE);
}

int cluster_relationship_destroy()
{
	return 0;
}

void cluster_relationship_add_to_inactive_sarray(FSClusterServerInfo *cs)
{
    FSClusterServerDetectEntry *entry;
    FSClusterServerDetectEntry *end;
    bool found;

    found = false;
    PTHREAD_MUTEX_LOCK(&INACTIVE_SERVER_ARRAY.lock);
    if (INACTIVE_SERVER_ARRAY.count > 0) {
        end = INACTIVE_SERVER_ARRAY.entries + INACTIVE_SERVER_ARRAY.count;
        for (entry=INACTIVE_SERVER_ARRAY.entries; entry<end; entry++) {
            if (entry->cs == cs) {
                found = true;
                break;
            }
        }
    }
    if (!found) {
        if (INACTIVE_SERVER_ARRAY.count < INACTIVE_SERVER_ARRAY.alloc) {
            entry = INACTIVE_SERVER_ARRAY.entries + INACTIVE_SERVER_ARRAY.count;
            SET_SERVER_DETECT_ENTRY(entry, cs);
            INACTIVE_SERVER_ARRAY.count++;
        } else {
            logError("file: "__FILE__", line: %d, "
                    "server id: %d, add to inactive array fail "
                    "because array is full", __LINE__, cs->server->id);
        }
    } else {
        logWarning("file: "__FILE__", line: %d, "
                "server id: %d, already in inactive array!",
                __LINE__, cs->server->id);
    }
    PTHREAD_MUTEX_UNLOCK(&INACTIVE_SERVER_ARRAY.lock);
}

void cluster_relationship_remove_from_inactive_sarray(FSClusterServerInfo *cs)
{
    FSClusterServerDetectEntry *entry;
    FSClusterServerDetectEntry *p;
    FSClusterServerDetectEntry *end;

    PTHREAD_MUTEX_LOCK(&INACTIVE_SERVER_ARRAY.lock);
    end = INACTIVE_SERVER_ARRAY.entries + INACTIVE_SERVER_ARRAY.count;
    for (entry=INACTIVE_SERVER_ARRAY.entries; entry<end; entry++) {
        if (entry->cs == cs) {
            break;
        }
    }

    if (entry < end) {
        for (p=entry+1; p<end; p++) {
            *(p - 1) = *p;
        }
        INACTIVE_SERVER_ARRAY.count--;
    }
    PTHREAD_MUTEX_UNLOCK(&INACTIVE_SERVER_ARRAY.lock);
}
