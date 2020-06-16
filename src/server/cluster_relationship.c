#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <fcntl.h>
#include <pthread.h>
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "common/fs_proto.h"
#include "server_global.h"
#include "cluster_relationship.h"

typedef struct fs_cluster_server_status {
    FSClusterServerInfo *cs;
    bool is_leader;
    int server_id;
    int up_time;
    int last_shutdown_time;
    int64_t version;
} FSClusterServerStatus;

typedef struct fs_my_data_group_info {
    int data_group_id;
    FSClusterDataServerInfo *sp;
} FSMyDataGroupInfo;

typedef struct fs_my_data_group_array {
    int count;
    FSMyDataGroupInfo *groups;
} FSMyDataGroupArray;

FSClusterServerInfo *g_next_leader = NULL;
static FSMyDataGroupArray my_data_group_array = {0, NULL};

static int proto_get_server_status(ConnectionInfo *conn,
        FSClusterServerStatus *server_status)
{
	int result;
	FSProtoHeader *header;
    FSProtoGetServerStatusReq *req;
    FSProtoGetServerStatusResp *resp;
    FSResponseInfo response;
	char out_buff[sizeof(FSProtoHeader) + sizeof(FSProtoGetServerStatusReq)];
	char in_body[sizeof(FSProtoGetServerStatusResp)];

    header = (FSProtoHeader *)out_buff;
    FS_PROTO_SET_HEADER(header, FS_CLUSTER_PROTO_GET_SERVER_STATUS_REQ,
            sizeof(out_buff) - sizeof(FSProtoHeader));

    req = (FSProtoGetServerStatusReq *)(out_buff + sizeof(FSProtoHeader));
    int2buff(CLUSTER_MY_SERVER_ID, req->server_id);
    memcpy(req->config_signs.cluster, CLUSTER_CONFIG_SIGN_BUF,
            CLUSTER_CONFIG_SIGN_LEN);
    memcpy(req->config_signs.servers, SERVERS_CONFIG_SIGN_BUF,
            SERVERS_CONFIG_SIGN_LEN);
	if ((result=fs_send_and_recv_response(conn, out_buff,
			sizeof(out_buff), &response, SF_G_NETWORK_TIMEOUT,
            FS_CLUSTER_PROTO_GET_SERVER_STATUS_RESP, in_body,
            sizeof(FSProtoGetServerStatusResp))) != 0)
    {
        fs_log_network_error(&response, conn, result);
        return result;
    }

    resp = (FSProtoGetServerStatusResp *)in_body;
    server_status->is_leader = resp->is_leader;
    server_status->server_id = buff2int(resp->server_id);
    server_status->up_time = buff2int(resp->up_time);
    server_status->last_shutdown_time = buff2int(resp->last_shutdown_time);
    server_status->version = buff2long(resp->version);
    return 0;
}

static int proto_join_leader(ConnectionInfo *conn)
{
	int result;
	FSProtoHeader *header;
    FSProtoJoinLeaderReq *req;
    FSResponseInfo response;
	char out_buff[sizeof(FSProtoHeader) + sizeof(FSProtoJoinLeaderReq)];

    header = (FSProtoHeader *)out_buff;
    FS_PROTO_SET_HEADER(header, FS_CLUSTER_PROTO_JOIN_LEADER_REQ,
            sizeof(out_buff) - sizeof(FSProtoHeader));

    req = (FSProtoJoinLeaderReq *)(out_buff + sizeof(FSProtoHeader));
    int2buff(CLUSTER_MY_SERVER_ID, req->server_id);
    memcpy(req->config_signs.cluster, CLUSTER_CONFIG_SIGN_BUF,
            CLUSTER_CONFIG_SIGN_LEN);
    memcpy(req->config_signs.servers, SERVERS_CONFIG_SIGN_BUF,
            SERVERS_CONFIG_SIGN_LEN);
    if ((result=fs_send_and_recv_none_body_response(conn, out_buff,
                    sizeof(out_buff), &response, SF_G_NETWORK_TIMEOUT,
                    FS_CLUSTER_PROTO_JOIN_LEADER_RESP)) != 0)
    {
        fs_log_network_error(&response, conn, result);
    }

    return result;
}

static void pack_changed_data_versions(char *buff, int *length, int *count)
{
    FSMyDataGroupInfo *group;
    FSMyDataGroupInfo *end;
    FSProtoPingLeaderReqBodyPart *body_part;
    int64_t last_data_version;

    *count = 0;
    body_part = (FSProtoPingLeaderReqBodyPart *)buff;
    end = my_data_group_array.groups + my_data_group_array.count;
    for (group=my_data_group_array.groups; group<end; group++) {
        last_data_version = group->sp->last_data_version;
        if (group->sp->last_report_version == last_data_version) {
            continue;
        }

        group->sp->last_report_version = last_data_version;
        int2buff(group->data_group_id, body_part->data_group_id);
        long2buff(last_data_version, body_part->data_version);
        body_part++;
        ++(*count);
    }

    *length = (char *)body_part - buff;
}

static int proto_ping_leader(ConnectionInfo *conn)
{
    FSProtoHeader *header;
    FSResponseInfo response;
    char out_buff[8 * 1024];
    char in_buff[8 * 1024];
    FSProtoPingLeaderReqHeader *req_header;
    FSProtoPingLeaderRespHeader *body_header;
    FSProtoPingLeaderRespBodyPart *body_part;
    FSProtoPingLeaderRespBodyPart *body_end;
    //FSClusterServerInfo *cs;
    int out_bytes;
    int length;
    int data_group_count;
    int server_count;
    //int server_id;
    int result;

    header = (FSProtoHeader *)out_buff;
    req_header = (FSProtoPingLeaderReqHeader *)(header + 1);
    out_bytes = sizeof(FSProtoHeader) + sizeof(FSProtoPingLeaderReqHeader);
    pack_changed_data_versions(out_buff + out_bytes,
            &length, &data_group_count);
    out_bytes += length;

    short2buff(data_group_count, req_header->data_group_count);
    FS_PROTO_SET_HEADER(header, FS_CLUSTER_PROTO_PING_LEADER_REQ,
            out_bytes - sizeof(FSProtoHeader));

    if ((result=fs_send_and_check_response_header(conn, out_buff,
                    out_bytes, &response, SF_G_NETWORK_TIMEOUT,
                    FS_CLUSTER_PROTO_PING_LEADER_RESP)) == 0)
    {
        if (response.header.body_len > sizeof(in_buff)) {
            response.error.length = sprintf(response.error.message,
                    "response body length: %d is too large",
                    response.header.body_len);
            result = EOVERFLOW;
        } else {
            result = tcprecvdata_nb(conn->sock, in_buff,
                    response.header.body_len, SF_G_NETWORK_TIMEOUT);
        }
    }

    body_header = (FSProtoPingLeaderRespHeader *)in_buff;
    if (result == 0) {
        int calc_size;
        server_count = buff2int(body_header->server_count);
        calc_size = sizeof(FSProtoPingLeaderRespHeader) +
            server_count * sizeof(FSProtoPingLeaderRespBodyPart);
        if (calc_size != response.header.body_len) {
            response.error.length = sprintf(response.error.message,
                    "response body length: %d != calculate size: %d, "
                    "server count: %d", response.header.body_len,
                    calc_size, server_count);
            result = EINVAL;
        }
    } else {
        server_count = 0;
    }

    if (result != 0) {
        fs_log_network_error(&response, conn, result);
        return result;
    }

    if (server_count == 0) {
        return 0;
    }

    body_part = (FSProtoPingLeaderRespBodyPart *)(in_buff +
            sizeof(FSProtoPingLeaderRespHeader));
    body_end = body_part + server_count;
    //TODO
    /*
    for (; body_part < body_end; body_part++) {
        server_id = buff2int(body_part->server_id);
        if ((cs=fs_get_server_by_id(server_id)) != NULL) {
            if (cs->status != body_part->status) {
                cluster_info_set_status(cs, body_part->status);
                //logInfo("server_id: %d, status: %d", server_id, body_part->status);
            }
        }
    }
    */

    return 0;
}

static int cluster_cmp_server_status(const void *p1, const void *p2)
{
	FSClusterServerStatus *status1;
	FSClusterServerStatus *status2;
	int sub;

	status1 = (FSClusterServerStatus *)p1;
	status2 = (FSClusterServerStatus *)p2;
	if (status1->version < status2->version) {
        return -1;
    } else if (status1->version > status2->version) {
        return 1;
	}

	sub = status1->is_leader - status2->is_leader;
	if (sub != 0) {
		return sub;
	}

	sub = status1->last_shutdown_time - status2->last_shutdown_time;
    if (sub != 0) {
        return sub;
    }

	sub = status1->up_time - status2->up_time;
    if (sub != 0) {
        return sub;
    }
	return status1->server_id - status2->server_id;
}

static int cluster_get_server_status(FSClusterServerStatus *server_status)
{
    ConnectionInfo conn;
    int result;

    if (server_status->cs == CLUSTER_MYSELF_PTR) {
        server_status->is_leader = MYSELF_IS_LEADER;
        server_status->up_time = g_sf_global_vars.up_time;
        server_status->server_id = CLUSTER_MY_SERVER_ID;
        server_status->version = CLUSTER_CURRENT_VERSION;
        server_status->last_shutdown_time = fs_get_last_shutdown_time();
        return 0;
    } else {
        if ((result=fc_server_make_connection(&CLUSTER_GROUP_ADDRESS_ARRAY(
                            server_status->cs->server), &conn,
                        SF_G_CONNECT_TIMEOUT)) != 0)
        {
            return result;
        }

        result = proto_get_server_status(&conn, server_status);
        conn_pool_disconnect_server(&conn);
        return result;
    }
}

static int cluster_get_leader(FSClusterServerStatus *server_status,
        int *active_count)
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
    if (CLUSTER_SERVER_ARRAY.count < STATUS_ARRAY_FIXED_COUNT) {
        cs_status = status_array;
    } else {
        int bytes;
        bytes = sizeof(FSClusterServerStatus) * CLUSTER_SERVER_ARRAY.count;
        cs_status = (FSClusterServerStatus *)malloc(bytes);
        if (cs_status == NULL) {
            logError("file: "__FILE__", line: %d, "
                    "malloc %d bytes fail", __LINE__, bytes);
            return ENOMEM;
        }
    }

	current_status = cs_status;
	result = 0;
	end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
	for (server=CLUSTER_SERVER_ARRAY.servers; server<end; server++) {
		current_status->cs = server;
        r = cluster_get_server_status(current_status);
		if (r == 0) {
			current_status++;
		} else if (r != ENOENT) {
			result = r;
		}
	}

	*active_count = current_status - cs_status;
    if (*active_count == 0) {
        logError("file: "__FILE__", line: %d, "
                "get server status fail, "
                "server count: %d", __LINE__,
                CLUSTER_SERVER_ARRAY.count);
        return result == 0 ? ENOENT : result;
    }

	qsort(cs_status, *active_count, sizeof(FSClusterServerStatus),
		cluster_cmp_server_status);

	for (i=0; i<*active_count; i++) {
        logInfo("file: "__FILE__", line: %d, "
                "server_id: %d, ip addr %s:%d, is_leader: %d, "
                "up_time: %d, last_shutdown_time: %d, version: %"PRId64,
                __LINE__, cs_status[i].server_id,
                CLUSTER_GROUP_ADDRESS_FIRST_IP(cs_status[i].cs->server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(cs_status[i].cs->server),
                cs_status[i].is_leader, cs_status[i].up_time,
                cs_status[i].last_shutdown_time, cs_status[i].version);
    }

	memcpy(server_status, cs_status + (*active_count - 1),
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
    FSResponseInfo response;
    int result;

    if ((result=fc_server_make_connection(&CLUSTER_GROUP_ADDRESS_ARRAY(
                        cs->server), &conn, SF_G_CONNECT_TIMEOUT)) != 0)
    {
        *bConnectFail = true;
        return result;
    }
    *bConnectFail = false;

    header = (FSProtoHeader *)out_buff;
    FS_PROTO_SET_HEADER(header, cmd, sizeof(out_buff) -
            sizeof(FSProtoHeader));
    int2buff(leader->server->id, out_buff + sizeof(FSProtoHeader));
    if ((result=fs_send_and_recv_none_body_response(&conn, out_buff,
                    sizeof(out_buff), &response, SF_G_NETWORK_TIMEOUT,
                    FS_PROTO_ACK)) != 0)
    {
        fs_log_network_error(&response, &conn, result);
    }

    conn_pool_disconnect_server(&conn);
    return result;
}

int cluster_relationship_pre_set_leader(FSClusterServerInfo *leader)
{
    FSClusterServerInfo *next_leader;

    next_leader = g_next_leader;
    if (next_leader == NULL) {
        g_next_leader = leader;
    } else if (next_leader != leader) {
        logError("file: "__FILE__", line: %d, "
                "try to set next leader id: %d, "
                "but next leader: %d already exist",
                __LINE__, leader->server->id, next_leader->server->id);
        g_next_leader = NULL;
        return EEXIST;
    }

    return 0;
}

static inline void cluster_unset_leader()
{
    FSClusterServerInfo *old_leader;

    old_leader = CLUSTER_LEADER_PTR;
    if (old_leader != NULL) {
        old_leader->is_leader = false;
        CLUSTER_LEADER_PTR = NULL;
    }
}

static int cluster_relationship_set_leader(FSClusterServerInfo *leader)
{
    CLUSTER_LEADER_PTR = leader;
    leader->is_leader = true;
    if (CLUSTER_MYSELF_PTR == leader) {
        //g_data_thread_vars.error_mode = FS_DATA_ERROR_MODE_STRICT;
        __sync_add_and_fetch(&CLUSTER_CURRENT_VERSION, 1);
    } else {
        if (MYSELF_IS_LEADER) {
            MYSELF_IS_LEADER = false;
            __sync_add_and_fetch(&CLUSTER_CURRENT_VERSION, 1);
        }

        logInfo("file: "__FILE__", line: %d, "
                "the leader server id: %d, ip %s:%d",
                __LINE__, leader->server->id,
                CLUSTER_GROUP_ADDRESS_FIRST_IP(leader->server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(leader->server));
    }

    return 0;
}

int cluster_relationship_commit_leader(FSClusterServerInfo *leader)
{
    FSClusterServerInfo *next_leader;
    int result;

    next_leader = g_next_leader;
    if (next_leader == NULL) {
        logError("file: "__FILE__", line: %d, "
                "next leader is NULL", __LINE__);
        return EBUSY;
    }
    if (next_leader != leader) {
        logError("file: "__FILE__", line: %d, "
                "next leader server id: %d != expected server id: %d",
                __LINE__, next_leader->server->id, leader->server->id);
        g_next_leader = NULL;
        return EBUSY;
    }

    result = cluster_relationship_set_leader(leader);
    g_next_leader = NULL;
    return result;
}

void cluster_relationship_trigger_reselect_leader()
{
    cluster_unset_leader();
    //g_data_thread_vars.error_mode = FS_DATA_ERROR_MODE_LOOSE;
}

static int cluster_notify_next_leader(FSClusterServerInfo *cs,
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

static int cluster_notify_leader_changed(FSClusterServerStatus *server_status)
{
	FSClusterServerInfo *server;
	FSClusterServerInfo *send;
	int result;
	bool bConnectFail;
	int success_count;

	result = ENOENT;
	send = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
	success_count = 0;
	for (server=CLUSTER_SERVER_ARRAY.servers; server<send; server++) {
		if ((result=cluster_notify_next_leader(server,
				server_status, &bConnectFail)) != 0)
		{
			if (!bConnectFail) {
				return result;
			}
		} else {
			success_count++;
		}
	}

	if (success_count == 0) {
		return result;
	}

	result = ENOENT;
	success_count = 0;
	for (server=CLUSTER_SERVER_ARRAY.servers; server<send; server++) {
		if ((result=cluster_commit_next_leader(server,
				server_status, &bConnectFail)) != 0)
		{
			if (!bConnectFail)
			{
				return result;
			}
		} else {
			success_count++;
		}
	}

	if (success_count == 0) {
		return result;
	}

	return 0;
}

static int cluster_select_leader()
{
	int result;
    int active_count;
    int i;
    int sleep_secs;
    char prompt[512];
	FSClusterServerStatus server_status;
    FSClusterServerInfo *next_leader;

	logInfo("file: "__FILE__", line: %d, "
		"selecting leader...", __LINE__);

    sleep_secs = 2;
    i = 0;
    while (1) {
        if ((result=cluster_get_leader(&server_status, &active_count)) != 0) {
            return result;
        }
        if ((active_count == CLUSTER_SERVER_ARRAY.count) ||
                (active_count >= 2 && server_status.is_leader))
        {
            break;
        }

        ++i;
        if (g_current_time - server_status.last_shutdown_time > 300) {
            sprintf(prompt, "the candidate leader server id: %d, "
                    "does not match the selection rule because it's "
                    "restart interval: %d exceeds 300, "
                    "you must start ALL servers in the first time, "
                    "or remove the deprecated server(s) from the config file. ",
                    server_status.cs->server->id, (int)(g_current_time -
                        server_status.last_shutdown_time));
        } else {
            *prompt = '\0';
            if (i == 5) {
                break;
            }
        }

        logInfo("file: "__FILE__", line: %d, "
                "round %dth select leader, alive server count: %d "
                "< server count: %d, %stry again after %d seconds.",
                __LINE__, i, active_count, CLUSTER_SERVER_ARRAY.count,
                prompt, sleep_secs);
        sleep(sleep_secs);
        if (sleep_secs < 32) {
            sleep_secs *= 2;
        }
    }

    next_leader = server_status.cs;
    if (CLUSTER_MYSELF_PTR == next_leader) {
		if ((result=cluster_notify_leader_changed(
                        &server_status)) != 0)
		{
			return result;
		}

		logInfo("file: "__FILE__", line: %d, "
			"I am the new leader, id: %d, ip %s:%d",
			__LINE__, next_leader->server->id,
            CLUSTER_GROUP_ADDRESS_FIRST_IP(next_leader->server),
            CLUSTER_GROUP_ADDRESS_FIRST_PORT(next_leader->server));
	} else {
        if (server_status.is_leader) {
            cluster_relationship_set_leader(next_leader);
        }
        else
		{
			logInfo("file: "__FILE__", line: %d, "
				"waiting for the candidate leader server id: %d, "
                "ip %s:%d notify ...", __LINE__, next_leader->server->id,
                CLUSTER_GROUP_ADDRESS_FIRST_IP(next_leader->server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(next_leader->server));
			return ENOENT;
		}
	}

	return 0;
}

static int cluster_ping_leader(ConnectionInfo *conn)
{
    int result;
    FSClusterServerInfo *leader;

    if (CLUSTER_MYSELF_PTR == CLUSTER_LEADER_PTR) {
        return 0;  //do not need ping myself
    }

    leader = CLUSTER_LEADER_PTR;
    if (leader == NULL) {
        return ENOENT;
    }

    if (conn->sock < 0) {
        if ((result=fc_server_make_connection(&CLUSTER_GROUP_ADDRESS_ARRAY(
                            leader->server), conn, SF_G_CONNECT_TIMEOUT)) != 0)
        {
            return result;
        }

        if ((result=proto_join_leader(conn)) != 0) {
            conn_pool_disconnect_server(conn);
            return result;
        }
    }

    if ((result=proto_ping_leader(conn)) != 0) {
        conn_pool_disconnect_server(conn);
    }

    return 0;
}

static void *cluster_thread_entrance(void* arg)
{
#define MAX_SLEEP_SECONDS  10

    int fail_count;
    int sleep_seconds;
    FSClusterServerInfo *leader;
    ConnectionInfo mconn;  //leader connection

    memset(&mconn, 0, sizeof(mconn));
    mconn.sock = -1;

    fail_count = 0;
    sleep_seconds = 1;
    while (SF_G_CONTINUE_FLAG) {
        leader = CLUSTER_LEADER_PTR;
        if (leader == NULL) {
            if (cluster_select_leader() != 0) {
                sleep_seconds = 1 + (int)((double)rand()
                        * (double)MAX_SLEEP_SECONDS / RAND_MAX);
            } else {
                sleep_seconds = 1;
            }
        } else {
            if (cluster_ping_leader(&mconn) == 0) {
                fail_count = 0;
                sleep_seconds = 1;
            } else {
                ++fail_count;
                logError("file: "__FILE__", line: %d, "
                        "%dth ping leader id: %d, ip %s:%d fail",
                        __LINE__, fail_count, leader->server->id,
                        CLUSTER_GROUP_ADDRESS_FIRST_IP(leader->server),
                        CLUSTER_GROUP_ADDRESS_FIRST_PORT(leader->server));

                sleep_seconds *= 2;
                if (fail_count >= 4) {
                    cluster_unset_leader();

                    fail_count = 0;
                    sleep_seconds = 1;
                }
            }
        }

        sleep(sleep_seconds);
    }

    return NULL;
}

static int init_my_data_group_array()
{
    FSIdArray *id_array;
    FSClusterDataGroupInfo *data_group;
    FSClusterDataServerInfo *sp;
    FSClusterDataServerInfo *end;
    int bytes;
    int data_group_id;
    int i;

    id_array = fs_cluster_cfg_get_server_group_ids(&CLUSTER_CONFIG_CTX,
            CLUSTER_MYSELF_PTR->server->id);

    bytes = sizeof(FSMyDataGroupInfo) * id_array->count;
    my_data_group_array.groups = (FSMyDataGroupInfo *)malloc(bytes);
    if (my_data_group_array.groups == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }

    for (i=0; i<id_array->count; i++) {
        data_group_id = id_array->ids[i];
        data_group = CLUSTER_DATA_RGOUP_ARRAY.groups + (data_group_id - 1);
        end = data_group->data_server_array.servers +
            data_group->data_server_array.count;
        for (sp=data_group->data_server_array.servers; sp<end; sp++) {
            if (sp->cs == CLUSTER_MYSELF_PTR) {
                my_data_group_array.groups[i].data_group_id = data_group_id;
                my_data_group_array.groups[i].sp = sp;
                break;
            }
        }
        if (sp == end) {
            logError("file: "__FILE__", line: %d, "
                    "data group id: %d, NOT found me, my server id: %d",
                    __LINE__, data_group_id, CLUSTER_MYSELF_PTR->server->id);
            return ENOENT;
        }
    }
    my_data_group_array.count = id_array->count;

    return 0;
}

int cluster_relationship_init()
{
	pthread_t tid;
    int result;

    if ((result=init_my_data_group_array()) != 0) {
        return result;
    }

	return fc_create_thread(&tid, cluster_thread_entrance, NULL,
            SF_G_THREAD_STACK_SIZE);
}

int cluster_relationship_destroy()
{
	return 0;
}
