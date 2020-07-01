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
#include "cluster_topology.h"
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

FSClusterServerInfo *g_next_leader = NULL;
static FSMyDataGroupArray my_data_group_array = {NULL, 0};
static FSClusterServerDetectArray inactive_server_array = {NULL, 0};

#define SET_SERVER_DETECT_ENTRY(entry, server) \
    do {  \
        entry->cs = server;    \
        entry->next_time = g_current_time + SF_G_NETWORK_TIMEOUT; \
    } while (0)

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
    response.error.length = 0;
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
    response.error.length = 0;
    if ((result=fs_send_and_recv_none_body_response(conn, out_buff,
                    sizeof(out_buff), &response, SF_G_NETWORK_TIMEOUT,
                    FS_CLUSTER_PROTO_JOIN_LEADER_RESP)) != 0)
    {
        fs_log_network_error(&response, conn, result);
    }

    return result;
}

static void pack_changed_data_versions(char *buff, int *length,
        int *count, const bool report_all)
{
    FSMyDataGroupInfo *group;
    FSMyDataGroupInfo *end;
    FSProtoPingLeaderReqBodyPart *body_part;
    int64_t data_version;

    *count = 0;
    body_part = (FSProtoPingLeaderReqBodyPart *)buff;
    end = my_data_group_array.groups + my_data_group_array.count;
    for (group=my_data_group_array.groups; group<end; group++) {
        data_version = group->sp->data_version;
        if (report_all || group->sp->last_report_version != data_version) {
            group->sp->last_report_version = data_version;
            int2buff(group->data_group_id, body_part->data_group_id);
            long2buff(data_version, body_part->data_version);
            body_part->status = group->sp->status;
            body_part++;
            ++(*count);
        }
    }

    *length = (char *)body_part - buff;
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

#define cluster_get_server_status(server_status) \
    cluster_get_server_status_ex(server_status, true)

static int cluster_get_server_status_ex(FSClusterServerStatus *server_status,
        const bool log_connect_error)
{
    ConnectionInfo conn;
    int result;

    if (server_status->cs == CLUSTER_MYSELF_PTR) {
        server_status->is_leader = MYSELF_IS_LEADER;
        server_status->up_time = g_sf_global_vars.up_time;
        server_status->server_id = CLUSTER_MY_SERVER_ID;
        server_status->version = __sync_add_and_fetch(&CLUSTER_CURRENT_VERSION, 0);
        server_status->last_shutdown_time = fs_get_last_shutdown_time();
        return 0;
    } else {
        if ((result=fc_server_make_connection_ex(&CLUSTER_GROUP_ADDRESS_ARRAY(
                            server_status->cs->server), &conn,
                        SF_G_CONNECT_TIMEOUT, NULL, log_connect_error)) != 0)
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
    response.error.length = 0;
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

static int do_check_brainsplit(FSClusterServerInfo *cs)
{
    const bool log_connect_error = false;
    FSClusterServerStatus server_status;

    server_status.cs = cs;
    if (cluster_get_server_status_ex(&server_status, log_connect_error) != 0) {
        return 0;
    }

    logInfo("server id: %d, leader: %d", cs->server->id, server_status.is_leader);
    if (server_status.is_leader) {
        logWarning("file: "__FILE__", line: %d, "
                "two leaders occurs, anonther leader is %s:%d, "
                "trigger re-select leader ...", __LINE__,
                CLUSTER_GROUP_ADDRESS_FIRST_IP(cs->server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(cs->server));
        cluster_unset_leader();
        return EEXIST;
    }

    return 0;
}

static int cluster_check_brainsplit(const int inactive_count)
{
    FSClusterServerDetectEntry *entry;
    FSClusterServerDetectEntry *end;
    int result;

    end = inactive_server_array.entries + inactive_count;
    for (entry=inactive_server_array.entries; entry<end; entry++) {
        if (entry >= inactive_server_array.entries +
                inactive_server_array.count)
        {
            break;
        }
        if (entry->next_time > g_current_time) {
            continue;
        }
        if ((result=do_check_brainsplit(entry->cs)) != 0) {
            return result;
        }

        entry->next_time = g_current_time + SF_G_NETWORK_TIMEOUT;
    }

    return 0;
}

static void init_inactive_server_array()
{
    FSClusterServerInfo *cs;
    FSClusterServerInfo *end;
    FSClusterServerDetectEntry *entry;

    entry = inactive_server_array.entries;
    end = CLUSTER_SERVER_ARRAY.servers + CLUSTER_SERVER_ARRAY.count;
    for (cs=CLUSTER_SERVER_ARRAY.servers; cs<end; cs++) {
        if (cs != CLUSTER_MYSELF_PTR) {
            SET_SERVER_DETECT_ENTRY(entry, cs);
            entry++;
        }
    }

    inactive_server_array.count = entry - inactive_server_array.entries;
}

static int cluster_relationship_set_leader(FSClusterServerInfo *leader)
{
    leader->is_leader = true;
    if (CLUSTER_MYSELF_PTR == leader) {
        if (CLUSTER_LEADER_PTR != CLUSTER_MYSELF_PTR) {
            init_inactive_server_array();
            cluster_topology_offline_all_data_servers();
            cluster_topology_set_check_master_flags();
        }
    } else {
        if (MYSELF_IS_LEADER) {
            MYSELF_IS_LEADER = false;
        }

        logInfo("file: "__FILE__", line: %d, "
                "the leader server id: %d, ip %s:%d",
                __LINE__, leader->server->id,
                CLUSTER_GROUP_ADDRESS_FIRST_IP(leader->server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(leader->server));
    }
    CLUSTER_LEADER_PTR = leader;

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

static int cluster_process_leader_push(FSResponseInfo *response,
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

    logInfo("file: "__FILE__", line: %d, func: %s, "
            "recv push from leader: %d, data_server_count: %d",
            __LINE__, __FUNCTION__, CLUSTER_LEADER_PTR->server->id,
            data_server_count);

    body_part = (FSProtoPushDataServerStatusBodyPart *)(body_header + 1);
    body_end = body_part + data_server_count;
    for (; body_part < body_end; body_part++) {
        data_group_id = buff2int(body_part->data_group_id);
        server_id = buff2int(body_part->server_id);

        //logInfo("data_group_id: %d, server_id: %d", data_group_id, server_id);
        if ((ds=fs_get_data_server(data_group_id, server_id)) != NULL) {
            if (ds->cs != CLUSTER_MYSELF_PTR) {
                ds->status = body_part->status;
                ds->data_version = buff2long(body_part->data_version);
            }
            if (ds->is_master != body_part->is_master) {
                ds->is_master = body_part->is_master;
                if (ds->is_master) {
                    if (ds->dg->master != NULL && ds->dg->master != ds) {
                        ds->dg->master->is_master = false;
                    }
                    ds->dg->master = ds;
                    logInfo("data_group_id: %d, set master server_id: %d",
                            data_group_id, ds->cs->server->id);
                } else if (ds->dg->master == ds) {
                    ds->dg->master = NULL;

                    logInfo("data_group_id: %d, unset master server_id: %d",
                            data_group_id, ds->cs->server->id);
                }
            }
        }
    }

    CLUSTER_CURRENT_VERSION = buff2long(body_header->current_version);
    return 0;
}

static int cluster_recv_from_leader(ConnectionInfo *conn,
        FSResponseInfo *response, const int timeout_ms,
        const bool ignore_timeout)
{
    FSProtoHeader header_proto;
    char in_buff[8 * 1024];
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

            result = EIO;
        }
        response->error.length = sprintf(response->error.message,
                "recv data fail, recv bytes: %d, "
                "errno: %d, error info: %s",
                recv_bytes, result, STRERROR(result));
        return result;
    }

    body_len = buff2int(header_proto.body_len);
    status = buff2short(header_proto.status);
    if (body_len > 0) {
        if (body_len >= sizeof(in_buff)) {
            response->error.length = sprintf(response->error.message,
                    "recv body length: %d exceeds buffer size: %d",
                    body_len, (int)sizeof(in_buff));
            return EOVERFLOW;
        }
        if ((result=tcprecvdata_nb_ms(conn->sock, in_buff,
                        body_len, timeout_ms, &recv_bytes)) != 0)
        {
            response->error.length = sprintf(response->error.message,
                    "recv data fail, recv bytes: %d, "
                    "errno: %d, error info: %s",
                    recv_bytes, result, STRERROR(result));
            return result;
        }
    }

    if (status != 0) {
        if (body_len > 0) {
            if (body_len >= sizeof(response->error.message)) {
                response->error.length = sizeof(response->error.message) - 1;
            } else {
                response->error.length = body_len;
            }

            memcpy(response->error.message, in_buff, response->error.length);
            *(response->error.message + response->error.length) = '\0';
        }

        return status;
    }

    if (header_proto.cmd == FS_CLUSTER_PROTO_PING_LEADER_RESP) {
        return 0;
    } else if (header_proto.cmd == FS_CLUSTER_PROTO_PUSH_DATA_SERVER_STATUS) {
        return cluster_process_leader_push(response, in_buff, body_len);
    } else {
        response->error.length = sprintf(response->error.message,
                "unexpect cmd: %d (%s)", header_proto.cmd,
                fs_get_cmd_caption(header_proto.cmd));
        return EINVAL;
    }
}

//TODO
static void set_status_for_test()
{
    FSMyDataGroupInfo *group;
    FSMyDataGroupInfo *end;

    end = my_data_group_array.groups + my_data_group_array.count;
    for (group=my_data_group_array.groups; group<end; group++) {
        group->sp->status = FS_SERVER_STATUS_ACTIVE;
        group->sp->last_report_version = -1;
    }
}

static int proto_ping_leader_ex(ConnectionInfo *conn, const bool report_all)
{
    FSProtoHeader *header;
    FSResponseInfo response;
    char out_buff[8 * 1024];
    FSProtoPingLeaderReqHeader *req_header;
    int out_bytes;
    int length;
    int data_group_count;
    int result;

    header = (FSProtoHeader *)out_buff;
    req_header = (FSProtoPingLeaderReqHeader *)(header + 1);
    out_bytes = sizeof(FSProtoHeader) + sizeof(FSProtoPingLeaderReqHeader);
    pack_changed_data_versions(out_buff + out_bytes,
            &length, &data_group_count, report_all);
    out_bytes += length;

    if (data_group_count > 0) {
        logInfo("file: "__FILE__", line: %d, func: %s, "
                "ping leader %s:%d, report_all: %d, "
                "report data_group_count: %d", __LINE__,
                __FUNCTION__, conn->ip_addr, conn->port,
                report_all, data_group_count);
    }

    int2buff(data_group_count, req_header->data_group_count);
    FS_PROTO_SET_HEADER(header, FS_CLUSTER_PROTO_PING_LEADER_REQ,
            out_bytes - sizeof(FSProtoHeader));

    response.error.length = 0;
    if ((result=tcpsenddata_nb(conn->sock, out_buff, out_bytes,
                    SF_G_NETWORK_TIMEOUT)) == 0)
    {
        result = cluster_recv_from_leader(conn, &response,
                1000 * SF_G_NETWORK_TIMEOUT, false);
    }

    if (result != 0) {
        fs_log_network_error(&response, conn, result);
    }

    return result;
}

static int cluster_try_recv_push_data(ConnectionInfo *conn)
{
    int result;
    int start_time;
    int timeout_ms;
    FSResponseInfo response;

    start_time = g_current_time;
    timeout_ms = 1000;
    response.error.length = 0;
    do {
        if ((result=cluster_recv_from_leader(conn, &response,
                        timeout_ms, true)) != 0)
        {
            fs_log_network_error(&response, conn, result);
            return result;
        }
        timeout_ms = 100;
    } while (start_time == g_current_time);

    return 0;
}

static int cluster_ping_leader(ConnectionInfo *conn)
{
    int result;
    int inactive_count;
    FSClusterServerInfo *leader;

    if (CLUSTER_MYSELF_PTR == CLUSTER_LEADER_PTR) {
        sleep(1);
        PTHREAD_MUTEX_LOCK(&inactive_server_array.lock);
        inactive_count = inactive_server_array.count;
        PTHREAD_MUTEX_UNLOCK(&inactive_server_array.lock);
        if (inactive_count > 0) {
            static int count = 0;
            if (count++ % 100 == 0) {
                logInfo("file: "__FILE__", line: %d, "
                        "inactive_count: %d", __LINE__, inactive_count);
            }
            if ((result=cluster_check_brainsplit(inactive_count)) != 0) {
                return result;
            }
        }
        cluster_topology_check_and_make_delay_decisions();
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

        if ((result=proto_ping_leader_ex(conn, true)) != 0) {
            conn_pool_disconnect_server(conn);
            return result;
        }
    }

    if ((result=cluster_try_recv_push_data(conn)) != 0) {
        conn_pool_disconnect_server(conn);
        return result;
    }

    if ((result=proto_ping_leader_ex(conn, false)) != 0) {
        conn_pool_disconnect_server(conn);
    }

    return result;
}

static void *cluster_thread_entrance(void* arg)
{
#define MAX_SLEEP_SECONDS  10

    int fail_count;
    int sleep_seconds;
    FSClusterServerInfo *leader;
    ConnectionInfo mconn;  //leader connection

    //TODO
    set_status_for_test();

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
                sleep_seconds = 0;
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
    FSClusterDataServerInfo *sp;
    FSClusterDataServerInfo *end;
    int bytes;
    int data_group_id;
    int i;

    id_array = fs_cluster_cfg_get_my_data_group_ids(&CLUSTER_CONFIG_CTX,
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
    int bytes;

    bytes = sizeof(FSClusterServerDetectEntry) * CLUSTER_SERVER_ARRAY.count;
    inactive_server_array.entries = (FSClusterServerDetectEntry *)malloc(bytes);
    if (inactive_server_array.entries == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    if ((result=init_pthread_lock(&inactive_server_array.lock)) != 0) {
        logError("file: "__FILE__", line: %d, "
            "init_pthread_lock fail, errno: %d, error info: %s",
            __LINE__, result, STRERROR(result));
        return result;
    }
    inactive_server_array.alloc = CLUSTER_SERVER_ARRAY.count;

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

void cluster_relationship_add_to_inactive_sarray(FSClusterServerInfo *cs)
{
    FSClusterServerDetectEntry *entry;

    PTHREAD_MUTEX_LOCK(&inactive_server_array.lock);
    if (inactive_server_array.count < inactive_server_array.alloc) {
        entry = inactive_server_array.entries + inactive_server_array.count;
        SET_SERVER_DETECT_ENTRY(entry, cs);
        inactive_server_array.count++;
    }
    PTHREAD_MUTEX_UNLOCK(&inactive_server_array.lock);
}

void cluster_relationship_remove_from_inactive_sarray(FSClusterServerInfo *cs)
{
    FSClusterServerDetectEntry *entry;
    FSClusterServerDetectEntry *p;
    FSClusterServerDetectEntry *end;

    PTHREAD_MUTEX_LOCK(&inactive_server_array.lock);
    end = inactive_server_array.entries + inactive_server_array.count;
    for (entry=inactive_server_array.entries; entry<end; entry++) {
        if (entry->cs == cs) {
            break;
        }
    }

    if (entry < end) {
        for (p=entry+1; p<end; p++) {
            *(p - 1) = *p;
        }
        inactive_server_array.count--;
    }
    PTHREAD_MUTEX_UNLOCK(&inactive_server_array.lock);
}
