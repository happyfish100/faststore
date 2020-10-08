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
#include "server_recovery.h"
#include "cluster_topology.h"
#include "cluster_relationship.h"

#define ALIGN_TIME(interval) (((interval) / 60) * 60)

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
    FSClusterServerInfo *next_leader;
    FSMyDataGroupArray my_data_group_array;
    FSClusterServerDetectArray inactive_server_array;
    volatile int immediate_report;
} FSClusterRelationshipContext;

#define MY_DATA_GROUP_ARRAY relationship_ctx.my_data_group_array
#define INACTIVE_SERVER_ARRAY relationship_ctx.inactive_server_array
#define IMMEDIATE_REPORT relationship_ctx.immediate_report

static FSClusterRelationshipContext relationship_ctx = {
    NULL, {NULL, 0}, {NULL, 0}, 0
};

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
    SFResponseInfo response;
	char out_buff[sizeof(FSProtoHeader) + sizeof(FSProtoGetServerStatusReq)];
	char in_body[sizeof(FSProtoGetServerStatusResp)];

    header = (FSProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, FS_CLUSTER_PROTO_GET_SERVER_STATUS_REQ,
            sizeof(out_buff) - sizeof(FSProtoHeader));

    req = (FSProtoGetServerStatusReq *)(out_buff + sizeof(FSProtoHeader));
    int2buff(CLUSTER_MY_SERVER_ID, req->server_id);
    memcpy(req->config_signs.cluster, CLUSTER_CONFIG_SIGN_BUF,
            CLUSTER_CONFIG_SIGN_LEN);
    memcpy(req->config_signs.servers, SERVERS_CONFIG_SIGN_BUF,
            SERVERS_CONFIG_SIGN_LEN);
    response.error.length = 0;
	if ((result=sf_send_and_recv_response(conn, out_buff,
			sizeof(out_buff), &response, SF_G_NETWORK_TIMEOUT,
            FS_CLUSTER_PROTO_GET_SERVER_STATUS_RESP, in_body,
            sizeof(FSProtoGetServerStatusResp))) != 0)
    {
        sf_log_network_error(&response, conn, result);
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
    SFResponseInfo response;
	char out_buff[sizeof(FSProtoHeader) + sizeof(FSProtoJoinLeaderReq)];

    header = (FSProtoHeader *)out_buff;
    SF_PROTO_SET_HEADER(header, FS_CLUSTER_PROTO_JOIN_LEADER_REQ,
            sizeof(out_buff) - sizeof(FSProtoHeader));

    req = (FSProtoJoinLeaderReq *)(out_buff + sizeof(FSProtoHeader));
    int2buff(CLUSTER_MY_SERVER_ID, req->server_id);
    memcpy(req->config_signs.cluster, CLUSTER_CONFIG_SIGN_BUF,
            CLUSTER_CONFIG_SIGN_LEN);
    memcpy(req->config_signs.servers, SERVERS_CONFIG_SIGN_BUF,
            SERVERS_CONFIG_SIGN_LEN);
    response.error.length = 0;
    if ((result=sf_send_and_recv_none_body_response(conn, out_buff,
                    sizeof(out_buff), &response, SF_G_NETWORK_TIMEOUT,
                    FS_CLUSTER_PROTO_JOIN_LEADER_RESP)) != 0)
    {
        sf_log_network_error(&response, conn, result);
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
    end = MY_DATA_GROUP_ARRAY.groups + MY_DATA_GROUP_ARRAY.count;
    for (group=MY_DATA_GROUP_ARRAY.groups; group<end; group++) {
        data_version = __sync_add_and_fetch(&group->ds->data.version, 0);
        if (report_all || group->ds->last_report_version != data_version) {
            group->ds->last_report_version = data_version;
            int2buff(group->data_group_id, body_part->data_group_id);
            long2buff(data_version, body_part->data_version);
            body_part->status = __sync_add_and_fetch(&group->ds->status, 0);
            body_part++;
            ++(*count);
        }
    }

    *length = (char *)body_part - buff;
}

static void leader_deal_data_version_changes()
{
    FSMyDataGroupInfo *group;
    FSMyDataGroupInfo *end;
    int64_t data_version;
    int count;

    count = 0;
    end = MY_DATA_GROUP_ARRAY.groups + MY_DATA_GROUP_ARRAY.count;
    for (group=MY_DATA_GROUP_ARRAY.groups; group<end; group++) {
        data_version = __sync_add_and_fetch(&group->ds->data.version, 0);
        if (group->ds->last_report_version != data_version) {
            group->ds->last_report_version = data_version;
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
        cs_status = (FSClusterServerStatus *)fc_malloc(bytes);
        if (cs_status == NULL) {
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
        int restart_interval;
        restart_interval = cs_status[i].up_time -
            cs_status[i].last_shutdown_time;
        logDebug("file: "__FILE__", line: %d, "
                "server_id: %d, ip addr %s:%u, version: %"PRId64", "
                "is_leader: %d, up_time: %d, restart interval: %d",
                __LINE__, cs_status[i].server_id,
                CLUSTER_GROUP_ADDRESS_FIRST_IP(cs_status[i].cs->server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(cs_status[i].cs->server),
                cs_status[i].version, cs_status[i].is_leader,
                ALIGN_TIME(cs_status[i].up_time),
                ALIGN_TIME(restart_interval));
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
    SFResponseInfo response;
    int result;

    if ((result=fc_server_make_connection(&CLUSTER_GROUP_ADDRESS_ARRAY(
                        cs->server), &conn, SF_G_CONNECT_TIMEOUT)) != 0)
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
        sf_log_network_error(&response, &conn, result);
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
                        leader->server), &conn, SF_G_CONNECT_TIMEOUT)) != 0)
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
        sf_log_network_error(&response, &conn, result);
    }

    conn_pool_disconnect_server(&conn);
    return result;
}

int cluster_relationship_pre_set_leader(FSClusterServerInfo *leader)
{
    FSClusterServerInfo *next_leader;

    next_leader = relationship_ctx.next_leader;
    if (next_leader == NULL) {
        relationship_ctx.next_leader = leader;
    } else if (next_leader != leader) {
        logError("file: "__FILE__", line: %d, "
                "try to set next leader id: %d, "
                "but next leader: %d already exist",
                __LINE__, leader->server->id, next_leader->server->id);
        relationship_ctx.next_leader = NULL;
        return EEXIST;
    }

    return 0;
}

static inline void cluster_unset_leader()
{
    FSClusterServerInfo *old_leader;

    old_leader = CLUSTER_LEADER_ATOM_PTR;
    if (old_leader != NULL) {
        old_leader->is_leader = false;
        __sync_bool_compare_and_swap(&CLUSTER_LEADER_PTR, old_leader, NULL);
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

    if (server_status.is_leader) {
        logWarning("file: "__FILE__", line: %d, "
                "two leaders occurs, anonther leader is %s:%u, "
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

    end = INACTIVE_SERVER_ARRAY.entries + inactive_count;
    for (entry=INACTIVE_SERVER_ARRAY.entries; entry<end; entry++) {
        if (entry >= INACTIVE_SERVER_ARRAY.entries +
                INACTIVE_SERVER_ARRAY.count)
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
            if (__sync_fetch_and_add(&cs->active, 0)) {
                __sync_bool_compare_and_swap(&cs->active, 1, 0);
            }
        }
    }
}

static int cluster_relationship_set_leader(FSClusterServerInfo *new_leader)
{
    FSClusterServerInfo *old_leader;

    old_leader = CLUSTER_LEADER_ATOM_PTR;
    new_leader->is_leader = true;
    if (CLUSTER_MYSELF_PTR == new_leader) {
        if (old_leader != CLUSTER_MYSELF_PTR) {
            cluster_relationship_deactivate_all_servers();
            __sync_bool_compare_and_swap(&CLUSTER_MYSELF_PTR->active, 0, 1);

            init_inactive_server_array();
            cluster_topology_offline_all_data_servers();
            cluster_topology_set_check_master_flags();
        }
    } else {
        if (MYSELF_IS_LEADER) {
            MYSELF_IS_LEADER = false;
        }

        if (new_leader != old_leader) {
            logInfo("file: "__FILE__", line: %d, "
                    "the leader server id: %d, ip %s:%u",
                    __LINE__, new_leader->server->id,
                    CLUSTER_GROUP_ADDRESS_FIRST_IP(new_leader->server),
                    CLUSTER_GROUP_ADDRESS_FIRST_PORT(new_leader->server));
        }
    }
    __sync_bool_compare_and_swap(&CLUSTER_LEADER_PTR,
            old_leader, new_leader);

    return 0;
}

int cluster_relationship_commit_leader(FSClusterServerInfo *leader)
{
    FSClusterServerInfo *next_leader;
    int result;

    next_leader = relationship_ctx.next_leader;
    if (next_leader == NULL) {
        logError("file: "__FILE__", line: %d, "
                "next leader is NULL", __LINE__);
        return EBUSY;
    }
    if (next_leader != leader) {
        logError("file: "__FILE__", line: %d, "
                "next leader server id: %d != expected server id: %d",
                __LINE__, next_leader->server->id, leader->server->id);
        relationship_ctx.next_leader = NULL;
        return EBUSY;
    }

    result = cluster_relationship_set_leader(leader);
    relationship_ctx.next_leader = NULL;
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
        if (server_status.up_time - server_status.last_shutdown_time > 3600 &&
                g_current_time - server_status.up_time < 300)
        {
            sprintf(prompt, "the candidate leader server id: %d, "
                    "does not match the selection rule because it's "
                    "restart interval: %d exceeds 3600, "
                    "you must start ALL servers in the first time, "
                    "or remove the deprecated server(s) from the config file. ",
                    server_status.cs->server->id, (int)(server_status.up_time -
                        server_status.last_shutdown_time));
        } else {
            *prompt = '\0';
            if (i == 5) {
                break;
            }
        }

        logWarning("file: "__FILE__", line: %d, "
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
			"I am the new leader, id: %d, ip %s:%u",
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
                "ip %s:%u notify ...", __LINE__, next_leader->server->id,
                CLUSTER_GROUP_ADDRESS_FIRST_IP(next_leader->server),
                CLUSTER_GROUP_ADDRESS_FIRST_PORT(next_leader->server));
			return ENOENT;
		}
	}

	return 0;
}

static void cluster_relationship_on_status_change(FSClusterDataServerInfo *ds,
        const int old_status, const int new_status)
{
    FSClusterDataServerInfo *master;

    master = (FSClusterDataServerInfo *)
        __sync_add_and_fetch(&ds->dg->master, 0);
    if (master == NULL) {
        return;
    }

    if (ds->cs == CLUSTER_MYSELF_PTR) {  //myself
        if ((new_status == FS_SERVER_STATUS_INIT ||
                    new_status == FS_SERVER_STATUS_OFFLINE) &&
                (master->cs != CLUSTER_MYSELF_PTR))
        {
            recovery_thread_push_to_queue(ds);
        }
    } else {
        if (master->cs == CLUSTER_MYSELF_PTR) {  //i am master
            if ((old_status == FS_SERVER_STATUS_ONLINE) ||
                    (old_status == FS_SERVER_STATUS_OFFLINE &&
                     new_status == FS_SERVER_STATUS_ACTIVE))
            {
                PTHREAD_MUTEX_LOCK(&ds->replica.notify.lock);
                pthread_cond_broadcast(&ds->replica.notify.cond);
                PTHREAD_MUTEX_UNLOCK(&ds->replica.notify.lock);
            }
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
        const int status, const uint64_t data_version)
{
    int flags;

    if (cluster_relationship_set_ds_status(ds, status)) {
        flags = FS_EVENT_TYPE_STATUS_CHANGE;
    } else {
        flags = 0;
    }
    if (ds->data.version != data_version) {
        ds->data.version = data_version;
        flags |= FS_EVENT_TYPE_DV_CHANGE;
    }

    return flags;
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
            ds->last_report_version = -1;
            __sync_bool_compare_and_swap(&IMMEDIATE_REPORT, 0, 1);
        }
    }

    return result;
}

bool cluster_relationship_swap_report_ds_status(FSClusterDataServerInfo *ds,
        const int old_status, const int new_status, const int source)
{
    bool changed;
    changed = cluster_relationship_set_ds_status_ex(ds, old_status, new_status);
    if (!changed) {
        return changed;
    }

    cluster_relationship_report_ds_status(ds,
            old_status, new_status, source);
    return changed;
}

int cluster_relationship_on_master_change(FSClusterDataServerInfo *old_master,
        FSClusterDataServerInfo *new_master)
{
    FSClusterDataGroupInfo *group;
    FSClusterDataServerInfo *ds;
    int old_status;
    int new_status;

    if (old_master != NULL) {
        group = old_master->dg;
    } else if (new_master != NULL) {
        group = new_master->dg;
    } else {
        return EINVAL;
    }

    if (new_master == old_master) {
        return 0;
    }

    if (group->myself == NULL) {
        return 0;
    }

    if (group->myself == new_master) {
        old_status = __sync_add_and_fetch(&group->myself->status, 0);
        new_status = FS_SERVER_STATUS_ACTIVE;
        ds = group->myself;
    } else {
        old_status = __sync_add_and_fetch(&group->myself->status, 0);
        new_status = FS_SERVER_STATUS_OFFLINE;
        if (group->myself == old_master) {
            ds = old_status == FS_SERVER_STATUS_ACTIVE ? group->myself : NULL;
        } else {
            ds = NULL;
        }
    }

    if (ds != NULL) {
        cluster_relationship_swap_report_ds_status(ds, old_status,
                new_status, FS_EVENT_SOURCE_SELF_REPORT);
    }

    if (new_master != NULL && group->myself != new_master) {
        int my_status;
        my_status = __sync_add_and_fetch(&group->myself->status, 0);
        if (my_status == FS_SERVER_STATUS_INIT ||
                my_status == FS_SERVER_STATUS_OFFLINE)
        {
            recovery_thread_push_to_queue(group->myself);
        }
    }

    return 0;
}

static void cluster_process_push_entry(FSClusterDataServerInfo *ds,
        const FSProtoPushDataServerStatusBodyPart *body_part)
{
    FSClusterDataServerInfo *old_master;
    FSClusterDataServerInfo *new_master;
    int old_status;
    int is_master;

    is_master = __sync_add_and_fetch(&ds->is_master, 0);
    if (ds->cs == CLUSTER_MYSELF_PTR) {  //myself
        old_status = __sync_add_and_fetch(&ds->status, 0);
        if ((body_part->status == FS_SERVER_STATUS_OFFLINE) &&
                (!is_master) && (old_status == FS_SERVER_STATUS_ACTIVE))
        {
            cluster_relationship_set_ds_status_ex(ds, old_status,
                    FS_SERVER_STATUS_OFFLINE);
        }
    } else {
        cluster_relationship_set_ds_status(ds, body_part->status);
        ds->data.version = buff2long(body_part->data_version);
    }

    if (is_master == body_part->is_master) { //master NOT changed
        return;
    }

    old_master = (FSClusterDataServerInfo *)
        __sync_add_and_fetch(&ds->dg->master, 0);
    __sync_bool_compare_and_swap(&ds->is_master,
            is_master, body_part->is_master);
    if (__sync_add_and_fetch(&ds->is_master, 0)) {
        new_master = ds;
        if (new_master != old_master) {
            if (old_master != NULL) {
                __sync_bool_compare_and_swap(&old_master->
                        is_master, true, false);
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

    cluster_relationship_on_master_change(old_master, new_master);
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

static int proto_ping_leader_ex(ConnectionInfo *conn,
        const unsigned char cmd, const bool report_all)
{
    FSProtoHeader *header;
    SFResponseInfo response;
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
        logDebug("file: "__FILE__", line: %d, "
                "ping leader %s:%u, report_all: %d, "
                "report data_group_count: %d", __LINE__,
                conn->ip_addr, conn->port, report_all, data_group_count);
    }

    int2buff(data_group_count, req_header->data_group_count);
    SF_PROTO_SET_HEADER(header, cmd, out_bytes - sizeof(FSProtoHeader));

    response.error.length = 0;
    if ((result=tcpsenddata_nb(conn->sock, out_buff, out_bytes,
                    SF_G_NETWORK_TIMEOUT)) == 0)
    {
        result = cluster_recv_from_leader(conn, &response,
                1000 * SF_G_NETWORK_TIMEOUT, false);
    }

    if (result != 0) {
        sf_log_network_error(&response, conn, result);
    }

    return result;
}

#define proto_activate_server(conn) \
    proto_ping_leader_ex(conn, FS_CLUSTER_PROTO_ACTIVATE_SERVER, true)

#define proto_ping_leader(conn) \
    proto_ping_leader_ex(conn, FS_CLUSTER_PROTO_PING_LEADER_REQ, false)


static int cluster_try_recv_push_data(ConnectionInfo *conn)
{
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
            sf_log_network_error(&response, conn, result);
            return result;
        }

        if (__sync_bool_compare_and_swap(&IMMEDIATE_REPORT, 1, 0)) {
            if ((result=proto_ping_leader(conn)) != 0) {
                return result;
            }
        }
    } while (start_time == g_current_time);

    return 0;
}

static int cluster_ping_leader(ConnectionInfo *conn)
{
    int result;
    int inactive_count;
    FSClusterServerInfo *leader;

    if (CLUSTER_MYSELF_PTR == CLUSTER_LEADER_ATOM_PTR) {
        sleep(1);
        PTHREAD_MUTEX_LOCK(&INACTIVE_SERVER_ARRAY.lock);
        inactive_count = INACTIVE_SERVER_ARRAY.count;
        PTHREAD_MUTEX_UNLOCK(&INACTIVE_SERVER_ARRAY.lock);
        if (inactive_count > 0) {
            if ((result=cluster_check_brainsplit(inactive_count)) != 0) {
                return result;
            }
        }

        leader_deal_data_version_changes();
        cluster_topology_check_and_make_delay_decisions();
        return 0;  //do not need ping myself
    }

    leader = CLUSTER_LEADER_ATOM_PTR;
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

        if ((result=proto_activate_server(conn)) != 0) {
            conn_pool_disconnect_server(conn);
            return result;
        }
    }

    if ((result=cluster_try_recv_push_data(conn)) != 0) {
        conn_pool_disconnect_server(conn);
        return result;
    }

    if ((result=proto_ping_leader(conn)) != 0) {
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

    memset(&mconn, 0, sizeof(mconn));
    mconn.sock = -1;

    fail_count = 0;
    sleep_seconds = 1;
    while (SF_G_CONTINUE_FLAG) {
        leader = CLUSTER_LEADER_ATOM_PTR;
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
                        "%dth ping leader id: %d, ip %s:%u fail",
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
    FSClusterDataServerInfo *ds;
    FSClusterDataServerInfo *end;
    int bytes;
    int data_group_id;
    int i;

    id_array = fs_cluster_cfg_get_my_data_group_ids(&CLUSTER_CONFIG_CTX,
            CLUSTER_MYSELF_PTR->server->id);

    bytes = sizeof(FSMyDataGroupInfo) * id_array->count;
    MY_DATA_GROUP_ARRAY.groups = (FSMyDataGroupInfo *)fc_malloc(bytes);
    if (MY_DATA_GROUP_ARRAY.groups == NULL) {
        return ENOMEM;
    }

    for (i=0; i<id_array->count; i++) {
        data_group_id = id_array->ids[i];
        data_group = CLUSTER_DATA_RGOUP_ARRAY.groups + (data_group_id - 1);
        end = data_group->data_server_array.servers +
            data_group->data_server_array.count;
        for (ds=data_group->data_server_array.servers; ds<end; ds++) {
            if (ds->cs == CLUSTER_MYSELF_PTR) {
                MY_DATA_GROUP_ARRAY.groups[i].data_group_id = data_group_id;
                MY_DATA_GROUP_ARRAY.groups[i].ds = ds;
                break;
            }
        }
        if (ds == end) {
            logError("file: "__FILE__", line: %d, "
                    "data group id: %d, NOT found me, my server id: %d",
                    __LINE__, data_group_id, CLUSTER_MYSELF_PTR->server->id);
            return ENOENT;
        }
    }
    MY_DATA_GROUP_ARRAY.count = id_array->count;

    return 0;
}

int cluster_relationship_init()
{
	pthread_t tid;
    int result;
    int bytes;

    bytes = sizeof(FSClusterServerDetectEntry) * CLUSTER_SERVER_ARRAY.count;
    INACTIVE_SERVER_ARRAY.entries = (FSClusterServerDetectEntry *)
        fc_malloc(bytes);
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
