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
#include "fastcommon/fc_queue.h"
#include "sf/sf_func.h"
#include "sf/sf_nio.h"
#include "../binlog/replica_binlog.h"
#include "../binlog/binlog_reader.h"
#include "../binlog/binlog_rollback.h"
#include "../recovery/recovery_thread.h"
#include "../server_global.h"
#include "../server_group_info.h"
#include "replication_quorum.h"

#define VERSION_CONFIRMED_FILE_COUNT  2  //double files for safty

typedef struct fs_repl_quorum_thread_thread {
    struct fc_queue queue;
    int64_t *data_versions;
} FSReplicationQuorumThread;

static FSReplicationQuorumThread fs_repl_quorum_thread;

#define THREAD_DATA_VERSIONS fs_repl_quorum_thread.data_versions

static inline const char *get_confirmed_filename(const int data_group_id,
        const int index, char *filename, const int size)
{
    char subdir_name[64];

    replica_binlog_get_subdir_name(subdir_name, data_group_id);
    snprintf(filename, size, "%s/%s/version-confirmed.%d",
            DATA_PATH_STR, subdir_name, index);
    return filename;
}

static int get_confirmed_version_from_file(const int data_group_id,
        const int index, int64_t *confirmed_version)
{
    char filename[PATH_MAX];
    char buff[32];
    char *endptr;
    int64_t file_size;
    struct {
        int value;
        int calc;
    } crc32;
    int result;

    get_confirmed_filename(data_group_id, index, filename, sizeof(filename));
    if (access(filename, F_OK) != 0) {
        result = errno != 0 ? errno : EPERM;
        if (result == ENOENT) {
            *confirmed_version = 0;
            return 0;
        }
        logError("file: "__FILE__", line: %d, "
                "access file %s fail, errno: %d, error info: %s",
                __LINE__, filename, result, STRERROR(result));
        return result;
    }

    file_size = sizeof(buff) - 1;
    if ((result=getFileContentEx(filename, buff, 0, &file_size)) != 0) {
        return result;
    }

    *confirmed_version = strtoll(buff, &endptr, 10);
    if (*endptr != ' ') {
        return EINVAL;
    }

    crc32.calc = CRC32(buff, endptr - buff);
    crc32.value = strtol(endptr + 1, NULL, 16);
    return (crc32.value == crc32.calc ? 0 : EINVAL);
}

static int load_confirmed_version(const int data_group_id,
        int64_t *confirmed_version)
{
    int result;
    int invalid_count;
    int i;
    int64_t current_version;

    invalid_count = 0;
    *confirmed_version = 0;
    for (i=0; i<VERSION_CONFIRMED_FILE_COUNT; i++) {
        result = get_confirmed_version_from_file(
                data_group_id, i, &current_version);
        if (result == 0) {
            if (current_version > *confirmed_version) {
                *confirmed_version = current_version;
            }
        } else if (result == EINVAL) {
            ++invalid_count;
        } else {
            return result;
        }
    }

    if (invalid_count == 0) {
        return 0;
    } else {
        if (*confirmed_version > 0) {  //one confirmed file is OK
            return 0;
        } else {
            logError("file: "__FILE__", line: %d, "
                    "all data version confirmed files "
                    "are invalid!", __LINE__);
            return EINVAL;
        }
    }
}

static int unlink_confirmed_files(const int data_group_id)
{
    int result;
    int index;
    char filename[PATH_MAX];

    for (index=0; index<VERSION_CONFIRMED_FILE_COUNT; index++) {
        get_confirmed_filename(data_group_id, index,
                filename, sizeof(filename));
        if ((result=fc_delete_file_ex(filename, "confirmed")) != 0) {
            return result;
        }
    }

    return 0;
}

static int rollback_binlogs(FSClusterDataServerInfo *myself,
        const uint64_t my_confirmed_version, const bool is_redo)
{
    int result;

    result = binlog_rollback(myself, my_confirmed_version, is_redo);
    if (result == 0) {
        result = unlink_confirmed_files(myself->dg->id);
    }
    return result;
}

static int init_context(FSReplicationQuorumContext *ctx,
        FSClusterDataServerInfo *myself)
{
    int result;
    int64_t confirmed_version;

    if ((result=fast_mblock_init_ex1(&ctx->list.allocator,
                    "repl_quorum_entry", sizeof(FSReplicationQuorumEntry),
                    4096, 0, NULL, NULL, true)) != 0)
    {
        return result;
    }

    if ((result=sf_synchronize_ctx_init(&ctx->sctx)) != 0) {
        return result;
    }

    if ((result=load_confirmed_version(myself->dg->id,
                    &confirmed_version)) != 0)
    {
        return result;
    }

    if (confirmed_version > 0) {
        FC_ATOMIC_SET(myself->data.confirmed_version, confirmed_version);
    }

    ctx->myself = myself;
    ctx->dealing = 0;
    ctx->confirmed.counter = 0;
    ctx->list.count = 0;
    ctx->list.head = ctx->list.tail = NULL;
    ctx->confirmed.slave_version_changed = 0;
    ctx->confirmed.set_version.flag = 0;
    ctx->confirmed.set_version.value = 0;
    ctx->sctx.finished = false;

    return 0;
}

static int init_all_contexts()
{
    FSIdArray *id_array;
    FSClusterDataGroupInfo *group;
    int result;
    int data_group_id;
    int i;

    if ((id_array=fs_cluster_cfg_get_my_data_group_ids(&CLUSTER_CONFIG_CTX,
                    CLUSTER_MY_SERVER_ID)) == NULL)
    {
        logError("file: "__FILE__", line: %d, "
                "cluster config file no data group",
                __LINE__);
        return ENOENT;
    }

    for (i=0; i<id_array->count; i++) {
        data_group_id = id_array->ids[i];
        group = CLUSTER_DATA_RGOUP_ARRAY.groups + (data_group_id -
                CLUSTER_DATA_RGOUP_ARRAY.base_id);
        if ((result=init_context(&group->repl_quorum_ctx,
                        group->myself)) != 0)
        {
            return result;
        }

        /*
        logInfo("file: "__FILE__", line: %d, func: %s, "
                "%d. data_group_id = %d", __LINE__, __FUNCTION__,
                i + 1, data_group_id);
                */
    }

    return 0;
}

static int rollback_binlog_and_notify(FSReplicationQuorumContext *ctx)
{
    int result;
    int count;
    FSReplicationQuorumEntry *entry;

    result = 0;
    count = 0;
    PTHREAD_MUTEX_LOCK(&ctx->sctx.lcp.lock);
    while (ctx->list.head != NULL) {
        if ((result=rollback_binlogs(ctx->myself, FC_ATOMIC_GET(ctx->myself->
                            data.confirmed_version), false)) != 0)
        {
            if (SF_G_CONTINUE_FLAG) {
                logCrit("file: "__FILE__", line: %d, "
                        "rollback binlog fail, error code: %d, "
                        "program exit!", __LINE__, result);
                sf_terminate_myself();
            }
            break;
        }

        while (ctx->list.head != NULL) {
            entry = ctx->list.head;
            ctx->list.head = ctx->list.head->next;

            sf_synchronize_finished_notify_no_lock(&ctx->sctx, EAGAIN);
            fast_mblock_free_object(&ctx->list.allocator, entry);
            ++count;
        }

        ctx->list.tail = NULL;
    }
    PTHREAD_MUTEX_UNLOCK(&ctx->sctx.lcp.lock);

    if (count > 0) {
        FC_ATOMIC_DEC_EX(ctx->list.count, count);
    }
    return result;
}

int replication_quorum_add(FSReplicationQuorumContext *ctx,
        struct fast_task_info *task, const int64_t data_version,
        bool *finished)
{
    FSReplicationQuorumEntry *previous;
    FSReplicationQuorumEntry *entry;
    int result;

    if (data_version <= FC_ATOMIC_GET(ctx->myself->data.confirmed_version)) {
        *finished = true;
        return 0;
    }

    *finished = false;
    entry = fast_mblock_alloc_object(&ctx->list.allocator);
    if (entry == NULL) {
        return ENOMEM;
    }

    FC_ATOMIC_INC(ctx->list.count);
    entry->task = task;
    entry->data_version = data_version;
    PTHREAD_MUTEX_LOCK(&ctx->sctx.lcp.lock);
    do {
        if (!FC_ATOMIC_GET(ctx->myself->dg->is_my_term)) {
            FC_ATOMIC_DEC(ctx->list.count);
            fast_mblock_free_object(&ctx->list.allocator, entry);
            result = EAGAIN;
            break;
        }

        if (ctx->list.head == NULL) {
            entry->next = NULL;
            ctx->list.head = entry;
            ctx->list.tail = entry;
        } else if (data_version >= ctx->list.tail->data_version) {
            entry->next = NULL;
            ctx->list.tail->next = entry;
            ctx->list.tail = entry;
        } else if (data_version <= ctx->list.head->data_version) {
            entry->next = ctx->list.head;
            ctx->list.head = entry;
        } else {
            previous = ctx->list.head;
            while (data_version > previous->next->data_version) {
                previous = previous->next;
            }
            entry->next = previous->next;
            previous->next = entry;
        }

        result = 0;
    } while (0);
    PTHREAD_MUTEX_UNLOCK(&ctx->sctx.lcp.lock);

    return result;
}

static int compare_int64(const int64_t *n1, const int64_t *n2)
{
    return fc_compare_int64(*n1, *n2);
}

static int write_to_confirmed_file(const int data_group_id,
        const int index, const int64_t confirmed_version)
{
    const int flags = O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC;
    FilenameFDPair pair;
    char buff[32];
    int crc32;
    int len;
    int result;

    len = sprintf(buff, "%"PRId64, confirmed_version);
    crc32 = CRC32(buff, len);
    len += sprintf(buff + len, " %08x", crc32);

    get_confirmed_filename(data_group_id, index,
            pair.filename, sizeof(pair.filename));
    if ((pair.fd=open(pair.filename, flags, 0644)) < 0) {
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "open file %s fail, errno: %d, error info: %s",
                __LINE__, pair.filename, result, STRERROR(result));
        return result;
    }

    if (fc_safe_write(pair.fd, buff, len) != len) {
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "write file %s fail, errno: %d, error info: %s",
                __LINE__, pair.filename, result, STRERROR(result));
        close(pair.fd);
        return result;
    }

    close(pair.fd);
    return 0;
}

static void notify_waiting_tasks(FSReplicationQuorumContext *ctx,
        const int64_t my_confirmed_version)
{
    struct fast_mblock_chain chain;
    struct fast_mblock_node *node;
    int count;

    count = 0;
    chain.head = chain.tail = NULL;
    PTHREAD_MUTEX_LOCK(&ctx->sctx.lcp.lock);
    if (ctx->list.head != NULL && my_confirmed_version >=
            ctx->list.head->data_version)
    {
        do {
            node = fast_mblock_to_node_ptr(ctx->list.head);
            if (chain.head == NULL) {
                chain.head = node;
            } else {
                chain.tail->next = node;
            }
            chain.tail = node;

            sf_synchronize_finished_notify_no_lock(&ctx->sctx, 0);
            ctx->list.head = ctx->list.head->next;
            ++count;
        } while (ctx->list.head != NULL && my_confirmed_version >=
                ctx->list.head->data_version);

        if (ctx->list.head == NULL) {
            ctx->list.tail = NULL;
        }
        chain.tail->next = NULL;
    }

    if (chain.head != NULL) {
        fast_mblock_batch_free(&ctx->list.allocator, &chain);
    }
    PTHREAD_MUTEX_UNLOCK(&ctx->sctx.lcp.lock);

    FC_ATOMIC_DEC_EX(ctx->list.count, count);
}

void replication_quorum_deal_version_change(
        FSReplicationQuorumContext *ctx,
        const int64_t slave_confirmed_version)
{
    if (slave_confirmed_version <= FC_ATOMIC_GET(ctx->
                myself->data.confirmed_version))
    {
        return;
    }

    __sync_bool_compare_and_swap(&ctx->confirmed.
            slave_version_changed, 0, 1);
    if (__sync_bool_compare_and_swap(&ctx->dealing, 0, 1)) {
        fc_queue_push(&fs_repl_quorum_thread.queue, ctx);
    }
}

void replication_quorum_push_confirmed_version(
        FSReplicationQuorumContext *ctx,
        const int64_t data_version)
{
    if (data_version > FC_ATOMIC_GET(ctx->myself->data.confirmed_version)) {
        __sync_bool_compare_and_swap(&ctx->confirmed.
                set_version.value, ctx->confirmed.
                set_version.value, data_version);
        __sync_bool_compare_and_swap(&ctx->confirmed.
                set_version.flag, 0, 1);
        if (__sync_bool_compare_and_swap(&ctx->dealing, 0, 1)) {
            fc_queue_push(&fs_repl_quorum_thread.queue, ctx);
        }
    }
}

static void deal_version_change(FSReplicationQuorumContext *ctx,
        int64_t *my_confirmed_version)
{
    FSClusterDataServerInfo **ds;
    FSClusterDataServerInfo **end;
    int64_t my_current_version;
    int64_t confirmed_version;
    int more_than_half;
    int count;
    int index;

    count = 0;
    my_current_version = FC_ATOMIC_GET(ctx->myself->data.current_version);
    end = ctx->myself->dg->slave_ds_array.servers +
        ctx->myself->dg->slave_ds_array.count;
    for (ds=ctx->myself->dg->slave_ds_array.servers; ds<end; ds++) {
        confirmed_version=FC_ATOMIC_GET((*ds)->data.confirmed_version);
        if (confirmed_version > *my_confirmed_version &&
                confirmed_version <= my_current_version)
        {
            THREAD_DATA_VERSIONS[count++] = confirmed_version;
        }
    }
    more_than_half = CLUSTER_SERVER_ARRAY.count / 2 + 1;

    if (count + 1 >= more_than_half) {  //quorum majority
        if (CLUSTER_SERVER_ARRAY.count == 3) {  //fast path
            if (count == 2) {
                *my_confirmed_version = FC_MAX(THREAD_DATA_VERSIONS[0],
                        THREAD_DATA_VERSIONS[1]);
            } else {  //count == 1
                *my_confirmed_version = THREAD_DATA_VERSIONS[0];
            }
        } else if (CLUSTER_SERVER_ARRAY.count == 2) {  //fast path
            *my_confirmed_version = THREAD_DATA_VERSIONS[0];
        } else {
            qsort(THREAD_DATA_VERSIONS, count, sizeof(int64_t),
                    (int (*)(const void *, const void *))
                    compare_int64);
            index = (count + 1) - more_than_half;
            *my_confirmed_version = THREAD_DATA_VERSIONS[index];
        }

        FC_ATOMIC_SET(ctx->myself->data.confirmed_version,
                *my_confirmed_version);
    }
}

static void *replication_quorum_thread_run(void *arg)
{
    FSReplicationQuorumContext *ctx;
    int64_t old_confirmed_version;
    int64_t new_confirmed_version;
    bool set_version;
    bool by_slave;

    while (1) {
        if ((ctx=fc_queue_pop(&fs_repl_quorum_thread.queue)) != NULL) {
            __sync_bool_compare_and_swap(&ctx->dealing, 1, 0);
            old_confirmed_version = FC_ATOMIC_GET(ctx->
                    myself->data.confirmed_version);
            if ((set_version=__sync_bool_compare_and_swap(&ctx->confirmed.
                        set_version.flag, 1, 0)))
            {
                new_confirmed_version = FC_ATOMIC_GET(ctx->
                        confirmed.set_version.value);
                if (new_confirmed_version > old_confirmed_version) {
                    __sync_bool_compare_and_swap(&ctx->myself->data.
                            confirmed_version, old_confirmed_version,
                            new_confirmed_version);
                } else if (new_confirmed_version < old_confirmed_version) {
                    new_confirmed_version = old_confirmed_version;
                }
            } else {
                new_confirmed_version = old_confirmed_version;
            }

            if ((by_slave=__sync_bool_compare_and_swap(&ctx->confirmed.
                        slave_version_changed, 1, 0)))
            {
                if (FC_ATOMIC_GET(ctx->myself->is_master)) {
                    deal_version_change(ctx, &new_confirmed_version);
                }
            }

            /*
            logInfo("data group id: %d, set_version: %d, by_slave: %d, "
                    "confirmed_versions {old: %"PRId64", new: %"PRId64"}, waiting count: %d",
                    ctx->myself->dg->id, set_version, by_slave,
                    old_confirmed_version, new_confirmed_version, FC_ATOMIC_GET(ctx->list.count));
                    */

            if (new_confirmed_version == old_confirmed_version) {
                continue;
            }

            if (write_to_confirmed_file(ctx->myself->dg->id, FC_ATOMIC_INC(ctx->
                            confirmed.counter) % VERSION_CONFIRMED_FILE_COUNT,
                        new_confirmed_version) != 0)
            {
                sf_terminate_myself();
                break;
            }

            if (FC_ATOMIC_GET(ctx->list.count) > 0) {
                notify_waiting_tasks(ctx, new_confirmed_version);
            }
        }
    }

    return NULL;
}

int replication_quorum_start_master_term(FSReplicationQuorumContext *ctx)
{
    return 0;
}

int replication_quorum_end_master_term(FSReplicationQuorumContext *ctx)
{
    int64_t my_confirmed_version;
    int64_t current_data_version;

    if (!REPLICA_QUORUM_NEED_MAJORITY) {
        return 0;
    }

    my_confirmed_version = FC_ATOMIC_GET(ctx->myself->data.confirmed_version);
    current_data_version = FC_ATOMIC_GET(ctx->myself->data.current_version);
    if (my_confirmed_version >= current_data_version) {
        return unlink_confirmed_files(ctx->myself->dg->id);
    }

    return rollback_binlog_and_notify(ctx);
}

int replication_quorum_init()
{
    int result;

    if (!REPLICA_QUORUM_NEED_MAJORITY) {
        REPLICA_QUORUM_ROLLBACK_DONE = true;
        return 0;
    }

    if ((result=init_all_contexts()) != 0) {
        return result;
    }

    THREAD_DATA_VERSIONS = fc_malloc(sizeof(int64_t) *
            CLUSTER_SERVER_ARRAY.count);
    if (THREAD_DATA_VERSIONS == NULL) {
        return ENOMEM;
    }

    if ((result=fc_queue_init(&fs_repl_quorum_thread.queue, (long)
                    &((FSReplicationQuorumContext *)NULL)->next)) != 0)
    {
        return result;
    }

    return 0;
}

static int check_rollback_data_groups()
{
    FSIdArray *id_array;
    FSClusterDataGroupInfo *group;
    int64_t confirmed_version;
    int data_group_id;
    int i;
    int result;

    if ((id_array=fs_cluster_cfg_get_my_data_group_ids(&CLUSTER_CONFIG_CTX,
                    CLUSTER_MY_SERVER_ID)) == NULL)
    {
        logError("file: "__FILE__", line: %d, "
                "cluster config file no data group",
                __LINE__);
        return ENOENT;
    }

    for (i=0; i<id_array->count; i++) {
        data_group_id = id_array->ids[i];
        group = CLUSTER_DATA_RGOUP_ARRAY.groups + (data_group_id -
                CLUSTER_DATA_RGOUP_ARRAY.base_id);
        confirmed_version = FC_ATOMIC_GET(group->
                myself->data.confirmed_version);
        if (confirmed_version < FC_ATOMIC_GET(group->
                    myself->data.current_version))
        {
            if ((result=rollback_binlogs(group->myself,
                            confirmed_version, true)) != 0)
            {
                return result;
            }
        }
    }

    return 0;
}

static int push_all_to_recovery_thread_queue()
{
    FSIdArray *id_array;
    FSClusterDataGroupInfo *group;
    FSClusterDataServerInfo *master;
    int data_group_id;
    int i;

    if ((id_array=fs_cluster_cfg_get_my_data_group_ids(&CLUSTER_CONFIG_CTX,
                    CLUSTER_MY_SERVER_ID)) == NULL)
    {
        logError("file: "__FILE__", line: %d, "
                "cluster config file no data group",
                __LINE__);
        return ENOENT;
    }

    for (i=0; i<id_array->count; i++) {
        data_group_id = id_array->ids[i];
        group = CLUSTER_DATA_RGOUP_ARRAY.groups + (data_group_id -
                CLUSTER_DATA_RGOUP_ARRAY.base_id);
        master = (FSClusterDataServerInfo *)FC_ATOMIC_GET(group->master);
        if (master != NULL && group->myself != master) {
            recovery_thread_push_to_queue(group->myself);
        }
    }

    return 0;
}

int replication_quorum_start()
{
    pthread_t tid;
    int result;

    if (!REPLICA_QUORUM_NEED_MAJORITY) {
        return 0;
    }

    if ((result=check_rollback_data_groups()) != 0) {
        return result;
    }

    REPLICA_QUORUM_ROLLBACK_DONE = true;
    push_all_to_recovery_thread_queue();

    return fc_create_thread(&tid, replication_quorum_thread_run,
            NULL, SF_G_THREAD_STACK_SIZE);
}

void replication_quorum_destroy()
{
}
