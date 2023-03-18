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
#include "../server_global.h"
#include "../binlog/replica_binlog.h"
#include "committed_version.h"

#define COMMITTED_VERSION_RING_SIZE   (16 * 1024)

static void deal_entry(FSVersionEntry *entry, const int64_t sn)
{
    int data_group_id;
    int64_t data_version;
    int64_t last_dv;
    SFBinlogWriterInfo *replica_writer;

    data_group_id = FC_ATOMIC_GET(entry->data_group_id);
    data_version = FC_ATOMIC_GET(entry->data_version);
    __sync_bool_compare_and_swap(&entry->data_group_id, data_group_id, 0);
    __sync_bool_compare_and_swap(&entry->data_version, data_version, 0);
    __sync_bool_compare_and_swap(&entry->sn, sn, 0);
    if (data_group_id > 0 && data_version > 0) {
        replica_writer = replica_binlog_get_writer(data_group_id);
        while (1) {
            last_dv = sf_binlog_writer_get_last_version_ex(
                    replica_writer, LOG_NOTHING);
            if (last_dv >= data_version || last_dv < 0) {
                break;
            }
            fc_sleep_ms(1);
        }
    }
}

static void *committed_version_thread_run(void *arg)
{
    int64_t old_sn;
    int64_t next_sn;
    int count;
    FSVersionEntry *entry;
    FSVersionEntry *end;

    next_sn = FC_ATOMIC_GET(COMMITTED_VERSION_RING.next_sn);
    entry = COMMITTED_VERSION_RING.versions +
        next_sn % COMMITTED_VERSION_RING_SIZE;
    end = COMMITTED_VERSION_RING.versions +
        COMMITTED_VERSION_RING_SIZE;
    while (SF_G_CONTINUE_FLAG) {
        if (FC_ATOMIC_GET(COMMITTED_VERSION_RING.count) == 0) {
            fc_sleep_ms(10);
            continue;
        }

        old_sn = next_sn;
        while (FC_ATOMIC_GET(entry->sn) == next_sn) {
            deal_entry(entry, next_sn);
            next_sn = FC_ATOMIC_INC(COMMITTED_VERSION_RING.next_sn);
            if (++entry == end) {
                entry = COMMITTED_VERSION_RING.versions;
            }
        }

        count = next_sn - old_sn;
        if (count > 0) {
            FC_ATOMIC_DEC_EX(COMMITTED_VERSION_RING.count, count);
            if (FC_ATOMIC_GET(COMMITTED_VERSION_RING.waitings) > 0) {
                pthread_cond_broadcast(&COMMITTED_VERSION_RING.lcp.cond);
            }
        } else {
            fc_sleep_ms(1);
        }
    }

    return NULL;
}

int committed_version_init(const int64_t sn)
{
    int result;
    int bytes;
    pthread_t tid;

    if ((result=init_pthread_lock_cond_pair(
                    &COMMITTED_VERSION_RING.lcp)) != 0)
    {
        return result;
    }

    bytes = sizeof(FSVersionEntry) * COMMITTED_VERSION_RING_SIZE;
    COMMITTED_VERSION_RING.versions = fc_malloc(bytes);
    if (COMMITTED_VERSION_RING.versions == NULL) {
        return ENOMEM;
    }
    memset(COMMITTED_VERSION_RING.versions, 0, bytes);
    COMMITTED_VERSION_RING.next_sn = sn + 1;
    COMMITTED_VERSION_RING.count = 0;
    COMMITTED_VERSION_RING.waitings = 0;

    return fc_create_thread(&tid, committed_version_thread_run,
            NULL, SF_G_THREAD_STACK_SIZE);
}

int committed_version_add(const int data_group_id,
        const int64_t data_version, const int64_t sn)
{
    int64_t next_sn;
    FSVersionEntry *entry;

    do {
        next_sn = FC_ATOMIC_GET(COMMITTED_VERSION_RING.next_sn);
        if (sn < next_sn) {
            logError("file: "__FILE__", line: %d, "
                    "invalid sn: %"PRId64", which < next sn: %"PRId64,
                    __LINE__, sn, next_sn);
            return EINVAL;
        }

        if (sn - next_sn < COMMITTED_VERSION_RING_SIZE) {
            break;
        }

        FC_ATOMIC_INC(COMMITTED_VERSION_RING.waitings);
        PTHREAD_MUTEX_LOCK(&COMMITTED_VERSION_RING.lcp.lock);
        pthread_cond_wait(&COMMITTED_VERSION_RING.lcp.cond,
                &COMMITTED_VERSION_RING.lcp.lock);
        PTHREAD_MUTEX_UNLOCK(&COMMITTED_VERSION_RING.lcp.lock);
        FC_ATOMIC_DEC(COMMITTED_VERSION_RING.waitings);
    } while (1);

    entry = COMMITTED_VERSION_RING.versions +
        sn % COMMITTED_VERSION_RING_SIZE;
    __sync_bool_compare_and_swap(&entry->data_group_id, 0, data_group_id);
    __sync_bool_compare_and_swap(&entry->data_version, 0, data_version);
    __sync_bool_compare_and_swap(&entry->sn, 0, sn);
    FC_ATOMIC_INC(COMMITTED_VERSION_RING.count);
    return 0;
}
