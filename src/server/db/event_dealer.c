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


#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/pthread_func.h"
#include "sf/sf_func.h"
#include "../server_global.h"
#include "block_serializer.h"
#include "db_updater.h"
#include "event_dealer.h"

#define BUFFER_BATCH_FREE_COUNT  1024

typedef struct fs_event_dealer_thread_context {
    BlockSerializerPacker packer;
    FSDBFetchContext db_fetch_ctx;
    FSChangeNotifyEventPtrArray event_ptr_array;
    int thread_index;
    pthread_lock_cond_pair_t lcp;
    OBSegment *segment;
    OBEntry *ob;
    int event_count;
    int old_slice_count;
    struct {
        int64_t ob_inc;
        int64_t slice_inc;
        int64_t slice_count;
    } stats;
} FSEventDealerThreadContext;

typedef struct fs_event_dealer_thread_array {
    FSEventDealerThreadContext *threads;
    FSEventDealerThreadContext *end;
    int count;
} FSEventDealerThreadArray;

typedef struct fs_event_dealer_context {
    FSEventDealerThreadArray thread_array;
    FSDBUpdaterContext updater_ctx;
    SFSynchronizeContext sctx;
    struct {
        FastBuffer *buffers[BUFFER_BATCH_FREE_COUNT];
        int count;
    } buffer_ptr_array;
} FSEventDealerContext;

static FSEventDealerContext event_dealer_ctx;

#define MERGED_BLOCK_ARRAY  event_dealer_ctx.updater_ctx.array
#define BUFFER_PTR_ARRAY    event_dealer_ctx.buffer_ptr_array

static int deal_events(FSEventDealerThreadContext *thread);

static void *event_dealer_thread_run(FSEventDealerThreadContext *thread)
{
#ifdef OS_LINUX
    char thread_name[16];
    snprintf(thread_name, sizeof(thread_name), "ev-dealer[%d]",
            thread->thread_index);
    prctl(PR_SET_NAME, thread_name);
#endif

    while (SF_G_CONTINUE_FLAG) {
        PTHREAD_MUTEX_LOCK(&thread->lcp.lock);
        pthread_cond_wait(&thread->lcp.cond, &thread->lcp.lock);
        PTHREAD_MUTEX_UNLOCK(&thread->lcp.lock);

        if (thread->event_ptr_array.count > 0) {
            deal_events(thread);
            sf_synchronize_counter_notify(&event_dealer_ctx.sctx, 1);
        }
    }

    return NULL;
}

int event_dealer_init()
{
    const int init_alloc = 1024;
    int result;
    int bytes;
    pthread_t tid;
    FSEventDealerThreadContext *thread;
    FSEventDealerThreadContext *end;

    if ((result=sf_synchronize_ctx_init(&event_dealer_ctx.sctx)) != 0) {
        return result;
    }

    bytes = sizeof(FSEventDealerThreadContext) * EVENT_DEALER_THREAD_COUNT;
    event_dealer_ctx.thread_array.threads = fc_malloc(bytes);
    if (event_dealer_ctx.thread_array.threads == NULL) {
        return ENOMEM;
    }
    memset(event_dealer_ctx.thread_array.threads, 0, bytes);

    end = event_dealer_ctx.thread_array.threads + EVENT_DEALER_THREAD_COUNT;
    for (thread=event_dealer_ctx.thread_array.threads; thread<end; thread++) {
        thread->thread_index = thread - event_dealer_ctx.thread_array.threads;
        if ((result=block_serializer_init_packer(&thread->
                        packer, init_alloc)) != 0)
        {
            return result;
        }

        if ((result=da_init_read_context(&thread->
                        db_fetch_ctx.read_ctx)) != 0)
        {
            return result;
        }
        sf_serializer_iterator_init(&thread->db_fetch_ctx.it);

        if ((result=init_pthread_lock_cond_pair(&thread->lcp)) != 0) {
            return result;
        }
        if ((result=fc_create_thread(&tid, (void *(*)(void *))
                        event_dealer_thread_run, thread,
                        SF_G_THREAD_STACK_SIZE)) != 0)
        {
            return result;
        }
    }

    event_dealer_ctx.thread_array.count = EVENT_DEALER_THREAD_COUNT;
    event_dealer_ctx.thread_array.end = end;
    return db_updater_init(&event_dealer_ctx.updater_ctx);
}

int64_t event_dealer_get_last_data_version()
{
    return event_dealer_ctx.updater_ctx.last_versions.block.commit;
}

static int realloc_event_ptr_array(FSChangeNotifyEventPtrArray *array)
{
    FSChangeNotifyEvent **events;
    uint32_t bytes;

    if (array->alloc == 0) {
        array->alloc = 8 * 1024;
    } else {
        array->alloc *= 2;
    }

    events = (FSChangeNotifyEvent **)fc_malloc(
            sizeof(FSChangeNotifyEvent *) * array->alloc);
    if (events == NULL) {
        return ENOMEM;
    }

    if (array->events != NULL) {
        bytes = sizeof(FSChangeNotifyEvent *) * array->count;
        memcpy(events, array->events, bytes);
        free(array->events);
    }

    array->events = events;
    return 0;
}

static int compare_event_ptr_func(const FSChangeNotifyEvent **ev1,
        const FSChangeNotifyEvent **ev2)
{
    int sub;

    if ((sub=ob_index_compare_block_key(&(*ev1)->ob->bkey,
                    &(*ev2)->ob->bkey)) != 0)
    {
        return sub;
    }

    return fc_compare_int64((*ev1)->sn, (*ev2)->sn);
}

static inline void ob_entry_release(OBSegment *segment,
        OBEntry *ob, const int dec_count)
{
    PTHREAD_MUTEX_LOCK(&segment->lcp.lock);
    ob_index_ob_entry_release_ex(&G_OB_HASHTABLE, ob, dec_count);
    PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);
}

static int deal_ob_events(FSEventDealerThreadContext *thread)
{
    int result;
    int count;
    int alloc;
    int new_slice_count;
    bool empty;
    int64_t version;
    FSDBUpdateBlockInfo *block;
    FSDBUpdateBlockInfo *entries;

    empty = thread->ob->db_args->slices == NULL || uniq_skiplist_empty(
            thread->ob->db_args->slices);
    if (empty && thread->old_slice_count == 0) {
        ob_entry_release(thread->segment, thread->ob, thread->event_count);
        return 0;
    }

    version = FC_ATOMIC_INC(event_dealer_ctx.
                updater_ctx.last_versions.field);
    count = FC_ATOMIC_INC(MERGED_BLOCK_ARRAY.count) - 1;
    while (1) {
        while (1) {
            alloc = FC_ATOMIC_GET(MERGED_BLOCK_ARRAY.alloc);
            if (count < alloc) {
                break;
            } else if (count == alloc) {
                if ((result=db_updater_realloc_block_array(
                                &MERGED_BLOCK_ARRAY, count)) != 0)
                {
                    return result;
                }
            }
        }

        entries = (FSDBUpdateBlockInfo *)FC_ATOMIC_GET(
                MERGED_BLOCK_ARRAY.entries);
        block = entries + count;
        block->version = version;
        block->bkey = thread->ob->bkey;
        if (empty) {
            block->buffer = NULL;
        } else {
            if ((result=block_serializer_pack(&thread->packer,
                            thread->ob, &block->buffer)) != 0)
            {
                return result;
            }
        }

        if (FC_ATOMIC_GET(MERGED_BLOCK_ARRAY.entries) == entries) {
            break;
        }
        if (block->buffer != NULL) {
            fast_mblock_free_object(&g_serializer_ctx.
                    buffer_allocator, block->buffer);
        }
    }

    if (empty) {
        thread->stats.ob_inc--;
        thread->stats.slice_inc -= thread->old_slice_count;
    } else {
        new_slice_count = uniq_skiplist_count(thread->ob->db_args->slices);
        thread->stats.slice_count += new_slice_count;
        if (thread->old_slice_count == 0) {
            thread->stats.ob_inc++;
        }
        thread->stats.slice_inc += new_slice_count - thread->old_slice_count;
    }

    ob_entry_release(thread->segment, thread->ob, thread->event_count);
    return 0;
}

static int deal_events(FSEventDealerThreadContext *thread)
{
    int result = 0;
    int batch_free_count;
    struct fast_mblock_chain chain;
    FSChangeNotifyEvent **event;
    FSChangeNotifyEvent **end;
    struct fast_mblock_node *node;

    if (thread->event_ptr_array.count > 1) {
        qsort(thread->event_ptr_array.events, thread->event_ptr_array.count,
                sizeof(FSChangeNotifyEvent *), (int (*)(const void *,
                        const void *))compare_event_ptr_func);
    }

    thread->event_count = 0;
    batch_free_count = 0;
    chain.head = chain.tail = NULL;
    thread->ob = thread->event_ptr_array.events[0]->ob;
    thread->segment = ob_index_get_segment(&thread->ob->bkey);
    if (thread->ob->db_args->slices == NULL) {
        if ((result=ob_index_load_db_slices(&thread->db_fetch_ctx,
                        thread->segment, thread->ob)) != 0)
        {
            return result;
        }
    }
    thread->old_slice_count = uniq_skiplist_count(thread->ob->db_args->slices);
    end = thread->event_ptr_array.events + thread->event_ptr_array.count;
    for (event=thread->event_ptr_array.events; event<end; event++) {
        if (ob_index_compare_block_key(&(*event)->ob->bkey,
                    &thread->ob->bkey) == 0)
        {
            if ((*event)->ob == thread->ob) {
                ++thread->event_count;
            } else {
                ob_entry_release(thread->segment, (*event)->ob, 1);
            }
        } else {
            if ((result=deal_ob_events(thread)) != 0) {
                break;
            }

            thread->ob = (*event)->ob;
            thread->segment = ob_index_get_segment(&thread->ob->bkey);
            if (thread->ob->db_args->slices == NULL) {
                if ((result=ob_index_load_db_slices(&thread->db_fetch_ctx,
                                thread->segment, thread->ob)) != 0)
                {
                    break;
                }
            }
            thread->old_slice_count = uniq_skiplist_count(
                    thread->ob->db_args->slices);
            thread->event_count = 1;
        }

        if ((*event)->op_type == da_binlog_op_type_remove &&
                (*event)->entry_type == fs_change_entry_type_block)
        {
            if ((result=ob_index_delete_block_by_db(thread->segment,
                            thread->ob)) != 0)
            {
                break;
            }
        } else {
            if (thread->ob->db_args->slices == NULL) {
                if ((result=ob_index_alloc_db_slices(thread->segment,
                                thread->ob)) != 0)
                {
                    break;
                }
            }
            if ((*event)->op_type == da_binlog_op_type_remove) {
                if ((result=ob_index_delete_slice_by_db(thread->segment,
                                thread->ob, &(*event)->ssize)) != 0)
                {
                    break;
                }
            } else {
                if ((result=ob_index_add_slice_by_db(thread->segment,
                                thread->ob, (*event)->slice.data_version,
                                (*event)->slice.type, &(*event)->slice.ssize,
                                &(*event)->slice.space)) != 0)
                {
                    break;
                }
            }
        }

        node = fast_mblock_to_node_ptr(*event);
        if (chain.head == NULL) {
            chain.head = node;
        } else {
            chain.tail->next = node;
        }
        chain.tail = node;
        if (++batch_free_count == 1024) {
            chain.tail->next = NULL;
            fast_mblock_batch_free(&STORAGE_EVENT_ALLOCATOR, &chain);
            chain.head = chain.tail = NULL;
            batch_free_count = 0;
        }
    }

    if (result == 0) {
        result = deal_ob_events(thread);
    }
    if (result != 0) {
        return result;
    }

    if (chain.head != NULL) {
        chain.tail->next = NULL;
        fast_mblock_batch_free(&STORAGE_EVENT_ALLOCATOR, &chain);
    }

    return result;
}

int event_dealer_do(struct fc_list_head *head, int *count)
{
    int result;
    int thread_count;
    FSEventDealerThreadContext *thread;
    FSChangeNotifyEvent *event;
    FSChangeNotifyEvent *last;

#ifdef DEBUG_FLAG
    int64_t slice_count;
    int64_t start_time;
    int64_t end_time;
    double avg_slices;
    int sort_time;
    int ob_deal_time;
    int db_deal_time;
    int64_t ob_deal_end_time;

    start_time = get_current_time_us();
#endif

    *count = 0;
    fc_list_for_each_entry (event, head, dlink) {
        ++(*count);

        thread = event_dealer_ctx.thread_array.threads + FS_BLOCK_HASH_CODE(
                event->ob->bkey) % event_dealer_ctx.thread_array.count;
        if (thread->event_ptr_array.count >= thread->event_ptr_array.alloc) {
            if ((result=realloc_event_ptr_array(&thread->
                            event_ptr_array)) != 0)
            {
                return result;
            }
        }
        thread->event_ptr_array.events[thread->event_ptr_array.count++] = event;
    }

#ifdef DEBUG_FLAG
    end_time = get_current_time_us();
    sort_time = end_time - start_time;
    start_time = end_time;
#endif

    last = fc_list_entry(head->prev, FSChangeNotifyEvent, dlink);
    event_dealer_ctx.updater_ctx.last_versions.block.prepare = last->sn;

    FC_ATOMIC_SET(MERGED_BLOCK_ARRAY.count, 0);
    thread_count = 0;
    for (thread=event_dealer_ctx.thread_array.threads;
            thread<event_dealer_ctx.thread_array.end; thread++)
    {
        if (thread->event_ptr_array.count > 0) {
            thread->stats.ob_inc = 0;
            thread->stats.slice_inc = 0;
            pthread_cond_signal(&thread->lcp.cond);
            ++thread_count;
        }
    }
    sf_synchronize_counter_add(&event_dealer_ctx.sctx, thread_count);
    sf_synchronize_counter_wait(&event_dealer_ctx.sctx);

#ifdef DEBUG_FLAG
    ob_deal_end_time = get_current_time_us();
    slice_count = 0;
#endif

    for (thread=event_dealer_ctx.thread_array.threads;
            thread<event_dealer_ctx.thread_array.end; thread++)
    {
        if (thread->event_ptr_array.count > 0) {
#ifdef DEBUG_FLAG
            slice_count += thread->stats.slice_count;
#endif
            STORAGE_ENGINE_OB_COUNT += thread->stats.ob_inc;
            STORAGE_ENGINE_SLICE_COUNT += thread->stats.slice_inc;
            thread->event_ptr_array.count = 0;
            thread->stats.slice_count = 0;
        }
    }

    if (FC_ATOMIC_GET(MERGED_BLOCK_ARRAY.count) > 0) {
        if ((result=db_updater_deal(&event_dealer_ctx.updater_ctx)) == 0) {
            event_dealer_free_buffers(&MERGED_BLOCK_ARRAY);
        }
#ifdef DEBUG_FLAG
        avg_slices = (double)slice_count / MERGED_BLOCK_ARRAY.count;
#endif
    } else {
#ifdef DEBUG_FLAG
        avg_slices = 0.00;
#endif
    }
    event_dealer_ctx.updater_ctx.last_versions.block.commit =
        event_dealer_ctx.updater_ctx.last_versions.block.prepare;

#ifdef DEBUG_FLAG
    end_time = get_current_time_us();
    ob_deal_time = ob_deal_end_time - start_time;
    db_deal_time = end_time - ob_deal_end_time;
    logInfo("db event count: %d, merged ob count: %d, packed slice count: "
            "%"PRId64", avg slices per ob: %.2f, last sn: %"PRId64", "
            "sort time: %d ms, ob deal time: %d ms, db deal time: %d ms",
            *count, MERGED_BLOCK_ARRAY.count, slice_count, avg_slices,
            event_dealer_ctx.updater_ctx.last_versions.block.commit,
            sort_time / 1000, ob_deal_time / 1000, db_deal_time / 1000);
#endif

    return result;
}

void event_dealer_free_buffers(FSDBUpdateBlockArray *array)
{
    FSDBUpdateBlockInfo *entry;
    FSDBUpdateBlockInfo *end;

    end = (FSDBUpdateBlockInfo *)array->entries + array->count;
    for (entry=(FSDBUpdateBlockInfo *)array->entries; entry<end; entry++) {
        if (entry->buffer == NULL) {
            continue;
        }

        BUFFER_PTR_ARRAY.buffers[BUFFER_PTR_ARRAY.count++] = entry->buffer;
        if (BUFFER_PTR_ARRAY.count == BUFFER_BATCH_FREE_COUNT) {
            block_serializer_batch_free_buffer(
                    BUFFER_PTR_ARRAY.buffers,
                    BUFFER_PTR_ARRAY.count);
            BUFFER_PTR_ARRAY.count = 0;
        }
    }

    if (BUFFER_PTR_ARRAY.count > 0) {
        block_serializer_batch_free_buffer(
                BUFFER_PTR_ARRAY.buffers,
                BUFFER_PTR_ARRAY.count);
        BUFFER_PTR_ARRAY.count = 0;
    }
}
