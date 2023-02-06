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
#include "../server_global.h"
#include "block_serializer.h"
#include "db_updater.h"
#include "event_dealer.h"

#define BUFFER_BATCH_FREE_COUNT  1024

typedef struct fs_event_dealer_context {
    FSChangeNotifyEventPtrArray event_ptr_array;
    FSDBUpdaterContext updater_ctx;
    struct {
        FastBuffer *buffers[BUFFER_BATCH_FREE_COUNT];
        int count;
    } buffer_ptr_array;
} FSEventDealerContext;

static FSEventDealerContext event_dealer_ctx;

#define EVENT_PTR_ARRAY     event_dealer_ctx.event_ptr_array
#define MERGED_DENTRY_ARRAY event_dealer_ctx.updater_ctx.array
#define BUFFER_PTR_ARRAY    event_dealer_ctx.buffer_ptr_array

int event_dealer_init()
{
    int result;

    if ((result=fast_buffer_init_ex(&event_dealer_ctx.
                    updater_ctx.buffer, 1024)) != 0)
    {
        return result;
    }

    return db_updater_init(&event_dealer_ctx.updater_ctx);
}

int64_t event_dealer_get_last_data_version()
{
    return event_dealer_ctx.updater_ctx.last_versions.dentry.commit;
}

static int realloc_event_ptr_array(FSChangeNotifyEventPtrArray *array)
{
    FSChangeNotifyEvent **events;

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
        memcpy(events, array->events,
                sizeof(FSChangeNotifyEvent *) * array->count);
        free(array->events);
    }

    array->events = events;
    return 0;
}

static inline int add_to_event_ptr_array(FSChangeNotifyEvent *event)
{
    int result;

    if (EVENT_PTR_ARRAY.count >= EVENT_PTR_ARRAY.alloc) {
        if ((result=realloc_event_ptr_array(&EVENT_PTR_ARRAY)) != 0) {
            return result;
        }
    }

    EVENT_PTR_ARRAY.events[EVENT_PTR_ARRAY.count++] = event;
    return 0;
}

static int compare_event_ptr_func(const FSChangeNotifyEvent **ev1,
        const FSChangeNotifyEvent **ev2)
{
    int sub;

    if ((sub=fc_compare_int64((*ev1)->ob->bkey.oid,
                    (*ev2)->ob->bkey.oid)) != 0)
    {
        return sub;
    }
    if ((sub=fc_compare_int64((*ev1)->ob->bkey.offset,
                    (*ev2)->ob->bkey.offset)) != 0)
    {
        return sub;
    }

    return fc_compare_int64((*ev1)->sn, (*ev2)->sn);
}

static int deal_merged_entries()
{
    int result;
    //FSDBUpdateFieldInfo *entry;
    FSDBUpdateFieldInfo *end;

    result = db_updater_deal(&event_dealer_ctx.updater_ctx);
    end = MERGED_DENTRY_ARRAY.entries + MERGED_DENTRY_ARRAY.count;
    /*
    for (entry=MERGED_DENTRY_ARRAY.entries; entry<end; entry++) {
        dentry_release_ex(entry->args, entry->merge_count);
    }
    */

    event_dealer_free_buffers(&MERGED_DENTRY_ARRAY);
    return result;
}

int event_dealer_do(FSChangeNotifyEvent *head, int *count)
{
    int result;
    FSChangeNotifyEvent *event;
    FSChangeNotifyEvent *last;

    EVENT_PTR_ARRAY.count = 0;
    *count = 0;
    event = head;
    do {
        ++(*count);

        if ((result=add_to_event_ptr_array(event)) != 0) {
            return result;
        }

        last = event;
        event = event->next;
    } while (event != NULL);

    if (EVENT_PTR_ARRAY.count > 1) {
        qsort(EVENT_PTR_ARRAY.events, EVENT_PTR_ARRAY.count,
                sizeof(FSChangeNotifyEvent *),
                (int (*)(const void *, const void *))
                compare_event_ptr_func);
    }

    /*
    if ((result=merge_events()) != 0) {
        return result;
    }
    */

    event_dealer_ctx.updater_ctx.last_versions.dentry.prepare = last->sn;
    if ((result=deal_merged_entries()) != 0) {
        return result;
    }
    event_dealer_ctx.updater_ctx.last_versions.dentry.commit =
        event_dealer_ctx.updater_ctx.last_versions.dentry.prepare;

    return result;
}

void event_dealer_free_buffers(FSDBUpdateFieldArray *array)
{
    FSDBUpdateFieldInfo *entry;
    FSDBUpdateFieldInfo *end;

    end = array->entries + array->count;
    for (entry=array->entries; entry<end; entry++) {
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
