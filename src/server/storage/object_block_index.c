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

#include <limits.h>
#include <sys/stat.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/uniq_skiplist.h"
#include "sf/sf_global.h"
#include "sf/sf_func.h"
#include "sf/sf_buffered_writer.h"
#include "diskallocator/binlog/trunk/trunk_space_log.h"
#include "../../common/fs_func.h"
#include "../server_global.h"
#include "../server_group_info.h"
#include "../binlog/slice_binlog.h"
#include "../binlog/replica_binlog.h"
#include "../rebuild/rebuild_binlog.h"
#include "../db/change_notify.h"
#include "../db/block_serializer.h"
#include "slice_op.h"
#include "object_block_index.h"

#define SLICE_ARRAY_FIXED_COUNT  64

typedef enum {
    fs_allocate_type_block,
    fs_allocate_type_slice
} OBAllocateType;

typedef struct {
    int count;
    OBSegment *segments;
} OBSharedSegmentArray;

typedef struct {
    int alloc;
    int count;
    OBSliceEntry **slices;
    OBSliceEntry *fixed[SLICE_ARRAY_FIXED_COUNT];
} OBSlicePtrSmartArray;

typedef struct {
    struct {
        FSChangeNotifyEvent *head;
        int avail;
    } event_allocator;
} OBThreadLocal;

typedef struct {
    int64_t memory_limit;
    volatile int64_t malloc_bytes;
    OBSharedSegmentArray segment_array;
    struct fast_mblock_man slice_allocator; //extra allocator for storage engine
    struct {
        pthread_key_t key;  //for OBThreadLocal
        struct fast_mblock_man allocator; //element: OBThreadLocal
    } tls;  //for storage engine
} OBSharedContext;

static inline int do_add_slice_ex(OBHashtable *htable, OBEntry *ob,
        UniqSkiplist *sl, OBSliceEntry *slice, DATrunkFileInfo *trunk,
        struct fc_queue_info *space_chain);
static inline int do_delete_slice_ex(OBHashtable *htable,
        OBEntry *ob, UniqSkiplist *sl, OBSliceEntry *slice,
        struct fc_queue_info *space_chain);

static OBSharedContext ob_shared_ctx = {
    0, 0, {0, NULL}
};

static OBEntry *get_ob_entry_ex(OBSegment *segment, OBHashtable *htable,
        OBEntry **bucket, const FSBlockKey *bkey, const bool create_flag,
        const bool need_reclaim, OBEntry **pprev);

static int unpack_ob_entry(OBSegment *segment, OBEntry *ob,
        UniqSkiplist *sl, const SFSerializerFieldValue *fv);

#define OB_INDEX_SET_HASHTABLE_SEGMENT(htable, bkey) \
    int64_t bucket_index;  \
    OBSegment *segment; \
    do {  \
        bucket_index = FS_BLOCK_HASH_CODE(bkey) % (htable)->capacity; \
        segment = ob_shared_ctx.segment_array.segments + bucket_index % \
            ob_shared_ctx.segment_array.count;  \
    } while (0)

#define OB_INDEX_SET_BUCKET_AND_SEGMENT(htable, bkey) \
    OBEntry **bucket;   \
    OB_INDEX_SET_HASHTABLE_SEGMENT(htable, bkey);  \
    do {  \
        bucket = (htable)->buckets + bucket_index; \
    } while (0)


static inline void ob_remove(OBSegment *segment, OBHashtable *htable,
        OBEntry **bucket, OBEntry *ob, OBEntry *previous)
{
    if (previous == NULL) {
        *bucket = ob->next;
    } else {
        previous->next = ob->next;
    }

    FC_ATOMIC_DEC(htable->count);
    if (htable->need_reclaim) {
        fc_list_del_init(&ob->db_args->dlink);
    }
}

static inline void ob_entry_remove(OBSegment *segment, OBHashtable *htable,
        OBEntry **bucket, OBEntry *ob, OBEntry *previous)
{
    if (!htable->need_reclaim) {
        ob_remove(segment, htable, bucket, ob, previous);
    } else if (ob->db_args->ref_count == 1) {
        if (ob->db_args->status == FS_OB_STATUS_DELETING) {
            ob->db_args->status = FS_OB_STATUS_NORMAL;
        }
        ob_remove(segment, htable, bucket, ob, previous);
    } else {
        ob->db_args->status = FS_OB_STATUS_DELETING;
    }
    ob_index_ob_entry_release(ob);
}

void ob_index_ob_entry_release_ex(OBEntry *ob, const int dec_count)
{
    bool need_free;

    if (G_OB_HASHTABLE.need_reclaim) {
        ob->db_args->ref_count -= dec_count;
        if (ob->db_args->ref_count == 0) {
            need_free = true;
            if (ob->db_args->slices != NULL) {
                uniq_skiplist_free(ob->db_args->slices);
                ob->db_args->slices = NULL;
            }

            if (!fc_list_empty(&ob->db_args->dlink)) {
                OBEntry *previous;
                int cmpr;
                bool found;

                OB_INDEX_SET_BUCKET_AND_SEGMENT(&G_OB_HASHTABLE, ob->bkey);
                if (*bucket == NULL) {
                    found = false;
                } else {
                    cmpr = ob_index_compare_block_key(
                            &ob->bkey, &(*bucket)->bkey);
                    if (cmpr == 0) {
                        previous = NULL;
                        found = true;
                    } else if (cmpr < 0) {
                        found = false;
                    } else {
                        found = false;
                        previous = *bucket;
                        while (previous->next != NULL) {
                            cmpr = ob_index_compare_block_key(&ob->bkey,
                                    &previous->next->bkey);
                            if (cmpr == 0) {
                                found = true;
                                break;
                            } else if (cmpr < 0) {
                                break;
                            }
                            previous = previous->next;
                        }
                    }
                }
                ob->db_args->status = FS_OB_STATUS_NORMAL;

                if (found) {
                    ob_remove(segment, &G_OB_HASHTABLE, bucket, ob, previous);
                } else {
                    logWarning("file: "__FILE__", line: %d, "
                            "can't found ob entry {oid: %"PRId64", "
                            "offset: %"PRId64"}", __LINE__,
                            ob->bkey.oid, ob->bkey.offset);
                }
            }
        } else {
            need_free = false;
        }
    } else {
        need_free = true;
    }

    if (need_free) {
        uniq_skiplist_free(ob->slices);
        ob->slices = NULL;
        fast_mblock_free_object(ob->allocator, ob);
    }
}

static int block_reclaim(OBSegment *segment, const OBAllocateType type,
        const int target_count)
{
    OBEntry *ob;
    OBEntry *tmp;
    OBEntry **bucket;
    OBEntry *previous;
    int ob_count;
    int slice_count;
    int skip;
    int result;

    ob_count = slice_count = skip = 0;
    result = ENOENT;
    fc_list_for_each_entry_safe(ob, tmp, &segment->lru, db_args->dlink) {
        if (ob->db_args->locked || ob->db_args->ref_count > 1
                || ob->db_args->status == FS_OB_STATUS_DELETING)
        {
            ++skip;
            continue;
        }

        bucket = G_OB_HASHTABLE.buckets + FS_BLOCK_HASH_CODE(ob->bkey) %
            G_OB_HASHTABLE.capacity;
        if (get_ob_entry_ex(segment, &G_OB_HASHTABLE, bucket,
                    &ob->bkey, false, false, &previous) != ob)
        {
            logWarning("file: "__FILE__", line: %d, "
                    "find ob entry {oid: %"PRId64", offset: %"PRId64"} "
                    "fail!", __LINE__, ob->bkey.oid, ob->bkey.offset);
            continue;
        }

        ++ob_count;
        slice_count += uniq_skiplist_count(ob->slices);
        uniq_skiplist_clear(ob->slices);
        ob_entry_remove(segment, &G_OB_HASHTABLE, bucket, ob, previous);
        if (slice_count >= target_count || type == fs_allocate_type_block) {
            result = 0;
            break;
        }
    }

    logInfo("file: "__FILE__", line: %d, "
            "alloc type: %s, target count: %d, scan ob count: %d, "
            "reclaimed count: %d, reclaimed slice count: %d", __LINE__,
            type == fs_allocate_type_block ? "block" : "slice",
            target_count, ob_count + skip, ob_count, slice_count);
    return result;
}

static inline void *reclaim_and_alloc(OBSegment *segment, const
        OBAllocateType type)
{
    const int target_count = 1;
    void *obj;
    int i;

    for (i=0; i<100; i++) {
        while (block_reclaim(segment, type, target_count) == 0) {
            if (type == fs_allocate_type_block) {
                obj = fast_mblock_alloc_object(&segment->allocators.ob);
            } else {
                obj = fast_mblock_alloc_object(&segment->allocators.slice);
            }
            if (obj != NULL) {
                return obj;
            }
        }

        if (i == 0) {
            logInfo("file: "__FILE__", line: %d, alloc type: %s, "
                    "ob elements {total: %"PRId64", used: %"PRId64"}, "
                    "slice elements {total: %"PRId64", used: %"PRId64"}",
                    __LINE__, type == fs_allocate_type_block ? "block" :
                    "slice", segment->allocators.ob.info.element_total_count,
                    segment->allocators.ob.info.element_used_count,
                    segment->allocators.slice.info.element_total_count,
                    segment->allocators.slice.info.element_used_count);

            change_notify_signal_to_deal();
        }
        fc_sleep_ms(10);
    }

    logCrit("file: "__FILE__", line: %d, "
            "alloc %s object fail, program exit!", __LINE__,
            type == fs_allocate_type_block ? "block" : "slice");
    sf_terminate_myself();
    return NULL;
}

static inline int reclaim_and_alloc_slices(OBSegment *segment,
        const int count, struct fast_mblock_chain *chain)
{
    int i;

    for (i=0; i<100; i++) {
        while (block_reclaim(segment, fs_allocate_type_slice, count) == 0) {
            if (fast_mblock_batch_alloc(&segment->allocators.
                        slice, count, chain) == 0)
            {
                return 0;
            }
        }

        if (i == 0) {
            change_notify_signal_to_deal();
        }
        fc_sleep_ms(10);
    }

    logCrit("file: "__FILE__", line: %d, "
            "batch alloc %d slices fail, program exit!",
            __LINE__, count);
    sf_terminate_myself();
    return ENOMEM;
}

#define reclaim_and_alloc_block(segment) \
    reclaim_and_alloc(segment, fs_allocate_type_block)

#define reclaim_and_alloc_slice(segment) \
    reclaim_and_alloc(segment, fs_allocate_type_slice)

static OBEntry *ob_entry_alloc(OBSegment *segment, OBHashtable *htable,
        const FSBlockKey *bkey)
{
    const int init_level_count = 2;
    OBEntry *ob;

    ob = fast_mblock_alloc_object(&segment->allocators.ob);
    if (ob == NULL) {
        if (!G_OB_HASHTABLE.need_reclaim) {
            return NULL;
        }

        ob = reclaim_and_alloc_block(segment);
        if (ob == NULL) {
            return NULL;
        }
    }

    ob->slices = uniq_skiplist_new(&segment->allocators.
            factory, init_level_count);
    if (ob->slices == NULL) {
        fast_mblock_free_object(ob->allocator, ob);
        return NULL;
    }

    FC_ATOMIC_INC(htable->count);
    if (htable->need_reclaim) {
        ob->db_args->ref_count++;
        fc_list_add_tail(&ob->db_args->dlink, &segment->lru);
    }

    return ob;
}

static inline int ob_load_slices(FSDBFetchContext *db_fetch_ctx,
        OBSegment *segment, OBEntry *ob, UniqSkiplist *sl)
{
    int result;
    const SFSerializerFieldValue *fv;

    if ((result=block_serializer_fetch_and_unpack(db_fetch_ctx,
                    &ob->bkey, &fv)) != 0)
    {
        return result;
    }

    if (fv != NULL && fv->value.str_array.count > 0) {
        return unpack_ob_entry(segment, ob, sl, fv);
    } else {
        return 0;
    }
}

static OBEntry *get_ob_entry_ex(OBSegment *segment, OBHashtable *htable,
        OBEntry **bucket, const FSBlockKey *bkey, const bool create_flag,
        const bool need_reclaim, OBEntry **pprev)
{
    OBEntry *previous;
    OBEntry *ob;
    int cmpr;

    if (pprev == NULL) {
        pprev = &previous;
    }
    if (*bucket == NULL) {
        *pprev = NULL;
        ob = NULL;
    } else {
        cmpr = ob_index_compare_block_key(bkey, &(*bucket)->bkey);
        if (cmpr == 0) {
            *pprev = NULL;
            ob = *bucket;
        } else if (cmpr < 0) {
            *pprev = NULL;
            ob = NULL;
        } else {
            ob = NULL;
            *pprev = *bucket;
            while ((*pprev)->next != NULL) {
                cmpr = ob_index_compare_block_key(bkey, &(*pprev)->next->bkey);
                if (cmpr == 0) {
                    ob = (*pprev)->next;
                    break;
                } else if (cmpr < 0) {
                    break;
                }

                *pprev = (*pprev)->next;
            }
        }
    }

    if (ob != NULL) {
        if (need_reclaim) {
            if (ob->db_args->status == FS_OB_STATUS_DELETING) {
                if (create_flag) {
                    ob->db_args->status = FS_OB_STATUS_NORMAL;
                    ob->db_args->ref_count++;
                } else {
                    return NULL;
                }
            }

            fc_list_move_tail(&ob->db_args->dlink, &segment->lru);
        }

        return ob;
    }

    if (!(create_flag || need_reclaim)) {
        return NULL;
    }

    if ((ob=ob_entry_alloc(segment, htable, bkey)) == NULL) {
        return NULL;
    }
    ob->bkey = *bkey;

    if (STORAGE_ENABLED) {
        if (ob_load_slices(&segment->db_fetch_ctx,
                    segment, ob, ob->slices) != 0)
        {
            return NULL;
        }
    }

    if (*pprev == NULL) {
        ob->next = *bucket;
        *bucket = ob;
    } else {
        ob->next = (*pprev)->next;
        (*pprev)->next = ob;
    }
    return ob;
}

#define get_ob_entry(segment, htable, bucket, bkey, create_flag)  \
    get_ob_entry_ex(segment, htable, bucket, bkey, create_flag, \
            (htable)->need_reclaim, NULL)

OBEntry *ob_index_get_ob_entry_ex(OBHashtable *htable,
        const FSBlockKey *bkey, const bool create_flag)
{
    OBEntry *ob;
    OB_INDEX_SET_BUCKET_AND_SEGMENT(htable, *bkey);

    PTHREAD_MUTEX_LOCK(&segment->lcp.lock);
    ob = get_ob_entry(segment, htable, bucket, bkey, create_flag);
    PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);

    return ob;
}

#define OB_INDEX_INIT_SLICE(slice, _ob, init_refer) \
    if (init_refer > 0) {  \
        __sync_add_and_fetch(&slice->ref_count, init_refer); \
    } \
    slice->ob = _ob

static inline int ob_alloc_slices_for_load(OBSegment *segment, OBEntry *ob,
        UniqSkiplist *sl, const int count, struct fast_mblock_chain *chain)
{
    int result;

    if (fast_mblock_batch_alloc(&segment->allocators.
                slice, count, chain) == 0)
    {
        return 0;
    }

    if (sl == ob->slices) {
        ob->db_args->locked = true;
        result = reclaim_and_alloc_slices(segment, count, chain);
        ob->db_args->locked = false;
    } else {
        result = fast_mblock_batch_alloc(&ob_shared_ctx.
                slice_allocator, count, chain);
    }

    return result;
}

static inline OBSliceEntry *ob_slice_alloc_ex(OBSegment *segment, OBEntry *ob,
        const int init_refer, const bool call_by_db_event_dealer)
{
    OBSliceEntry *slice;

    slice = fast_mblock_alloc_object(&segment->allocators.slice);
    if (slice != NULL) {
        OB_INDEX_INIT_SLICE(slice, ob, init_refer);
        return slice;
    }

    if (!G_OB_HASHTABLE.need_reclaim) {
        return NULL;
    }

    if (call_by_db_event_dealer) {
        static int counter = 0;
        if (counter++ % 100 == 0) {
            logInfo("%d. extra slice elements {total: %"PRId64", used: %"PRId64"}",
                    counter, ob_shared_ctx.slice_allocator.info.element_total_count,
                    ob_shared_ctx.slice_allocator.info.element_used_count);
        }

        if ((slice=fast_mblock_alloc_object(&ob_shared_ctx.
                        slice_allocator)) != NULL)
        {
            OB_INDEX_INIT_SLICE(slice, ob, init_refer);
        }
    } else {
        ob->db_args->locked = true;
        if ((slice=reclaim_and_alloc_slice(segment)) != NULL) {
            OB_INDEX_INIT_SLICE(slice, ob, init_refer);
        }
        ob->db_args->locked = false;
    }
    return slice;
}

#define ob_slice_alloc(segment, ob, init_refer) \
    ob_slice_alloc_ex(segment, ob, init_refer, false)

static int slice_compare(const void *p1, const void *p2)
{
    return ((OBSliceEntry *)p1)->ssize.offset -
        ((OBSliceEntry *)p2)->ssize.offset;
}

static void slice_free_func(void *ptr, const int delay_seconds)
{
    ob_index_free_slice((OBSliceEntry *)ptr);
}

static int ob_alloc_init(OBEntry *ob, struct fast_mblock_man *allocator)
{
    ob->allocator = allocator;
    return 0;
}

static int slice_alloc_init(OBSliceEntry *slice,
        struct fast_mblock_man *allocator)
{
    slice->allocator = allocator;
    return 0;
}

static int block_malloc_trunk_check(const int alloc_bytes, void *args)
{
    return __sync_add_and_fetch(&ob_shared_ctx.malloc_bytes, 0) +
        alloc_bytes <= ob_shared_ctx.memory_limit ? 0 : EOVERFLOW;
}

static void block_malloc_trunk_notify_func(
        const enum fast_mblock_notify_type type,
        const struct fast_mblock_malloc *node, void *args)
{
    if (type == fast_mblock_notify_type_alloc) {
        __sync_add_and_fetch(&ob_shared_ctx.malloc_bytes, node->trunk_size);
    } else {
        __sync_sub_and_fetch(&ob_shared_ctx.malloc_bytes, node->trunk_size);
    }
}

static int init_ob_shared_allocators(OBSharedSegmentArray *segment_array)
{
    int result;
    const int max_level_count = 8;
    const int alloc_skiplist_once = 8 * 1024;
    const int min_alloc_elements_once = 2;
    const int delay_free_seconds = 0;
    const bool bidirection = true;  //need previous link
    const bool allocator_use_lock = true;
    const int alloc_elements_once = 16 * 1024;
    const int64_t alloc_elements_limit = 0;
    int block_prealloc_count;
    int slice_prealloc_count;
    int ob_element_size;
    int64_t block_min_memory;
    int64_t slice_min_memory;
    int64_t total_min_memory;
    OBSegment *segment;
    OBSegment *end;
    struct {
        struct fast_mblock_trunk_callbacks holder;
        struct fast_mblock_trunk_callbacks *ptr;
    } trunk_callbacks;
    struct fast_mblock_object_callbacks obj_callbacks_obentry;
    struct fast_mblock_object_callbacks obj_callbacks_slice;

    if (STORAGE_ENABLED) {
        block_prealloc_count = 1;
        slice_prealloc_count = 2;
        ob_element_size = sizeof(OBEntry) + sizeof(OBDBArgs);
        block_min_memory = fast_mblock_get_trunk_size(
                fast_mblock_get_block_size(ob_element_size),
                alloc_elements_once) * block_prealloc_count *
            segment_array->count * 2;
        slice_min_memory = fast_mblock_get_trunk_size(
                fast_mblock_get_block_size(sizeof(OBSliceEntry)),
                alloc_elements_once) * slice_prealloc_count *
            segment_array->count * 2;
        total_min_memory = block_min_memory + slice_min_memory;
        ob_shared_ctx.memory_limit = (int64_t)(SYSTEM_TOTAL_MEMORY *
                STORAGE_MEMORY_TOTAL_LIMIT * MEMORY_LIMIT_LEVEL0_RATIO);
        if (ob_shared_ctx.memory_limit < total_min_memory) {
            ob_shared_ctx.memory_limit = 256 * 1024 * 1024;
            while (ob_shared_ctx.memory_limit < total_min_memory) {
                ob_shared_ctx.memory_limit *= 2;
            }
        } else if (ob_shared_ctx.memory_limit < 256 * 1024 * 1024) {
            ob_shared_ctx.memory_limit = 256 * 1024 * 1024;
        }

        logInfo("file: "__FILE__", line: %d, memory limit %"PRId64" MB",
                __LINE__, ob_shared_ctx.memory_limit / (1024 * 1024));

        trunk_callbacks.holder.check_func = block_malloc_trunk_check;
        trunk_callbacks.holder.notify_func = block_malloc_trunk_notify_func;
        trunk_callbacks.holder.args = NULL;
        trunk_callbacks.ptr = &trunk_callbacks.holder;
    } else {
        ob_element_size = sizeof(OBEntry);
        block_prealloc_count = 0;
        slice_prealloc_count = 0;
        trunk_callbacks.ptr = NULL;
    }

    obj_callbacks_obentry.init_func = (fast_mblock_object_init_func)
        ob_alloc_init;
    obj_callbacks_obentry.destroy_func = NULL;
    obj_callbacks_slice.init_func = (fast_mblock_object_init_func)
        slice_alloc_init;
    obj_callbacks_slice.destroy_func = NULL;

    end = segment_array->segments + segment_array->count;
    for (segment=segment_array->segments; segment<end; segment++) {
        if ((result=uniq_skiplist_init_ex2(&segment->allocators.factory,
                        max_level_count, slice_compare, slice_free_func,
                        alloc_skiplist_once, min_alloc_elements_once,
                        delay_free_seconds, bidirection,
                        allocator_use_lock)) != 0)
        {
            return result;
        }

        obj_callbacks_obentry.args = &segment->allocators.ob;
        if ((result=fast_mblock_init_ex2(&segment->allocators.ob, "ob_entry",
                        ob_element_size, alloc_elements_once,
                        alloc_elements_limit, block_prealloc_count,
                        &obj_callbacks_obentry, true,
                        trunk_callbacks.ptr)) != 0)
        {
            return result;
        }

        obj_callbacks_slice.args = &segment->allocators.slice;
        if ((result=fast_mblock_init_ex2(&segment->allocators.slice,
                        "slice_entry", sizeof(OBSliceEntry),
                        alloc_elements_once, alloc_elements_limit,
                        slice_prealloc_count, &obj_callbacks_slice,
                        true, trunk_callbacks.ptr)) != 0)
        {
            return result;
        }

        if (STORAGE_ENABLED) {
            fast_mblock_set_exceed_silence(&segment->allocators.ob);
            fast_mblock_set_exceed_silence(&segment->allocators.slice);
        }
    }

    return 0;
}

static int init_ob_shared_segment_array(
        OBSharedSegmentArray *segment_array)
{
    int result;
    int bytes;
    OBSegment *segment;
    OBSegment *end;

    segment_array->count = OB_SHARED_LOCK_COUNT;
    bytes = sizeof(OBSegment) * segment_array->count;
    segment_array->segments = (OBSegment *)fc_malloc(bytes);
    if (segment_array->segments == NULL) {
        return ENOMEM;
    }
    memset(segment_array->segments, 0, bytes);

    end = segment_array->segments + segment_array->count;
    for (segment=segment_array->segments; segment<end; segment++) {
        if ((result=init_pthread_lock_cond_pair(&segment->lcp)) != 0) {
            return result;
        }

        if (STORAGE_ENABLED) {
            if ((result=da_init_read_context(&segment->
                            db_fetch_ctx.read_ctx)) != 0)
            {
                return result;
            }
            sf_serializer_iterator_init(&segment->db_fetch_ctx.it);
            FC_INIT_LIST_HEAD(&segment->lru);
        }
    }

    return 0;
}

int ob_index_init_htable(OBHashtable *htable, const int64_t capacity,
        const bool need_reclaim)
{
    int64_t bytes;

    htable->capacity = fc_ceil_prime(capacity);
    bytes = sizeof(OBEntry *) * htable->capacity;
    htable->buckets = (OBEntry **)fc_malloc(bytes);
    if (htable->buckets == NULL) {
        return ENOMEM;
    }
    memset(htable->buckets, 0, bytes);

    htable->need_reclaim = need_reclaim;
    return 0;
}

void ob_index_destroy_htable(OBHashtable *htable)
{
    OBEntry **bucket;
    OBEntry **end;
    OBEntry *ob;
    OBEntry *deleted;
    OBSegment *segment;

    end = htable->buckets + htable->capacity;
    for (bucket=htable->buckets; bucket<end; bucket++) {
        if (*bucket == NULL) {
            continue;
        }

        segment = ob_shared_ctx.segment_array.segments +
            (bucket - htable->buckets) %
            ob_shared_ctx.segment_array.count;
        PTHREAD_MUTEX_LOCK(&segment->lcp.lock);

        ob = *bucket;
        do {
            uniq_skiplist_free(ob->slices);

            deleted = ob;
            ob = ob->next;
            fast_mblock_free_object(deleted->allocator, deleted);
        } while (ob != NULL);

        PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);
    }

    free(htable->buckets);
    htable->buckets = NULL;
}

static void ob_tls_destroy(void *ptr)
{
    OBThreadLocal *tls;
    struct fast_mblock_chain chain;
    struct fast_mblock_node *node;

    tls = ptr;
    if (tls->event_allocator.head != NULL) {
        chain.head = chain.tail = fast_mblock_to_node_ptr(
                tls->event_allocator.head);
        tls->event_allocator.head = tls->event_allocator.head->next;
        while (tls->event_allocator.head != NULL) {
            node = fast_mblock_to_node_ptr(tls->event_allocator.head);
            chain.tail->next = node;
            chain.tail = node;
            tls->event_allocator.head = tls->event_allocator.head->next;
        }

        chain.tail->next = NULL;
        fast_mblock_batch_free(&STORAGE_EVENT_ALLOCATOR, &chain);
        tls->event_allocator.avail = 0;
    }

    fast_mblock_free_object(&ob_shared_ctx.tls.allocator, ptr);
}

static int init_ob_tls()
{
    const int alloc_elements_once = 1024;
    const int64_t alloc_elements_limit = 0;
    int result;

    if ((result=pthread_key_create(&ob_shared_ctx.tls.
                    key, ob_tls_destroy)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "pthread_key_create fail, errno: %d, error info: %s",
                __LINE__, result, STRERROR(result));
        return result;
    }

    if ((result=fast_mblock_init_ex1(&ob_shared_ctx.tls.allocator,
                    "ob-thread-local", sizeof(OBThreadLocal),
                    alloc_elements_once, alloc_elements_limit,
                    NULL, NULL, true)) != 0)
    {
        return result;
    }

    return 0;
}

static inline OBThreadLocal *get_tls_obj(const int target_event_count)
{
    OBThreadLocal *tls;
    struct fast_mblock_node *node;
    FSChangeNotifyEvent *event;
    struct fast_mblock_chain chain;
    int result;

    tls = pthread_getspecific(ob_shared_ctx.tls.key);
    if (tls == NULL) {
        if ((tls=fast_mblock_alloc_object(&ob_shared_ctx.
                        tls.allocator)) == NULL)
        {
            return NULL;
        }

        if ((result=pthread_setspecific(ob_shared_ctx.tls.key, tls)) != 0) {
            logError("file: "__FILE__", line: %d, "
                    "pthread_setspecific fail, errno: %d, error info: %s",
                    __LINE__, result, STRERROR(result));
            fast_mblock_free_object(&ob_shared_ctx.tls.allocator, tls);
            return NULL;
        }
    }

    if (tls->event_allocator.avail < target_event_count) {
        int alloc_count = FC_MAX(target_event_count,
                FS_CHANGE_NOTIFY_EVENT_TLS_BATCH_ALLOC);
        if (fast_mblock_batch_alloc(&STORAGE_EVENT_ALLOCATOR,
                    alloc_count, &chain) != 0)
        {
            return NULL;
        }

        node = chain.head;
        do {
            event = (FSChangeNotifyEvent *)node->data;
            event->next = tls->event_allocator.head;
            tls->event_allocator.head = event;
            tls->event_allocator.avail++;
        } while ((node=node->next) != NULL);
    }

    return tls;
}

#define OB_GET_TLS_OBJ(tls, target_event_count) \
    if (STORAGE_ENABLED) {  \
        if ((tls=get_tls_obj(target_event_count)) == NULL) { \
            return ENOMEM; \
        } \
    }


static inline FSChangeNotifyEvent *tls_alloc_event(OBThreadLocal *tls)
{
    FSChangeNotifyEvent *event;

    if (tls->event_allocator.head != NULL) {
        event = tls->event_allocator.head;
        tls->event_allocator.head = tls->event_allocator.head->next;
        tls->event_allocator.avail--;
        return event;
    } else {
        logWarning("file: "__FILE__", line: %d, "
                "can't alloc event from thread local, try to "
                "alloc from global event allocator", __LINE__);
        return fast_mblock_alloc_object(&STORAGE_EVENT_ALLOCATOR);
    }
}

int ob_index_init()
{
    const int alloc_elements_once = 16 * 1024;
    const int64_t alloc_elements_limit = 0;
    int result;

    if ((result=init_ob_shared_segment_array(&ob_shared_ctx.
                    segment_array)) != 0)
    {
        return result;
    }
    if ((result=init_ob_shared_allocators(&ob_shared_ctx.
                    segment_array)) != 0)
    {
        return result;
    }

    if (STORAGE_ENABLED) {
        if ((result=fast_mblock_init_ex1(&ob_shared_ctx.slice_allocator,
                        "extra_slice_entry", sizeof(OBSliceEntry),
                        alloc_elements_once, alloc_elements_limit,
                        (fast_mblock_object_init_func)slice_alloc_init,
                        &ob_shared_ctx.slice_allocator, true)) != 0)
        {
            return result;
        }

        if ((result=init_ob_tls()) != 0) {
            return result;
        }
    }

    if ((result=ob_index_init_htable(&G_OB_HASHTABLE,
                    OB_HASHTABLE_CAPACITY, STORAGE_ENABLED)) != 0)
    {
        return result;
    }

    return 0;
}

void ob_index_destroy()
{
}

void ob_index_delete_tls()
{
    OBThreadLocal *tls;

    if (STORAGE_ENABLED) {
        tls = pthread_getspecific(ob_shared_ctx.tls.key);
        if (tls != NULL) {
            pthread_setspecific(ob_shared_ctx.tls.key, NULL);
            ob_tls_destroy(tls);
        }
    }
}

static int add_to_space_chain(struct fc_queue_info *space_chain,
        OBSliceEntry *slice, const char op_type)
{
    DATrunkSpaceLogRecord *record;

    if ((record=da_trunk_space_log_alloc_record(&DA_CTX)) == NULL) {
        return ENOMEM;
    }

    record->oid = slice->ob->bkey.oid;
    record->fid = slice->ob->bkey.offset;
    record->extra = slice->ssize.offset;
    record->op_type = op_type;
    record->slice_type = slice->type;
    record->storage.version = slice->data_version;
    record->storage.trunk_id = slice->space.id_info.id;
    record->storage.length = slice->ssize.length;
    record->storage.offset = slice->space.offset;
    record->storage.size = slice->space.size;
    DA_SPACE_LOG_ADD_TO_CHAIN(space_chain, record);
    return 0;
}

static inline int do_add_slice_ex(OBHashtable *htable, OBEntry *ob,
        UniqSkiplist *sl, OBSliceEntry *slice, DATrunkFileInfo *trunk,
        struct fc_queue_info *space_chain)
{
    int result;

    if ((result=uniq_skiplist_insert(sl, slice)) != 0) {
        logError("file: "__FILE__", line: %d, add slice "
                "to skiplist fail, errno: %d, error info: %s, "
                "block {oid: %"PRId64", offset: %"PRId64"}, "
                "slice {offset: %d, length: %d}", __LINE__,
                result, STRERROR(result), ob->bkey.oid,
                ob->bkey.offset, slice->ssize.offset,
                slice->ssize.length);
        return result;
    }

    if (space_chain != NULL) {
        if ((result=add_to_space_chain(space_chain, slice,
                        da_binlog_op_type_consume_space)) != 0)
        {
            return result;
        }

        if (trunk != NULL) {
            ((DATrunkSpaceLogRecord *)space_chain->tail)->trunk = trunk;
        }
    }

    return 0;
}

static inline int do_delete_slice_ex(OBHashtable *htable,
        OBEntry *ob, UniqSkiplist *sl, OBSliceEntry *slice,
        struct fc_queue_info *space_chain)
{
    int result;

    if (space_chain != NULL) {
        if ((result=add_to_space_chain(space_chain, slice,
                        da_binlog_op_type_reclaim_space)) != 0)
        {
            return result;
        }
    }

    if ((result=uniq_skiplist_delete(sl, slice)) != 0) {
        logError("file: "__FILE__", line: %d, remove slice "
                "from skiplist fail, errno: %d, error info: %s, "
                "block {oid: %"PRId64", offset: %"PRId64"}, "
                "slice {offset: %d, length: %d}", __LINE__,
                result, STRERROR(result), ob->bkey.oid,
                ob->bkey.offset, slice->ssize.offset,
                slice->ssize.length);
    }

    return result;
}

#define do_add_slice(htable, ob, sl, slice, trunk, _space_chain) \
    do_add_slice_ex(htable, ob, sl, slice, trunk, slice->type == \
            DA_SLICE_TYPE_CACHE ? slice->space_chain : _space_chain)

#define do_delete_slice(htable, ob, sl, slice, _space_chain) \
    do_delete_slice_ex(htable, ob, sl, slice, slice->type == \
            DA_SLICE_TYPE_CACHE ? slice->space_chain : _space_chain)


static inline OBSliceEntry *slice_dup(OBSegment *segment,
        const OBSliceEntry *src, const DASliceType dest_type,
        const int offset, const int length,
        const bool call_by_db_event_dealer)
{
    const int init_refer = 1;
    OBSliceEntry *slice;
    int extra_offset;

    if ((slice=ob_slice_alloc_ex(segment, src->ob, init_refer,
                    call_by_db_event_dealer)) == NULL)
    {
        return NULL;
    }

    slice->data_version = src->data_version;
    slice->type = dest_type;
    slice->space = src->space;
    extra_offset = offset - src->ssize.offset;
    if (extra_offset > 0) {
        slice->space.offset += extra_offset;
        slice->ssize.offset = offset;
    } else {
        slice->ssize.offset = src->ssize.offset;
    }
    slice->ssize.length = length;

    if (slice->type == DA_SLICE_TYPE_CACHE) {
        slice->cache.mbuffer = src->cache.mbuffer;
        slice->cache.buff = src->cache.buff + extra_offset;
        sf_shared_mbuffer_hold(slice->cache.mbuffer);
    }

    return slice;
}

static int add_to_slice_ptr_smart_array(OBSlicePtrSmartArray *array,
        OBSliceEntry *slice)
{
    if (array->alloc <= array->count) {
        int alloc;
        int bytes;
        OBSliceEntry **slices;

        alloc = array->alloc * 2;
        bytes = sizeof(OBSliceEntry *) * alloc;
        slices = (OBSliceEntry **)fc_malloc(bytes);
        if (slices == NULL) {
            return ENOMEM;
        }

        memcpy(slices, array->slices, sizeof(OBSliceEntry *) * array->count);
        if (array->slices != array->fixed) {
            free(array->slices);
        }

        array->alloc = alloc;
        array->slices = slices;
    }

    array->slices[array->count++] = slice;
    return 0;
}

static inline int dup_slice_to_smart_array(OBSegment *segment,
        const OBSliceEntry *src_slice, const int offset,
        const int length, const bool call_by_db_event_dealer,
        OBSlicePtrSmartArray *array)
{
    OBSliceEntry *new_slice;

    new_slice = slice_dup(segment, src_slice, src_slice->type,
            offset, length, call_by_db_event_dealer);
    if (new_slice == NULL) {
        return ENOMEM;
    }

    new_slice->space.size = length;  //for calculating trunk used bytes correctly
    if (src_slice->type == DA_SLICE_TYPE_CACHE) {
        new_slice->space_chain = src_slice->space_chain;
    }
    return add_to_slice_ptr_smart_array(array, new_slice);
}

#define INIT_SLICE_PTR_ARRAY(sarray) \
    do {   \
        sarray.count = 0;  \
        sarray.alloc = SLICE_ARRAY_FIXED_COUNT;  \
        sarray.slices = sarray.fixed;  \
    } while (0)

#define FREE_SLICE_PTR_ARRAY(sarray) \
    do { \
        if (sarray.slices != sarray.fixed) { \
            free(sarray.slices);  \
        } \
    } while (0)


static int add_slice(OBSegment *segment, OBHashtable *htable, OBEntry *ob,
        UniqSkiplist *sl, OBSliceEntry *slice, DATrunkFileInfo *trunk,
        int *inc_alloc, struct fc_queue_info *space_chain,
        const bool call_by_db_event_dealer)
{
    UniqSkiplistNode *node;
    UniqSkiplistNode *previous;
    OBSliceEntry *curr_slice;
    OBSlicePtrSmartArray add_slice_array;
    OBSlicePtrSmartArray del_slice_array;
    int result;
    int curr_end;
    int slice_end;
    int new_space_start;
    int i;

    node = uniq_skiplist_find_ge_node(sl, slice);
    if (node == NULL) {
        previous = UNIQ_SKIPLIST_LEVEL0_TAIL_NODE(sl);
        if (previous == sl->top) {
            if (inc_alloc != NULL) {
                *inc_alloc += slice->ssize.length;
            }
            return do_add_slice(htable, ob, sl, slice, trunk, space_chain);
        }
    } else {
        previous = UNIQ_SKIPLIST_LEVEL0_PREV_NODE(node);
    }

    INIT_SLICE_PTR_ARRAY(add_slice_array);
    INIT_SLICE_PTR_ARRAY(del_slice_array);

    new_space_start = slice->ssize.offset;
    slice_end = slice->ssize.offset + slice->ssize.length;
    if (previous != sl->top) {
        curr_slice = (OBSliceEntry *)previous->data;
        curr_end = curr_slice->ssize.offset + curr_slice->ssize.length;
        if (curr_end > slice->ssize.offset) {  //overlap
            if ((result=add_to_slice_ptr_smart_array(&del_slice_array,
                            curr_slice)) != 0)
            {
                return result;
            }

            if ((result=dup_slice_to_smart_array(segment, curr_slice,
                            curr_slice->ssize.offset, slice->ssize.offset -
                            curr_slice->ssize.offset, call_by_db_event_dealer,
                            &add_slice_array)) != 0)
            {
                return result;
            }

            new_space_start = curr_end;
            if (curr_end > slice_end) {
                if ((result=dup_slice_to_smart_array(segment, curr_slice,
                                slice_end, curr_end - slice_end,
                                call_by_db_event_dealer,
                                &add_slice_array)) != 0)
                {
                    return result;
                }
            }
        }
    }

    if (node != NULL) {
        do {
            curr_slice = (OBSliceEntry *)node->data;
            if (slice_end <= curr_slice->ssize.offset) {  //not overlap
                break;
            }

            if ((result=add_to_slice_ptr_smart_array(&del_slice_array,
                            curr_slice)) != 0)
            {
                return result;
            }

            if (curr_slice->ssize.offset > new_space_start) {
                if (inc_alloc != NULL) {
                    *inc_alloc += curr_slice->ssize.offset - new_space_start;
                }
            }

            curr_end = curr_slice->ssize.offset + curr_slice->ssize.length;
            new_space_start = curr_end;
            if (curr_end > slice_end) {
                if ((result=dup_slice_to_smart_array(segment, curr_slice,
                                slice_end, curr_end - slice_end,
                                call_by_db_event_dealer,
                                &add_slice_array)) != 0)
                {
                    return result;
                }

                break;
            }

            node = UNIQ_SKIPLIST_LEVEL0_NEXT_NODE(node);
        } while (node != sl->factory->tail);
    }

    if (slice_end > new_space_start) {
        if (inc_alloc != NULL) {
            *inc_alloc += slice_end - new_space_start;
        }
    }

    if (del_slice_array.count > 0) {
        for (i=0; i<del_slice_array.count; i++) {
            do_delete_slice(htable, ob, sl, del_slice_array.
                    slices[i], space_chain);
        }
        FREE_SLICE_PTR_ARRAY(del_slice_array);
    }

    if (add_slice_array.count > 0) {
        for (i=0; i<add_slice_array.count; i++) {
            do_add_slice(htable, ob, sl, add_slice_array.
                    slices[i], NULL, space_chain);
        }
        FREE_SLICE_PTR_ARRAY(add_slice_array);
    }

    return do_add_slice(htable, ob, sl, slice, trunk, space_chain);
}

int ob_index_add_slice_to_wbuffer_chain(FSSliceSpaceLogRecord *record,
        SFBinlogWriterBuffer **slice_tail, const DASliceType slice_type,
        const FSBlockKey *bkey, const FSSliceSize *ssize,
        const DATrunkSpaceInfo *space, const time_t timestamp,
        const uint64_t sn, const uint64_t data_version, const char source)
{
    SFBinlogWriterBuffer *wbuffer;

    if ((wbuffer=sf_binlog_writer_alloc_buffer(
                    &SLICE_BINLOG_WRITER.thread)) == NULL)
    {
        return ENOMEM;
    }

    SF_BINLOG_BUFFER_SET_VERSION(wbuffer, sn);
    wbuffer->bf.length = slice_binlog_log_add_slice_to_buff1(slice_type, bkey,
        ssize, space, timestamp, sn, data_version, source, wbuffer->bf.buff);
    wbuffer->next = NULL;
    if (record->slice_chain.head == NULL) {
        record->slice_chain.head = wbuffer;
        record->slice_chain.count = 1;
    } else {
        (*slice_tail)->next = wbuffer;
        record->slice_chain.count++;
    }
    *slice_tail = wbuffer;
    record->last_sn = sn;
    return 0;
}

int ob_index_del_slice_to_wbuffer_chain(FSSliceSpaceLogRecord *record,
        const FSBlockSliceKeyInfo *bs_key, const time_t timestamp,
        const int64_t sn, const int64_t data_version, const char source)
{
    SFBinlogWriterBuffer *wbuffer;

    if ((wbuffer=sf_binlog_writer_alloc_buffer(
                    &SLICE_BINLOG_WRITER.thread)) == NULL)
    {
        return ENOMEM;
    }

    SF_BINLOG_BUFFER_SET_VERSION(wbuffer, sn);
    wbuffer->bf.length = slice_binlog_log_del_slice_to_buff(bs_key,
            timestamp, sn, data_version, source, wbuffer->bf.buff);
    wbuffer->next = NULL;
    record->slice_chain.head = wbuffer;
    record->slice_chain.count = 1;
    record->last_sn = sn;
    return 0;
}

int ob_index_del_block_to_wbuffer_chain(FSSliceSpaceLogRecord *record,
        const FSBlockKey *bkey, const time_t timestamp, const int64_t sn,
        const int64_t data_version, const char source)
{
    SFBinlogWriterBuffer *wbuffer;

    if ((wbuffer=sf_binlog_writer_alloc_buffer(
                    &SLICE_BINLOG_WRITER.thread)) == NULL)
    {
        return ENOMEM;
    }

    SF_BINLOG_BUFFER_SET_VERSION(wbuffer, sn);
    wbuffer->bf.length = slice_binlog_log_del_block_to_buff(bkey,
            timestamp, sn, data_version, source, wbuffer->bf.buff);
    wbuffer->next = NULL;
    record->slice_chain.head = wbuffer;
    record->slice_chain.count = 1;
    record->last_sn = sn;
    return 0;
}

static inline int add_slice_for_reclaim(OBThreadLocal *tls,
        FSSliceSpaceLogRecord *record, SFBinlogWriterBuffer **slice_tail,
        OBSliceEntry *slice, const int64_t sn)
{
    int result;
    FSChangeNotifyEvent *event;

    if ((result=ob_index_add_slice_to_wbuffer_chain(record, slice_tail,
                    slice->type, &slice->ob->bkey, &slice->ssize,
                    &slice->space, g_current_time, sn, slice->
                    data_version, BINLOG_SOURCE_RECLAIM)) != 0)
    {
        return result;
    }

    if (STORAGE_ENABLED) {
        if ((event=tls_alloc_event(tls)) == NULL) {
            return ENOMEM;
        }
        change_notify_push_add_slice(event, sn, slice);
    }
    return 0;
}

static int update_slice(OBThreadLocal *tls, OBSegment *segment,
        OBHashtable *htable, OBEntry *ob, OBSliceEntry *slice,
        int *update_count, bool *release_slice,
        FSSliceSpaceLogRecord *record, const bool call_by_reclaim)
{
    const bool call_by_db_event_dealer = false;
    UniqSkiplistNode *node;
    OBSliceEntry *current;
    SFBinlogWriterBuffer *slice_tail = NULL;
    OBSlicePtrSmartArray add_slice_array;
    OBSlicePtrSmartArray del_slice_array;
    int64_t sn;
    int result;
    int slice_end;
    DASliceType expect_slice_type;
    int i;

    node = uniq_skiplist_find_ge_node(ob->slices, slice);
    if (node == NULL) {
        *update_count = 0;
        *release_slice = true;
        return 0;
    }

    expect_slice_type = call_by_reclaim ? slice->type : DA_SLICE_TYPE_CACHE;
    current = (OBSliceEntry *)node->data;
    if ((current->type == expect_slice_type) &&
            (current->data_version == slice->data_version) &&
            (current->ssize.offset == slice->ssize.offset &&
             current->ssize.length == slice->ssize.length))
    {
        *update_count = 1;
        *release_slice = false;
        do_delete_slice(htable, ob, ob->slices,
                current, &record->space_chain);

        if (call_by_reclaim) {
            sn = ob_index_generate_alone_sn();
            if ((result=add_slice_for_reclaim(tls, record,
                            &slice_tail, slice, sn)) != 0)
            {
                return result;
            }
        }
        return do_add_slice(htable, ob, ob->slices,
                slice, NULL, &record->space_chain);
    }

    *release_slice = true;
    slice_end = slice->ssize.offset + slice->ssize.length;
    if (slice_end <= current->ssize.offset) { //not overlap
        *update_count = 0;
        return 0;
    }

    INIT_SLICE_PTR_ARRAY(add_slice_array);
    INIT_SLICE_PTR_ARRAY(del_slice_array);
    do {
        current = (OBSliceEntry *)node->data;
        if (slice_end <= current->ssize.offset) {  //not overlap
            break;
        }

        if (current->data_version == slice->data_version) {
            if (current->type != expect_slice_type || (current->ssize.
                        offset + current->ssize.length) > slice_end)
            {
                logCrit("file: "__FILE__", line: %d, "
                        "some mistake happen! call_by_reclaim: %d, "
                        "expect type: %c, data version: %"PRId64", "
                        "block {oid: %"PRId64", offset: %"PRId64"}, "
                        "old slice {offset: %d, length: %d, type: %c}, "
                        "new slice {offset: %d, length: %d, type: %c}",
                        __LINE__, call_by_reclaim, expect_slice_type,
                        slice->data_version, ob->bkey.oid,
                        ob->bkey.offset, current->ssize.offset,
                        current->ssize.length, current->type,
                        slice->ssize.offset, slice->ssize.length,
                        slice->type);
                sf_terminate_myself();
                return EFAULT;
            }

            if ((result=add_to_slice_ptr_smart_array(&del_slice_array,
                            current)) != 0)
            {
                return result;
            }

            if ((result=dup_slice_to_smart_array(segment, slice,
                            current->ssize.offset, current->ssize.length,
                            call_by_db_event_dealer, &add_slice_array)) != 0)
            {
                return result;
            }
        }

        node = UNIQ_SKIPLIST_LEVEL0_NEXT_NODE(node);
    } while (node != ob->slices->factory->tail);

    *update_count = add_slice_array.count;
    if (*update_count > 0) {
        for (i=0; i<del_slice_array.count; i++) {
            do_delete_slice(htable, ob, ob->slices, del_slice_array.
                    slices[i], &record->space_chain);
        }
        FREE_SLICE_PTR_ARRAY(del_slice_array);

        if (call_by_reclaim) {
            sn = ob_index_batch_generate_alone_sn(add_slice_array.count);
            for (i=0; i<add_slice_array.count; i++) {
                if ((result=add_slice_for_reclaim(tls, record, &slice_tail,
                                add_slice_array.slices[i], sn + i)) != 0)
                {
                    return result;
                }

                do_add_slice(htable, ob, ob->slices, add_slice_array.
                        slices[i], NULL, &record->space_chain);
            }
        } else {
            for (i=0; i<add_slice_array.count; i++) {
                do_add_slice(htable, ob, ob->slices, add_slice_array.
                        slices[i], NULL, &record->space_chain);
            }
        }

        FREE_SLICE_PTR_ARRAY(add_slice_array);
    }

    return 0;
}

int ob_index_add_slice_no_db_ex(OBHashtable *htable,
        const DASliceType slice_type, const FSBlockKey *bkey,
        const FSSliceSize *ssize, const int64_t data_version,
        FSSliceSNPair *slice_sn_pair, SFSharedMBuffer *mbuffer,
        uint64_t *sn, int *inc_alloc, struct fc_queue_info *space_chain)
{
    const int init_refer = 1;
    const bool call_by_db_event_dealer = false;
    int result;
    OBEntry *ob;
    OBSliceEntry *slice;

    /*
    logInfo("#######ob_index_add_slice_no_db: %p, ref_count: %d, "
            "block {oid: %"PRId64", offset: %"PRId64"}",
            slice, __sync_add_and_fetch(&slice->ref_count, 0),
            bkey->oid, bkey->offset);
            */

    OB_INDEX_SET_BUCKET_AND_SEGMENT(htable, *bkey);
    *inc_alloc = 0;
    PTHREAD_MUTEX_LOCK(&segment->lcp.lock);
    ob = get_ob_entry(segment, htable, bucket, bkey, true);
    if (ob == NULL) {
        result = ENOMEM;
    } else if ((slice=ob_slice_alloc_ex(segment, ob, init_refer,
                    call_by_db_event_dealer)) == NULL)
    {
        result = ENOMEM;
    } else {
        slice->data_version = data_version;
        slice->type = slice_type;
        slice->ssize = *ssize;
        if (slice_sn_pair != NULL) {
            slice->space = slice_sn_pair->space;
            if (slice_type == DA_SLICE_TYPE_CACHE) {
                slice->cache.mbuffer = mbuffer;
                slice->cache.buff = slice_sn_pair->cache_buff;
                sf_shared_mbuffer_hold(mbuffer);
                slice->space_chain = space_chain;
            }
        }

        result = add_slice(segment, htable, ob, ob->slices, slice,
                slice_sn_pair != NULL ? slice_sn_pair->trunk : NULL,
                inc_alloc, space_chain, call_by_db_event_dealer);
        if (result == 0) {
            if (sn != NULL) {
                *sn = __sync_add_and_fetch(&SLICE_BINLOG_SN, 1);
            }
        }
    }
    PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);

    return result;
}

int ob_index_batch_add_slice(const int64_t data_version,
        const FSBlockKey *bkey, FSSliceSNPairArray *sarray,
        uint64_t *last_sn, int *total_alloc,
        struct fc_queue_info *space_chain)
{
    const int init_refer = 1;
    const bool call_by_db_event_dealer = false;
    int inc_alloc;
    int result = 0;
    int64_t sn;
    OBThreadLocal *tls = NULL;
    FSChangeNotifyEvent *event;
    FSSliceSNPair *slice_sn_pair;
    FSSliceSNPair *slice_sn_end;
    OBEntry *ob;
    OBSliceEntry *slice;

    OB_INDEX_SET_BUCKET_AND_SEGMENT(&G_OB_HASHTABLE, *bkey);
    OB_GET_TLS_OBJ(tls, sarray->count);
    *total_alloc = 0;
    PTHREAD_MUTEX_LOCK(&segment->lcp.lock);
    ob = get_ob_entry(segment, &G_OB_HASHTABLE, bucket, bkey, true);
    if (ob == NULL) {
        result = ENOMEM;
    } else {
        *last_sn = __sync_add_and_fetch(&SLICE_BINLOG_SN, sarray->count);
        slice_sn_end = sarray->slice_sn_pairs + sarray->count;
        for (slice_sn_pair=sarray->slice_sn_pairs,
                sn = (*last_sn) - (sarray->count - 1);
                slice_sn_pair<slice_sn_end;
                slice_sn_pair++, sn++)
        {
            if ((slice=ob_slice_alloc(segment, ob, init_refer)) == NULL) {
                result = ENOMEM;
                break;
            }

            inc_alloc = 0;
            slice->data_version = data_version;
            slice->type = slice_sn_pair->type;
            slice->ssize = slice_sn_pair->ssize;
            slice->space = slice_sn_pair->space;
            if ((result=add_slice(segment, &G_OB_HASHTABLE, ob, ob->slices,
                            slice, slice_sn_pair->trunk, &inc_alloc,
                            space_chain, call_by_db_event_dealer)) != 0)
            {
                break;
            }

            *total_alloc += inc_alloc;
            slice_sn_pair->sn = sn;
            if (STORAGE_ENABLED) {
                if ((event=tls_alloc_event(tls)) != NULL) {
                    change_notify_push_add_slice(event, sn, slice);
                } else {
                    result = ENOMEM;
                    break;
                }
            }
        }
    }
    PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);

    return result;
}

int ob_index_add_slice_by_binlog(const uint64_t sn,
        const int64_t data_version, const FSBlockSliceKeyInfo *bs_key,
        const DASliceType slice_type, const DATrunkSpaceInfo *space)
{
    const int init_refer = 1;
    const bool call_by_db_event_dealer = false;
    int result;
    OBThreadLocal *tls = NULL;
    FSChangeNotifyEvent *event;
    OBEntry *ob;
    OBSliceEntry *slice;

    OB_INDEX_SET_BUCKET_AND_SEGMENT(&G_OB_HASHTABLE, bs_key->block);
    OB_GET_TLS_OBJ(tls, 1);
    PTHREAD_MUTEX_LOCK(&segment->lcp.lock);
    ob = get_ob_entry(segment, &G_OB_HASHTABLE, bucket, &bs_key->block, true);
    if (ob != NULL) {
        if ((slice=ob_slice_alloc(segment, ob, init_refer)) != NULL) {
            slice->data_version = data_version;
            slice->type = slice_type;
            slice->ssize = bs_key->slice;
            slice->space = *space;
            if ((result=add_slice(segment, &G_OB_HASHTABLE, ob,
                            ob->slices, slice, NULL, NULL, NULL,
                            call_by_db_event_dealer)) == 0)
            {
                if (STORAGE_ENABLED) {
                    if ((event=tls_alloc_event(tls)) != NULL) {
                        change_notify_push_add_slice(event, sn, slice);
                    } else {
                        result = ENOMEM;
                    }
                }
            }
        } else {
            result = ENOMEM;
        }
    } else {
        result = ENOMEM;
    }
    PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);

    return result;
}

int ob_index_update_slice_ex(OBHashtable *htable, const DASliceEntry *se,
        const DATrunkSpaceInfo *space, int *update_count,
        FSSliceSpaceLogRecord *record, const DASliceType slice_type,
        const bool call_by_reclaim)
{
    const int init_refer = 1;
    int result;
    bool release_slice;
    OBThreadLocal *tls = NULL;
    FSChangeNotifyEvent *event;
    OBEntry *ob;
    OBSliceEntry *slice;

    /*
    logInfo("####### ob_index_update_slice: %p, "
            "block {oid: %"PRId64", offset: %"PRId64"}, "
            "slice: {offset: %d, length: %d}",
            se, se->bs_key.block.oid, se->bs_key.block.offset,
            se->bs_key.slice.offset, se->bs_key.slice.length);
            */

    OB_INDEX_SET_BUCKET_AND_SEGMENT(htable, se->bs_key.block);
    OB_GET_TLS_OBJ(tls, call_by_reclaim ?
            FS_CHANGE_NOTIFY_EVENT_TLS_BATCH_ALLOC : 1);
    PTHREAD_MUTEX_LOCK(&segment->lcp.lock);
    ob = get_ob_entry(segment, htable, bucket, &se->bs_key.block, false);
    if (ob != NULL) {
        if ((slice=ob_slice_alloc(segment, ob, init_refer)) == NULL) {
            *update_count = 0;
            result = ENOMEM;
        } else {
            slice->data_version = se->data_version;
            slice->type = slice_type;
            slice->ssize = se->bs_key.slice;
            slice->space = *space;
            if ((result=update_slice(tls, segment, htable, ob, slice,
                            update_count, &release_slice, record,
                            call_by_reclaim)) == 0)
            {
                if (STORAGE_ENABLED && !call_by_reclaim) {
                    if ((event=tls_alloc_event(tls)) != NULL) {
                        change_notify_push_add_slice(event, se->sn, slice);
                    } else {
                        result = ENOMEM;
                    }
                }
            }
            if (release_slice) {
                ob_index_free_slice(slice);
            }
        }
    } else {
        *update_count = 0;
        result = 0;
    }
    PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);

    return result;
}

static int delete_slice(OBSegment *segment, OBHashtable *htable,
        OBEntry *ob, UniqSkiplist *sl, const FSBlockSliceKeyInfo *bs_key,
        int *count, int *dec_alloc, struct fc_queue_info *space_chain,
        const bool call_by_db_event_dealer)
{
    OBSliceEntry target;
    UniqSkiplistNode *node;
    UniqSkiplistNode *previous;
    OBSliceEntry *curr_slice;
    OBSlicePtrSmartArray add_slice_array;
    OBSlicePtrSmartArray del_slice_array;
    int result;
    int curr_end;
    int slice_end;
    int i;

    *count = 0;
    target.ssize = bs_key->slice;
    node = uniq_skiplist_find_ge_node(sl, &target);
    if (node == NULL) {
        previous = UNIQ_SKIPLIST_LEVEL0_TAIL_NODE(sl);
        if (previous == sl->top) {
            return ENOENT;
        }
    } else {
        previous = UNIQ_SKIPLIST_LEVEL0_PREV_NODE(node);
    }

    INIT_SLICE_PTR_ARRAY(add_slice_array);
    INIT_SLICE_PTR_ARRAY(del_slice_array);

    slice_end = bs_key->slice.offset + bs_key->slice.length;
    if (previous != sl->top) {
        curr_slice = (OBSliceEntry *)previous->data;
        curr_end = curr_slice->ssize.offset + curr_slice->ssize.length;
        if (curr_end > bs_key->slice.offset) {  //overlap
            if ((result=add_to_slice_ptr_smart_array(&del_slice_array,
                            curr_slice)) != 0)
            {
                return result;
            }

            if ((result=dup_slice_to_smart_array(segment, curr_slice,
                            curr_slice->ssize.offset, bs_key->slice.offset -
                            curr_slice->ssize.offset, call_by_db_event_dealer,
                            &add_slice_array)) != 0)
            {
                return result;
            }

            if (curr_end > slice_end) {
                if ((result=dup_slice_to_smart_array(segment, curr_slice,
                                slice_end, curr_end - slice_end,
                                call_by_db_event_dealer,
                                &add_slice_array)) != 0)
                {
                    return result;
                }

                if (dec_alloc != NULL) {
                    *dec_alloc += bs_key->slice.length;
                }
            } else {
                if (dec_alloc != NULL) {
                    *dec_alloc += curr_end - bs_key->slice.offset;
                }
            }
        }
    }

    if (node != NULL) {
        do {
            curr_slice = (OBSliceEntry *)node->data;
            if (slice_end <= curr_slice->ssize.offset) {  //not overlap
                break;
            }

            if ((result=add_to_slice_ptr_smart_array(&del_slice_array,
                            curr_slice)) != 0)
            {
                return result;
            }

            curr_end = curr_slice->ssize.offset + curr_slice->ssize.length;
            if (curr_end > slice_end) {
                if ((result=dup_slice_to_smart_array(segment, curr_slice,
                                slice_end, curr_end - slice_end,
                                call_by_db_event_dealer,
                                &add_slice_array)) != 0)
                {
                    return result;
                }

                if (dec_alloc != NULL) {
                    *dec_alloc += slice_end - curr_slice->ssize.offset;
                }
                break;
            } else {
                if (dec_alloc != NULL) {
                    *dec_alloc += curr_slice->ssize.length;
                }
            }

            node = UNIQ_SKIPLIST_LEVEL0_NEXT_NODE(node);
        } while (node != sl->factory->tail);
    }

    if (del_slice_array.count == 0) {
        return ENOENT;
    }

    *count = del_slice_array.count;
    if (del_slice_array.count > 0) {
        for (i=0; i<del_slice_array.count; i++) {
            do_delete_slice(htable, ob, sl, del_slice_array.
                    slices[i], space_chain);
        }
        FREE_SLICE_PTR_ARRAY(del_slice_array);
    }

    if (add_slice_array.count > 0) {
        for (i=0; i<add_slice_array.count; i++) {
            do_add_slice(htable, ob, sl, add_slice_array.
                    slices[i], NULL, space_chain);
        }
        FREE_SLICE_PTR_ARRAY(add_slice_array);
    }

    return 0;
}

int ob_index_delete_slice_ex(OBHashtable *htable,
        const FSBlockSliceKeyInfo *bs_key, uint64_t *sn,
        int *dec_alloc, struct fc_queue_info *space_chain)
{
    const bool call_by_db_event_dealer = false;
    OBThreadLocal *tls = NULL;
    FSChangeNotifyEvent *event;
    OBEntry *ob;
    OBEntry *previous;
    int result;
    int count;

    OB_INDEX_SET_BUCKET_AND_SEGMENT(htable, bs_key->block);
    OB_GET_TLS_OBJ(tls, 1);
    *dec_alloc = 0;
    PTHREAD_MUTEX_LOCK(&segment->lcp.lock);
    ob = get_ob_entry_ex(segment, htable, bucket, &bs_key->block,
            false, htable->need_reclaim, &previous);
    if (ob == NULL) {
        result = ENOENT;
    } else {
        result = delete_slice(segment, htable, ob, ob->slices,
                bs_key, &count, dec_alloc, space_chain,
                call_by_db_event_dealer);
        if (result == 0) {
            if (sn != NULL) {
                if (*sn == 0) {
                    *sn = __sync_add_and_fetch(&SLICE_BINLOG_SN, 1);
                }

                if (STORAGE_ENABLED) {
                    if ((event=tls_alloc_event(tls)) != NULL) {
                        change_notify_push_del_slice(event,
                                *sn, ob, &bs_key->slice);
                    } else {
                        result = ENOMEM;
                    }
                }
            }

            if (uniq_skiplist_empty(ob->slices)) {
                ob_entry_remove(segment, htable, bucket, ob, previous);
            }
        }
    }
    PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);

    return result;
}

int ob_index_delete_block_ex(OBHashtable *htable, const FSBlockKey *bkey,
        uint64_t *sn, int *dec_alloc, struct fc_queue_info *space_chain)
{
    OBThreadLocal *tls = NULL;
    FSChangeNotifyEvent *event;
    OBEntry *ob;
    OBEntry *previous;
    OBSliceEntry *slice;
    struct fc_queue_info *chain;
    UniqSkiplistIterator it;
    int result;

    OB_INDEX_SET_BUCKET_AND_SEGMENT(htable, *bkey);
    OB_GET_TLS_OBJ(tls, 1);
    *dec_alloc = 0;
    PTHREAD_MUTEX_LOCK(&segment->lcp.lock);
    ob = get_ob_entry_ex(segment, htable, bucket, bkey,
            false, htable->need_reclaim, &previous);
    if (ob != NULL) {
        uniq_skiplist_iterator(ob->slices, &it);
        while ((slice=(OBSliceEntry *)uniq_skiplist_next(&it)) != NULL) {
            *dec_alloc += slice->ssize.length;
            chain = (slice->type == DA_SLICE_TYPE_CACHE ?
                    slice->space_chain : space_chain);
            if (chain != NULL) {
                if ((result=add_to_space_chain(chain, slice,
                                da_binlog_op_type_reclaim_space)) != 0)
                {
                    return result;
                }
            }
        }

        if (*dec_alloc > 0) {
            result = 0;
            if (sn != NULL) {
                if (*sn == 0) {
                    *sn = __sync_add_and_fetch(&SLICE_BINLOG_SN, 1);
                }
                if (STORAGE_ENABLED) {
                    uniq_skiplist_clear(ob->slices);
                    if ((event=tls_alloc_event(tls)) != NULL) {
                        change_notify_push_del_block(event, *sn, ob);
                    } else {
                        result = ENOMEM;
                    }
                }
            }
        } else {  //no slices deleted
            result = ENOENT;
        }
        ob_entry_remove(segment, htable, bucket, ob, previous);
    } else {
        result = ENOENT;
    }
    PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);

    return result;
}

static int add_to_slice_rbuffer_array(OBSliceReadBufferArray *array,
        OBSliceEntry *slice)
{
    if (array->alloc <= array->count) {
        int alloc;
        int bytes;
        OBSliceReadBufferPair *pairs;

        if (array->alloc == 0) {
            alloc = 256;
        } else {
            alloc = array->alloc * 2;
        }
        bytes = sizeof(OBSliceReadBufferPair) * alloc;
        pairs = (OBSliceReadBufferPair *)fc_malloc(bytes);
        if (pairs == NULL) {
            return ENOMEM;
        }

        if (array->pairs != NULL) {
            memcpy(pairs, array->pairs, array->count *
                    sizeof(OBSliceReadBufferPair));
            free(array->pairs);
        }

        array->alloc = alloc;
        array->pairs = pairs;
    }

    array->pairs[array->count++].slice = slice;
    return 0;
}

static inline int dup_slice_to_array(OBSegment *segment, OBHashtable *htable,
        const OBSliceEntry *src_slice, const int offset, const int length,
        OBSliceReadBufferArray *array)
{
    const bool call_by_db_event_dealer = false;
    OBSliceEntry *new_slice;

    new_slice = slice_dup(segment, src_slice, src_slice->type,
            offset, length, call_by_db_event_dealer);
    if (new_slice == NULL) {
        return ENOMEM;
    }

    return add_to_slice_rbuffer_array(array, new_slice);
}

/*
static void print_skiplist(OBEntry *ob)
{
    UniqSkiplistIterator it;
    OBSliceEntry *slice;
    int count = 0;

    logInfo("forward iterator:");
    uniq_skiplist_iterator(ob->slices, &it);
    while ((slice=(OBSliceEntry *)uniq_skiplist_next(&it)) != NULL) {

        ++count;
        //if (count <= 1)
        {
            logInfo("%d. slice offset: %d, length: %d, end: %d",
                    count, slice->ssize.offset, slice->ssize.length,
                    slice->ssize.offset + slice->ssize.length);
        }
    }


    {
    UniqSkiplistNode *node;
    logInfo("reverse iterator:");
    node = UNIQ_SKIPLIST_LEVEL0_TAIL_NODE(ob->slices);
    while (node != ob->slices->top) {
        slice = (OBSliceEntry *)node->data;

        //if (count <= 1)
        {
            logInfo("%d. slice offset: %d, length: %d, end: %d",
                    count, slice->ssize.offset, slice->ssize.length,
                    slice->ssize.offset + slice->ssize.length);
        }
        --count;
        if (count < 0) {
            break;
        }

        node = UNIQ_SKIPLIST_LEVEL0_PREV_NODE(node);
    }
    }
}
*/

static int get_slices(OBSegment *segment, OBHashtable *htable, OBEntry *ob,
        const FSBlockSliceKeyInfo *bs_key, OBSliceReadBufferArray *sarray)
{
    UniqSkiplistNode *node;
    UniqSkiplistNode *previous;
    OBSliceEntry target;
    OBSliceEntry *curr_slice;
    int slice_end;
    int curr_end;
    int length;
    int result;

    //print_skiplist(ob);

    target.ssize = bs_key->slice;

    /*
    logInfo("target slice.offset: %d, length: %d",
            target.ssize.offset, target.ssize.length);
            */

    node = uniq_skiplist_find_ge_node(ob->slices, &target);
    if (node == NULL) {
        previous = UNIQ_SKIPLIST_LEVEL0_TAIL_NODE(ob->slices);
    } else {
        previous = UNIQ_SKIPLIST_LEVEL0_PREV_NODE(node);
    }

    slice_end = bs_key->slice.offset + bs_key->slice.length;

    /*
    logInfo("bs_key->slice.offset: %d, length: %d, slice_end: %d, ge "
            "node: %p, top: %p", bs_key->slice.offset, bs_key->slice.length,
            slice_end, node, ob->slices->top);
            */

    if (previous != ob->slices->top) {
        curr_slice = (OBSliceEntry *)previous->data;
        curr_end = curr_slice->ssize.offset + curr_slice->ssize.length;

        /*
        logInfo("previous slice.offset: %d, length: %d, curr_end: %d",
                curr_slice->ssize.offset, curr_slice->ssize.length, curr_end);
                */

        if (curr_end > bs_key->slice.offset) {  //overlap
            length = FC_MIN(curr_end, slice_end) - bs_key->slice.offset;
            if ((result=dup_slice_to_array(segment, htable, curr_slice,
                            bs_key->slice.offset, length, sarray)) != 0)
            {
                return result;
            }
        }
    }

    if (node == NULL) {
        return sarray->count > 0 ? 0 : ENOENT;
    }

    result = 0;
    do {
        curr_slice = (OBSliceEntry *)node->data;
        if (slice_end <= curr_slice->ssize.offset) {  //not overlap
            break;
        }

        curr_end = curr_slice->ssize.offset + curr_slice->ssize.length;

        /*
        logInfo("current slice.offset: %d, length: %d, curr_end: %d",
                curr_slice->ssize.offset, curr_slice->ssize.length, curr_end);
                */

        if (curr_end > slice_end) {  //the last slice
            if ((result=dup_slice_to_array(segment, htable, curr_slice,
                            curr_slice->ssize.offset, slice_end -
                            curr_slice->ssize.offset, sarray)) != 0)
            {
                return result;
            }

            break;
        } else {
            __sync_add_and_fetch(&curr_slice->ref_count, 1);
            if ((result=add_to_slice_rbuffer_array(sarray, curr_slice)) != 0) {
                return result;
            }
        }

        node = UNIQ_SKIPLIST_LEVEL0_NEXT_NODE(node);
    } while (node != ob->slices->factory->tail);

    return sarray->count > 0 ? 0 : ENOENT;
}

static int get_slice_count(OBEntry *ob, const FSBlockSliceKeyInfo *bs_key)
{
    UniqSkiplistNode *node;
    UniqSkiplistNode *previous;
    OBSliceEntry target;
    OBSliceEntry *curr_slice;
    int slice_end;
    int curr_end;
    int count;

    target.ssize = bs_key->slice;
    node = uniq_skiplist_find_ge_node(ob->slices, &target);
    if (node == NULL) {
        previous = UNIQ_SKIPLIST_LEVEL0_TAIL_NODE(ob->slices);
    } else {
        previous = UNIQ_SKIPLIST_LEVEL0_PREV_NODE(node);
    }

    count = 0;
    slice_end = bs_key->slice.offset + bs_key->slice.length;
    if (previous != ob->slices->top) {
        curr_slice = (OBSliceEntry *)previous->data;
        curr_end = curr_slice->ssize.offset + curr_slice->ssize.length;
        if (curr_end > bs_key->slice.offset) {  //overlap
            ++count;
        }
    }

    if (node == NULL) {
        return count;
    }

    do {
        curr_slice = (OBSliceEntry *)node->data;
        if (slice_end <= curr_slice->ssize.offset) {  //not overlap
            break;
        }

        ++count;
        curr_end = curr_slice->ssize.offset + curr_slice->ssize.length;
        if (curr_end > slice_end) {  //the last slice
            break;
        }

        node = UNIQ_SKIPLIST_LEVEL0_NEXT_NODE(node);
    } while (node != ob->slices->factory->tail);

    return count;
}

static void free_slices(OBSliceReadBufferArray *array)
{
    OBSliceReadBufferPair *pair;
    OBSliceReadBufferPair *end;

    if (array->count == 0) {
        return;
    }

    end = array->pairs + array->count;
    for (pair=array->pairs; pair<end; pair++) {
        ob_index_free_slice(pair->slice);
    }

    array->count = 0;
}

int ob_index_get_slices_ex(OBHashtable *htable,
        const FSBlockSliceKeyInfo *bs_key,
        OBSliceReadBufferArray *sarray)
{
    OBEntry *ob;
    int result;

    OB_INDEX_SET_BUCKET_AND_SEGMENT(htable, bs_key->block);
    sarray->count = 0;

    /*
    logInfo("file: "__FILE__", line: %d, func: %s, "
            "block key: %"PRId64", offset: %"PRId64,
            __LINE__, __FUNCTION__, bs_key->block.oid, bs_key->block.offset);
            */

    PTHREAD_MUTEX_LOCK(&segment->lcp.lock);
    ob = get_ob_entry(segment, htable, bucket, &bs_key->block, false);
    if (ob == NULL) {
        result = ENOENT;
    } else {
        result = get_slices(segment, htable, ob, bs_key, sarray);
    }
    PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);

    if (result != 0 && sarray->count > 0) {
        free_slices(sarray);
    }
    return result;
}

int ob_index_get_slice_count_ex(OBHashtable *htable,
        const FSBlockSliceKeyInfo *bs_key)
{
    OBEntry *ob;
    int count;

    OB_INDEX_SET_BUCKET_AND_SEGMENT(htable, bs_key->block);
    PTHREAD_MUTEX_LOCK(&segment->lcp.lock);
    ob = get_ob_entry(segment, htable, bucket, &bs_key->block, false);
    if (ob == NULL) {
        count = 0;
    } else {
        count = get_slice_count(ob, bs_key);
    }
    PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);

    return count;
}

void ob_index_get_ob_and_slice_counts(int64_t *ob_count, int64_t *slice_count)
{
    OBSegment *segment;
    OBSegment *end;

    *ob_count = *slice_count = 0;
    end = ob_shared_ctx.segment_array.segments +
        ob_shared_ctx.segment_array.count;
    for (segment=ob_shared_ctx.segment_array.segments;
            segment<end; segment++)
    {
        *ob_count += segment->allocators.ob.info.element_used_count;
        *slice_count += segment->allocators.slice.info.element_used_count;
    }
}

int64_t ob_index_get_total_slice_count()
{
    int64_t slice_count;
    OBSegment *segment;
    OBSegment *end;

    slice_count = 0;
    end = ob_shared_ctx.segment_array.segments +
        ob_shared_ctx.segment_array.count;
    for (segment=ob_shared_ctx.segment_array.segments;
            segment<end; segment++)
    {
        slice_count += segment->allocators.slice.info.element_used_count;
    }

    return slice_count;
}

typedef struct {
    SFBufferedWriter *writer;
    short is_my_data_group;
    short exclude_path_index;
    int source;
    time_t current_time;
    int64_t current_sn;
    int64_t total_slice_count;
} DumpSlicesContext;

static int walk_callback_for_dump_slices(const FSBlockKey *bkey, void *arg)
{
    DumpSlicesContext *dump_ctx;
    int result;
    OBSegment *segment;
    const SFSerializerFieldValue *fv;
    string_t *s;
    string_t *end;
    int64_t old_sn;
    int64_t data_version;
    int is_my_data_group;
    DASliceType slice_type;
    FSSliceSize ssize;
    DATrunkSpaceInfo space;

    dump_ctx = arg;
    if (dump_ctx->is_my_data_group >= 0) {
        is_my_data_group = fs_is_my_data_group(
                FS_DATA_GROUP_ID(*bkey)) ? 1 : 0;
        if (is_my_data_group != dump_ctx->is_my_data_group) {
            return 0;
        }
    }

    segment = ob_shared_ctx.segment_array.segments;
    if ((result=block_serializer_fetch_and_unpack(&segment->
                    db_fetch_ctx, bkey, &fv)) != 0)
    {
        return result;
    }
    if (fv == NULL) {
        return 0;
    }

    old_sn = dump_ctx->current_sn;
    end = fv->value.str_array.strings + fv->value.str_array.count;
    for (s=fv->value.str_array.strings; s<end; s++) {
        if ((result=block_serializer_parse_slice_ex(s, &data_version,
                        &slice_type, &ssize, &space)) != 0)
        {
            return result;
        }

        if (dump_ctx->exclude_path_index >= 0) {
            if (space.store->index == dump_ctx->exclude_path_index) {
                continue;
            }
        }

        if (SF_BUFFERED_WRITER_REMAIN(*dump_ctx->writer) <
                FS_SLICE_BINLOG_MAX_RECORD_SIZE)
        {
            if ((result=sf_buffered_writer_save(dump_ctx->writer)) != 0) {
                return result;
            }
        }
        dump_ctx->writer->buffer.current += slice_binlog_log_add_slice_to_buff1(
                slice_type, bkey, &ssize, &space, dump_ctx->current_time,
                ++(dump_ctx->current_sn), data_version, dump_ctx->source,
                dump_ctx->writer->buffer.current);
    }

    dump_ctx->total_slice_count += (dump_ctx->current_sn - old_sn);
    return 0;
}

int ob_index_dump_slices_to_file_ex(OBHashtable *htable,
        const int64_t start_index, const int64_t end_index,
        const char *filename, int64_t *slice_count, const int source,
        const bool need_padding, const bool need_lock)
{
    const int64_t data_version = 0;
    int result;
    int i;
    int64_t current_sn;
    SFBufferedWriter writer;
    OBSegment *segment = NULL;
    OBEntry **bucket;
    OBEntry **end;
    OBEntry *ob;
    time_t current_time;
    OBSliceEntry *slice;
    UniqSkiplistIterator it;

    current_sn = 0;
    *slice_count = 0;
    if ((result=sf_buffered_writer_init(&writer, filename)) != 0) {
        return result;
    }

    current_time = g_current_time;
    if (STORAGE_ENABLED) {
        if (end_index == htable->capacity) {
            DumpSlicesContext dump_ctx;

            if (source == BINLOG_SOURCE_REBUILD) {
                dump_ctx.exclude_path_index = DATA_REBUILD_PATH_INDEX;
                dump_ctx.is_my_data_group = -1;
            } else {
                dump_ctx.exclude_path_index = -1;
                dump_ctx.is_my_data_group = 1;
            }
            dump_ctx.writer = &writer;
            dump_ctx.source = source;
            dump_ctx.current_time = current_time;
            dump_ctx.current_sn = 0;
            dump_ctx.total_slice_count = 0;
            if ((result=STORAGE_ENGINE_WALK_API(walk_callback_for_dump_slices,
                            &dump_ctx)) == 0)
            {
                *slice_count = dump_ctx.total_slice_count;
            }
        }
    } else {
        end = htable->buckets + end_index;
        for (bucket=htable->buckets+start_index; result == 0 &&
                bucket<end && SF_G_CONTINUE_FLAG; bucket++)
        {
            if (need_lock) {
                segment = ob_shared_ctx.segment_array.segments +
                    (bucket - htable->buckets) %
                    ob_shared_ctx.segment_array.count;
                PTHREAD_MUTEX_LOCK(&segment->lcp.lock);
            }

            if (*bucket == NULL) {
                if (need_lock) {
                    PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);
                }
                continue;
            }

            ob = *bucket;
            do {
                uniq_skiplist_iterator(ob->slices, &it);
                while ((slice=(OBSliceEntry *)uniq_skiplist_next(&it)) != NULL) {
                    if (slice->type == DA_SLICE_TYPE_CACHE) {
                        continue;
                    }

                    ++(*slice_count);
                    if (SF_BUFFERED_WRITER_REMAIN(writer) <
                            FS_SLICE_BINLOG_MAX_RECORD_SIZE)
                    {
                        if ((result=sf_buffered_writer_save(&writer)) != 0) {
                            break;
                        }
                    }

                    writer.buffer.current += slice_binlog_log_add_slice_to_buff_ex(
                            slice, current_time, ++current_sn, slice->data_version,
                            source, writer.buffer.current);
                }

                ob = ob->next;
            } while (ob != NULL && result == 0);

            if (need_lock) {
                PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);
            }
        }
    }

    if (!SF_G_CONTINUE_FLAG) {
        result = EINTR;
    }

    if ((need_padding || *slice_count == 0) && result == 0) {
        FSBlockKey bkey;
        int count;

        bkey.oid = 1;
        bkey.offset = 0;
        count = (need_padding ? LOCAL_BINLOG_CHECK_LAST_SECONDS : 1);
        for (i=1; i<=count; i++) {
            if (SF_BUFFERED_WRITER_REMAIN(writer) <
                    FS_SLICE_BINLOG_MAX_RECORD_SIZE)
            {
                if ((result=sf_buffered_writer_save(&writer)) != 0) {
                    break;
                }
            }

            writer.buffer.current += slice_binlog_log_no_op_to_buff_ex(
                    &bkey, current_time + i, ++current_sn, data_version,
                    source, writer.buffer.current);
        }
    }

    if (result == 0 && SF_BUFFERED_WRITER_LENGTH(writer) > 0) {
        result = sf_buffered_writer_save(&writer);
    }

    sf_buffered_writer_destroy(&writer);
    return result;
}

static int realloc_slice_parray(OBSlicePtrArray *array)
{
    OBSliceEntry **new_slices;
    int new_alloc;
    int bytes;

    new_alloc = 2 * array->alloc;
    bytes = sizeof(OBSliceEntry *) * new_alloc;
    new_slices = (OBSliceEntry **)fc_malloc(bytes);
    if (new_slices == NULL) {
        return ENOMEM;
    }

    memcpy(new_slices, array->slices, array->count *
            sizeof(OBSliceEntry *));
    free(array->slices);

    array->slices = new_slices;
    array->alloc = new_alloc;
    return 0;
}

typedef struct {
    short is_my_data_group;
    short rebuild_path_index;
    int source;
    OBSlicePtrArray slice_parray;
    SFBufferedWriter writer;
    int64_t total_slice_count;
} RemoveSliceContext;

static inline int write_slice_index_to_file(const FSBlockKey *bkey,
        const int slice_type, const FSSliceSize *ssize,
        SFBufferedWriter *writer)
{
    int result;

    if (SF_BUFFERED_WRITER_REMAIN(*writer) <
            FS_SLICE_BINLOG_MAX_RECORD_SIZE)
    {
        if ((result=sf_buffered_writer_save(writer)) != 0) {
            return result;
        }
    }

    writer->buffer.current += rebuild_binlog_log_to_buff(
            slice_type == DA_SLICE_TYPE_FILE ?
            BINLOG_OP_TYPE_WRITE_SLICE :
            BINLOG_OP_TYPE_ALLOC_SLICE,
            bkey, ssize, writer->buffer.current);
    return 0;
}

#define SET_SLICE_TYPE_SSIZE(_slice_type, _ssize, slice) \
    _slice_type = (slice)->type; \
    _ssize = (slice)->ssize


static inline int remove_trunk_to_file(const FSBlockKey *bkey,
        RemoveSliceContext *ctx)
{
    int result;
    FSSliceSize ssize;

    if (SF_BUFFERED_WRITER_REMAIN(ctx->writer) <
            FS_SLICE_BINLOG_MAX_RECORD_SIZE)
    {
        if ((result=sf_buffered_writer_save(&ctx->writer)) != 0) {
            return result;
        }
    }

    ssize.offset = 0;
    ssize.length = FILE_BLOCK_SIZE;
    ctx->writer.buffer.current += rebuild_binlog_log_to_buff(
            BINLOG_OP_TYPE_WRITE_SLICE, bkey, &ssize,
            ctx->writer.buffer.current);
    return 0;
}

static int remove_slices_to_file(OBEntry *ob, RemoveSliceContext *ctx)
{
    int result;
    int slice_type;
    FSSliceSize ssize;
    OBSliceEntry **sp;
    OBSliceEntry **se;

    SET_SLICE_TYPE_SSIZE(slice_type, ssize, ctx->slice_parray.slices[0]);
    se = ctx->slice_parray.slices + ctx->slice_parray.count;
    for (sp=ctx->slice_parray.slices+1; sp<se; sp++) {
        if ((*sp)->ssize.offset == (ssize.offset + ssize.length)
                && (*sp)->type == slice_type)
        {
            ssize.length += (*sp)->ssize.length;
            continue;
        }

        if ((result=write_slice_index_to_file(&ob->bkey, slice_type,
                        &ssize, &ctx->writer)) != 0)
        {
            return result;
        }

        SET_SLICE_TYPE_SSIZE(slice_type, ssize, *sp);
    }

    if ((result=write_slice_index_to_file(&ob->bkey, slice_type,
                    &ssize, &ctx->writer)) != 0)
    {
        return result;
    }

    for (sp=ctx->slice_parray.slices; sp<se; sp++) {
        uniq_skiplist_delete(ob->slices, *sp);
    }

    return 0;
}

static int walk_callback_for_remove_slices(const FSBlockKey *bkey, void *arg)
{
    int result;
    RemoveSliceContext *ctx;
    OBSegment *segment;
    const SFSerializerFieldValue *fv;
    string_t *s;
    string_t *end;
    int64_t data_version;
    int is_my_data_group;
    DASliceType slice_type;
    FSSliceSize ssize;
    int prev_slice_type;
    FSSliceSize prev_ssize;
    DATrunkSpaceInfo space;

    ctx = arg;
    if (ctx->is_my_data_group >= 0) {
        is_my_data_group = fs_is_my_data_group(
                FS_DATA_GROUP_ID(*bkey)) ? 1 : 0;
        if (is_my_data_group == ctx->is_my_data_group) {
            if ((result=remove_trunk_to_file(bkey, ctx)) != 0) {
                return result;
            }
            ctx->total_slice_count++;
        }

        return 0;
    }

    if (ctx->rebuild_path_index < 0) {
        return 0;
    }

    segment = ob_shared_ctx.segment_array.segments;
    if ((result=block_serializer_fetch_and_unpack(&segment->
                    db_fetch_ctx, bkey, &fv)) != 0)
    {
        return result;
    }
    if (fv == NULL) {
        return 0;
    }

    prev_slice_type = -1;
    prev_ssize.offset = -1;
    prev_ssize.length = 0;
    end = fv->value.str_array.strings + fv->value.str_array.count;
    for (s=fv->value.str_array.strings; s<end; s++) {
        if ((result=block_serializer_parse_slice_ex(s, &data_version,
                        &slice_type, &ssize, &space)) != 0)
        {
            return result;
        }

        if (space.store->index != ctx->rebuild_path_index) {
            continue;
        }

        if (ssize.offset == (prev_ssize.offset + prev_ssize.length)
                && slice_type == prev_slice_type)
        {
            prev_ssize.length += ssize.length;
            continue;
        }

        if (prev_slice_type > 0) {
            ctx->total_slice_count++;
            if ((result=write_slice_index_to_file(bkey, prev_slice_type,
                            &prev_ssize, &ctx->writer)) != 0)
            {
                return result;
            }
        }

        prev_slice_type = slice_type;
        prev_ssize = ssize;
    }

    if (prev_slice_type > 0) {
        ctx->total_slice_count++;
        return write_slice_index_to_file(bkey, prev_slice_type,
                &prev_ssize, &ctx->writer);
    } else {
        return 0;
    }
}

int ob_index_remove_slices_to_file_ex(OBHashtable *htable,
        const int64_t start_index, const int64_t end_index,
        const char *filename, int64_t *slice_count, const int source)
{
    int result;
    int bytes;
    RemoveSliceContext ctx;
    OBEntry **bucket;
    OBEntry **end;
    OBEntry *ob;
    OBEntry *current;
    OBEntry *previous;
    OBSegment *segment;
    OBSliceEntry *slice;
    OBSliceEntry **sp;
    UniqSkiplistIterator it;

    *slice_count = 0;
    if ((result=sf_buffered_writer_init(&ctx.writer, filename)) != 0) {
        return result;
    }

    if (STORAGE_ENABLED) {
        if (end_index == htable->capacity) {
            if (source == BINLOG_SOURCE_REBUILD) {
                ctx.rebuild_path_index = DATA_REBUILD_PATH_INDEX;
                ctx.is_my_data_group = -1;
            } else {
                ctx.rebuild_path_index = -1;
                ctx.is_my_data_group = 0;
            }
            ctx.source = source;
            ctx.total_slice_count = 0;
            if ((result=STORAGE_ENGINE_WALK_API(walk_callback_for_remove_slices,
                            &ctx)) == 0)
            {
                *slice_count = ctx.total_slice_count;
            }
        }
    } else {
        ctx.slice_parray.alloc = 16 * 1024;
        bytes = sizeof(OBSliceEntry *) * ctx.slice_parray.alloc;
        if ((ctx.slice_parray.slices=fc_malloc(bytes)) == NULL) {
            return ENOMEM;
        }

        end = htable->buckets + end_index;
        for (bucket=htable->buckets+start_index; result == 0 &&
                bucket<end && SF_G_CONTINUE_FLAG; bucket++)
        {
            if (*bucket == NULL) {
                continue;
            }

            segment = ob_shared_ctx.segment_array.segments + (bucket -
                    htable->buckets) % ob_shared_ctx.segment_array.count;
            previous = NULL;
            ob = *bucket;
            while (ob != NULL) {
                if (source == BINLOG_SOURCE_MIGRATE_CLEAN) {
                    if (fs_is_my_data_group(FS_DATA_GROUP_ID(ob->bkey))) {
                        previous = ob;
                    } else {
                        if ((result=remove_trunk_to_file(&ob->bkey,
                                        &ctx)) != 0)
                        {
                            return result;
                        }
                    }

                    ob = ob->next;
                    continue;
                }

                sp = ctx.slice_parray.slices;
                uniq_skiplist_iterator(ob->slices, &it);
                while ((slice=(OBSliceEntry *)uniq_skiplist_next(&it)) != NULL) {
                    if (source == BINLOG_SOURCE_REBUILD) {
                        if (slice->space.store->index != DATA_REBUILD_PATH_INDEX) {
                            continue;
                        }
                    } else if (source != BINLOG_SOURCE_MIGRATE_CLEAN) {
                        continue;
                    }

                    if (sp - ctx.slice_parray.slices == ctx.slice_parray.alloc) {
                        ctx.slice_parray.count = sp - ctx.slice_parray.slices;
                        if ((result=realloc_slice_parray(&ctx.slice_parray)) != 0) {
                            return result;
                        }
                        sp = ctx.slice_parray.slices + ctx.slice_parray.count;
                    }
                    *sp++ = slice;
                }

                ctx.slice_parray.count = sp - ctx.slice_parray.slices;
                if (ctx.slice_parray.count > 0) {
                    if ((result=remove_slices_to_file(ob, &ctx)) != 0) {
                        return result;
                    }
                    *slice_count += ctx.slice_parray.count;

                    if (uniq_skiplist_empty(ob->slices)) {
                        current = ob;
                        ob = ob->next;
                        ob_entry_remove(segment, htable, bucket, current, previous);
                    } else {
                        previous = ob;
                        ob = ob->next;
                    }
                } else {
                    previous = ob;
                    ob = ob->next;
                }
            }
        }
        free(ctx.slice_parray.slices);
    }

    if (!SF_G_CONTINUE_FLAG) {
        result = EINTR;
    }

    if (result == 0 && SF_BUFFERED_WRITER_LENGTH(ctx.writer) > 0) {
        result = sf_buffered_writer_save(&ctx.writer);
    }

    sf_buffered_writer_destroy(&ctx.writer);
    return result;
}

int ob_index_remove_slices_to_file_for_reclaim_ex(OBHashtable *htable,
        const char *filename, int64_t *slice_count)
{
    const int source = BINLOG_SOURCE_MIGRATE_CLEAN;
    int result;
    uint64_t sn;
    SFBufferedWriter writer;
    OBEntry **bucket;
    OBEntry **end;
    OBEntry *ob;
    OBEntry *current;
    OBEntry *previous;
    OBSegment *segment;
    OBSliceEntry *slice;
    time_t current_time;
    UniqSkiplistIterator it;

    *slice_count = 0;
    if ((result=sf_buffered_writer_init(&writer, filename)) != 0) {
        return result;
    }

    current_time = g_current_time;
    if (STORAGE_ENABLED) {
        DumpSlicesContext dump_ctx;

        dump_ctx.exclude_path_index = -1;
        dump_ctx.is_my_data_group = 0;
        dump_ctx.writer = &writer;
        dump_ctx.source = source;
        dump_ctx.current_time = current_time;
        dump_ctx.current_sn = 0;
        dump_ctx.total_slice_count = 0;
        if ((result=STORAGE_ENGINE_WALK_API(walk_callback_for_dump_slices,
                        &dump_ctx)) == 0)
        {
            *slice_count = dump_ctx.total_slice_count;
        }
    } else {
        sn = 0;
        end = htable->buckets + htable->capacity;
        for (bucket=htable->buckets; result == 0 && bucket<end &&
                SF_G_CONTINUE_FLAG; bucket++)
        {
            if (*bucket == NULL) {
                continue;
            }

            segment = ob_shared_ctx.segment_array.segments + (bucket - htable->
                    buckets) % ob_shared_ctx.segment_array.count;
            previous = NULL;
            ob = *bucket;
            while (ob != NULL) {
                if (fs_is_my_data_group(FS_DATA_GROUP_ID(ob->bkey))) {
                    previous = ob;
                    ob = ob->next;
                    continue;
                }

                uniq_skiplist_iterator(ob->slices, &it);
                while ((slice=(OBSliceEntry *)uniq_skiplist_next(&it)) != NULL) {
                    if (SF_BUFFERED_WRITER_REMAIN(writer) <
                            FS_SLICE_BINLOG_MAX_RECORD_SIZE)
                    {
                        if ((result=sf_buffered_writer_save(&writer)) != 0) {
                            return result;
                        }
                    }
                    writer.buffer.current += slice_binlog_log_add_slice_to_buff_ex(
                            slice, g_current_time, ++sn, slice->data_version,
                            source, writer.buffer.current);
                    ++(*slice_count);
                }

                current = ob;
                ob = ob->next;
                uniq_skiplist_clear(current->slices);
                ob_entry_remove(segment, htable, bucket, current, previous);
            }
        }
    }

    if (!SF_G_CONTINUE_FLAG) {
        result = EINTR;
    }

    if (result == 0 && SF_BUFFERED_WRITER_LENGTH(writer) > 0) {
        result = sf_buffered_writer_save(&writer);
    }

    sf_buffered_writer_destroy(&writer);
    return result;
}

static int do_write_replica_binlog(OBEntry *ob, const int slice_type,
        const FSSliceSize *ssize, const int64_t data_version,
        SFBufferedWriter *writer)
{
    int result;
    FSBlockSliceKeyInfo bs_key;

    if (SF_BUFFERED_WRITER_REMAIN(*writer) <
            FS_REPLICA_BINLOG_MAX_RECORD_SIZE)
    {
        if ((result=sf_buffered_writer_save(writer)) != 0) {
            return result;
        }
    }

    bs_key.block = ob->bkey;
    bs_key.slice = *ssize;
    writer->buffer.current += replica_binlog_log_slice_to_buff(
            g_current_time, data_version, &bs_key, BINLOG_SOURCE_DUMP,
            slice_type == DA_SLICE_TYPE_FILE ? BINLOG_OP_TYPE_WRITE_SLICE :
            BINLOG_OP_TYPE_ALLOC_SLICE, writer->buffer.current);
    return 0;
}

static int dump_replica_binlog_to_file(OBEntry *ob, SFBufferedWriter
        *writer, int64_t *total_slice_count, int64_t *total_replica_count)
{
    int result;
    int slice_type;
    FSSliceSize ssize;
    UniqSkiplistIterator it;
    OBSliceEntry *slice;

    uniq_skiplist_iterator(ob->slices, &it);
    if ((slice=(OBSliceEntry *)uniq_skiplist_next(&it)) == NULL) {
        return 0;
    }

    ++(*total_slice_count);
    ++(*total_replica_count);
    SET_SLICE_TYPE_SSIZE(slice_type, ssize, slice);
    while ((slice=(OBSliceEntry *)uniq_skiplist_next(&it)) != NULL) {
        if (slice->type == DA_SLICE_TYPE_CACHE) {
            continue;
        }

        ++(*total_slice_count);
        if (slice->ssize.offset == (ssize.offset + ssize.length)
                && slice->type == slice_type)
        {
            ssize.length += slice->ssize.length;
            continue;
        }

        if ((result=do_write_replica_binlog(ob, slice_type, &ssize,
                        *total_replica_count, writer)) != 0)
        {
            return result;
        }

        ++(*total_replica_count);
        SET_SLICE_TYPE_SSIZE(slice_type, ssize, slice);
    }

    if ((result=do_write_replica_binlog(ob, slice_type, &ssize,
                    *total_replica_count, writer)) != 0)
    {
        return result;
    }

    return 0;
}

int ob_index_dump_replica_binlog_to_file_ex(OBHashtable *htable,
        const int data_group_id, const int64_t padding_data_version,
        const char *filename, int64_t *total_slice_count,
        int64_t *total_replica_count)
{
    int result;
    SFBufferedWriter writer;
    OBSegment *segment;
    OBEntry **bucket;
    OBEntry **end;
    OBEntry *ob;

    *total_slice_count = 0;
    *total_replica_count = 0;
    if ((result=sf_buffered_writer_init(&writer, filename)) != 0) {
        return result;
    }

    end = htable->buckets + htable->capacity;
    for (bucket=htable->buckets; result == 0 &&
            bucket<end && SF_G_CONTINUE_FLAG; bucket++)
    {
        segment = ob_shared_ctx.segment_array.segments +
            (bucket - htable->buckets) %
            ob_shared_ctx.segment_array.count;
        PTHREAD_MUTEX_LOCK(&segment->lcp.lock);

        if (*bucket == NULL) {
            PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);
            continue;
        }

        ob = *bucket;
        do {
            if (FS_DATA_GROUP_ID(ob->bkey) == data_group_id) {
                if ((result=dump_replica_binlog_to_file(ob, &writer,
                                total_slice_count, total_replica_count)) != 0)
                {
                    break;
                }
            }
            ob = ob->next;
        } while (ob != NULL && result == 0);

        PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);
    }

    if (!SF_G_CONTINUE_FLAG) {
        result = EINTR;
    }

    if (result == 0) {
        if (SF_BUFFERED_WRITER_LENGTH(writer) > 0) {
            result = sf_buffered_writer_save(&writer);
        }

        if (result == 0 && padding_data_version > *total_replica_count) {
            FSBlockKey bkey;
            fs_fill_padding_bkey(data_group_id, &bkey);
            writer.buffer.current += replica_binlog_log_block_to_buff(
                    g_current_time, padding_data_version, &bkey,
                    BINLOG_SOURCE_DUMP, BINLOG_OP_TYPE_NO_OP,
                    writer.buffer.current);
            result = sf_buffered_writer_save(&writer);
        }
    }

    sf_buffered_writer_destroy(&writer);
    return result;
}

#ifdef FS_DUMP_SLICE_FOR_DEBUG

#define FS_DUMP_SLICE_CALC_CRC32 32

typedef struct {
#if FS_DUMP_SLICE_FOR_DEBUG == FS_DUMP_SLICE_CALC_CRC32
    FSSliceBlockedOpContext bctx;
#endif
    SFBufferedWriter writer;
    int64_t total_block_count;
    int64_t total_slice_count;
} DumpSliceIndexContext;

#if FS_DUMP_SLICE_FOR_DEBUG == FS_DUMP_SLICE_CALC_CRC32
static inline int calc_slice_crc32(OBEntry *ob, const FSSliceSize *ssize,
        DumpSliceIndexContext *dump_ctx, int *crc32)
{
    int result;
    FSBlockSliceKeyInfo bs_key;
#ifdef OS_LINUX
    AlignedReadBuffer **aligned_buffer;
    AlignedReadBuffer **aligned_bend;
#endif

    bs_key.block = ob->bkey;
    bs_key.slice = *ssize;
    if ((result=fs_slice_blocked_read(&dump_ctx->bctx,
                    &bs_key, 0)) != 0)
    {
        return result;
    }

#ifdef OS_LINUX
    *crc32 = CRC32_XINIT;
    aligned_bend = dump_ctx->bctx.op_ctx.aio_buffer_parray.buffers +
        dump_ctx->bctx.op_ctx.aio_buffer_parray.count;
    for (aligned_buffer=dump_ctx->bctx.op_ctx.aio_buffer_parray.buffers;
            aligned_buffer<aligned_bend; aligned_buffer++)
    {
        *crc32 = CRC32_ex((*aligned_buffer)->buff + (*aligned_buffer)->
                offset, (*aligned_buffer)->length, *crc32);
    }

    fs_release_aio_buffers(&dump_ctx->bctx.op_ctx);
#else
    *crc32 = CRC32(dump_ctx->bctx.op_ctx.info.buff,
            dump_ctx->bctx.op_ctx.done_bytes);
#endif

    return 0;
}
#endif

static int do_write_slice_index(OBEntry *ob, const int slice_type,
        const FSSliceSize *ssize, DumpSliceIndexContext *dump_ctx)
{
#if FS_DUMP_SLICE_FOR_DEBUG == FS_DUMP_SLICE_CALC_CRC32
    int result;
    int crc32;

    if ((result=calc_slice_crc32(ob, ssize, dump_ctx, &crc32)) != 0) {
        return result;
    }

    if (SF_BUFFERED_WRITER_REMAIN(dump_ctx->writer) <
            FS_SLICE_BINLOG_MAX_RECORD_SIZE)
    {
        if ((result=sf_buffered_writer_save(&dump_ctx->writer)) != 0) {
            return result;
        }
    }

    dump_ctx->writer.buffer.current += sprintf(dump_ctx->writer.buffer.
            current, "%c %"PRId64" %"PRId64" %d %d %d %u\n",
            slice_type == DA_SLICE_TYPE_FILE ? BINLOG_OP_TYPE_WRITE_SLICE :
            BINLOG_OP_TYPE_ALLOC_SLICE, ob->bkey.oid, ob->bkey.offset,
            ssize->offset, ssize->length, dump_ctx->bctx.op_ctx.done_bytes,
            crc32);

    return 0;
#else
    return write_slice_index_to_file(&ob->bkey, slice_type,
            ssize, &dump_ctx->writer);
#endif
}

static int dump_slice_index_to_file(OBEntry *ob,
        DumpSliceIndexContext *dump_ctx)
{
    int result;
    int slice_type;
    FSSliceSize ssize;
    UniqSkiplistIterator it;
    OBSliceEntry *slice;

    uniq_skiplist_iterator(ob->slices, &it);
    if ((slice=(OBSliceEntry *)uniq_skiplist_next(&it)) == NULL) {
        return 0;
    }

    dump_ctx->total_block_count++;
    dump_ctx->total_slice_count++;
    SET_SLICE_TYPE_SSIZE(slice_type, ssize, slice);
    while ((slice=(OBSliceEntry *)uniq_skiplist_next(&it)) != NULL) {
        if (slice->ssize.offset == (ssize.offset + ssize.length)
                && slice->type == slice_type)
        {
            ssize.length += slice->ssize.length;
            continue;
        }

        if ((result=do_write_slice_index(ob, slice_type,
                        &ssize, dump_ctx)) != 0)
        {
            return result;
        }

        dump_ctx->total_slice_count++;
        SET_SLICE_TYPE_SSIZE(slice_type, ssize, slice);
    }

    if ((result=do_write_slice_index(ob, slice_type,
                    &ssize, dump_ctx)) != 0)
    {
        return result;
    }

    return 0;
}

static int walk_callback_for_dump_slice(const FSBlockKey *bkey, void *arg)
{
    int result;
    DumpSliceIndexContext *dump_ctx;
    OBEntry *ob;

    dump_ctx = arg;
    OB_INDEX_SET_BUCKET_AND_SEGMENT(&G_OB_HASHTABLE, *bkey);
    PTHREAD_MUTEX_LOCK(&segment->lcp.lock);
    ob = get_ob_entry(segment, &G_OB_HASHTABLE, bucket, bkey, true);
    PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);
    if (ob != NULL) {
        result = dump_slice_index_to_file(ob, dump_ctx);
    } else {
        result = ENOMEM;
    }

    return result;
}

int ob_index_dump_slice_index_to_file(const char *filename,
        int64_t *total_block_count, int64_t *total_slice_count)
{
    int result;
    char tmp_filename[PATH_MAX];
    char cmd[2 * PATH_MAX];
    char output[1024];
    DumpSliceIndexContext dump_ctx;
    OBEntry **bucket;
    OBEntry **end;
    OBEntry *ob;

    *total_block_count = 0;
    *total_slice_count = 0;
    memset(&dump_ctx, 0, sizeof(dump_ctx));
#if FS_DUMP_SLICE_FOR_DEBUG == FS_DUMP_SLICE_CALC_CRC32
    if ((result=fs_slice_blocked_op_ctx_init(&dump_ctx.bctx)) != 0) {
        return result;
    }
#endif

    snprintf(tmp_filename, sizeof(tmp_filename), "%s.tmp", filename);
    if ((result=sf_buffered_writer_init(&dump_ctx.writer, tmp_filename)) != 0) {
        return result;
    }

    if (STORAGE_ENABLED) {
        if ((result=STORAGE_ENGINE_WALK_API(walk_callback_for_dump_slice,
                        &dump_ctx)) != 0)
        {
            return result;
        }
    } else {
        end = G_OB_HASHTABLE.buckets + G_OB_HASHTABLE.capacity;
        for (bucket=G_OB_HASHTABLE.buckets; result == 0 &&
                bucket<end && SF_G_CONTINUE_FLAG; bucket++)
        {
            if (*bucket == NULL) {
                continue;
            }

            ob = *bucket;
            do {
                if ((result=dump_slice_index_to_file(ob, &dump_ctx)) != 0) {
                    break;
                }

                ob = ob->next;
            } while (ob != NULL && result == 0);
        }
    }

    *total_block_count = dump_ctx.total_block_count;
    *total_slice_count = dump_ctx.total_slice_count;
    if (!SF_G_CONTINUE_FLAG) {
        result = EINTR;
    }

    if (result == 0 && SF_BUFFERED_WRITER_LENGTH(dump_ctx.writer) > 0) {
        result = sf_buffered_writer_save(&dump_ctx.writer);
    }

#if FS_DUMP_SLICE_FOR_DEBUG == FS_DUMP_SLICE_CALC_CRC32
    fs_slice_blocked_op_ctx_destroy(&dump_ctx.bctx);
#endif

    sf_buffered_writer_destroy(&dump_ctx.writer);
    if (result != 0) {
        return result;
    }

    snprintf(cmd, sizeof(cmd), "/usr/bin/sort -k2,5 -o %s %s 2>&1",
            filename, tmp_filename);
    if ((result=getExecResult(cmd, output, sizeof(output))) != 0) {
        logError("file: "__FILE__", line: %d, "
                "execute command \"%s\" fail, errno: %d, error info: %s",
                __LINE__, cmd, result, STRERROR(result));
    }
    if (*output != '\0') {
        logWarning("file: "__FILE__", line: %d, "
                "execute command \"%s\" output: %s",
                __LINE__, cmd, output);
    }
    unlink(tmp_filename);

    return result;
}
#endif

static int unpack_ob_entry(OBSegment *segment, OBEntry *ob,
        UniqSkiplist *sl, const SFSerializerFieldValue *fv)
{
    const int init_refer = 1;
    int result;
    string_t *s;
    string_t *end;
    struct fast_mblock_chain chain;
    OBSliceEntry *slice;

    if ((result=ob_alloc_slices_for_load(segment, ob, sl, fv->
                    value.str_array.count, &chain)) != 0)
    {
        return result;
    }

    end = fv->value.str_array.strings + fv->value.str_array.count;
    for (s=fv->value.str_array.strings; s<end; s++) {
        slice = (OBSliceEntry *)chain.head->data;
        chain.head = chain.head->next;
        OB_INDEX_INIT_SLICE(slice, ob, init_refer);
        if ((result=block_serializer_parse_slice(s, slice)) != 0) {
            return result;
        }

        if ((result=uniq_skiplist_insert(sl, slice)) != 0) {
            logError("file: "__FILE__", line: %d, add slice "
                    "to skiplist fail, errno: %d, error info: %s, "
                    "block {oid: %"PRId64", offset: %"PRId64"}, "
                    "slice {offset: %d, length: %d}", __LINE__,
                    result, STRERROR(result), ob->bkey.oid,
                    ob->bkey.offset, slice->ssize.offset,
                    slice->ssize.length);
            return result;
        }
    }

    return 0;
}

OBSegment *ob_index_get_segment(const FSBlockKey *bkey)
{
    OB_INDEX_SET_HASHTABLE_SEGMENT(&G_OB_HASHTABLE, *bkey);
    return segment;
}

int ob_index_load_db_slices(FSDBFetchContext *db_fetch_ctx,
        OBSegment *segment, OBEntry *ob)
{
    int result;

    if ((result=ob_index_alloc_db_slices(segment, ob)) != 0) {
        return result;
    }
    return ob_load_slices(db_fetch_ctx, segment, ob, ob->db_args->slices);
}

int ob_index_add_slice_by_db(OBSegment *segment, OBEntry *ob,
        const int64_t data_version, const DASliceType type,
        const FSSliceSize *ssize, const DATrunkSpaceInfo *space)
{
    const int init_refer = 1;
    const bool call_by_db_event_dealer = true;
    OBSliceEntry *slice;

    if ((slice=ob_slice_alloc_ex(segment, ob, init_refer,
                    call_by_db_event_dealer)) == NULL)
    {
        return ENOMEM;
    }
    slice->data_version = data_version;
    slice->type = type;
    slice->ssize = *ssize;
    slice->space = *space;
    return add_slice(segment, &G_OB_HASHTABLE, ob, ob->
            db_args->slices, slice, NULL, NULL, NULL,
            call_by_db_event_dealer);
}

int ob_index_delete_slice_by_db(OBSegment *segment,
        OBEntry *ob, const FSSliceSize *ssize)
{
    const bool call_by_db_event_dealer = true;
    int count;
    int result;
    FSBlockSliceKeyInfo bs_key;

    bs_key.block = ob->bkey;
    bs_key.slice = *ssize;
    result = delete_slice(segment, &G_OB_HASHTABLE, ob, ob->
            db_args->slices, &bs_key, &count, NULL, NULL,
            call_by_db_event_dealer);
    return (result == ENOENT ? 0 : result);
}

int ob_index_delete_block_by_db(OBSegment *segment, OBEntry *ob)
{
    if (ob->db_args->slices != NULL) {
        uniq_skiplist_free(ob->db_args->slices);
        ob->db_args->slices = NULL;
    }

    return 0;
}
