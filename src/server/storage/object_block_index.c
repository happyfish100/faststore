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
#include "../binlog/slice_binlog.h"
#include "../binlog/replica_binlog.h"
#include "../rebuild/rebuild_binlog.h"
#include "../db/change_notify.h"
#include "../db/block_serializer.h"
#include "slice_op.h"
#include "object_block_index.h"

#define SLICE_ARRAY_FIXED_COUNT  64

typedef struct {
    UniqSkiplistFactory factory;
    struct fast_mblock_man ob;     //for ob_entry
    struct fast_mblock_man slice;  //for slice_entry
} OBSharedAllocator;

typedef struct {
    int count;
    volatile uint32_t reclaim_index;  //for storage engine
    OBSegment *segments;
} OBSharedSegmentArray;

typedef struct {
    int count;
    OBSharedAllocator *allocators;
} OBSharedAllocatorArray;

typedef struct {
    int alloc;
    int count;
    OBSliceEntry **slices;
    OBSliceEntry *fixed[SLICE_ARRAY_FIXED_COUNT];
} OBSlicePtrSmartArray;

typedef struct {
    int64_t memory_limit;
    volatile int64_t malloc_bytes;
    OBSharedSegmentArray segment_array;
    OBSharedAllocatorArray allocator_array;
} OBSharedContext;

static inline int do_add_slice(OBHashtable *htable, OBEntry *ob,
        UniqSkiplist *sl, OBSliceEntry *slice,
        struct fc_queue_info *space_chain);
static inline int do_delete_slice(OBHashtable *htable, OBEntry *ob,
        UniqSkiplist *sl, OBSliceEntry *slice,
        struct fc_queue_info *space_chain);

static OBSharedContext ob_shared_ctx = {0, 0, {0, 0, NULL}, {0, NULL}};

OBHashtable g_ob_hashtable = {0, 0, NULL};

static OBEntry *get_ob_entry_ex(OBSegment *segment, OBHashtable *htable,
        OBEntry **bucket, const FSBlockKey *bkey, const bool create_flag,
        const bool need_reclaim, OBEntry **pprev);

#define OB_INDEX_SET_HASHTABLE_SEGMENT(htable, bkey) \
    int64_t bucket_index;  \
    OBSegment *segment; \
    do {  \
        bucket_index = FS_BLOCK_HASH_CODE(bkey) % (htable)->capacity; \
        segment = ob_shared_ctx.segment_array.segments + bucket_index % \
            ob_shared_ctx.segment_array.count;  \
    } while (0)


#define OB_INDEX_SET_HASHTABLE_ALLOCATOR(bkey) \
    OBSharedAllocator *allocator;  \
    do {  \
        allocator = ob_shared_ctx.allocator_array.allocators + \
            FS_BLOCK_HASH_CODE(bkey) % ob_shared_ctx.allocator_array.count; \
    } while (0)


#define OB_INDEX_SET_BUCKET_AND_SEGMENT(htable, bkey) \
    OBEntry **bucket;   \
    OB_INDEX_SET_HASHTABLE_SEGMENT(htable, bkey);  \
    do {  \
        bucket = (htable)->buckets + bucket_index; \
    } while (0)


static inline void ob_entry_remove(OBSegment *segment, OBHashtable *htable,
        OBEntry **bucket, OBEntry *ob, OBEntry *previous)
{
    if (previous == NULL) {
        *bucket = ob->next;
    } else {
        previous->next = ob->next;
    }

    FC_ATOMIC_DEC(htable->count);
    if (htable->need_reclaim) {
        FC_ATOMIC_DEC(segment->count);
        fc_list_del_init(&ob->db_args->dlink);
    }
    ob_index_ob_entry_release(ob);
}

static void block_reclaim(OBSegment *segment, const int64_t target_count)
{
    OBEntry *ob;
    OBEntry *tmp;
    OBEntry **bucket;
    OBEntry *previous;
    int64_t count;

    count = target_count;
    PTHREAD_MUTEX_LOCK(&segment->lcp.lock);
    fc_list_for_each_entry_safe(ob, tmp, &segment->lru, db_args->dlink) {
        if (ob->db_args->locked || FC_ATOMIC_GET(ob->db_args->ref_count) > 1) {
            continue;
        }

        bucket = g_ob_hashtable.buckets + FS_BLOCK_HASH_CODE(ob->bkey) %
            g_ob_hashtable.capacity;
        if (get_ob_entry_ex(segment, &g_ob_hashtable, bucket,
                    &ob->bkey, false, false, &previous) != ob)
        {
            logWarning("file: "__FILE__", line: %d, "
                    "find ob entry {oid: %"PRId64", offset: %"PRId64"} "
                    "fail!", __LINE__, ob->bkey.oid, ob->bkey.offset);
            continue;
        }
        ob_entry_remove(segment, &g_ob_hashtable, bucket, ob, previous);

        if (--count == 0) {
            break;
        }
    }
    PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);
}

static void *reclaim_and_alloc(struct fast_mblock_man *allocator)
{
    void *obj;
    OBSegment *segment;
    int64_t avg_count;
    int64_t current_count;
    int64_t target_count;

    avg_count = FC_ATOMIC_GET(g_ob_hashtable.count) /
        ob_shared_ctx.segment_array.count;
    while (1) {
        segment = ob_shared_ctx.segment_array.segments + __sync_fetch_and_add(
                &ob_shared_ctx.segment_array.reclaim_index, 1) %
            ob_shared_ctx.segment_array.count;
        current_count = FC_ATOMIC_GET(segment->count);
        if (current_count <= avg_count / 2) {
            continue;
        }

        target_count = current_count - avg_count;
        if (target_count <= 0) {
            target_count = 1;
        }
        block_reclaim(segment, target_count);

        obj = fast_mblock_alloc_object(allocator);
        if (obj != NULL) {
            return obj;
        }
    }

    return NULL;
}

static OBEntry *ob_entry_alloc(OBSegment *segment, OBHashtable *htable,
        const FSBlockKey *bkey)
{
    const int init_level_count = 2;
    OBEntry *ob;

    OB_INDEX_SET_HASHTABLE_ALLOCATOR(*bkey);
    ob = fast_mblock_alloc_object(&allocator->ob);
    if (ob == NULL) {
        if (!g_ob_hashtable.need_reclaim) {
            return NULL;
        }

        PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);
        ob = reclaim_and_alloc(&allocator->ob);
        PTHREAD_MUTEX_LOCK(&segment->lcp.lock);
        if (ob == NULL) {
            return NULL;
        }
    }

    ob->slices = uniq_skiplist_new(&allocator->factory, init_level_count);
    if (ob->slices == NULL) {
        fast_mblock_free_object(&allocator->ob, ob);
        return NULL;
    }

    FC_ATOMIC_INC(htable->count);
    if (htable->need_reclaim) {
        FC_ATOMIC_INC(ob->db_args->ref_count);
        FC_ATOMIC_INC(segment->count);
        fc_list_add_tail(&ob->db_args->dlink, &segment->lru);
    }

    return ob;
}

static int ob_load_slices(OBSegment *segment, OBEntry *ob, UniqSkiplist *sl)
{
    int result;
    string_t content;

    if ((result=STORAGE_ENGINE_FETCH_API(&ob->bkey, &segment->
                    db_fetch_ctx.read_ctx)) != 0)
    {
        if (result == ENOENT) {
            return 0;
        }

        logError("file: "__FILE__", line: %d, "
                "block {oid: %"PRId64", offset: %"PRId64"}, "
                "load slices from db fail, result: %d", __LINE__,
                ob->bkey.oid, ob->bkey.offset, result);
        return result;
    }

    FC_SET_STRING_EX(content, DA_OP_CTX_BUFFER_PTR(segment->
                db_fetch_ctx.read_ctx.op_ctx), DA_OP_CTX_BUFFER_LEN(
                    segment->db_fetch_ctx.read_ctx.op_ctx));
    return block_serializer_unpack(segment, ob, sl, &content);
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
    } else {
        cmpr = ob_index_compare_block_key(bkey, &(*bucket)->bkey);
        if (cmpr == 0) {
            *pprev = NULL;
            if (need_reclaim) {
                fc_list_move_tail(&(*bucket)->db_args->dlink, &segment->lru);
            }
            return *bucket;
        } else if (cmpr < 0) {
            *pprev = NULL;
        } else {
            *pprev = *bucket;
            while ((*pprev)->next != NULL) {
                cmpr = ob_index_compare_block_key(bkey, &(*pprev)->next->bkey);
                if (cmpr == 0) {
                    if (need_reclaim) {
                        fc_list_move_tail(&(*pprev)->next->
                                db_args->dlink, &segment->lru);
                    }
                    return (*pprev)->next;
                } else if (cmpr < 0) {
                    break;
                }

                *pprev = (*pprev)->next;
            }
        }
    }

    if (!create_flag) {
        return NULL;
    }

    if ((ob=ob_entry_alloc(segment, htable, bkey)) == NULL) {
        return NULL;
    }
    ob->bkey = *bkey;

    if (STORAGE_ENABLED) {
        if (ob_load_slices(segment, ob, ob->slices) != 0) {
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
        const FSBlockKey *bkey)
{
    OBEntry *ob;
    OB_INDEX_SET_BUCKET_AND_SEGMENT(htable, *bkey);

    PTHREAD_MUTEX_LOCK(&segment->lcp.lock);
    ob = get_ob_entry(segment, htable, bucket, bkey, false);
    PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);

    return ob;
}

#define OB_INDEX_INIT_SLICE(slice, _ob, init_refer) \
    if (init_refer > 0) {  \
        __sync_add_and_fetch(&slice->ref_count, init_refer); \
    } \
    slice->ob = _ob

static inline OBSliceEntry *ob_alloc_slice_for_load(OBSegment *segment,
        OBSharedAllocator *allocator, OBEntry *ob, const int init_refer)
{
    OBSliceEntry *slice;

    slice = fast_mblock_alloc_object(&allocator->slice);
    if (slice != NULL) {
        OB_INDEX_INIT_SLICE(slice, ob, init_refer);
        return slice;
    }

    ob->db_args->locked = true;
    PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);

    if ((slice=reclaim_and_alloc(&allocator->slice)) != NULL) {
        OB_INDEX_INIT_SLICE(slice, ob, init_refer);
    }

    ob->db_args->locked = false;
    PTHREAD_MUTEX_LOCK(&segment->lcp.lock);
    return slice;
}

static inline OBSliceEntry *ob_slice_alloc(OBSegment *segment,
        OBEntry *ob, const int init_refer)
{
    OBSliceEntry *slice;

    OB_INDEX_SET_HASHTABLE_ALLOCATOR(ob->bkey);
    slice = fast_mblock_alloc_object(&allocator->slice);
    if (slice != NULL) {
        OB_INDEX_INIT_SLICE(slice, ob, init_refer);
        return slice;
    }

    if (!g_ob_hashtable.need_reclaim) {
        return NULL;
    }

    ob->db_args->locked = true;
    PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);

    if ((slice=reclaim_and_alloc(&allocator->slice)) != NULL) {
        OB_INDEX_INIT_SLICE(slice, ob, init_refer);
    }

    PTHREAD_MUTEX_LOCK(&segment->lcp.lock);
    ob->db_args->locked = false;
    return slice;
}

OBSliceEntry *ob_index_alloc_slice_ex(OBHashtable *htable,
        const FSBlockKey *bkey, const int init_refer)
{
    OBEntry *ob;
    OBSliceEntry *slice;

    OB_INDEX_SET_BUCKET_AND_SEGMENT(htable, *bkey);
    PTHREAD_MUTEX_LOCK(&segment->lcp.lock);
    ob = get_ob_entry(segment, htable, bucket, bkey, true);
    if (ob == NULL) {
        slice = NULL;
    } else {
        slice = ob_slice_alloc(segment, ob, init_refer);
    }
    PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);

    return slice;
}

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

static void block_malloc_trunk_notify_func(const int alloc_bytes, void *args)
{
    if (alloc_bytes > 0) {
        __sync_add_and_fetch(&ob_shared_ctx.malloc_bytes, alloc_bytes);
    } else {
        __sync_sub_and_fetch(&ob_shared_ctx.malloc_bytes, -1 * alloc_bytes);
    }
}

static int init_ob_shared_allocator_array(
        OBSharedAllocatorArray *allocator_array)
{
    int result;
    int bytes;
    const int max_level_count = 8;
    const int alloc_skiplist_once = 8 * 1024;
    const int min_alloc_elements_once = 2;
    const int delay_free_seconds = 0;
    const bool bidirection = true;  //need previous link
    const bool allocator_use_lock = true;
    int element_size;
    OBSharedAllocator *allocator;
    OBSharedAllocator *end;
    struct {
        struct fast_mblock_trunk_callbacks holder;
        struct fast_mblock_trunk_callbacks *ptr;
    } trunk_callbacks;
    struct fast_mblock_object_callbacks obj_callbacks_obentry;
    struct fast_mblock_object_callbacks obj_callbacks_slice;

    allocator_array->count = OB_SHARED_ALLOCATOR_COUNT;
    bytes = sizeof(OBSharedAllocator) * allocator_array->count;
    allocator_array->allocators = (OBSharedAllocator *)fc_malloc(bytes);
    if (allocator_array->allocators == NULL) {
        return ENOMEM;
    }

    if (STORAGE_ENABLED) {
        element_size = sizeof(OBEntry) + sizeof(OBDBArgs);
        ob_shared_ctx.memory_limit = (int64_t)
            (SYSTEM_TOTAL_MEMORY * STORAGE_MEMORY_LIMIT *
             MEMORY_LIMIT_LEVEL0_RATIO);
        if (ob_shared_ctx.memory_limit < 128 * 1024 * 1024)
        {
            ob_shared_ctx.memory_limit = 128 * 1024 * 1024;
        }

        trunk_callbacks.holder.check_func = block_malloc_trunk_check;
        trunk_callbacks.holder.notify_func = block_malloc_trunk_notify_func;
        trunk_callbacks.holder.args = NULL;
        trunk_callbacks.ptr = &trunk_callbacks.holder;
    } else {
        element_size = sizeof(OBEntry);
        trunk_callbacks.ptr = NULL;
    }

    obj_callbacks_obentry.init_func = (fast_mblock_object_init_func)
        ob_alloc_init;
    obj_callbacks_obentry.destroy_func = NULL;
    obj_callbacks_slice.init_func = (fast_mblock_object_init_func)
        slice_alloc_init;
    obj_callbacks_slice.destroy_func = NULL;

    end = allocator_array->allocators + allocator_array->count;
    for (allocator=allocator_array->allocators; allocator<end; allocator++) {
        if ((result=uniq_skiplist_init_ex2(&allocator->factory, max_level_count,
                        slice_compare, slice_free_func, alloc_skiplist_once,
                        min_alloc_elements_once, delay_free_seconds,
                        bidirection, allocator_use_lock)) != 0)
        {
            return result;
        }

        obj_callbacks_obentry.args = &allocator->ob;
        if ((result=fast_mblock_init_ex2(&allocator->ob, "ob_entry",
                        element_size, 16 * 1024, 0, &obj_callbacks_obentry,
                        true, trunk_callbacks.ptr)) != 0)
        {
            return result;
        }

        obj_callbacks_slice.args = &allocator->slice;
        if ((result=fast_mblock_init_ex2(&allocator->slice,
                        "slice_entry", sizeof(OBSliceEntry),
                        64 * 1024, 0, &obj_callbacks_slice,
                        true, trunk_callbacks.ptr)) != 0)
        {
            return result;
        }

        if (STORAGE_ENABLED) {
            fast_mblock_set_exceed_silence(&allocator->ob);
            fast_mblock_set_exceed_silence(&allocator->slice);
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

int ob_index_init_htable_ex(OBHashtable *htable, const int64_t capacity)
{
    int64_t bytes;

    htable->capacity = fc_ceil_prime(capacity);
    bytes = sizeof(OBEntry *) * htable->capacity;
    htable->buckets = (OBEntry **)fc_malloc(bytes);
    if (htable->buckets == NULL) {
        return ENOMEM;
    }
    memset(htable->buckets, 0, bytes);

    htable->need_reclaim = false;
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

int ob_index_init()
{
    int result;
    if ((result=init_ob_shared_allocator_array(&ob_shared_ctx.
                    allocator_array)) != 0)
    {
        return result;
    }
    if ((result=init_ob_shared_segment_array(&ob_shared_ctx.
                    segment_array)) != 0)
    {
        return result;
    }

    if ((result=ob_index_init_htable_ex(&g_ob_hashtable,
                    OB_HASHTABLE_CAPACITY)) != 0)
    {
        return result;
    }

    if (STORAGE_ENABLED) {
        g_ob_hashtable.need_reclaim = true;
    }
    return 0;
}

void ob_index_destroy()
{
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
    record->storage.version = slice->data_version;
    record->storage.trunk_id = slice->space.id_info.id;
    record->storage.length = slice->ssize.length;  //data length
    record->storage.offset = slice->space.offset;  //space offset
    record->storage.size = slice->space.size;      //space size
    DA_SPACE_LOG_ADD_TO_CHAIN(space_chain, record);
    return 0;
}

static inline int do_add_slice(OBHashtable *htable, OBEntry *ob,
        UniqSkiplist *sl, OBSliceEntry *slice,
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
    }

    return 0;
}

static inline int do_delete_slice(OBHashtable *htable, OBEntry *ob,
        UniqSkiplist *sl, OBSliceEntry *slice,
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

static inline OBSliceEntry *slice_dup(OBSegment *segment,
        const OBSliceEntry *src, const DASliceType dest_type,
        const int offset, const int length)
{
    OBSliceEntry *slice;
    int extra_offset;

    if ((slice=ob_slice_alloc(segment, src->ob, 1)) == NULL) {
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

static inline int dup_slice_to_smart_array_ex(OBSegment *segment,
        const OBSliceEntry *src_slice, const DASliceType dest_type,
        const int offset, const int length, OBSlicePtrSmartArray *array)
{
    OBSliceEntry *new_slice;

    new_slice = slice_dup(segment, src_slice, dest_type, offset, length);
    if (new_slice == NULL) {
        return ENOMEM;
    }

    new_slice->space.size = length;  //for calculating trunk used bytes correctly
    return add_to_slice_ptr_smart_array(array, new_slice);
}

#define dup_slice_to_smart_array(segment, src, offset, length, array) \
    dup_slice_to_smart_array_ex(segment, src, \
            src->type, offset, length, array)

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
        UniqSkiplist *sl, OBSliceEntry *slice, int *inc_alloc,
        struct fc_queue_info *space_chain)
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
            return do_add_slice(htable, ob, sl, slice, space_chain);
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
                            curr_slice->ssize.offset, &add_slice_array)) != 0)
            {
                return result;
            }

            new_space_start = curr_end;
            if (curr_end > slice_end) {
                if ((result=dup_slice_to_smart_array(segment, curr_slice,
                                slice_end, curr_end - slice_end,
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
                    slices[i], space_chain);
        }
        FREE_SLICE_PTR_ARRAY(add_slice_array);
    }

    return do_add_slice(htable, ob, sl, slice, space_chain);
}

int ob_index_add_slice_to_wbuffer_chain(FSSliceSpaceLogRecord *record,
        SFBinlogWriterBuffer **slice_tail, OBSliceEntry *slice,
        const time_t timestamp, const int64_t sn, const char source)
{
    SFBinlogWriterBuffer *wbuffer;

    if ((wbuffer=sf_binlog_writer_alloc_buffer(
                    &SLICE_BINLOG_WRITER.thread)) == NULL)
    {
        return ENOMEM;
    }

    SF_BINLOG_BUFFER_SET_VERSION(wbuffer, sn);
    wbuffer->bf.length = slice_binlog_log_add_slice_to_buff_ex(
            slice, timestamp, sn, slice->data_version, source,
            wbuffer->bf.buff);
    wbuffer->next = NULL;
    if (record->slice_head == NULL) {
        record->slice_head = wbuffer;
    } else {
        (*slice_tail)->next = wbuffer;
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
    record->slice_head = wbuffer;
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
    record->slice_head = wbuffer;
    record->last_sn = sn;
    return 0;
}

static inline int add_slice_for_reclaim(FSSliceSpaceLogRecord *record,
        SFBinlogWriterBuffer **slice_tail, OBSliceEntry *slice)
{
    int64_t sn;
    int result;

    sn = ob_index_generate_alone_sn();
    if ((result=ob_index_add_slice_to_wbuffer_chain(record, slice_tail, slice,
                    g_current_time, sn, BINLOG_SOURCE_RECLAIM)) != 0)
    {
        return result;
    }

    if (STORAGE_ENABLED) {
        return change_notify_push_add_slice(sn, slice);
    } else {
        return 0;
    }
}

static int update_slice(OBSegment *segment, OBHashtable *htable,
        OBEntry *ob, OBSliceEntry *slice, int *update_count,
        bool *release_slice, FSSliceSpaceLogRecord *record,
        const bool call_by_reclaim)
{
    UniqSkiplistNode *node;
    OBSliceEntry *current;
    SFBinlogWriterBuffer *slice_tail = NULL;
    OBSlicePtrSmartArray add_slice_array;
    OBSlicePtrSmartArray del_slice_array;
    int result;
    int slice_end;
    int i;

    node = uniq_skiplist_find_ge_node(ob->slices, slice);
    if (node == NULL) {
        *update_count = 0;
        *release_slice = true;
        return 0;
    }

    current = (OBSliceEntry *)node->data;
    if ((call_by_reclaim || current->type == DA_SLICE_TYPE_CACHE) &&
            (current->data_version == slice->data_version) &&
            (current->ssize.offset == slice->ssize.offset &&
             current->ssize.length == slice->ssize.length))
    {
        *update_count = 1;
        *release_slice = false;
        do_delete_slice(htable, ob, ob->slices,
                current, &record->space_chain);
        if (call_by_reclaim) {
            if ((result=add_slice_for_reclaim(record,
                            &slice_tail, slice)) != 0)
            {
                return result;
            }
        }
        return do_add_slice(htable, ob, ob->slices,
                slice, &record->space_chain);
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
            if (!(call_by_reclaim || current->type == DA_SLICE_TYPE_CACHE) ||
                    current->ssize.offset + current->ssize.length > slice_end)
            {
                logCrit("file: "__FILE__", line: %d, "
                        "some mistake happen! data version: %"PRId64", "
                        "block {oid: %"PRId64", offset: %"PRId64"}, "
                        "old slice {offset: %d, length: %d, type: %c}, "
                        "new slice {offset: %d, length: %d}",
                        __LINE__, slice->data_version, ob->bkey.oid,
                        ob->bkey.offset, current->ssize.offset,
                        current->ssize.length, current->type,
                        slice->ssize.offset, slice->ssize.length);
                sf_terminate_myself();
                return EFAULT;
            }

            if ((result=add_to_slice_ptr_smart_array(&del_slice_array,
                            current)) != 0)
            {
                return result;
            }

            if ((result=dup_slice_to_smart_array_ex(segment, slice,
                            DA_SLICE_TYPE_FILE, current->ssize.offset,
                            current->ssize.length, &add_slice_array)) != 0)
            {
                return result;
            }
        }

        node = UNIQ_SKIPLIST_LEVEL0_NEXT_NODE(node);
    } while (node != ob->slices->factory->tail);

    *update_count = add_slice_array.count;
    if (del_slice_array.count > 0) {
        for (i=0; i<del_slice_array.count; i++) {
            do_delete_slice(htable, ob, ob->slices, del_slice_array.
                    slices[i], &record->space_chain);
        }
        FREE_SLICE_PTR_ARRAY(del_slice_array);
    }

    if (add_slice_array.count > 0) {
        for (i=0; i<add_slice_array.count; i++) {
            if (call_by_reclaim) {
                if ((result=add_slice_for_reclaim(record, &slice_tail,
                                add_slice_array.slices[i])) != 0)
                {
                    return result;
                }
            }

            do_add_slice(htable, ob, ob->slices, add_slice_array.
                    slices[i], &record->space_chain);
        }
        FREE_SLICE_PTR_ARRAY(add_slice_array);
    }

    return 0;
}

int ob_index_add_slice_ex(OBHashtable *htable, const FSBlockKey *bkey,
        OBSliceEntry *slice, uint64_t *sn, int *inc_alloc,
        struct fc_queue_info *space_chain)
{
    OBEntry *ob;
    int result;

    /*
    logInfo("#######ob_index_add_slice: %p, ref_count: %d, "
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
    } else {
        if (slice->ob != ob) {
            slice->ob = ob;
        }
        result = add_slice(segment, htable, ob, ob->slices,
                slice, inc_alloc, space_chain);
        if (result == 0) {
            __sync_add_and_fetch(&slice->ref_count, 1);
            if (sn != NULL) {
                if (*sn == 0) {
                    *sn = __sync_add_and_fetch(&SLICE_BINLOG_SN, 1);
                }

                if (slice->type != DA_SLICE_TYPE_CACHE) {
                    if (STORAGE_ENABLED) {
                        result = change_notify_push_add_slice(*sn, slice);
                    }
                }
            }
        }
    }
    PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);

    return result;
}

int ob_index_add_slice_by_binlog(const uint64_t sn, OBSliceEntry *slice)
{
    int result;

    OB_INDEX_SET_HASHTABLE_SEGMENT(&g_ob_hashtable, slice->ob->bkey);
    PTHREAD_MUTEX_LOCK(&segment->lcp.lock);
    if ((result=add_slice(segment, &g_ob_hashtable, slice->ob,
                    slice->ob->slices, slice, NULL, NULL)) == 0)
    {
        if (STORAGE_ENABLED) {
            result = change_notify_push_add_slice(sn, slice);
        }
    }
    PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);

    return result;
}

int ob_index_update_slice_ex(OBHashtable *htable, const DASliceEntry *se,
        const DATrunkSpaceInfo *space, int *update_count,
        FSSliceSpaceLogRecord *record, const bool call_by_reclaim)
{
    const int init_refer = 1;
    int result;
    bool release_slice;
    OBEntry *ob;
    OBSliceEntry *slice;

    logInfo("####### ob_index_update_slice: %p, "
            "block {oid: %"PRId64", offset: %"PRId64"}, "
            "slice: {offset: %d, length: %d}",
            se, se->bs_key.block.oid, se->bs_key.block.offset,
            se->bs_key.slice.offset, se->bs_key.slice.length);

    OB_INDEX_SET_BUCKET_AND_SEGMENT(htable, se->bs_key.block);
    PTHREAD_MUTEX_LOCK(&segment->lcp.lock);
    ob = get_ob_entry(segment, htable, bucket, &se->bs_key.block, false);
    if (ob != NULL) {
        if ((slice=ob_slice_alloc(segment, ob, init_refer)) == NULL) {
            *update_count = 0;
            result = ENOMEM;
        } else {
            slice->data_version = se->data_version;
            slice->type = DA_SLICE_TYPE_FILE;
            slice->ssize = se->bs_key.slice;
            slice->space = *space;
            if ((result=update_slice(segment, htable, ob, slice,
                            update_count, &release_slice, record,
                            call_by_reclaim)) == 0)
            {
                if (STORAGE_ENABLED && !call_by_reclaim) {
                    result = change_notify_push_add_slice(se->sn, slice);
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
        int *count, int *dec_alloc, struct fc_queue_info *space_chain)
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
                            curr_slice->ssize.offset, &add_slice_array)) != 0)
            {
                return result;
            }

            if (curr_end > slice_end) {
                if ((result=dup_slice_to_smart_array(segment, curr_slice,
                                slice_end, curr_end - slice_end,
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
                    slices[i], space_chain);
        }
        FREE_SLICE_PTR_ARRAY(add_slice_array);
    }

    return 0;
}

int ob_index_delete_slice_ex(OBHashtable *htable,
        const FSBlockSliceKeyInfo *bs_key, uint64_t *sn,
        int *dec_alloc, struct fc_queue_info *space_chain)
{
    OBEntry *ob;
    OBEntry *previous;
    int result;
    int count;

    OB_INDEX_SET_BUCKET_AND_SEGMENT(htable, bs_key->block);

    *dec_alloc = 0;
    PTHREAD_MUTEX_LOCK(&segment->lcp.lock);
    ob = get_ob_entry_ex(segment, htable, bucket, &bs_key->block,
            false, htable->need_reclaim, &previous);
    if (ob == NULL) {
        result = ENOENT;
    } else {
        result = delete_slice(segment, htable, ob, ob->slices,
                bs_key, &count, dec_alloc, space_chain);
        if (result == 0) {
            if (sn != NULL) {
                if (*sn == 0) {
                    *sn = __sync_add_and_fetch(&SLICE_BINLOG_SN, 1);
                }

                if (STORAGE_ENABLED) {
                    result = change_notify_push_del_slice(
                            *sn, ob, &bs_key->slice);
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
    OBEntry *ob;
    OBEntry *previous;
    OBSliceEntry *slice;
    UniqSkiplistIterator it;
    int result;

    OB_INDEX_SET_BUCKET_AND_SEGMENT(htable, *bkey);

    *dec_alloc = 0;
    PTHREAD_MUTEX_LOCK(&segment->lcp.lock);
    ob = get_ob_entry_ex(segment, htable, bucket, bkey,
            false, htable->need_reclaim, &previous);
    if (ob != NULL) {
        uniq_skiplist_iterator(ob->slices, &it);
        while ((slice=(OBSliceEntry *)uniq_skiplist_next(&it)) != NULL) {
            *dec_alloc += slice->ssize.length;
            if (space_chain != NULL) {
                if ((result=add_to_space_chain(space_chain, slice,
                                da_binlog_op_type_reclaim_space)) != 0)
                {
                    return result;
                }
            }
        }

        if (*dec_alloc > 0) {
            if (sn != NULL) {
                if (*sn == 0) {
                    *sn = __sync_add_and_fetch(&SLICE_BINLOG_SN, 1);
                }
                if (STORAGE_ENABLED) {
                    result = change_notify_push_del_block(*sn, ob);
                }
            }
            result = 0;
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
    OBSliceEntry *new_slice;

    new_slice = slice_dup(segment, src_slice,
            src_slice->type, offset, length);
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
    OBSharedAllocator *allocator;
    OBSharedAllocator *end;

    *ob_count = *slice_count = 0;
    end = ob_shared_ctx.allocator_array.allocators +
        ob_shared_ctx.allocator_array.count;
    for (allocator=ob_shared_ctx.allocator_array.allocators;
            allocator<end; allocator++)
    {
        *ob_count += allocator->ob.info.element_used_count;
        *slice_count += allocator->slice.info.element_used_count;
    }
}

int64_t ob_index_get_total_slice_count()
{
    int64_t slice_count;
    OBSharedAllocator *allocator;
    OBSharedAllocator *end;

    slice_count = 0;
    end = ob_shared_ctx.allocator_array.allocators +
        ob_shared_ctx.allocator_array.count;
    for (allocator=ob_shared_ctx.allocator_array.allocators;
            allocator<end; allocator++)
    {
        slice_count += allocator->slice.info.element_used_count;
    }

    return slice_count;
}

int ob_index_dump_slices_to_file_ex(OBHashtable *htable,
        const int64_t start_index, const int64_t end_index,
        const char *filename, int64_t *slice_count,
        const bool need_padding, const bool need_lock)
{
    const int64_t data_version = 0;
    const int source = BINLOG_SOURCE_DUMP;
    int result;
    int i;
    SFBufferedWriter writer;
    OBSegment *segment = NULL;
    OBEntry **bucket;
    OBEntry **end;
    OBEntry *ob;
    time_t current_time;
    OBSliceEntry *slice;
    UniqSkiplistIterator it;

    *slice_count = 0;
    if ((result=sf_buffered_writer_init(&writer, filename)) != 0) {
        return result;
    }

    current_time = g_current_time;
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

                writer.buffer.current += slice_binlog_log_add_slice_to_buff(
                        slice, current_time, slice->data_version, source,
                        writer.buffer.current);
            }

            ob = ob->next;
        } while (ob != NULL && result == 0);

        if (need_lock) {
            PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);
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

            writer.buffer.current += slice_binlog_log_no_op_to_buff(
                    &bkey, current_time + i, data_version, source,
                    writer.buffer.current);
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
    OBSlicePtrArray slice_parray;
    SFBufferedWriter writer;
} DumpSliceContext;

static inline int write_slice_index_to_file(OBEntry *ob,
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
            &ob->bkey, ssize, writer->buffer.current);
    return 0;
}

#define SET_SLICE_TYPE_SSIZE(_slice_type, _ssize, slice) \
    _slice_type = (slice)->type; \
    _ssize = (slice)->ssize

static int remove_slices_to_file(OBEntry *ob, DumpSliceContext *ctx)
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

        if ((result=write_slice_index_to_file(ob, slice_type,
                        &ssize, &ctx->writer)) != 0)
        {
            return result;
        }

        SET_SLICE_TYPE_SSIZE(slice_type, ssize, *sp);
    }

    if ((result=write_slice_index_to_file(ob, slice_type,
                    &ssize, &ctx->writer)) != 0)
    {
        return result;
    }

    for (sp=ctx->slice_parray.slices; sp<se; sp++) {
        uniq_skiplist_delete(ob->slices, *sp);
    }

    return 0;
}

int ob_index_remove_slices_to_file_ex(OBHashtable *htable,
        const int64_t start_index, const int64_t end_index,
        const int rebuild_store_index, const char *filename,
        int64_t *slice_count)
{
    int result;
    int bytes;
    DumpSliceContext ctx;
    OBEntry **bucket;
    OBEntry **end;
    OBEntry *ob;
    OBSliceEntry *slice;
    OBSliceEntry **sp;
    UniqSkiplistIterator it;

    *slice_count = 0;
    ctx.slice_parray.alloc = 16 * 1024;
    bytes = sizeof(OBSliceEntry *) * ctx.slice_parray.alloc;
    if ((ctx.slice_parray.slices=fc_malloc(bytes)) == NULL) {
        return ENOMEM;
    }

    if ((result=sf_buffered_writer_init(&ctx.writer, filename)) != 0) {
        return result;
    }

    end = htable->buckets + end_index;
    for (bucket=htable->buckets+start_index; result == 0 &&
            bucket<end && SF_G_CONTINUE_FLAG; bucket++)
    {
        if (*bucket == NULL) {
            continue;
        }

        ob = *bucket;
        do {
            sp = ctx.slice_parray.slices;
            uniq_skiplist_iterator(ob->slices, &it);
            while ((slice=(OBSliceEntry *)uniq_skiplist_next(&it)) != NULL) {
                if (slice->space.store->index == rebuild_store_index) {
                    if (sp - ctx.slice_parray.slices == ctx.slice_parray.alloc) {
                        ctx.slice_parray.count = sp - ctx.slice_parray.slices;
                        if ((result=realloc_slice_parray(&ctx.slice_parray)) != 0) {
                            ctx.slice_parray.count = 0;
                            break;
                        }
                        sp = ctx.slice_parray.slices + ctx.slice_parray.count;
                    }
                    *sp++ = slice;
                }
            }

            ctx.slice_parray.count = sp - ctx.slice_parray.slices;
            if (ctx.slice_parray.count > 0) {
                result = remove_slices_to_file(ob, &ctx);
                if (result != 0) {
                    break;
                }
                *slice_count += ctx.slice_parray.count;
            }

            ob = ob->next;
        } while (ob != NULL && result == 0);
    }

    if (!SF_G_CONTINUE_FLAG) {
        result = EINTR;
    }

    if (result == 0 && SF_BUFFERED_WRITER_LENGTH(ctx.writer) > 0) {
        result = sf_buffered_writer_save(&ctx.writer);
    }

    free(ctx.slice_parray.slices);
    sf_buffered_writer_destroy(&ctx.writer);
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
    return write_slice_index_to_file(ob, slice_type,
            ssize, &dump_ctx->writer);
#endif
}

static int dump_slice_index_to_file(OBEntry *ob,
        DumpSliceIndexContext *dump_ctx, int *slice_count)
{
    int result;
    int slice_type;
    FSSliceSize ssize;
    UniqSkiplistIterator it;
    OBSliceEntry *slice;

    uniq_skiplist_iterator(ob->slices, &it);
    if ((slice=(OBSliceEntry *)uniq_skiplist_next(&it)) == NULL) {
        *slice_count = 0;
        return 0;
    }

    *slice_count = 1;
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

        ++(*slice_count);
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
    int slice_count;
    DumpSliceIndexContext *dump_ctx;
    OBEntry *ob;

    dump_ctx = arg;
    OB_INDEX_SET_BUCKET_AND_SEGMENT(&g_ob_hashtable, *bkey);
    PTHREAD_MUTEX_LOCK(&segment->lcp.lock);
    ob = get_ob_entry(segment, &g_ob_hashtable, bucket, bkey, true);
    PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);
    if (ob != NULL) {
        if ((result=dump_slice_index_to_file(ob, dump_ctx,
                                &slice_count)) == 0)
        {
            dump_ctx->total_slice_count += slice_count;
        }
    } else {
        result = ENOMEM;
    }

    return result;
}

int ob_index_dump_slice_index_to_file(const char *filename,
        int64_t *total_slice_count)
{
    int result;
    int slice_count;
    DumpSliceIndexContext dump_ctx;
    OBEntry **bucket;
    OBEntry **end;
    OBEntry *ob;

    *total_slice_count = 0;
    memset(&dump_ctx, 0, sizeof(dump_ctx));
#if FS_DUMP_SLICE_FOR_DEBUG == FS_DUMP_SLICE_CALC_CRC32
    if ((result=fs_slice_blocked_op_ctx_init(&dump_ctx.bctx)) != 0) {
        return result;
    }
#endif

    if ((result=sf_buffered_writer_init(&dump_ctx.writer, filename)) != 0) {
        return result;
    }

    if (STORAGE_ENABLED) {
        if ((result=STORAGE_ENGINE_WALK_API(walk_callback_for_dump_slice,
                        &dump_ctx)) != 0)
        {
            return result;
        }
    } else {
        end = g_ob_hashtable.buckets + g_ob_hashtable.capacity;
        for (bucket=g_ob_hashtable.buckets; result == 0 &&
                bucket<end && SF_G_CONTINUE_FLAG; bucket++)
        {
            if (*bucket == NULL) {
                continue;
            }

            ob = *bucket;
            do {
                if ((result=dump_slice_index_to_file(ob, &dump_ctx,
                                &slice_count)) != 0)
                {
                    break;
                }

                dump_ctx.total_slice_count += slice_count;
                ob = ob->next;
            } while (ob != NULL && result == 0);
        }
    }

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
    return result;
}
#endif

int ob_index_unpack_ob_entry(OBSegment *segment, OBEntry *ob,
        UniqSkiplist *sl, const SFSerializerFieldValue *fv)
{
    const int init_refer = 1;
    int result;
    string_t *s;
    string_t *end;
    OBSliceEntry *slice;

    OB_INDEX_SET_HASHTABLE_ALLOCATOR(ob->bkey);
    end = fv->value.str_array.strings + fv->value.str_array.count;
    for (s=fv->value.str_array.strings; s<end; s++) {
        slice = ob_alloc_slice_for_load(segment, allocator, ob, init_refer);
        if (slice == NULL) {
            return ENOMEM;
        }

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
    OB_INDEX_SET_HASHTABLE_SEGMENT(&g_ob_hashtable, *bkey);
    return segment;
}

int ob_index_alloc_db_slices(OBEntry *ob)
{
    const int init_level_count = 2;

    OB_INDEX_SET_HASHTABLE_ALLOCATOR(ob->bkey);
    if ((ob->db_args->slices=uniq_skiplist_new(&allocator->
                    factory, init_level_count)) != NULL)
    {
        return 0;
    } else {
        return ENOMEM;
    }
}

int ob_index_load_db_slices(OBSegment *segment, OBEntry *ob)
{
    int result;

    if ((result=ob_index_alloc_db_slices(ob)) != 0) {
        return result;
    }
    return ob_load_slices(segment, ob, ob->db_args->slices);
}

int ob_index_add_slice_by_db(OBSegment *segment, OBEntry *ob,
        const int64_t data_version, const DASliceType type,
        const FSSliceSize *ssize, const DATrunkSpaceInfo *space)
{
    const int init_refer = 1;
    OBSliceEntry *slice;

    if ((slice=ob_slice_alloc(segment, ob, init_refer)) == NULL) {
        return ENOMEM;
    }
    slice->data_version = data_version;
    slice->type = type;
    slice->ssize = *ssize;
    slice->space = *space;
    return add_slice(segment, &g_ob_hashtable, ob,
            ob->db_args->slices, slice, NULL, NULL);
}

int ob_index_delete_slice_by_db(OBSegment *segment,
        OBEntry *ob, const FSSliceSize *ssize)
{
    int count;
    int result;
    FSBlockSliceKeyInfo bs_key;

    bs_key.block = ob->bkey;
    bs_key.slice = *ssize;
    result = delete_slice(segment, &g_ob_hashtable, ob, ob->
            db_args->slices, &bs_key, &count, NULL, NULL);
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
