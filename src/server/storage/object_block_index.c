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
#include "../server_global.h"
#include "../binlog/slice_binlog.h"
#include "storage_allocator.h"
#include "object_block_index.h"

#define SLICE_ARRAY_FIXED_COUNT  64

typedef struct {
    int count;
    OBSharedContext *contexts;
} OBSharedContextArray;

typedef struct {
    int alloc;
    int count;
    OBSliceEntry **slices;
    OBSliceEntry *fixed[SLICE_ARRAY_FIXED_COUNT];
} OBSlicePtrSmartArray;

static OBSharedContextArray ob_shared_ctx_array = {0, NULL};

OBHashtable g_ob_hashtable = {0, 0, NULL};

#define OB_INDEX_SET_HASHTABLE_CTX(htable, bkey) \
    int64_t bucket_index;  \
    OBSharedContext *ctx;  \
    do {  \
        bucket_index = FS_BLOCK_HASH_CODE(bkey) % (htable)->capacity; \
        ctx = ob_shared_ctx_array.contexts + bucket_index %   \
            ob_shared_ctx_array.count;  \
    } while (0)

#define OB_INDEX_SET_BUCKET_AND_CTX(htable, bkey) \
    OBEntry **bucket;   \
    OB_INDEX_SET_HASHTABLE_CTX(htable, bkey);  \
    do {  \
        bucket = (htable)->buckets + bucket_index; \
    } while (0)


static OBEntry *get_ob_entry_ex(OBSharedContext *ctx, OBEntry **bucket,
        const FSBlockKey *bkey, const bool create_flag, OBEntry **pprev)
{
    const int init_level_count = 2;
    OBEntry *previous;
    OBEntry *ob;
    int cmpr;

    if (pprev == NULL) {
        pprev = &previous;
    }
    if (*bucket == NULL) {
        if (!create_flag) {
            return NULL;
        }
        *pprev = NULL;
    } else {
        cmpr = ob_index_compare_block_key(bkey, &(*bucket)->bkey);
        if (cmpr == 0) {
            *pprev = NULL;
            return *bucket;
        } else if (cmpr < 0) {
            *pprev = NULL;
        } else {
            *pprev = *bucket;
            while ((*pprev)->next != NULL) {
                cmpr = ob_index_compare_block_key(bkey, &(*pprev)->next->bkey);
                if (cmpr == 0) {
                    return (*pprev)->next;
                } else if (cmpr < 0) {
                    break;
                }

                *pprev = (*pprev)->next;
            }
        }

        if (!create_flag) {
            return NULL;
        }
    }

    ob = (OBEntry *)fast_mblock_alloc_object(&ctx->ob_allocator);
    if (ob == NULL) {
        return NULL;
    }
    ob->slices = uniq_skiplist_new(&ctx->factory, init_level_count);
    if (ob->slices == NULL) {
        fast_mblock_free_object(&ctx->ob_allocator, ob);
        return NULL;
    }

    ob->bkey = *bkey;
    if (*pprev == NULL) {
        ob->next = *bucket;
        *bucket = ob;
    } else {
        ob->next = (*pprev)->next;
        (*pprev)->next = ob;
    }

    return ob;
}

#define get_ob_entry(ctx, bucket, bkey, create_flag)  \
    get_ob_entry_ex(ctx, bucket, bkey, create_flag, NULL)

OBEntry *ob_index_get_ob_entry_ex(OBHashtable *htable,
        const FSBlockKey *bkey)
{
    OBEntry *ob;
    OB_INDEX_SET_BUCKET_AND_CTX(htable, *bkey);

    PTHREAD_MUTEX_LOCK(&ctx->lcp.lock);
    ob = get_ob_entry(ctx, bucket, bkey, false);
    PTHREAD_MUTEX_UNLOCK(&ctx->lcp.lock);

    return ob;
}

OBEntry *ob_index_reclaim_lock(const FSBlockKey *bkey)
{
    OBEntry *ob;

    OB_INDEX_SET_BUCKET_AND_CTX(&g_ob_hashtable, *bkey);
    PTHREAD_MUTEX_LOCK(&ctx->lcp.lock);
    ob = get_ob_entry(ctx, bucket, bkey, false);
    if (ob != NULL) {
        ++(ob->reclaiming_count);
    }
    PTHREAD_MUTEX_UNLOCK(&ctx->lcp.lock);

    return ob;
}

void ob_index_reclaim_unlock(OBEntry *ob)
{
    OB_INDEX_SET_HASHTABLE_CTX(&g_ob_hashtable, ob->bkey);
    PTHREAD_MUTEX_LOCK(&ctx->lcp.lock);
    if (--(ob->reclaiming_count) == 0) {
        pthread_cond_broadcast(&ctx->lcp.cond);
    }
    PTHREAD_MUTEX_UNLOCK(&ctx->lcp.lock);
}

OBSliceEntry *ob_index_alloc_slice_ex(OBHashtable *htable,
        const FSBlockKey *bkey, const int init_refer)
{
    OBEntry *ob;
    OBSliceEntry *slice;

    OB_INDEX_SET_BUCKET_AND_CTX(htable, *bkey);
    PTHREAD_MUTEX_LOCK(&ctx->lcp.lock);
    ob = get_ob_entry(ctx, bucket, bkey, true);
    PTHREAD_MUTEX_UNLOCK(&ctx->lcp.lock);

    if (ob == NULL) {
        slice = NULL;
    } else {
        slice = (OBSliceEntry *)fast_mblock_alloc_object(
                &ctx->slice_allocator);
        if (slice != NULL) {
            slice->ob = ob;
            if (init_refer > 0) {
                __sync_add_and_fetch(&slice->ref_count, init_refer);
            }
        }
    }

    return slice;
}

void ob_index_free_slice(OBSliceEntry *slice)
{
    if (__sync_sub_and_fetch(&slice->ref_count, 1) == 0) {
        /*
        logInfo("free slice: %p, ref_count: %d, block "
                "{oid: %"PRId64", offset: %"PRId64"}, ctx: %p",
                slice, __sync_add_and_fetch(&slice->ref_count, 0),
                slice->ob->bkey.oid, slice->ob->bkey.offset, ctx);
                */

        fast_mblock_free_object(slice->allocator, slice);
    }
}

static int slice_compare(const void *p1, const void *p2)
{
    return ((OBSliceEntry *)p1)->ssize.offset -
        ((OBSliceEntry *)p2)->ssize.offset;
}

static void slice_free_func(void *ptr, const int delay_seconds)
{
    OBSliceEntry *slice;

    slice = (OBSliceEntry *)ptr;
    if (__sync_sub_and_fetch(&slice->ref_count, 1) == 0) {
        /*
           logInfo("free slice3: %p, ref_count: %d, block "
           "{oid: %"PRId64", offset: %"PRId64"}, ctx: %p",
           slice, __sync_add_and_fetch(&slice->ref_count, 0),
           slice->ob->bkey.oid, slice->ob->bkey.offset, ctx);
         */

        fast_mblock_free_object(slice->allocator, slice);
    }
}

static int slice_alloc_init(OBSliceEntry *slice,
        struct fast_mblock_man *allocator)
{
    slice->allocator = allocator;
    return 0;
}

static int init_ob_shared_ctx_array()
{
    int result;
    int bytes;
    const int max_level_count = 8;
    const int alloc_skiplist_once = 8 * 1024;
    const int min_alloc_elements_once = 2;
    const int delay_free_seconds = 0;
    const bool bidirection = true;  //need previous link
    OBSharedContext *ctx;
    OBSharedContext *end;

    ob_shared_ctx_array.count = STORAGE_CFG.object_block.shared_locks_count;
    bytes = sizeof(OBSharedContext) * ob_shared_ctx_array.count;
    ob_shared_ctx_array.contexts = (OBSharedContext *)fc_malloc(bytes);
    if (ob_shared_ctx_array.contexts == NULL) {
        return ENOMEM;
    }

    end = ob_shared_ctx_array.contexts + ob_shared_ctx_array.count;
    for (ctx=ob_shared_ctx_array.contexts; ctx<end; ctx++) {
        if ((result=uniq_skiplist_init_ex2(&ctx->factory, max_level_count,
                        slice_compare, slice_free_func, alloc_skiplist_once,
                        min_alloc_elements_once, delay_free_seconds,
                        bidirection)) != 0)
        {
            return result;
        }

        if ((result=fast_mblock_init_ex1(&ctx->ob_allocator,
                        "ob_entry", sizeof(OBEntry), 4 * 1024,
                        0, NULL, NULL, false)) != 0)
        {
            return result;
        }

        if ((result=fast_mblock_init_ex1(&ctx->slice_allocator,
                        "slice_entry", sizeof(OBSliceEntry), 16 * 1024, 0,
                        (fast_mblock_alloc_init_func)slice_alloc_init,
                        &ctx->slice_allocator, true)) != 0)
        {
            return result;
        }

        if ((result=init_pthread_lock_cond_pair(&ctx->lcp)) != 0) {
            return result;
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

    htable->modify_sallocator = false;
    htable->modify_used_space = false;
    return 0;
}

void ob_index_destroy_htable(OBHashtable *htable)
{
    OBEntry **bucket;
    OBEntry **end;
    OBEntry *ob;
    OBEntry *deleted;
    OBSharedContext *ctx;

    end = htable->buckets + htable->capacity;
    for (bucket=htable->buckets; bucket<end; bucket++) {
        if (*bucket == NULL) {
            continue;
        }

        ctx = ob_shared_ctx_array.contexts + (bucket - htable->buckets) %
            ob_shared_ctx_array.count;
        PTHREAD_MUTEX_LOCK(&ctx->lcp.lock);

        ob = *bucket;
        do {
            uniq_skiplist_free(ob->slices);

            deleted = ob;
            ob = ob->next;
            fast_mblock_free_object(&ctx->ob_allocator, deleted);
        } while (ob != NULL);

        PTHREAD_MUTEX_UNLOCK(&ctx->lcp.lock);
    }

    free(htable->buckets);
    htable->buckets = NULL;
}

int ob_index_init()
{
    int result;
    if ((result=init_ob_shared_ctx_array()) != 0) {
        return result;
    }

    return ob_index_init_htable_ex(&g_ob_hashtable,
            STORAGE_CFG.object_block.hashtable_capacity);
}

void ob_index_destroy()
{
}

static inline int do_delete_slice(OBHashtable *htable,
        OBEntry *ob, OBSliceEntry *slice)
{
    if (htable->modify_sallocator) {
        storage_allocator_delete_slice(slice,
                htable->modify_used_space);
    }

    return uniq_skiplist_delete(ob->slices, slice);
}

static inline int do_add_slice(OBHashtable *htable,
        OBEntry *ob, OBSliceEntry *slice)
{
    int result;

    if ((result=uniq_skiplist_insert(ob->slices, slice)) != 0) {
        return result;
    }
    if (htable->modify_sallocator) {
        return storage_allocator_add_slice(slice, htable->modify_used_space);
    } else {
        return 0;
    }
}

static inline OBSliceEntry *slice_dup(OBSharedContext *ctx,
        const OBSliceEntry *src, const int offset, const int length)
{
    OBSliceEntry *slice;
    int extra_offset;

    slice = (OBSliceEntry *)fast_mblock_alloc_object(&ctx->slice_allocator);
    if (slice == NULL) {
        return NULL;
    }

    slice->ob = src->ob;
    slice->type = src->type;
    slice->space = src->space;
    extra_offset = offset - src->ssize.offset;
    if (extra_offset > 0) {
        slice->space.offset += extra_offset;
        slice->ssize.offset = offset;
    } else {
        slice->ssize.offset = src->ssize.offset;
    }
    slice->ssize.length = length;
    __sync_add_and_fetch(&slice->ref_count, 1);
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

static inline int dup_slice_to_smart_array(OBSharedContext *ctx,
        const OBSliceEntry *src_slice, const int offset,
        const int length, OBSlicePtrSmartArray *array)
{
    OBSliceEntry *new_slice;

    new_slice = slice_dup(ctx, src_slice, offset, length);
    if (new_slice == NULL) {
        return ENOMEM;
    }

    new_slice->space.size = length;  //for calculating trunk used bytes correctly
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


static int add_slice(OBHashtable *htable, OBSharedContext *ctx,
        OBEntry *ob, OBSliceEntry *slice, int *inc_alloc)
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

    *inc_alloc = 0;
    node = uniq_skiplist_find_ge_node(ob->slices, (void *)slice);
    if (node == NULL) {
        previous = UNIQ_SKIPLIST_LEVEL0_TAIL_NODE(ob->slices);
        if (previous == ob->slices->top) {
            *inc_alloc += slice->ssize.length;
            return do_add_slice(htable, ob, slice);
        }
    } else {
        previous = UNIQ_SKIPLIST_LEVEL0_PREV_NODE(node);
    }

    INIT_SLICE_PTR_ARRAY(add_slice_array);
    INIT_SLICE_PTR_ARRAY(del_slice_array);

    new_space_start = slice->ssize.offset;
    slice_end = slice->ssize.offset + slice->ssize.length;
    if (previous != ob->slices->top) {
        curr_slice = (OBSliceEntry *)previous->data;
        curr_end = curr_slice->ssize.offset + curr_slice->ssize.length;
        if (curr_end > slice->ssize.offset) {  //overlap
            if ((result=add_to_slice_ptr_smart_array(&del_slice_array,
                            curr_slice)) != 0)
            {
                return result;
            }

            if ((result=dup_slice_to_smart_array(ctx, curr_slice,
                            curr_slice->ssize.offset, slice->ssize.offset -
                            curr_slice->ssize.offset, &add_slice_array)) != 0)
            {
                return result;
            }

            new_space_start = curr_end;
            if (curr_end > slice_end) {
                if ((result=dup_slice_to_smart_array(ctx, curr_slice, slice_end,
                                curr_end - slice_end, &add_slice_array)) != 0)
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
                *inc_alloc += curr_slice->ssize.offset - new_space_start;
            }

            curr_end = curr_slice->ssize.offset + curr_slice->ssize.length;
            new_space_start = curr_end;
            if (curr_end > slice_end) {
                if ((result=dup_slice_to_smart_array(ctx, curr_slice,
                                slice_end, curr_end - slice_end,
                                &add_slice_array)) != 0)
                {
                    return result;
                }

                break;
            }

            node = UNIQ_SKIPLIST_LEVEL0_NEXT_NODE(node);
        } while (node != ob->slices->factory->tail);
    }

    if (slice_end > new_space_start) {
        *inc_alloc += slice_end - new_space_start;
    }

    for (i=0; i<del_slice_array.count; i++) {
        do_delete_slice(htable, ob, del_slice_array.slices[i]);
    }
    FREE_SLICE_PTR_ARRAY(del_slice_array);

    for (i=0; i<add_slice_array.count; i++) {
        do_add_slice(htable, ob, add_slice_array.slices[i]);
    }
    FREE_SLICE_PTR_ARRAY(add_slice_array);

    return do_add_slice(htable, ob, slice);
}

#define CHECK_AND_WAIT_RECLAIM_DONE(ctx, ob) \
    do {  \
        if (!is_reclaim) {  \
            while (ob->reclaiming_count > 0) {  \
                pthread_cond_wait(&ctx->lcp.cond, &ctx->lcp.lock); \
            } \
        } \
    } while (0)

int ob_index_add_slice_ex(OBHashtable *htable, OBSliceEntry *slice,
        uint64_t *sn, int *inc_alloc, const bool is_reclaim)
{
    int result;

    /*
    logInfo("#######ob_index_add_slice: %p, ref_count: %d, "
            "block {oid: %"PRId64", offset: %"PRId64"}",
            slice, __sync_add_and_fetch(&slice->ref_count, 0),
            slice->ob->bkey.oid, slice->ob->bkey.offset);
            */

    OB_INDEX_SET_HASHTABLE_CTX(htable, slice->ob->bkey);
    PTHREAD_MUTEX_LOCK(&ctx->lcp.lock);

    CHECK_AND_WAIT_RECLAIM_DONE(ctx, slice->ob);
    result = add_slice(htable, ctx, slice->ob, slice, inc_alloc);
    if (result == 0) {
        __sync_add_and_fetch(&slice->ref_count, 1);
        if (sn != NULL) {
            *sn = __sync_add_and_fetch(&SLICE_BINLOG_SN, 1);
        }
    }
    PTHREAD_MUTEX_UNLOCK(&ctx->lcp.lock);

    return result;
}

int ob_index_add_slice_by_binlog(OBSliceEntry *slice)
{
    int result;
    int inc_alloc;

    OB_INDEX_SET_HASHTABLE_CTX(&g_ob_hashtable, slice->ob->bkey);
    PTHREAD_MUTEX_LOCK(&ctx->lcp.lock);
    result = add_slice(&g_ob_hashtable, ctx, slice->ob, slice, &inc_alloc);
    PTHREAD_MUTEX_UNLOCK(&ctx->lcp.lock);

    return result;
}

static int delete_slices(OBHashtable *htable, OBSharedContext *ctx, OBEntry *ob,
        const FSBlockSliceKeyInfo *bs_key, int *count, int *dec_alloc)
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

    *dec_alloc = 0;
    *count = 0;
    target.ssize = bs_key->slice;
    node = uniq_skiplist_find_ge_node(ob->slices, &target);
    if (node == NULL) {
        previous = UNIQ_SKIPLIST_LEVEL0_TAIL_NODE(ob->slices);
        if (previous == ob->slices->top) {
            return ENOENT;
        }
    } else {
        previous = UNIQ_SKIPLIST_LEVEL0_PREV_NODE(node);
    }

    INIT_SLICE_PTR_ARRAY(add_slice_array);
    INIT_SLICE_PTR_ARRAY(del_slice_array);

    slice_end = bs_key->slice.offset + bs_key->slice.length;
    if (previous != ob->slices->top) {
        curr_slice = (OBSliceEntry *)previous->data;
        curr_end = curr_slice->ssize.offset + curr_slice->ssize.length;
        if (curr_end > bs_key->slice.offset) {  //overlap
            if ((result=add_to_slice_ptr_smart_array(&del_slice_array,
                            curr_slice)) != 0)
            {
                return result;
            }

            if ((result=dup_slice_to_smart_array(ctx, curr_slice,
                            curr_slice->ssize.offset, bs_key->slice.offset -
                            curr_slice->ssize.offset, &add_slice_array)) != 0)
            {
                return result;
            }

            if (curr_end > slice_end) {
                if ((result=dup_slice_to_smart_array(ctx, curr_slice, slice_end,
                                curr_end - slice_end, &add_slice_array)) != 0)
                {
                    return result;
                }

                *dec_alloc += bs_key->slice.length;
            } else {
                *dec_alloc += curr_end - bs_key->slice.offset;
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
                if ((result=dup_slice_to_smart_array(ctx, curr_slice,
                                slice_end, curr_end - slice_end,
                                &add_slice_array)) != 0)
                {
                    return result;
                }

                *dec_alloc += slice_end - curr_slice->ssize.offset;
                break;
            } else {
                *dec_alloc += curr_slice->ssize.length;
            }

            node = UNIQ_SKIPLIST_LEVEL0_NEXT_NODE(node);
        } while (node != ob->slices->factory->tail);
    }

    if (del_slice_array.count == 0) {
        return ENOENT;
    }

    *count = del_slice_array.count;
    for (i=0; i<del_slice_array.count; i++) {
        do_delete_slice(htable, ob, del_slice_array.slices[i]);
    }
    FREE_SLICE_PTR_ARRAY(del_slice_array);

    if (add_slice_array.count > 0) {
        for (i=0; i<add_slice_array.count; i++) {
            do_add_slice(htable, ob, add_slice_array.slices[i]);
        }
        FREE_SLICE_PTR_ARRAY(add_slice_array);
    }

    return 0;
}


#define OB_INDEX_DELETE_OB_ENTRY(ctx, bucket, ob, previous) \
    do {  \
        if (previous == NULL) {  \
            *bucket = ob->next;  \
        } else {  \
            previous->next = ob->next;  \
        } \
        uniq_skiplist_free(ob->slices); \
        fast_mblock_free_object(&ctx->ob_allocator, ob); \
    } while (0)


int ob_index_delete_slices_ex(OBHashtable *htable,
        const FSBlockSliceKeyInfo *bs_key, uint64_t *sn,
        int *dec_alloc, const bool is_reclaim)
{
    OBEntry *ob;
    OBEntry *previous;
    int result;
    int count;

    OB_INDEX_SET_BUCKET_AND_CTX(htable, bs_key->block);
    PTHREAD_MUTEX_LOCK(&ctx->lcp.lock);
    ob = get_ob_entry_ex(ctx, bucket, &bs_key->block, false, &previous);
    if (ob == NULL) {
        *dec_alloc = 0;
        result = ENOENT;
    } else {
        CHECK_AND_WAIT_RECLAIM_DONE(ctx, ob);
        result = delete_slices(htable, ctx, ob, bs_key, &count, dec_alloc);
        if (result == 0) {
            if (uniq_skiplist_empty(ob->slices)) {
                OB_INDEX_DELETE_OB_ENTRY(ctx, bucket, ob, previous);
            }

            if (sn != NULL) {
                *sn = __sync_add_and_fetch(&SLICE_BINLOG_SN, 1);
            }
        }
    }
    PTHREAD_MUTEX_UNLOCK(&ctx->lcp.lock);

    return result;
}

int ob_index_delete_block_ex(OBHashtable *htable,
        const FSBlockKey *bkey, uint64_t *sn,
        int *dec_alloc, const bool is_reclaim)
{
    OBEntry *ob;
    OBEntry *previous;
    OBSliceEntry *slice;
    UniqSkiplistIterator it;
    int result;

    OB_INDEX_SET_BUCKET_AND_CTX(htable, *bkey);

    *dec_alloc = 0;
    PTHREAD_MUTEX_LOCK(&ctx->lcp.lock);
    ob = get_ob_entry_ex(ctx, bucket, bkey, false, &previous);
    if (ob != NULL) {
        CHECK_AND_WAIT_RECLAIM_DONE(ctx, ob);
        uniq_skiplist_iterator(ob->slices, &it);
        while ((slice=(OBSliceEntry *)uniq_skiplist_next(&it)) != NULL) {
            *dec_alloc += slice->ssize.length;
            if (htable->modify_sallocator) {
                storage_allocator_delete_slice(slice,
                        htable->modify_used_space);
            }
        }

        OB_INDEX_DELETE_OB_ENTRY(ctx, bucket, ob, previous);
        if (*dec_alloc > 0) {
            if (sn != NULL) {
                *sn = __sync_add_and_fetch(&SLICE_BINLOG_SN, 1);
            }
            result = 0;
        } else {  //no slices deleted
            result = ENOENT;
        }
    } else {
        result = ENOENT;
    }
    PTHREAD_MUTEX_UNLOCK(&ctx->lcp.lock);

    return result;
}

static int add_to_slice_ptr_array(OBSlicePtrArray *array,
        OBSliceEntry *slice)
{
    if (array->alloc <= array->count) {
        int alloc;
        int bytes;
        OBSliceEntry **slices;

        if (array->alloc == 0) {
            alloc = 256;
        } else {
            alloc = array->alloc * 2;
        }
        bytes = sizeof(OBSliceEntry *) * alloc;
        slices = (OBSliceEntry **)fc_malloc(bytes);
        if (slices == NULL) {
            return ENOMEM;
        }

        if (array->slices != NULL) {
            memcpy(slices, array->slices, sizeof(OBSliceEntry *) *
                    array->count);
            free(array->slices);
        }

        array->alloc = alloc;
        array->slices = slices;
    }

    array->slices[array->count++] = slice;
    return 0;
}

static inline int dup_slice_to_array(OBSharedContext *ctx,
        const OBSliceEntry *src_slice, const int offset,
        const int length, OBSlicePtrArray *array)
{
    OBSliceEntry *new_slice;

    new_slice = slice_dup(ctx, src_slice, offset, length);
    if (new_slice == NULL) {
        return ENOMEM;
    }

    return add_to_slice_ptr_array(array, new_slice);
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

static int get_slices(OBSharedContext *ctx, OBEntry *ob,
        const FSBlockSliceKeyInfo *bs_key, OBSlicePtrArray *sarray)
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
            if ((result=dup_slice_to_array(ctx, curr_slice, bs_key->
                            slice.offset, length, sarray)) != 0)
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
            if ((result=dup_slice_to_array(ctx, curr_slice, curr_slice->
                            ssize.offset, slice_end - curr_slice->
                            ssize.offset, sarray)) != 0)
            {
                return result;
            }
        } else {
            __sync_add_and_fetch(&curr_slice->ref_count, 1);
            if ((result=add_to_slice_ptr_array(sarray, curr_slice)) != 0) {
                return result;
            }
        }

        node = UNIQ_SKIPLIST_LEVEL0_NEXT_NODE(node);
    } while (node != ob->slices->factory->tail);

    return sarray->count > 0 ? 0 : ENOENT;
}

static void free_slices(OBSlicePtrArray *sarray)
{
    OBSliceEntry **pp;
    OBSliceEntry **end;

    if (sarray->count == 0) {
        return;
    }

    end = sarray->slices + sarray->count;
    for (pp=sarray->slices; pp<end; pp++) {
        ob_index_free_slice(*pp);
    }

    sarray->count = 0;
}

int ob_index_get_slices_ex(OBHashtable *htable,
        const FSBlockSliceKeyInfo *bs_key,
        OBSlicePtrArray *sarray, const bool is_reclaim)
{
    OBEntry *ob;
    int result;

    OB_INDEX_SET_BUCKET_AND_CTX(htable, bs_key->block);
    sarray->count = 0;

    /*
    logInfo("file: "__FILE__", line: %d, func: %s, "
            "block key: %"PRId64", offset: %"PRId64,
            __LINE__, __FUNCTION__, bs_key->block.oid, bs_key->block.offset);
            */

    PTHREAD_MUTEX_LOCK(&ctx->lcp.lock);
    ob = get_ob_entry(ctx, bucket, &bs_key->block, false);
    if (ob == NULL) {
        result = ENOENT;
    } else {
        CHECK_AND_WAIT_RECLAIM_DONE(ctx, ob);
        result = get_slices(ctx, ob, bs_key, sarray);
    }
    PTHREAD_MUTEX_UNLOCK(&ctx->lcp.lock);

    if (result != 0 && sarray->count > 0) {
        free_slices(sarray);
    }
    return result;
}

void ob_index_get_ob_and_slice_counts(int64_t *ob_count, int64_t *slice_count)
{
    OBSharedContext *ctx;
    OBSharedContext *end;

    *ob_count = *slice_count = 0;
    end = ob_shared_ctx_array.contexts + ob_shared_ctx_array.count;
    for (ctx=ob_shared_ctx_array.contexts; ctx<end; ctx++) {
        *ob_count += ctx->ob_allocator.info.element_used_count;
        *slice_count += ctx->slice_allocator.info.element_used_count;
    }
}

int ob_index_dump_slices_to_trunk_ex(OBHashtable *htable,
        int64_t *slice_count)
{
    int result;
    OBEntry **bucket;
    OBEntry **end;
    OBEntry *ob;
    OBSliceEntry *slice;
    UniqSkiplistIterator it;

    *slice_count = 0;
    end = htable->buckets + htable->capacity;
    for (bucket=htable->buckets; bucket<end; bucket++) {
        if (*bucket == NULL) {
            continue;
        }

        ob = *bucket;
        do {
            uniq_skiplist_iterator(ob->slices, &it);
            while ((slice=(OBSliceEntry *)uniq_skiplist_next(&it)) != NULL) {
                if ((result=storage_allocator_add_slice(slice,
                                htable->modify_used_space)) != 0)
                {
                    return result;
                }
                ++(*slice_count);
            }

            ob = ob->next;
        } while (ob != NULL);
    }

    htable->modify_sallocator = true;
    return 0;
}
