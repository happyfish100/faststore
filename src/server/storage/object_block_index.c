#include <limits.h>
#include <sys/stat.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/uniq_skiplist.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "storage_allocator.h"
#include "object_block_index.h"

//TODO fixeme!!!
#define SLICE_ARRAY_FIXED_COUNT     4
//#define SLICE_ARRAY_FIXED_COUNT  64

typedef struct {
    UniqSkiplistFactory factory;
    struct fast_mblock_man ob_allocator;    //for ob_entry
    struct fast_mblock_man slice_allocator; //for slice_entry 
    pthread_mutex_t lock;
} OBSharedContext;

typedef struct {
    int count;
    OBSharedContext *contexts;
} OBSharedContextArray;

typedef struct {
    int64_t count;
    int64_t capacity;
    OBEntry **buckets;
} OBHashtable;

typedef struct ob_slice_ptr_array {
    int alloc;
    int count;
    OBSliceEntry **entries;
    OBSliceEntry *fixed[SLICE_ARRAY_FIXED_COUNT];
} OBSlicePtrArray;

static OBSharedContextArray ob_shared_ctx_array = {0, NULL};
static OBHashtable ob_hashtable = {0, 0, NULL};

static int slice_compare(const void *p1, const void *p2)
{
    return ((OBSliceEntry *)p1)->offset - ((OBSliceEntry *)p2)->offset;
}

static void slice_free_func(void *ptr, const int delay_seconds)
{
    OBSharedContext *ctx;
    int64_t bucket_index;

    bucket_index = ((OBSliceEntry *)ptr)->ob->bkey.hash_code %
        ob_hashtable.capacity;
    ctx = ob_shared_ctx_array.contexts + bucket_index %
        ob_shared_ctx_array.count;
    pthread_mutex_lock(&ctx->lock);
    if (delay_seconds > 0) {
        fast_mblock_delay_free_object(&ctx->ob_allocator, ptr, delay_seconds);
    } else {
        fast_mblock_free_object(&ctx->ob_allocator, ptr);
    }
    pthread_mutex_unlock(&ctx->lock);
}

static int init_ob_shared_ctx_array()
{
    int result;
    int bytes;
    const int max_level_count = 12;
    const int alloc_skiplist_once = 8 * 1024;
    const int min_alloc_elements_once = 4;
    const int delay_free_seconds = 0;
    const bool bidirection = true;  //need previous link in level 0
    OBSharedContext *ctx;
    OBSharedContext *end;

    ob_shared_ctx_array.count = STORAGE_CFG.object_block.locks_count;
    bytes = sizeof(OBSharedContext) * ob_shared_ctx_array.count;
    ob_shared_ctx_array.contexts = (OBSharedContext *)malloc(bytes);
    if (ob_shared_ctx_array.contexts == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
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
                        "ob_entry", sizeof(OBEntry), 16 * 1024,
                        NULL, NULL, false)) != 0)
        {
            return result;
        }

        if ((result=fast_mblock_init_ex1(&ctx->slice_allocator,
                        "slice_entry", sizeof(OBSliceEntry),
                        64 * 1024, NULL, NULL, false)) != 0)
        {
            return result;
        }

        if ((result=init_pthread_lock(&ctx->lock)) != 0) {
            logError("file: "__FILE__", line: %d, "
                    "init_pthread_lock fail, errno: %d, error info: %s",
                    __LINE__, result, STRERROR(result));
            return result;
        }
    }

    return 0;
}

static int init_ob_hashtable()
{
    int bytes;

    ob_hashtable.capacity = STORAGE_CFG.object_block.hashtable_capacity;
    bytes = sizeof(OBEntry *) * ob_hashtable.capacity;
    ob_hashtable.buckets = (OBEntry **)malloc(bytes);
    if (ob_hashtable.buckets == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(ob_hashtable.buckets, 0, bytes);

    return 0;
}

int ob_index_init()
{
    int result;

    if ((result=init_ob_shared_ctx_array()) != 0) {
        return result;
    }

    if ((result=init_ob_hashtable()) != 0) {
        return result;
    }

    return 0;
}

void ob_index_destroy()
{
}

static int compare_block_key(const FSBlockKey *bkey1, const FSBlockKey *bkey2)
{
    int64_t sub;

    sub = bkey1->inode - bkey2->inode;
    if (sub < 0) {
        return -1;
    } else if (sub > 0) {
        return 1;
    }

    sub = bkey1->offset - bkey2->offset;
    if (sub < 0) {
        return -1;
    } else if (sub > 0) {
        return 1;
    }
    return 0;
}

static OBEntry *get_ob_entry(OBSharedContext *ctx, OBEntry **bucket,
        const FSBlockKey *bkey)
{
    const int init_level_count = 2;
    OBEntry *previous;
    OBEntry *ob;
    int cmpr;

    if (*bucket == NULL) {
        previous = NULL;
    } else {
        cmpr = compare_block_key(bkey, &(*bucket)->bkey);
        if (cmpr == 0) {
            return *bucket;
        } else if (cmpr < 0) {
            previous = NULL;
        } else {
            previous = *bucket;
            while (previous->next != NULL) {
                cmpr = compare_block_key(bkey, &previous->next->bkey);
                if (cmpr == 0) {
                    return previous->next;
                } else if (cmpr < 0) {
                    break;
                }

                previous = previous->next;
            }
        }
    }

    ob = fast_mblock_alloc_object(&ctx->ob_allocator);
    if (ob == NULL) {
        return NULL;
    }
    ob->slices = uniq_skiplist_new(&ctx->factory, init_level_count);
    if (ob->slices == NULL) {
        fast_mblock_free_object(&ctx->ob_allocator, ob);
        return NULL;
    }

    ob->bkey = *bkey;
    if (previous == NULL) {
        ob->next = *bucket;
        *bucket = ob;
    } else {
        ob->next = previous->next;
        previous->next = ob;
    }
    return ob;
}

static inline int do_delete_slice(OBEntry *ob, OBSliceEntry *slice)
{
    int result;

    if ((result=uniq_skiplist_delete(ob->slices, slice)) != 0) {
        return result;
    }
    return storage_allocator_delete_slice(slice);
}

static inline int do_add_slice(OBEntry *ob, OBSliceEntry *slice)
{
    int result;
    if ((result=uniq_skiplist_insert(ob->slices, slice)) != 0) {
        return result;
    }
    return storage_allocator_add_slice(slice);
}

static inline OBSliceEntry *splice_dup(OBSharedContext *ctx,
        const OBSliceEntry *src)
{
    OBSliceEntry *slice;

    slice = fast_mblock_alloc_object(&ctx->slice_allocator);
    if (slice == NULL) {
        return NULL;
    }
    *slice = *src;
    return slice;
}

static int add_to_slice_ptr_array(OBSlicePtrArray *array, OBSliceEntry *slice)
{
    if (array->alloc <= array->count) {
        int alloc;
        int bytes;
        OBSliceEntry **entries;

        alloc = array->alloc * 2;
        bytes = sizeof(OBSliceEntry *) * alloc;
        entries = (OBSliceEntry **)malloc(bytes);
        if (entries == NULL) {
            logError("file: "__FILE__", line: %d, "
                    "malloc %d bytes fail", __LINE__, bytes);
            return ENOMEM;
        }

        memcpy(entries, array->entries, sizeof(OBSliceEntry *) * array->count);
        if (array->entries != array->fixed) {
            free(array->entries);
        }

        array->alloc = alloc;
        array->entries = entries;
    }

    array->entries[array->count++] = slice;
    return 0;
}

static inline int dup_slice_to_array(OBSharedContext *ctx,
        const OBSliceEntry *src_slice, const int offset,
        const int length, OBSlicePtrArray *array)
{
    OBSliceEntry *new_slice;

    new_slice = splice_dup(ctx, src_slice);
    if (new_slice == NULL) {
        return ENOMEM;
    }

    new_slice->offset = offset;
    new_slice->length = length;
    return add_to_slice_ptr_array(array, new_slice);
}

#define INIT_SLICE_PTR_ARRAY(sarray) \
    do {   \
        sarray.count = 0;  \
        sarray.alloc = SLICE_ARRAY_FIXED_COUNT;  \
        sarray.entries = sarray.fixed;  \
    } while (0)

#define FREE_SLICE_PTR_ARRAY(sarray) \
    do { \
        if (sarray.entries != sarray.fixed) { \
            free(sarray.entries);  \
        } \
    } while (0)


static int add_slice(OBSharedContext *ctx, OBEntry *ob, OBSliceEntry *slice)
{
    UniqSkiplistNode *node;
    UniqSkiplistNode *previous;
    OBSliceEntry *curr_slice;
    OBSlicePtrArray add_slice_array;
    OBSlicePtrArray del_slice_array;
    int result;
    int curr_end;
    int slice_end;
    int i;

    node = uniq_skiplist_find_ge_node(ob->slices, (void *)slice);
    if (node == NULL) {
        return do_add_slice(ob, slice);
    }

    INIT_SLICE_PTR_ARRAY(add_slice_array);
    INIT_SLICE_PTR_ARRAY(del_slice_array);

    slice_end = slice->offset + slice->length;
    previous = SKIPLIST_LEVEL0_PREV_NODE(node);
    if (previous != ob->slices->top) {
        curr_slice = (OBSliceEntry *)previous->data;
        curr_end = curr_slice->offset + curr_slice->length;
        if (curr_end > slice->offset) {  //overlap
            if ((result=add_to_slice_ptr_array(&del_slice_array,
                            curr_slice)) != 0)
            {
                return result;
            }

            if ((result=dup_slice_to_array(ctx, curr_slice, curr_slice->
                            offset, slice->offset - curr_slice->offset,
                            &add_slice_array)) != 0)
            {
                return result;
            }

            if (curr_end > slice_end) {
                if ((result=dup_slice_to_array(ctx, curr_slice, slice_end,
                                curr_end - slice_end, &add_slice_array)) != 0)
                {
                    return result;
                }
            }
        }
    }

    do {
        curr_slice = (OBSliceEntry *)node->data;
        if (slice_end <= curr_slice->offset) {  //not overlap
            break;
        }

        if ((result=add_to_slice_ptr_array(&del_slice_array,
                        curr_slice)) != 0)
        {
            return result;
        }

        curr_end = curr_slice->offset + curr_slice->length;
        if (curr_end > slice_end) {
            if ((result=dup_slice_to_array(ctx, curr_slice, slice_end,
                            curr_end - slice_end, &add_slice_array)) != 0)
            {
                return result;
            }

            break;
        }

        node = SKIPLIST_LEVEL0_NEXT_NODE(node);
    } while (node != ob->slices->factory->tail);

    for (i=0; i<del_slice_array.count; i++) {
        do_delete_slice(ob, del_slice_array.entries[i]);
    }
    FREE_SLICE_PTR_ARRAY(del_slice_array);

    for (i=0; i<add_slice_array.count; i++) {
        do_add_slice(ob, add_slice_array.entries[i]);
    }
    FREE_SLICE_PTR_ARRAY(add_slice_array);

    return do_add_slice(ob, slice);
}

int ob_index_add_slice(OBSliceEntry *slice)
{
    OBSharedContext *ctx;
    int64_t bucket_index;
    int result;

    bucket_index = slice->ob->bkey.hash_code % ob_hashtable.capacity;
    ctx = ob_shared_ctx_array.contexts + bucket_index %
        ob_shared_ctx_array.count;

    pthread_mutex_lock(&ctx->lock);
    result = add_slice(ctx, slice->ob, slice);
    pthread_mutex_unlock(&ctx->lock);

    return result;
}

OBSliceEntry *ob_index_alloc_slice(const FSBlockKey *bkey)
{
    OBSharedContext *ctx;
    OBEntry **bucket;
    OBEntry *ob;
    OBSliceEntry *slice;
    int64_t bucket_index;

    bucket_index = bkey->hash_code % ob_hashtable.capacity;
    ctx = ob_shared_ctx_array.contexts + bucket_index %
        ob_shared_ctx_array.count;
    bucket = ob_hashtable.buckets + bucket_index;

    pthread_mutex_lock(&ctx->lock);
    ob = get_ob_entry(ctx, bucket, bkey);
    if (ob == NULL) {
        slice = NULL;
    } else {
        slice = fast_mblock_alloc_object(&ctx->slice_allocator);
        slice->ob = ob;
    }
    pthread_mutex_unlock(&ctx->lock);

    return slice;
}

void ob_index_free_slice(OBSliceEntry *slice)
{
    OBSharedContext *ctx;
    int64_t bucket_index;

    bucket_index = slice->ob->bkey.hash_code % ob_hashtable.capacity;
    ctx = ob_shared_ctx_array.contexts + bucket_index %
        ob_shared_ctx_array.count;

    pthread_mutex_lock(&ctx->lock);
    fast_mblock_free_object(&ctx->slice_allocator, slice);
    pthread_mutex_unlock(&ctx->lock);
}
