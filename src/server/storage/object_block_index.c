#include <limits.h>
#include <sys/stat.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/uniq_skiplist.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "object_block_index.h"

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

typedef struct ob_entry {
    FSBlockKey bkey;
    UniqSkiplist *slices;   //the element is OBSliceEntry
    struct ob_entry *next; //for hashtable
} OBEntry;

typedef struct {
    int64_t count;
    int64_t capacity;
    OBEntry **buckets;
} OBHashtable;

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

    bucket_index = ((OBSliceEntry *)ptr)->bkey->hash_code %
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
    const bool bidirection = true;
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

int object_block_index_init()
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

void object_block_index_destroy()
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

static int delete_one_splice(OBSharedContext *ctx, OBEntry *ob,
        const OBSliceEntry *src)
{
}

static int add_one_splice(OBSharedContext *ctx, OBEntry *ob,
        const OBSliceEntry *src)
{
    OBSliceEntry *slice;
    slice = fast_mblock_alloc_object(&ctx->slice_allocator);
    if (slice == NULL) {
        return ENOMEM;
    }

    *slice = *src;
    slice->bkey = &ob->bkey;

    //TODO add to trunk
    return uniq_skiplist_insert(ob->slices, slice);
}

static int add_splice(OBSharedContext *ctx, OBEntry *ob,
        const OBSliceEntry *slice)
{
    UniqSkiplistNode *node;

    node = uniq_skiplist_find_ge_node(ob->slices, (void *)slice);
    if (node == NULL) {
        return add_one_splice(ctx, ob, slice);
    }

    return 0;
}

int object_block_index_add(const OBSliceEntry *slice)
{
    OBEntry **bucket;
    OBSharedContext *ctx;
    OBEntry *ob;
    int64_t bucket_index;
    int result;

    bucket_index = slice->bkey->hash_code % ob_hashtable.capacity;
    ctx = ob_shared_ctx_array.contexts + bucket_index %
        ob_shared_ctx_array.count;
    bucket = ob_hashtable.buckets + bucket_index;

    pthread_mutex_lock(&ctx->lock);
    ob = get_ob_entry(ctx, bucket, slice->bkey);
    if (ob == NULL) {
        result = ENOMEM;
    } else {
        result = add_splice(ctx, ob, slice);
    }
    pthread_mutex_unlock(&ctx->lock);

    return result;
}
