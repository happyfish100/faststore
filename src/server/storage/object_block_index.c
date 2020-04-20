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
    struct fast_mblock_man allocator;  //for ob_entry
    pthread_mutex_t lock;
} OBSharedContext;

typedef struct {
    int count;
    OBSharedContext *contexts;
} OBSharedContextArray;

typedef struct {
    int offset; //offset in the object block
    int length; //slice length
    FSTrunkSpaceInfo space;
} OBSliceEntry;

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
    //TODO
}

static int init_ob_shared_ctx_array()
{
    int result;
    int bytes;
    const int max_level_count = 12;
    const int alloc_skiplist_once = 8 * 1024;
    const int min_alloc_elements_once = 4;
    const int delay_free_seconds = 0;
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
        if ((result=uniq_skiplist_init_ex(&ctx->factory, max_level_count,
                        slice_compare, slice_free_func, alloc_skiplist_once,
                        min_alloc_elements_once, delay_free_seconds)) != 0)
        {
            return result;
        }

        if ((result=fast_mblock_init_ex1(&ctx->allocator, "ob_entry",
                        sizeof(OBEntry), 16 * 1024, NULL, NULL, false)) != 0)
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
