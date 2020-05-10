#include <limits.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fast_mblock.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "trunk_prealloc.h"
#include "trunk_allocator.h"

typedef struct {
    bool allocator_inited;
    struct fast_mblock_man trunk_allocator;
    struct fast_mblock_man free_node_allocator;
    UniqSkiplistFactory skiplist_factory;
} TrunkAllocatorGlobalVars;

static TrunkAllocatorGlobalVars g_trunk_allocator_vars = {false};

#define G_TRUNK_ALLOCATOR     g_trunk_allocator_vars.trunk_allocator
#define G_FREE_NODE_ALLOCATOR g_trunk_allocator_vars.free_node_allocator
#define G_SKIPLIST_FACTORY    g_trunk_allocator_vars.skiplist_factory

static int compare_trunk_info(const void *p1, const void *p2)
{
    int64_t sub;
    sub = ((FSTrunkFileInfo *)p1)->id_info.id -
        ((FSTrunkFileInfo *)p2)->id_info.id;
    if (sub < 0) {
        return -1;
    } else if (sub > 0) {
        return 1;
    } else {
        return 0;
    }
}

static void trunk_free_func(void *ptr, const int delay_seconds)
{
    FSTrunkFileInfo *trunk_info;
    trunk_info = (FSTrunkFileInfo *)ptr;

    if (delay_seconds > 0) {
        fast_mblock_delay_free_object(&G_TRUNK_ALLOCATOR, trunk_info,
                delay_seconds);
    } else {
        fast_mblock_free_object(&G_TRUNK_ALLOCATOR, trunk_info);
    }
}

static void init_freelists(FSTrunkAllocator *allocator)
{
    FSTrunkFreelistPair *pair;
    FSTrunkFreelistPair *end;

    end = allocator->freelists + allocator->path_info->write_thread_count;
    for (pair=allocator->freelists; pair<end; pair++) {
        pair->normal.prealloc_trunks = allocator->path_info->prealloc_trunks;
        pair->reclaim.prealloc_trunks = 2;
    }
}

int trunk_allocator_init(FSTrunkAllocator *allocator,
        FSStoragePathInfo *path_info)
{
    const int min_alloc_elements_once = 4;
    int alloc_skiplist_once;
    int result;
    int bytes;

    if (!g_trunk_allocator_vars.allocator_inited) {
        g_trunk_allocator_vars.allocator_inited = true;
        if ((result=fast_mblock_init_ex2(&G_TRUNK_ALLOCATOR,
                        "trunk_file_info", sizeof(FSTrunkFileInfo),
                        16384, NULL, NULL, true, NULL, NULL, NULL)) != 0)
        {
            return result;
        }

        if ((result=fast_mblock_init_ex2(&G_FREE_NODE_ALLOCATOR,
                        "trunk_free_node", sizeof(FSTrunkFreeNode),
                        8 * 1024, NULL, NULL, true, NULL, NULL, NULL)) != 0)
        {
            return result;
        }

        alloc_skiplist_once = STORAGE_CFG.store_path.count +
            STORAGE_CFG.write_cache.count;
        if ((result=uniq_skiplist_init_ex(&G_SKIPLIST_FACTORY,
                        FS_TRUNK_SKIPLIST_MAX_LEVEL_COUNT,
                        compare_trunk_info, trunk_free_func,
                        alloc_skiplist_once, min_alloc_elements_once,
                        FS_TRUNK_SKIPLIST_DELAY_FREE_SECONDS)) != 0)
        {
            return result;
        }
    }

    if ((result=init_pthread_lock(&allocator->lock)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "init_pthread_lock fail, errno: %d, error info: %s",
                __LINE__, result, STRERROR(result));
        return result;
    }
        
    if ((result=pthread_cond_init(&allocator->cond, NULL)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "pthread_cond_init fail, errno: %d, error info: %s",
                __LINE__, result, STRERROR(result));
        return result;
    }

    if ((allocator->sl_trunks=uniq_skiplist_new(&G_SKIPLIST_FACTORY,
            FS_TRUNK_SKIPLIST_INIT_LEVEL_COUNT)) == NULL)
    {
        return ENOMEM;
    }

    bytes = sizeof(FSTrunkFreelistPair) * path_info->write_thread_count;
    allocator->freelists = (FSTrunkFreelistPair *)malloc(bytes);
    if (allocator->freelists == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(allocator->freelists, 0, bytes);

    allocator->priority_array.alloc = allocator->priority_array.count = 0;
    allocator->priority_array.trunks = NULL;
    allocator->path_info = path_info;

    init_freelists(allocator);
    return 0;
}

int trunk_allocator_add(FSTrunkAllocator *allocator,
        const FSTrunkIdInfo *id_info, const int64_t size,
        FSTrunkFileInfo **pp_trunk)
{
    FSTrunkFileInfo *trunk_info;
    int result;

    trunk_info = (FSTrunkFileInfo *)fast_mblock_alloc_object(
            &G_TRUNK_ALLOCATOR);
    if (trunk_info == NULL) {
        if (pp_trunk != NULL) {
            *pp_trunk = NULL;
        }
        return ENOMEM;
    }

    trunk_info->status = FS_TRUNK_STATUS_NONE;
    trunk_info->id_info = *id_info;
    trunk_info->size = size;
    trunk_info->used.bytes = 0;
    trunk_info->used.count = 0;
    trunk_info->free_start = 0;
    FC_INIT_LIST_HEAD(&trunk_info->used.slice_head);

    PTHREAD_MUTEX_LOCK(&allocator->lock);
    result = uniq_skiplist_insert(allocator->sl_trunks, trunk_info);
    PTHREAD_MUTEX_UNLOCK(&allocator->lock);

    if (result != 0) {
        fast_mblock_free_object(&G_TRUNK_ALLOCATOR, trunk_info);
        trunk_info = NULL;
    }
    if (pp_trunk != NULL) {
        *pp_trunk = trunk_info;
    }
    return result;
}

int trunk_allocator_delete(FSTrunkAllocator *allocator, const int64_t id)
{
    FSTrunkFileInfo target;
    int result;

    target.id_info.id = id;
    PTHREAD_MUTEX_LOCK(&allocator->lock);
    result = uniq_skiplist_delete(allocator->sl_trunks, &target);
    PTHREAD_MUTEX_UNLOCK(&allocator->lock);

    return result;
}

#define TRUNK_ALLOC_SPACE(allocator, trunk_info, space_info, alloc_size) \
    do { \
        space_info->store = &allocator->path_info->store; \
        space_info->id_info = trunk_info->id_info;   \
        space_info->offset = trunk_info->free_start; \
        space_info->size = alloc_size;         \
        trunk_info->free_start += alloc_size;  \
    } while (0)

static void remove_trunk_from_freelist(FSTrunkAllocator *allocator,
        FSTrunkFreelist *freelist)
{
    FSTrunkFreeNode *node;

    node = freelist->head;
    node->trunk_info->status = FS_TRUNK_STATUS_NONE;
    freelist->head = freelist->head->next;
    if (freelist->head == NULL) {
        freelist->tail = NULL;
    }
    freelist->count--;

    fast_mblock_free_object(&G_FREE_NODE_ALLOCATOR, node);
    trunk_prealloc_push(allocator, freelist, freelist->prealloc_trunks);
}

static void prealloc_trunks(FSTrunkAllocator *allocator,
        FSTrunkFreelist *freelist)
{
    int count;
    int i;

    count = freelist->prealloc_trunks - freelist->count;
    logInfo("%s prealloc count: %d", allocator->path_info->store.path.str, count);
    for (i=0; i<count; i++) {
        trunk_prealloc_push(allocator, freelist, freelist->prealloc_trunks);
    }
}

void trunk_allocator_prealloc_trunks(FSTrunkAllocator *allocator)
{
    FSTrunkFreelistPair *pair;
    FSTrunkFreelistPair *end;

    end = allocator->freelists + allocator->path_info->write_thread_count;
    for (pair=allocator->freelists; pair<end; pair++) {
        prealloc_trunks(allocator, &pair->normal);
        prealloc_trunks(allocator, &pair->reclaim);
    }
}

void trunk_allocator_add_to_freelist(FSTrunkAllocator *allocator,
        FSTrunkFreelist *freelist, FSTrunkFileInfo *trunk_info)
{
    FSTrunkFreeNode *node;
    bool notify;

    node = (FSTrunkFreeNode *)fast_mblock_alloc_object(
            &G_FREE_NODE_ALLOCATOR);
    if (node == NULL) {
        return;
    }

    node->trunk_info = trunk_info;
    node->next = NULL;

    PTHREAD_MUTEX_LOCK(&allocator->lock);
    if (freelist->head == NULL) {
        freelist->head = node;
        notify = true;
    } else {
        freelist->tail->next = node;
        notify = false;
    }
    freelist->tail = node;

    freelist->count++;
    trunk_info->status = FS_TRUNK_STATUS_ALLOCING;
    PTHREAD_MUTEX_UNLOCK(&allocator->lock);

    if (notify) {
        pthread_cond_signal(&allocator->cond);
    }
}

void trunk_allocator_array_to_freelists(FSTrunkAllocator *allocator,
        const FSTrunkInfoPtrArray *trunk_ptr_array)
{
    FSTrunkFileInfo **pp;
    int count;
    int i;

    if (trunk_ptr_array->count == 0) {
        return;
    }

    if (trunk_ptr_array->count > 2 * allocator->path_info->write_thread_count) {
        count = 2 * allocator->path_info->write_thread_count;
    } else {
        count = trunk_ptr_array->count;
    }

    pp = trunk_ptr_array->trunks + trunk_ptr_array->count - 1;
    for (i=0; i<count/2; i++) {
        trunk_allocator_add_to_freelist(allocator,
                &allocator->freelists[i].reclaim, *pp--);
        trunk_allocator_add_to_freelist(allocator,
                &allocator->freelists[i].reclaim, *pp--);
    }

    for (; pp>=trunk_ptr_array->trunks; pp--) {
        trunk_allocator_add_to_freelist(allocator,
                &allocator->freelists[(pp - trunk_ptr_array->trunks) %
                allocator->path_info->write_thread_count].normal, *pp);
    }
}

static int alloc_space(FSTrunkAllocator *allocator, FSTrunkFreelist *freelist,
        const uint32_t blk_hc, const int size, FSTrunkSpaceInfo *spaces,
        int *count, const bool blocked)
{
    int aligned_size;
    int result;
    int remain_bytes;
    FSTrunkSpaceInfo *space_info;
    FSTrunkFileInfo *trunk_info;

    aligned_size = MEM_ALIGN(size);
    space_info = spaces;

    PTHREAD_MUTEX_LOCK(&allocator->lock);
    do {
        if (freelist->head != NULL) {
            trunk_info = freelist->head->trunk_info;
            remain_bytes = trunk_info->size - trunk_info->free_start;
            if (remain_bytes < aligned_size) {
                if (!blocked && freelist->count <= 1) {
                    result = EAGAIN;
                    break;
                }

                TRUNK_ALLOC_SPACE(allocator, trunk_info,
                        space_info, remain_bytes);
                space_info++;

                aligned_size -= remain_bytes;
                remove_trunk_from_freelist(allocator, freelist);
            }
        }

        if (freelist->head == NULL) {
            if (!blocked) {
                result = EAGAIN;
                break;
            }
            pthread_cond_wait(&allocator->cond, &allocator->lock);
        }

        if (freelist->head == NULL) {
            result = EINTR;
            break;
        }

        TRUNK_ALLOC_SPACE(allocator, freelist->head->trunk_info,
                space_info, aligned_size);
        space_info++;
        if (freelist->head->trunk_info->size - freelist->head->trunk_info->
                free_start < STORAGE_CFG.discard_remain_space_size)
        {
            remove_trunk_from_freelist(allocator, freelist);
        }
        result = 0;
    } while (0);
    PTHREAD_MUTEX_UNLOCK(&allocator->lock);

    *count = space_info - spaces;
    return result;
}

int trunk_allocator_normal_alloc(FSTrunkAllocator *allocator,
        const uint32_t blk_hc, const int size,
        FSTrunkSpaceInfo *spaces, int *count)
{
    FSTrunkFreelist *freelist;

    freelist = &allocator->freelists[blk_hc % allocator->
        path_info->write_thread_count].normal;
    return alloc_space(allocator, freelist, blk_hc, size, spaces,
            count, true);
}

int trunk_allocator_reclaim_alloc(FSTrunkAllocator *allocator,
        const uint32_t blk_hc, const int size,
        FSTrunkSpaceInfo *spaces, int *count)
{
    int result;
    FSTrunkFreelist *freelist;

    freelist = &allocator->freelists[blk_hc % allocator->
        path_info->write_thread_count].normal;
    if ((result=alloc_space(allocator, freelist, blk_hc, size, spaces,
                    count, false)) == 0)
    {
        return 0;
    }

    freelist = &allocator->freelists[blk_hc % allocator->
        path_info->write_thread_count].reclaim;
    return alloc_space(allocator, freelist, blk_hc, size, spaces,
            count, false);
}

static int check_alloc_trunk_ptr_array(FSTrunkInfoPtrArray *parray,
        const int target_count)
{
    int alloc;
    int bytes;
    FSTrunkFileInfo **trunks;

    if (parray->alloc >= target_count) {
        return 0;
    }

    if (parray->alloc == 0) {
        alloc = 64;
    } else {
        alloc = parray->alloc * 2;
    }

    while (alloc < target_count) {
        alloc *= 2;
    }

    bytes = sizeof(FSTrunkFileInfo *) * alloc;
    trunks = (FSTrunkFileInfo **)malloc(bytes);
    if (trunks == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }

    if (parray->trunks != NULL) {
        free(parray->trunks);
    }

    parray->trunks = trunks;
    parray->alloc = alloc;
    return 0;
}

const FSTrunkInfoPtrArray *trunk_allocator_free_size_top_n(
            FSTrunkAllocator *allocator, const int count)
{
    UniqSkiplistIterator it;
    FSTrunkFileInfo *trunk_info;
    FSTrunkFileInfo **pp;
    FSTrunkFileInfo **end;
    int64_t remain_size;

    allocator->priority_array.count = 0;
    if (check_alloc_trunk_ptr_array(&allocator->priority_array, count) != 0) {
        return &allocator->priority_array;
    }

    end = allocator->priority_array.trunks + count;
    uniq_skiplist_iterator(allocator->sl_trunks, &it);
    while ((trunk_info=uniq_skiplist_next(&it)) != NULL) {
        remain_size = trunk_info->size - trunk_info->free_start;
        if (remain_size < FS_FILE_BLOCK_SIZE) {
            continue;
        }
        if ((trunk_info->used.bytes > 0) && ((double)trunk_info->used.bytes /
                    (double)trunk_info->free_start <= 0.80))
        {
            continue;
        }

        if (allocator->priority_array.count < count) {
            allocator->priority_array.trunks[allocator->
                priority_array.count++] = trunk_info;
            continue;
        } else if (remain_size <= allocator->priority_array.trunks[0]->size -
                allocator->priority_array.trunks[0]->free_start)
        {
            continue;
        }

        pp = allocator->priority_array.trunks + 1;
        while ((pp < end) && (remain_size > (*pp)->size - (*pp)->free_start)) {
            *(pp - 1) = *pp;
            pp++;
        }
        *(pp - 1) = trunk_info;
    }

    return &allocator->priority_array;
}

int trunk_allocator_add_slice(FSTrunkAllocator *allocator, OBSliceEntry *slice)
{
    int result;
    FSTrunkFileInfo target;
    FSTrunkFileInfo *trunk_info;

    target.id_info.id = slice->space.id_info.id;
    PTHREAD_MUTEX_LOCK(&allocator->lock);
    if ((trunk_info=(FSTrunkFileInfo *)uniq_skiplist_find(
                    allocator->sl_trunks, &target)) == NULL)
    {
        logError("file: "__FILE__", line: %d, "
                "trunk id: %"PRId64" not exist",
                __LINE__, slice->space.id_info.id);
        result = ENOENT;
    } else {
        trunk_info->used.bytes += slice->space.size;
        trunk_info->used.count++;
        fc_list_add_tail(&slice->dlink, &trunk_info->used.slice_head);
        result = 0;
    }
    PTHREAD_MUTEX_UNLOCK(&allocator->lock);

    return result;
}

int trunk_allocator_delete_slice(FSTrunkAllocator *allocator,
        OBSliceEntry *slice)
{
    int result;
    FSTrunkFileInfo target;
    FSTrunkFileInfo *trunk_info;

    target.id_info.id = slice->space.id_info.id;
    PTHREAD_MUTEX_LOCK(&allocator->lock);
    if ((trunk_info=(FSTrunkFileInfo *)uniq_skiplist_find(
                    allocator->sl_trunks, &target)) == NULL)
    {
        logError("file: "__FILE__", line: %d, "
                "trunk id: %"PRId64" not exist",
                __LINE__, slice->space.id_info.id);
        result = ENOENT;
    } else {
        trunk_info->used.bytes -= slice->space.size;
        trunk_info->used.count--;
        fc_list_del_init(&slice->dlink);
        result = 0;
    }
    PTHREAD_MUTEX_UNLOCK(&allocator->lock);

    return result;
}
