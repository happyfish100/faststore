#include <limits.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fast_mblock.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "trunk_allocator.h"

static bool g_allocator_inited = false;
static struct fast_mblock_man g_trunk_allocator;
static UniqSkiplistFactory g_skiplist_factory;

static int compare_trunk_info(const void *p1, const void *p2)
{
    int64_t sub;
    sub = ((FSTrunkFileInfo *)p1)->id - ((FSTrunkFileInfo *)p2)->id;
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
        fast_mblock_delay_free_object(&g_trunk_allocator, trunk_info,
                delay_seconds);
    } else {
        fast_mblock_free_object(&g_trunk_allocator, trunk_info);
    }
}

int trunk_allocator_init(FSTrunkAllocator *allocator,
        FSStoragePathInfo *path_info)
{
    int result;

    if (!g_allocator_inited) {
        g_allocator_inited = true;
        if ((result=fast_mblock_init_ex2(&g_trunk_allocator,
                        "trunk_file_info", sizeof(FSTrunkFileInfo),
                        16384, NULL, NULL, true, NULL, NULL, NULL)) != 0)
        {
            return result;
        }

        if ((result=uniq_skiplist_init_ex(&g_skiplist_factory,
                        FS_TRUNK_SKIPLIST_MAX_LEVEL_COUNT,
                        compare_trunk_info, trunk_free_func, STORAGE_CFG.
                        store_path.count + STORAGE_CFG.write_cache.count,
                        SKIPLIST_DEFAULT_MIN_ALLOC_ELEMENTS_ONCE,
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

    if ((allocator->sl_trunks=uniq_skiplist_new(&g_skiplist_factory,
            FS_TRUNK_SKIPLIST_INIT_LEVEL_COUNT)) == NULL)
    {
        return ENOMEM;
    }

    allocator->path_info = path_info;
    allocator->current = NULL;
    allocator->free_list = NULL;
    return 0;
}

int trunk_allocator_add(FSTrunkAllocator *allocator,
        const int64_t id, const int subdir, const int64_t size)
{
    FSTrunkFileInfo *trunk_info;
    int result;

    trunk_info = (FSTrunkFileInfo *)fast_mblock_alloc_object(
            &g_trunk_allocator);
    if (trunk_info == NULL) {
        return ENOMEM;
    }

    trunk_info->id = id;
    trunk_info->subdir = subdir;
    trunk_info->size = size;
    trunk_info->used.bytes = 0;
    trunk_info->used.count = 0;
    trunk_info->free_start = 0;

    pthread_mutex_lock(&allocator->lock);
    result = uniq_skiplist_insert(allocator->sl_trunks, trunk_info);
    pthread_mutex_unlock(&allocator->lock);

    return result;
}

int trunk_allocator_delete(FSTrunkAllocator *allocator, const int64_t id)
{
    FSTrunkFileInfo target;
    int result;

    target.id = id;
    pthread_mutex_lock(&allocator->lock);
    result = uniq_skiplist_delete(allocator->sl_trunks, &target);
    pthread_mutex_unlock(&allocator->lock);

    return result;
}

static int alloc_trunk(FSTrunkAllocator *allocator)
{
    int result;

    //free_list

    if ((result=storage_config_calc_path_spaces(allocator->path_info)) != 0) {
        return result;
    }

    if (allocator->path_info->avail_space - STORAGE_CFG.trunk_file_size >
            allocator->path_info->reserved_space.value)
    {
    }

    //TODO reclaim
    return 0;
}

int trunk_allocator_alloc(FSTrunkAllocator *allocator,
        const int size, FSTrunkSpaceInfo *space_info)
{
    int aligned_size;
    int result;

    aligned_size = MEM_ALIGN(size);
    pthread_mutex_lock(&allocator->lock);

    //TODO
    result = 0;
    /*
    if (allocator->current != NULL && allocator->current->size -
            allocator->current->free_start >= aligned_size)
    {
        result = 0;
    } else {
        result = alloc_trunk(allocator);
    }

    if (result == 0) {
        space_info->path = &allocator->path_info->path;
        space_info->id = allocator->current->id;
        space_info->subdir = allocator->current->subdir;
        space_info->offset = allocator->current->free_start;
        space_info->size = aligned_size;

        allocator->current->last_alloc_time = g_current_time;
        allocator->current->free_start += aligned_size;
        allocator->current->used.bytes += aligned_size;
        allocator->current->used.count++;
    }
    */
    pthread_mutex_unlock(&allocator->lock);

    return result;
}

int trunk_allocator_free(FSTrunkAllocator *allocator,
            const int id, const int size)
{
    FSTrunkFileInfo target;
    FSTrunkFileInfo *found;

    target.id = id;
    pthread_mutex_lock(&allocator->lock);
    if ((found=(FSTrunkFileInfo *)uniq_skiplist_find(
                    allocator->sl_trunks, &target)) != NULL)
    {
        found->used.bytes -= size;
        found->used.count--;
    }
    pthread_mutex_unlock(&allocator->lock);

    return found != NULL ? 0 : ENOENT;
}
