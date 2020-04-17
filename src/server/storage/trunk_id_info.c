#include <limits.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fast_mblock.h"
#include "fastcommon/uniq_skiplist.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "trunk_id_info.h"

typedef struct {
    int subdir;
    int file_count;
} StoreSubdirInfo;

typedef struct {
    UniqSkiplist *all;
    UniqSkiplist *freelist;
    pthread_mutex_t lock;
} SortedSubdirs;

typedef struct {
    int count;
    SortedSubdirs *subdirs;  //mapped by store path index
} SortedSubdirArray;

typedef struct {
    int64_t max_trunk_id;
    int64_t max_subdir_id;
    SortedSubdirArray subdir_array;
    struct {
        UniqSkiplistFactory all;
        UniqSkiplistFactory freelist;
    } factories;
    struct fast_mblock_man subdir_allocator;
} TrunkIdInfoContext;

static TrunkIdInfoContext id_info_context = {0, 0, {0, NULL}};

static int compare_by_id(const void *p1, const void *p2)
{
    return ((StoreSubdirInfo *)p1)->subdir -
        ((StoreSubdirInfo *)p2)->subdir;
}

void id_info_free_func(void *ptr, const int delay_seconds)
{
    if (delay_seconds > 0) {
        fast_mblock_delay_free_object(&id_info_context.subdir_allocator,
                ptr, delay_seconds);
    } else {
        fast_mblock_free_object(&id_info_context.subdir_allocator, ptr);
    }
}

static int alloc_sorted_subdirs()
{
    int bytes;

    id_info_context.subdir_array.count = STORAGE_CFG.max_store_path_index + 1;
    bytes = sizeof(SortedSubdirs) * id_info_context.subdir_array.count;
    id_info_context.subdir_array.subdirs = (SortedSubdirs *)malloc(bytes);
    if (id_info_context.subdir_array.subdirs == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(id_info_context.subdir_array.subdirs, 0, bytes);
    return 0;
}

static int init_sorted_subdirs(FSStoragePathArray *parray)
{
    int result;
    const int init_level_count = 8;
    FSStoragePathInfo *p;
    FSStoragePathInfo *end;
    SortedSubdirs *sorted_subdirs;

    end = parray->paths + parray->count;
    for (p=parray->paths; p<end; p++) {
        sorted_subdirs = id_info_context.subdir_array.subdirs + p->store.index;
        sorted_subdirs->all = uniq_skiplist_new(&id_info_context.
                factories.all, init_level_count);
        if (sorted_subdirs->all == NULL) {
            return ENOMEM;
        }

        sorted_subdirs->freelist = uniq_skiplist_new(&id_info_context.
                factories.freelist, init_level_count);
        if (sorted_subdirs->freelist == NULL) {
            return ENOMEM;
        }

        if ((result=init_pthread_lock(&sorted_subdirs->lock)) != 0) {
            logError("file: "__FILE__", line: %d, "
                    "init_pthread_lock fail, errno: %d, error info: %s",
                    __LINE__, result, STRERROR(result));
            return result;
        }
    }

    return 0;
}

int trunk_id_info_init()
{
    const int max_level_count = 16;
    const int alloc_elements_once = 8 * 1024;
    int result;

    if ((result=uniq_skiplist_init_ex(&id_info_context.factories.all,
                    max_level_count, compare_by_id,
                    id_info_free_func, alloc_elements_once,
                    SKIPLIST_DEFAULT_MIN_ALLOC_ELEMENTS_ONCE, 0)) != 0)
    {
        return result;
    }

    if ((result=uniq_skiplist_init_ex(&id_info_context.factories.freelist,
                    max_level_count, compare_by_id,
                    NULL, alloc_elements_once,
                    SKIPLIST_DEFAULT_MIN_ALLOC_ELEMENTS_ONCE, 0)) != 0)
    {
        return result;
    }

    if ((result=fast_mblock_init_ex2(&id_info_context.subdir_allocator,
                    "subdir_info", sizeof(StoreSubdirInfo),
                    alloc_elements_once, NULL, NULL, false,
                    NULL, NULL, NULL)) != 0)
    {
        return result;
    }

    if ((result=alloc_sorted_subdirs()) != 0) {
        return result;
    }
    if ((result=init_sorted_subdirs(&STORAGE_CFG.write_cache)) != 0) {
        return result;
    }
    if ((result=init_sorted_subdirs(&STORAGE_CFG.store_path)) != 0) {
        return result;
    }

    return 0;
}

int trunk_id_info_add(const int path_index, const FSTrunkIdInfo *id_info)
{
    SortedSubdirs *sorted_subdirs;
    StoreSubdirInfo target;
    StoreSubdirInfo *subdir;
    int result;

    target.subdir = id_info->subdir;
    target.file_count = 1;
    result = 0;
    sorted_subdirs = id_info_context.subdir_array.subdirs + path_index;

    pthread_mutex_lock(&sorted_subdirs->lock);
    do {
        subdir = (StoreSubdirInfo *)uniq_skiplist_find(
                sorted_subdirs->all, &target);
        if (subdir != NULL) {
            subdir->file_count++;
            if (subdir->file_count >= STORAGE_CFG.max_trunk_files_per_subdir) {
                uniq_skiplist_delete(sorted_subdirs->
                        freelist, subdir);
            }
        } else {
            subdir = (StoreSubdirInfo *)fast_mblock_alloc_object(
                    &id_info_context.subdir_allocator);
            if (subdir == NULL) {
                result = ENOMEM;
                break;
            }
            *subdir = target;
            if ((result=uniq_skiplist_insert(sorted_subdirs->
                            all, subdir)) != 0)
            {
                break;
            }
            if (subdir->file_count < STORAGE_CFG.max_trunk_files_per_subdir) {
                result = uniq_skiplist_insert(sorted_subdirs->
                        freelist, subdir);
            }
        }

        if (id_info_context.max_trunk_id < id_info->id) {
            id_info_context.max_trunk_id = id_info->id;
        }
        if (id_info_context.max_subdir_id < id_info->subdir) {
            id_info_context.max_subdir_id = id_info->subdir;
        }
    } while (0);
    pthread_mutex_unlock(&sorted_subdirs->lock);

    return result;
}

int trunk_id_info_delete(const int path_index, const FSTrunkIdInfo *id_info)
{
    SortedSubdirs *sorted_subdirs;
    StoreSubdirInfo target;
    StoreSubdirInfo *subdir;
    int result;

    target.subdir = id_info->subdir;
    target.file_count = 1;
    result = 0;
    sorted_subdirs = id_info_context.subdir_array.subdirs + path_index;

    pthread_mutex_lock(&sorted_subdirs->lock);
    do {
        subdir = (StoreSubdirInfo *)uniq_skiplist_find(
                sorted_subdirs->all, &target);
        if (subdir != NULL) {
            if (subdir->file_count >= STORAGE_CFG.max_trunk_files_per_subdir) {
                uniq_skiplist_insert(sorted_subdirs->
                        freelist, subdir);
            }
            subdir->file_count--;
        }
    } while (0);
    pthread_mutex_unlock(&sorted_subdirs->lock);

    return result;
}

int trunk_id_info_generate(const int path_index, FSTrunkIdInfo *id_info)
{
    SortedSubdirs *sorted_subdirs;
    StoreSubdirInfo *sd_info;

    sorted_subdirs = id_info_context.subdir_array.subdirs + path_index;
    pthread_mutex_lock(&sorted_subdirs->lock);
    sd_info = (StoreSubdirInfo *)uniq_skiplist_get_first(
                sorted_subdirs->freelist);
    if (sd_info != NULL) {
        id_info->subdir = sd_info->subdir;
    } else {
        id_info->subdir = ++(id_info_context.max_subdir_id);
    }
    id_info->id = ++(id_info_context.max_trunk_id);
    pthread_mutex_unlock(&sorted_subdirs->lock);

    return 0;
}
