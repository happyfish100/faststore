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
    UniqSkiplist *by_id;
    UniqSkiplist *by_count;
} SortedSubdirs;

typedef struct {
    int count;
    SortedSubdirs *subdirs;  //mapped by store path index
} SortedSubdirArray;

typedef struct {
    int64_t current_trunk_id;
    SortedSubdirArray subdir_array;
    struct {
        UniqSkiplistFactory by_id;
        UniqSkiplistFactory by_count;
    } factories;
    struct fast_mblock_man subdir_allocator;
} TrunkIdInfoContext;

static TrunkIdInfoContext id_info_context = {0, {0, NULL}};

static int compare_by_id(const void *p1, const void *p2)
{
    return ((StoreSubdirInfo *)p1)->subdir -
        ((StoreSubdirInfo *)p2)->subdir;
}

static int compare_by_count(const void *p1, const void *p2)
{
    return ((StoreSubdirInfo *)p1)->file_count-
        ((StoreSubdirInfo *)p2)->file_count;
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

static int trunk_id_info_load()
{
    return 0;
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
    const int init_level_count = 8;
    FSStoragePathInfo *p;
    FSStoragePathInfo *end;
    SortedSubdirs *sorted_subdirs;

    end = parray->paths + parray->count;
    for (p=parray->paths; p<end; p++) {
        sorted_subdirs = id_info_context.subdir_array.subdirs + p->store.index;
        sorted_subdirs->by_id = uniq_skiplist_new(&id_info_context.
                factories.by_id, init_level_count);
        if (sorted_subdirs->by_id == NULL) {
            return ENOMEM;
        }

        sorted_subdirs->by_count = uniq_skiplist_new(&id_info_context.
                factories.by_count, init_level_count);
        if (sorted_subdirs->by_count == NULL) {
            return ENOMEM;
        }
    }

    return 0;
}

int trunk_id_info_init()
{
    const int max_level_count = 16;
    const int alloc_elements_once = 8 * 1024;
    int result;

    if ((result=uniq_skiplist_init_ex(&id_info_context.factories.by_id,
                    max_level_count, compare_by_id,
                    id_info_free_func, alloc_elements_once,
                    SKIPLIST_DEFAULT_MIN_ALLOC_ELEMENTS_ONCE, 0)) != 0)
    {
        return result;
    }

    if ((result=uniq_skiplist_init_ex(&id_info_context.factories.by_count,
                    max_level_count, compare_by_count,
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

    return trunk_id_info_load();
}

int trunk_id_info_generate(const int path_index, FSTrunkIdInfo *id_info)
{
    return 0;
}
