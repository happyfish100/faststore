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
    UniqSkiplist by_id;
    UniqSkiplist by_count;
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

#define TRUNK_BINLOG_FILENAME        ".trunk_binlog.dat"

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

    return 0;
}

int trunk_id_info_generate(const int path_index, FSTrunkIdInfo *id_info)
{
    return 0;
}
