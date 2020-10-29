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
#include <sys/statvfs.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fast_mblock.h"
#include "fastcommon/uniq_skiplist.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "trunk_id_info.h"

#define TRUNK_ID_DATA_FILENAME  ".trunk_id.dat"
#define ITEM_NAME_TRUNK_ID      "trunk_id"
#define ITEM_NAME_SUBDIR_ID     "subdir_id"
#define ITEM_NAME_NORMAL_EXIT   "normal_exit"

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
    volatile int64_t current_trunk_id;
    volatile int64_t current_subdir_id;
    int64_t last_trunk_id;
    int64_t last_subdir_id;
    SortedSubdirArray subdir_array;
    struct {
        UniqSkiplistFactory all;
        UniqSkiplistFactory freelist;
    } factories;
    struct fast_mblock_man subdir_allocator;
} TrunkIdInfoContext;

static TrunkIdInfoContext id_info_context = {0, 0, 0, 0, {0, NULL}};


static inline void get_trunk_id_dat_filename(
        char *full_filename, const int size)
{
    snprintf(full_filename, size, "%s/%s",
            DATA_PATH_STR, TRUNK_ID_DATA_FILENAME);
}

#define save_current_trunk_id(current_trunk_id, current_subdir_id) \
    save_current_trunk_id_ex(current_trunk_id, current_subdir_id, false)

static int save_current_trunk_id_ex(const int64_t current_trunk_id,
        const int64_t current_subdir_id, const bool on_exit)
{
    char full_filename[PATH_MAX];
    char buff[256];
    int len;
    int result;

    get_trunk_id_dat_filename(full_filename, sizeof(full_filename));
    len = sprintf(buff, "%s=%"PRId64"\n"
            "%s=%"PRId64"\n",
            ITEM_NAME_TRUNK_ID, current_trunk_id,
            ITEM_NAME_SUBDIR_ID, current_subdir_id);
    if (on_exit) {
        len += sprintf(buff + len, "%s=1\n",
                ITEM_NAME_NORMAL_EXIT);
    }
    if ((result=safeWriteToFile(full_filename, buff, len)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "write to file \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, full_filename,
                result, STRERROR(result));
    }

    return result;
}

static int load_current_trunk_id()
{
    char full_filename[PATH_MAX];
    IniContext ini_context;
    int result;

    get_trunk_id_dat_filename(full_filename, sizeof(full_filename));
    if (access(full_filename, F_OK) != 0) {
        if (errno == ENOENT) {
            return 0;
        }
    }

    if ((result=iniLoadFromFile(full_filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load from file \"%s\" fail, error code: %d",
                __LINE__, full_filename, result);
        return result;
    }

    id_info_context.current_trunk_id = iniGetInt64Value(NULL,
            ITEM_NAME_TRUNK_ID, &ini_context, 0);
    id_info_context.current_subdir_id  = iniGetInt64Value(NULL,
            ITEM_NAME_SUBDIR_ID, &ini_context, 0);

    if (!iniGetBoolValue(NULL, ITEM_NAME_NORMAL_EXIT, &ini_context, false)) {
        id_info_context.current_trunk_id += 10000;
        id_info_context.current_subdir_id += 100;
    }

    iniFreeContext(&ini_context);
    return result;
}

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
    id_info_context.subdir_array.subdirs = (SortedSubdirs *)fc_malloc(bytes);
    if (id_info_context.subdir_array.subdirs == NULL) {
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

static int trunk_id_sync_to_file(void *arg)
{
    int64_t current_trunk_id;
    int64_t current_subdir_id;

    current_trunk_id = __sync_add_and_fetch(
            &id_info_context.current_trunk_id, 0);
    current_subdir_id = __sync_add_and_fetch(
            &id_info_context.current_subdir_id, 0);

    if (id_info_context.last_trunk_id != id_info_context.current_trunk_id ||
            id_info_context.last_subdir_id != id_info_context.current_subdir_id)
    {
        id_info_context.last_trunk_id = id_info_context.current_trunk_id;
        id_info_context.last_subdir_id = id_info_context.current_subdir_id;
        return save_current_trunk_id(current_trunk_id, current_subdir_id);
    }

    return 0;
}

static int setup_sync_to_file_task()
{
    ScheduleEntry schedule_entry;
    ScheduleArray schedule_array;

    INIT_SCHEDULE_ENTRY(schedule_entry, sched_generate_next_id(),
            0, 0, 0, 1, trunk_id_sync_to_file, NULL);

    schedule_array.count = 1;
    schedule_array.entries = &schedule_entry;
    return sched_add_entries(&schedule_array);
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

    if ((result=fast_mblock_init_ex1(&id_info_context.subdir_allocator,
                    "subdir_info", sizeof(StoreSubdirInfo),
                    alloc_elements_once, 0, NULL, NULL, false)) != 0)
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

    if ((result=load_current_trunk_id()) != 0) {
        return result;
    }

    id_info_context.last_trunk_id = id_info_context.current_trunk_id;
    id_info_context.last_subdir_id = id_info_context.current_subdir_id;
    return setup_sync_to_file_task();
}

void trunk_id_info_destroy()
{
    int64_t current_trunk_id;
    int64_t current_subdir_id;

    current_trunk_id = __sync_add_and_fetch(
            &id_info_context.current_trunk_id, 0);
    current_subdir_id = __sync_add_and_fetch(
            &id_info_context.current_subdir_id, 0);
    save_current_trunk_id_ex(current_trunk_id, current_subdir_id, true);
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
    if (sorted_subdirs->all == NULL) {
        return ENOENT;
    }

    PTHREAD_MUTEX_LOCK(&sorted_subdirs->lock);
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
    } while (0);
    PTHREAD_MUTEX_UNLOCK(&sorted_subdirs->lock);

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
    if (sorted_subdirs->all == NULL) {
        return ENOENT;
    }

    PTHREAD_MUTEX_LOCK(&sorted_subdirs->lock);
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
    PTHREAD_MUTEX_UNLOCK(&sorted_subdirs->lock);

    return result;
}

int trunk_id_info_generate(const int path_index, FSTrunkIdInfo *id_info)
{
    SortedSubdirs *sorted_subdirs;
    StoreSubdirInfo *sd_info;

    sorted_subdirs = id_info_context.subdir_array.subdirs + path_index;
    if (sorted_subdirs->all == NULL) {
        return ENOENT;
    }

    PTHREAD_MUTEX_LOCK(&sorted_subdirs->lock);
    sd_info = (StoreSubdirInfo *)uniq_skiplist_get_first(
                sorted_subdirs->freelist);
    PTHREAD_MUTEX_UNLOCK(&sorted_subdirs->lock);

    if (sd_info != NULL) {
        id_info->subdir = sd_info->subdir;
    } else {
        id_info->subdir = __sync_add_and_fetch(
                &id_info_context.current_subdir_id, 1);
    }
    id_info->id = __sync_add_and_fetch(
            &id_info_context.current_trunk_id, 1);

    return 0;
}
