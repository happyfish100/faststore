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

#include <sys/stat.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fast_buffer.h"
#include "fastcommon/sched_thread.h"
#include "fastcommon/hash.h"
#include "fastcommon/base64.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "store_path_index.h"

typedef struct {
    int alloc;
    int count;
    StorePathEntry *entries;
} StorePathArray;

typedef struct
{
    int server_id;
    int crc32;
    int index;
    time_t create_time;
} StorePathMarkInfo;

#define STORE_PATH_INDEX_FILENAME        ".store_path_index.dat"
#define STORE_PATH_INDEX_SECTION_PREFIX_STR  "path-"
#define STORE_PATH_INDEX_SECTION_PREFIX_LEN  \
    ((int)sizeof(STORE_PATH_INDEX_SECTION_PREFIX_STR) - 1)

#define STORE_PATH_INDEX_ITEM_PATH       "path"
#define STORE_PATH_INDEX_ITEM_MARK       "mark"

#define STORE_PATH_MARK_FILENAME    ".fs_vars"

static StorePathArray store_paths = {0, 0, NULL};   //sort by index
static struct base64_context base64_ctx;

static int store_path_generate_mark(const char *store_path,
        const int index, char *mark_str)
{
    StorePathMarkInfo mark_info;
    char filename[PATH_MAX];
    char buff[256];
    int mark_len;
    int buff_len;

    mark_info.server_id = CLUSTER_MY_SERVER_ID;
    mark_info.index = index;
    mark_info.crc32 = CRC32(store_path, strlen(store_path));
    mark_info.create_time = g_current_time;
    base64_encode_ex(&base64_ctx, (char *)&mark_info,
            sizeof(mark_info), mark_str, &mark_len, false);

    snprintf(filename, sizeof(filename), "%s/%s",
            store_path, STORE_PATH_MARK_FILENAME);
    buff_len = sprintf(buff, "%s=%s\n",
            STORE_PATH_INDEX_ITEM_MARK, mark_str);
    return safeWriteToFile(filename, buff, buff_len);
}

static int store_path_get_mark(const char *filename,
        char *mark, const int size)
{
    IniContext ini_context;
    char *value;
    int len;
    int result;

    *mark = '\0';
    if (!fileExists(filename)) {
        return ENOENT;
    }

    if ((result=iniLoadFromFile(filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load from file \"%s\" fail, error code: %d",
                __LINE__, filename, result);
        return result;
    }

    value = iniGetStrValue(NULL, STORE_PATH_INDEX_ITEM_MARK, &ini_context);
    if (value != NULL && *value != '\0') {
        len = strlen(value);
        if (len < size) {
            strcpy(mark, value);
        } else {
            logError("file: "__FILE__", line: %d, "
                    "mark file: %s, mark length: %d "
                    "is too long exceeds %d",
                    __LINE__, filename, len, size);
            result = EOVERFLOW;
        }
    } else {
        result = ENOENT;
    }

    iniFreeContext(&ini_context);
    return result;
}

int store_path_check_mark(StorePathEntry *pentry, bool *regenerated)
{
    StorePathMarkInfo mark_info;
    char filename[PATH_MAX];
    char mark[64];
    int mark_len;
    int dest_len;
    int result;

    *regenerated = false;
    snprintf(filename, sizeof(filename), "%s/%s",
            pentry->path, STORE_PATH_MARK_FILENAME);
    if ((result=store_path_get_mark(filename, mark, sizeof(mark))) != 0) {
        if (result == ENOENT) {
            if ((result=store_path_generate_mark(pentry->path,
                            pentry->index, pentry->mark)) == 0)
            {
                *regenerated = true;
            }
        }
        return result;
    }

    if (strcmp(mark, pentry->mark) == 0) {
        return 0;
    }

    mark_len = strlen(mark);
    dest_len = (sizeof(StorePathMarkInfo) * 4 + 2) / 3;
    if (mark_len > dest_len) {
        logError("file: "__FILE__", line: %d, "
                "the mark length: %d is too long exceed %d, "
                "the mark file: %s, the mark string: %s",
                __LINE__, mark_len, dest_len, filename, mark);
        memset(&mark_info, 0, sizeof(StorePathMarkInfo));
    } else if (base64_decode_auto(&base64_ctx, mark, mark_len,
                (char *)&mark_info, &dest_len) == NULL)
    {
        logError("file: "__FILE__", line: %d, "
                "the mark string is not base64 encoded, "
                "the mark file: %s, the mark string: %s",
                __LINE__, filename, mark);
        memset(&mark_info, 0, sizeof(StorePathMarkInfo));
    }

    if (mark_info.server_id > 0) {
        char time_str[32];

        formatDatetime(mark_info.create_time,
                "%Y-%m-%d %H:%M:%S",
                time_str, sizeof(time_str));
        logCrit("file: "__FILE__", line: %d, "
                "the store path %s maybe used by other "
                "store server. fields in the mark file: "
                "{ server_id: %d, path_index: %d, crc32: %d,"
                " create_time: %s }, if you confirm that it is NOT "
                "used by other store server, you can delete "
                "the mark file %s then try again.", __LINE__,
                pentry->path, mark_info.server_id, mark_info.index,
                mark_info.crc32, time_str, filename);
    } else {
        logCrit("file: "__FILE__", line: %d, "
                "the store path %s maybe used by other "
                "store server. if you confirm that it is NOT "
                "used by other storage server, you can delete "
                "the mark file %s then try again", __LINE__,
                pentry->path, filename);
    }

    return EINVAL;
}

static char *get_store_path_index_filename(char *full_filename, const int size)
{
    snprintf(full_filename, size, "%s/%s",
            DATA_PATH_STR, STORE_PATH_INDEX_FILENAME);
    return full_filename;
}

static int check_alloc_store_paths(const int inc_count)
{
    int alloc;
    int target_count;
    int bytes;
    StorePathEntry *entries;

    target_count = store_paths.count + inc_count;
    if (store_paths.alloc >= target_count) {
        return 0;
    }

    if (store_paths.alloc == 0) {
        alloc = 8;
    } else {
        alloc = store_paths.alloc * 2;
    }

    while (alloc < target_count) {
        alloc *= 2;
    }

    bytes = sizeof(StorePathEntry) * alloc;
    entries = (StorePathEntry *)fc_malloc(bytes);
    if (entries == NULL) {
        return ENOMEM;
    }

    if (store_paths.entries != NULL) {
        memcpy(entries, store_paths.entries,
                sizeof(StorePathEntry) * store_paths.count);
        free(store_paths.entries);
    }

    store_paths.entries = entries;
    store_paths.alloc = alloc;
    return 0;
}

static int load_one_store_path_index(IniContext *ini_context, char *full_filename,
        IniSectionInfo *section, StorePathEntry *pentry)
{
    char *index_str;
    char *path;
    char *mark;
    char *endptr;

    index_str = section->section_name + STORE_PATH_INDEX_SECTION_PREFIX_LEN;
    pentry->index = strtol(index_str, &endptr, 10);
    if (*endptr != '\0') {
        logError("file: "__FILE__", line: %d, "
                "data file: %s, section: %s, index is invalid",
                __LINE__, full_filename, section->section_name);
        return EINVAL;
    }

    path = iniGetStrValue(section->section_name, "path", ini_context);
    if (path == NULL) {
        logError("file: "__FILE__", line: %d, "
                "data file: %s, section: %s, item \"path\" not exist",
                __LINE__, full_filename, section->section_name);
        return ENOENT;
    }

    mark = iniGetStrValue(section->section_name, "mark", ini_context);
    if (mark == NULL) {
        logError("file: "__FILE__", line: %d, "
                "data file: %s, section: %s, item \"mark\" not exist",
                __LINE__, full_filename, section->section_name);
        return ENOENT;
    }

    snprintf(pentry->path, sizeof(pentry->path), "%s", path);
    snprintf(pentry->mark, sizeof(pentry->mark), "%s", mark);
    return 0;
}

static int compare_store_path_index(const void *p1, const void *p2)
{
    return ((StorePathEntry *)p1)->index - ((StorePathEntry *)p2)->index;
}

static int load_store_path_index(IniContext *ini_context,
        char *full_filename)
{
#define FIXED_SECTION_COUNT 64
    int result;
    int alloc_size;
    int count;
    int bytes;
    IniSectionInfo fixed_sections[FIXED_SECTION_COUNT];
    IniSectionInfo *sections;
    IniSectionInfo *section;
    IniSectionInfo *end;
    StorePathEntry *pentry;

    sections = fixed_sections;
    alloc_size = FIXED_SECTION_COUNT;
    result = iniGetSectionNamesByPrefix(ini_context,
            STORE_PATH_INDEX_SECTION_PREFIX_STR, sections,
            alloc_size, &count);
    if (result == ENOSPC) {
        sections = NULL;
        do {
            alloc_size *= 2;
            bytes = sizeof(IniSectionInfo) * alloc_size;
            sections = (IniSectionInfo *)fc_realloc(sections, bytes);
            if (sections == NULL) {
                return ENOMEM;
            }
            result = iniGetSectionNamesByPrefix(ini_context,
                    STORE_PATH_INDEX_SECTION_PREFIX_STR, sections,
                    alloc_size, &count);
        } while (result == ENOSPC);
    }

    if (result != 0) {
        return result;
    }

    if ((result=check_alloc_store_paths(count)) != 0) {
        return result;
    }

    pentry = store_paths.entries;
    end = sections + count;
    for (section=sections; section<end; section++,pentry++) {
        if ((result=load_one_store_path_index(ini_context, full_filename,
                        section, pentry)) != 0)
        {
            return result;
        }
    }
    store_paths.count = count;

    if (store_paths.count > 1) {
        qsort(store_paths.entries, store_paths.count,
                sizeof(StorePathEntry), compare_store_path_index);
    }

    if (sections != fixed_sections) {
        free(sections);
    }
    return 0;
}

int store_path_index_count()
{
    return store_paths.count;
}

int store_path_index_max()
{
    if (store_paths.count > 0) {
        return store_paths.entries[store_paths.count - 1].index;
    } else {
        return 0;
    }
}

int store_path_index_init()
{
    int result;
    IniContext ini_context;
    char full_filename[PATH_MAX];

    base64_init_ex(&base64_ctx, 0, '-', '_', '.');

    get_store_path_index_filename(full_filename, sizeof(full_filename));
    if (access(full_filename, F_OK) != 0) {
        if (errno == ENOENT) {
            return 0;
        }

        result = errno != 0 ? errno : EPERM;
        logError("file: "__FILE__", line: %d, "
                "access file %s fail, errno: %d, error info: %s",
                __LINE__, full_filename, result, STRERROR(result));
        return result;
    }

    if ((result=iniLoadFromFile(full_filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load conf file \"%s\" fail, ret code: %d",
                __LINE__, full_filename, result);
        return result;
    }

    result = load_store_path_index(&ini_context, full_filename);
    iniFreeContext(&ini_context);
    return result;
}

void store_path_index_destroy()
{
    if (store_paths.entries != NULL) {
        free(store_paths.entries);
        store_paths.entries = NULL;
        store_paths.count = store_paths.alloc = 0;
    }
}

StorePathEntry *store_path_index_get(const char *path)
{
    StorePathEntry *entry;

    if (store_paths.count == 0) {
        return NULL;
    }

    for (entry=store_paths.entries + store_paths.count - 1;
            entry>=store_paths.entries; entry--)
    {
        if (strcmp(path, entry->path) == 0) {
            return entry;
        }
    }

    return NULL;
}

int store_path_index_add(const char *path, int *index)
{
    int result;
    char filename[PATH_MAX];
    char mark[64];
    StorePathEntry *pentry;

    if ((result=check_alloc_store_paths(1)) != 0) {
        return result;
    }

    if (store_paths.count > 0) {
        *index = store_paths.entries[store_paths.count - 1].index + 1;
    } else {
        *index = 0;
    }

    pentry = store_paths.entries + store_paths.count;
    snprintf(filename, sizeof(filename), "%s/%s",
            path, STORE_PATH_MARK_FILENAME);
    result = store_path_get_mark(filename, mark, sizeof(mark));
    if (result != ENOENT) {
        if (result == 0) {
            logCrit("file: "__FILE__", line: %d, "
                    "store path: %s, the mark file %s already exist, "
                    "if you confirm that it is NOT used by other "
                    "store server, you can delete this mark file "
                    "then try again.", __LINE__, path, filename);
            return EEXIST;
        }
        return result;
    }

    if ((result=store_path_generate_mark(path, *index, pentry->mark)) != 0) {
        return result;
    }

    pentry->index = *index;
    snprintf(pentry->path, sizeof(pentry->path), "%s", path);
    store_paths.count++;
    return 0;
}

int store_path_index_save()
{
    int result;
    FastBuffer buffer;
    StorePathEntry *pentry;
    StorePathEntry *end;
    char full_filename[PATH_MAX];

    if ((result=fast_buffer_init_ex(&buffer, 128 * store_paths.count)) != 0) {
        return result;
    }

    end  = store_paths.entries + store_paths.count;
    for (pentry=store_paths.entries; pentry<end; pentry++) {
        result = fast_buffer_append(&buffer,
                "[%s%d]\n"
                "%s=%s\n"
                "%s=%s\n\n",
                STORE_PATH_INDEX_SECTION_PREFIX_STR, pentry->index,
                STORE_PATH_INDEX_ITEM_PATH, pentry->path,
                STORE_PATH_INDEX_ITEM_MARK, pentry->mark);
        if (result != 0) {
            return result;
        }
    }

    get_store_path_index_filename(full_filename, sizeof(full_filename));
    result = safeWriteToFile(full_filename, buffer.data, buffer.length);

    fast_buffer_destroy(&buffer);
    return result;
}
