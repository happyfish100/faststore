#include <limits.h>
#include <sys/stat.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fast_buffer.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "store_path_index.h"

typedef struct {
    int index;
    char path[PATH_MAX];
    char mark[64];
} StorePathEntry;

typedef struct {
    int alloc;
    int count;
    StorePathEntry *entries;
} StorePathArray;

#define STORE_PATH_INDEX_FILENAME        ".store_path_index.dat"
#define STORE_PATH_INDEX_SECTION_PREFIX_STR  "path-"
#define STORE_PATH_INDEX_SECTION_PREFIX_LEN  \
    ((int)sizeof(STORE_PATH_INDEX_SECTION_PREFIX_STR) - 1)

#define STORE_PATH_INDEX_ITEM_PATH       "path"
#define STORE_PATH_INDEX_ITEM_MARK       "mark"

static StorePathArray store_paths = {0, 0, NULL};   //sort by index

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

static int load_store_path_index(IniContext *ini_context, char *full_filename)
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

int store_path_index_get(const char *path)
{
    StorePathEntry *entry;

    if (store_paths.count == 0) {
        return -1;
    }

    for (entry=store_paths.entries + store_paths.count - 1;
            entry>=store_paths.entries; entry--)
    {
        if (strcmp(path, entry->path) == 0) {
            return entry->index;
        }
    }

    return -1;
}

int store_path_index_add(const char *path, const char *mark, int *index)
{
    int result;
    StorePathEntry *pentry;

    if ((result=check_alloc_store_paths(1)) != 0) {
        return result;
    }

    if (store_paths.count > 0) {
        *index = store_paths.entries[store_paths.count - 1].index + 1;
    } else {
        *index = 0;
    }

    pentry = store_paths.entries + store_paths.count++;
    pentry->index = *index;
    snprintf(pentry->path, sizeof(pentry->path), "%s", path);
    snprintf(pentry->mark, sizeof(pentry->mark), "%s", mark);
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
