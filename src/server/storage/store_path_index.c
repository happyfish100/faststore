#include <limits.h>
#include <sys/stat.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "store_path_index.h"

typedef struct {
    int index;
    char path[PATH_MAX];
    char mark[64];
} StorePathEntry;

typedef struct {
    int count;
    StorePathEntry *entries;
} StorePathArray;

#define STORE_PATH_INDEX_FILENAME        ".store_path_index.dat"
#define STORE_PATH_INDEX_SECTION_PREFIX  "path-"
#define STORE_PATH_INDEX_ITEM_PATH       "path"
#define STORE_PATH_INDEX_ITEM_MARK       "mark"

static StorePathArray store_paths = {0, NULL};

static char *get_store_path_index_filename(char *full_filename, const int size)
{
    snprintf(full_filename, size, "%s/%s",
            DATA_PATH_STR, STORE_PATH_INDEX_FILENAME);
    return full_filename;
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

    sections = fixed_sections;
    alloc_size = FIXED_SECTION_COUNT;
    result = iniGetSectionNamesByPrefix(ini_context,
            STORE_PATH_INDEX_SECTION_PREFIX, sections,
            alloc_size, &count);
    if (result == ENOSPC) {
        sections = NULL;
        do {
            alloc_size *= 2;
            bytes = sizeof(IniSectionInfo) * alloc_size;
            sections = (IniSectionInfo *)realloc(sections, bytes);
            if (sections == NULL) {
                logError("file: "__FILE__", line: %d, "
                        "malloc %d bytes fail", __LINE__, bytes);
                return ENOMEM;
            }
            result = iniGetSectionNamesByPrefix(ini_context,
                    STORE_PATH_INDEX_SECTION_PREFIX, sections,
                    alloc_size, &count);
        } while (result == ENOSPC);
    }

    if (result != 0) {
        return result;
    }

    //TODO
    if (sections != fixed_sections) {
        free(sections);
    }
    return 0;
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
}
