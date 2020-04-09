#include <limits.h>
#include <sys/stat.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fast_mblock.h"
#include "sf/sf_global.h"
#include "trunk_allocator.h"

static bool g_allocator_inited = false;
static struct fast_mblock_man g_trunk_allocator;

int trunk_allocator_init(FSTrunkAllocator *allocator,
        FSStoragePathInfo *path_info)
{
    int result;

    allocator->path_info = path_info;
    allocator->trunks.alloc = allocator->trunks.count = 0;
    allocator->trunks.files = NULL;

    if (!g_allocator_inited) {
        g_allocator_inited = true;
        if ((result=fast_mblock_init_ex2(&g_trunk_allocator,
                        "trunk_file_info", sizeof(FSTrunkFileInfo),
                        16384, NULL, NULL, true, NULL, NULL, NULL)) != 0)
        {
            return result;
        }
    }

    return 0;
}

static int check_alloc_trunk_ptr_array(FSTrunkAllocator *allocator)
{
    int bytes;
    int alloc;
    FSTrunkFileInfo **files;

    if (allocator->trunks.alloc > allocator->trunks.count) {
        return 0;
    }

    if (allocator->trunks.alloc == 0) {
        alloc = 256;
    } else {
        alloc = allocator->trunks.alloc * 2;
    }
    bytes = sizeof(FSTrunkFileInfo *) * alloc;
    files = (FSTrunkFileInfo **)malloc(bytes);
    if (files == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }

    if (allocator->trunks.count > 0) {
        memcpy(files, allocator->trunks.files, sizeof(FSTrunkFileInfo *) *
                allocator->trunks.count);
        free(allocator->trunks.files);
    }

    allocator->trunks.files = files;
    allocator->trunks.alloc = alloc;
    return 0;
}

int trunk_allocator_add(FSTrunkAllocator *allocator,
        const int64_t id, const int subdir, const int64_t size)
{
    FSTrunkFileInfo *trunk_info;
    int result;

    if ((result=check_alloc_trunk_ptr_array(allocator)) != 0) {
        return result;
    }

    trunk_info = (FSTrunkFileInfo *)fast_mblock_alloc_object(
            &g_trunk_allocator);
    if (trunk_info == NULL) {
        return ENOMEM;
    }

    trunk_info->id = id;
    trunk_info->subdir = subdir;
    trunk_info->size = size;
    trunk_info->refer_count = 0;
    trunk_info->used = 0;
    trunk_info->offset = 0;
    allocator->trunks.files[allocator->trunks.count++] = trunk_info;
    return 0;
}

int trunk_allocator_delete(FSTrunkAllocator *allocator, const int64_t id)
{
    FSTrunkFileInfo *trunk_info;

    //TODO
    trunk_info = NULL;
    fast_mblock_free_object(&g_trunk_allocator, trunk_info);

    return 0;
}
