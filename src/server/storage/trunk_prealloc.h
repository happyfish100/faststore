
#ifndef _TRUNK_RPEALLOC_H
#define _TRUNK_RPEALLOC_H

#include "../../common/fs_types.h"
#include "storage_config.h"
#include "trunk_allocator.h"

#ifdef __cplusplus
extern "C" {
#endif

    int trunk_prealloc_init();

    int trunk_prealloc_push(FSTrunkAllocator *allocator,
            FSTrunkFreelist *freelist, const int target_count);

#ifdef __cplusplus
}
#endif

#endif
