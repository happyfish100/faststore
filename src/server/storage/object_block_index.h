
#ifndef _OBJECT_BLOCK_INDEX_H
#define _OBJECT_BLOCK_INDEX_H

#include "fastcommon/fc_list.h"
#include "../../common/fs_types.h"
#include "storage_config.h"

typedef struct ob_slice_entry {
    FSBlockKey *bkey;
    int offset; //offset in the object block
    int length; //slice length
    FSTrunkSpaceInfo space;
    struct fc_list_head dlink;  //used in trunk entry for trunk reclaiming
} OBSliceEntry;

#ifdef __cplusplus
extern "C" {
#endif

    int object_block_index_init();
    void object_block_index_destroy();

    int object_block_index_add(const OBSliceEntry *slice);

#ifdef __cplusplus
}
#endif

#endif
