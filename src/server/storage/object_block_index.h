
#ifndef _OBJECT_BLOCK_INDEX_H
#define _OBJECT_BLOCK_INDEX_H

#include "fastcommon/fc_list.h"
#include "fastcommon/uniq_skiplist.h"
#include "../../common/fs_types.h"
#include "storage_config.h"

typedef struct ob_entry {
    FSBlockKey bkey;
    UniqSkiplist *slices;   //the element is OBSliceEntry
    struct ob_entry *next; //for hashtable
} OBEntry;

typedef struct ob_slice_entry {
    OBEntry *ob;
    int offset; //offset within the object block
    int length; //slice length
    FSTrunkSpaceInfo space;
    struct fc_list_head dlink;  //used in trunk entry for trunk reclaiming
} OBSliceEntry;

#ifdef __cplusplus
extern "C" {
#endif

    int ob_index_init();
    void ob_index_destroy();

    int ob_index_add_slice(OBSliceEntry *slice);

    OBSliceEntry *ob_index_alloc_slice(const FSBlockKey *bkey);

    void ob_index_free_slice(OBSliceEntry *slice);

#ifdef __cplusplus
}
#endif

#endif
