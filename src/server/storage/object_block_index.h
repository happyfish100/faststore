
#ifndef _OBJECT_BLOCK_INDEX_H
#define _OBJECT_BLOCK_INDEX_H

#include "fastcommon/fc_list.h"
#include "fastcommon/uniq_skiplist.h"
#include "../server_types.h"

typedef enum ob_slice_type {
    OB_SLICE_TYPE_FILE  = 'F', /* in file slice */
    OB_SLICE_TYPE_ALLOC = 'A'  /* allocate slice (index and space allocate only) */
} OBSliceType;

typedef struct {
    UniqSkiplistFactory factory;
    struct fast_mblock_man ob_allocator;    //for ob_entry
    struct fast_mblock_man slice_allocator; //for slice_entry 
    pthread_mutex_t lock;
} OBSharedContext;

typedef struct ob_entry {
    FSBlockKey bkey;
    UniqSkiplist *slices;  //the element is OBSliceEntry
    struct ob_entry *next; //for hashtable
} OBEntry;

typedef struct {
    int64_t count;
    int64_t capacity;
    OBEntry **buckets;
    bool need_lock;
    bool modify_sallocator; //if modify storage allocator
} OBHashtable;

typedef struct ob_slice_entry {
    OBEntry *ob;
    OBSliceType type;    //in file or memory as fallocate
    int read_offset;     //offset of the space start offset
    volatile int ref_count;
    FSSliceSize ssize;
    FSTrunkSpaceInfo space;
    struct fc_list_head dlink;  //used in trunk entry for trunk reclaiming
} OBSliceEntry;

typedef struct ob_slice_ptr_array {
    int alloc;
    int count;
    OBSliceEntry **slices;
} OBSlicePtrArray;

#ifdef __cplusplus
extern "C" {
#endif

    extern OBHashtable g_ob_hashtable;

#define ob_index_add_slice(slice, sn, inc_alloc) \
    ob_index_add_slice_ex(&g_ob_hashtable, slice, sn, inc_alloc)

#define ob_index_delete_slices(bs_key, sn, dec_alloc) \
    ob_index_delete_slices_ex(&g_ob_hashtable, bs_key, sn, dec_alloc)

#define ob_index_delete_block(bkey, sn, dec_alloc) \
    ob_index_delete_block_ex(&g_ob_hashtable, bkey, sn, dec_alloc)

#define ob_index_get_slices(bs_key, sarray) \
    ob_index_get_slices_ex(&g_ob_hashtable, bs_key, sarray)

#define ob_index_alloc_slice(bkey) \
    ob_index_alloc_slice_ex(&g_ob_hashtable, bkey, 1)

#define ob_index_init_htable(ht) \
    ob_index_init_htable_ex(ht, STORAGE_CFG.object_block.hashtable_capacity, \
            false, false)

    int ob_index_init();
    void ob_index_destroy();

    int ob_index_init_htable_ex(OBHashtable *htable, const int64_t capacity,
        const bool need_lock, const bool modify_sallocator);
    void ob_index_destroy_htable(OBHashtable *htable);

    int ob_index_add_slice_ex(OBHashtable *htable, OBSliceEntry *slice,
            uint64_t *sn, int *inc_alloc);

    int ob_index_delete_slices_ex(OBHashtable *htable,
            const FSBlockSliceKeyInfo *bs_key,
            uint64_t *sn, int *dec_alloc);

    int ob_index_delete_block_ex(OBHashtable *htable,
            const FSBlockKey *bkey,
            uint64_t *sn, int *dec_alloc);

    OBEntry *ob_index_get_ob_entry(OBHashtable *htable,
            const FSBlockKey *bkey);

    OBSliceEntry *ob_index_alloc_slice_ex(OBHashtable *htable,
            const FSBlockKey *bkey, const int init_refer);

    void ob_index_free_slice(OBSliceEntry *slice);

    int ob_index_get_slices_ex(OBHashtable *htable,
            const FSBlockSliceKeyInfo *bs_key,
            OBSlicePtrArray *sarray);

    static inline void ob_index_init_slice_ptr_array(OBSlicePtrArray *sarray)
    {
        sarray->slices = NULL;
        sarray->alloc = sarray->count = 0;
    }

    static inline void ob_index_free_slice_ptr_array(OBSlicePtrArray *sarray)
    {
        if (sarray->slices != NULL) {
            free(sarray->slices);
            sarray->slices = NULL;
            sarray->alloc = sarray->count = 0;
        }
    }

    int ob_index_add_slice_by_binlog(OBSliceEntry *slice);

    static inline int ob_index_delete_slices_by_binlog(
            const FSBlockSliceKeyInfo *bs_key)
    {
        int dec_alloc;
        return ob_index_delete_slices(bs_key, NULL, &dec_alloc);
    }

    static inline int ob_index_delete_block_by_binlog(
            const FSBlockKey *bkey)
    {
        int dec_alloc;
        return ob_index_delete_block(bkey, NULL, &dec_alloc);
    }

#ifdef __cplusplus
}
#endif

#endif
