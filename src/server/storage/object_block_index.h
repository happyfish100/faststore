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


#ifndef _OBJECT_BLOCK_INDEX_H
#define _OBJECT_BLOCK_INDEX_H

#include "../server_types.h"

#ifdef __cplusplus
extern "C" {
#endif

    extern OBHashtable g_ob_hashtable;

#define ob_index_add_slice(bkey, slice, sn, inc_alloc, is_reclaim) \
    ob_index_add_slice_ex(&g_ob_hashtable, bkey, slice, \
            sn, inc_alloc, is_reclaim)

#define ob_index_update_slice(bkey, slice)  \
    ob_index_update_slice_ex(&g_ob_hashtable, bkey, slice)

#define ob_index_delete_slices(bs_key, sn, dec_alloc, is_reclaim) \
    ob_index_delete_slices_ex(&g_ob_hashtable, bs_key, sn, dec_alloc, is_reclaim)

#define ob_index_delete_block(bkey, sn, dec_alloc, is_reclaim) \
    ob_index_delete_block_ex(&g_ob_hashtable, bkey, sn, dec_alloc, is_reclaim)

#define ob_index_get_slices(bs_key, sarray, is_reclaim) \
    ob_index_get_slices_ex(&g_ob_hashtable, bs_key, sarray, is_reclaim)

#define ob_index_get_slice_count(bs_key) \
    ob_index_get_slice_count_ex(&g_ob_hashtable, bs_key)

#define ob_index_get_ob_entry(bkey) \
    ob_index_get_ob_entry_ex(&g_ob_hashtable, bkey)

#define ob_index_alloc_slice(bkey) \
    ob_index_alloc_slice_ex(&g_ob_hashtable, bkey, 1)

#define ob_index_init_htable(ht) \
    ob_index_init_htable_ex(ht, STORAGE_CFG.object_block.  \
            hashtable_capacity)

#define ob_index_dump_slices_to_trunk(start_index, end_index, slice_count) \
    ob_index_dump_slices_to_trunk_ex(&g_ob_hashtable, \
            start_index, end_index, slice_count)

#define ob_index_dump_slices_to_file(start_index, \
        end_index, filename, slice_count) \
    ob_index_dump_slices_to_file_ex(&g_ob_hashtable, start_index, end_index, \
            filename, slice_count, end_index == g_ob_hashtable.capacity, false)

#define ob_index_remove_slices_to_file(start_index, end_index, \
        rebuild_store_index, filename, slice_count) \
    ob_index_remove_slices_to_file_ex(&g_ob_hashtable, start_index, \
            end_index, rebuild_store_index, filename, slice_count)

#define ob_index_dump_replica_binlog_to_file(data_group_id, \
        padding_data_version, filename, \
        total_slice_count, total_replica_count) \
    ob_index_dump_replica_binlog_to_file_ex(&g_ob_hashtable, \
            data_group_id, padding_data_version, filename, \
            total_slice_count, total_replica_count)

    int ob_index_init();
    void ob_index_destroy();

    int ob_index_init_htable_ex(OBHashtable *htable, const int64_t capacity);
    void ob_index_destroy_htable(OBHashtable *htable);

    int ob_index_add_slice_ex(OBHashtable *htable, const FSBlockKey *bkey,
            OBSliceEntry *slice, uint64_t *sn, int *inc_alloc,
            const bool is_reclaim);

    int ob_index_update_slice_ex(OBHashtable *htable,
            const FSBlockKey *bkey, OBSliceEntry *slice);

    int ob_index_delete_slices_ex(OBHashtable *htable,
            const FSBlockSliceKeyInfo *bs_key, uint64_t *sn,
            int *dec_alloc, const bool is_reclaim);

    int ob_index_delete_block_ex(OBHashtable *htable,
            const FSBlockKey *bkey, uint64_t *sn,
            int *dec_alloc, const bool is_reclaim);

    OBEntry *ob_index_get_ob_entry_ex(OBHashtable *htable,
            const FSBlockKey *bkey);

    OBSliceEntry *ob_index_alloc_slice_ex(OBHashtable *htable,
            const FSBlockKey *bkey, const int init_refer);


    static inline void ob_index_free_slice(OBSliceEntry *slice)
    {
        /*
           logInfo("free slice: %p, data version: %"PRId64", ref_count: %d, "
           "block {oid: %"PRId64", offset: %"PRId64"}, "
           "slice {offset: %d, length: %d}, alloctor: %p",
           slice, slice->data_version, FC_ATOMIC_GET(slice->ref_count),
           slice->ob->bkey.oid, slice->ob->bkey.offset,
           slice->ssize.offset, slice->ssize.length,
           slice->allocator);
         */

        if (__sync_sub_and_fetch(&slice->ref_count, 1) == 0) {

            if (slice->cache.mbuffer != NULL) {

                /*
                   logInfo("free slice: %p, data version: %"PRId64", ref_count: %d, block "
                   "{oid: %"PRId64", offset: %"PRId64"}, slice {offset: %d, length: %d}, "
                   "alloctor: %p, mbuffer: %p,  ref_count: %d",
                   slice, slice->data_version, FC_ATOMIC_GET(slice->ref_count),
                   slice->ob->bkey.oid, slice->ob->bkey.offset,
                   slice->ssize.offset, slice->ssize.length,
                   slice->allocator, slice->cache.mbuffer,
                   FC_ATOMIC_GET(slice->cache.mbuffer->reffer_count));
                 */

                sf_shared_mbuffer_release(slice->cache.mbuffer);
                slice->cache.mbuffer = NULL;
            }
            fast_mblock_free_object(slice->allocator, slice);
        }
    }

    void ob_index_free_slice(OBSliceEntry *slice);

    int ob_index_get_slices_ex(OBHashtable *htable,
            const FSBlockSliceKeyInfo *bs_key,
            OBSlicePtrArray *sarray, const bool is_reclaim);

    int ob_index_get_slice_count_ex(OBHashtable *htable,
            const FSBlockSliceKeyInfo *bs_key);

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
        const bool is_reclaim = false;
        int dec_alloc;
        return ob_index_delete_slices(bs_key, NULL, &dec_alloc, is_reclaim);
    }

    static inline int ob_index_delete_block_by_binlog(
            const FSBlockKey *bkey)
    {
        const bool is_reclaim = false;
        int dec_alloc;
        return ob_index_delete_block(bkey, NULL, &dec_alloc, is_reclaim);
    }

    static inline void ob_index_enable_modify_used_space()
    {
        g_ob_hashtable.modify_used_space = true;
    }

    static inline int ob_index_compare_block_key(const FSBlockKey *bkey1,
            const FSBlockKey *bkey2)
    {
        int sub;
        if ((sub=fc_compare_int64(bkey1->oid, bkey2->oid)) != 0) {
            return sub;
        }

        return fc_compare_int64(bkey1->offset, bkey2->offset);
    }

    OBEntry *ob_index_reclaim_lock(const FSBlockKey *bkey);
    void ob_index_reclaim_unlock(OBEntry *ob);

    void ob_index_get_ob_and_slice_counts(int64_t *ob_count,
            int64_t *slice_count);

    int64_t ob_index_get_total_slice_count();

    int ob_index_dump_slices_to_trunk_ex(OBHashtable *htable,
            const int64_t start_index, const int64_t end_index,
            int64_t *slice_count);

    int ob_index_dump_slices_to_file_ex(OBHashtable *htable,
            const int64_t start_index, const int64_t end_index,
            const char *filename, int64_t *slice_count,
            const bool need_padding, const bool need_lock);

    int ob_index_remove_slices_to_file_ex(OBHashtable *htable,
            const int64_t start_index, const int64_t end_index,
            const int rebuild_store_index, const char *filename,
            int64_t *slice_count);

    int ob_index_dump_replica_binlog_to_file_ex(OBHashtable *htable,
            const int data_group_id, const int64_t padding_data_version,
            const char *filename, int64_t *total_slice_count,
            int64_t *total_replica_count);

#ifdef FS_DUMP_SLICE_FOR_DEBUG
    int ob_index_dump_slice_index_to_file(const char *filename,
            int64_t *total_slice_count);
#endif

#ifdef __cplusplus
}
#endif

#endif
