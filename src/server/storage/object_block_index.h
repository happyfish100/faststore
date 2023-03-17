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

#include "sf/sf_serializer.h"
#include "diskallocator/dio/trunk_read_thread.h"
#include "diskallocator/dio/trunk_write_thread.h"
#include "../server_types.h"

typedef struct fs_db_fetch_context {
    DASynchronizedReadContext read_ctx;
    SFSerializerIterator it;
} FSDBFetchContext;

typedef struct {
    pthread_lock_cond_pair_t lcp; //for lock and notify

    /* following fields for storage engine */
    FSDBFetchContext db_fetch_ctx;
    volatile int64_t count;    //ob count
    struct fc_list_head lru;   //element: OBEntry
} OBSegment;

#ifdef __cplusplus
extern "C" {
#endif

    extern OBHashtable g_ob_hashtable;

#define ob_index_add_slice(bkey, slice, sn, inc_alloc, space_chain) \
    ob_index_add_slice_ex(&g_ob_hashtable, bkey, slice, \
            sn, inc_alloc, space_chain)

#define ob_index_update_slice(se, space, update_count, record, call_by_reclaim)\
    ob_index_update_slice_ex(&g_ob_hashtable, se, space, \
            update_count, record, call_by_reclaim)

#define ob_index_delete_slices(bs_key, sn, dec_alloc, space_chain) \
    ob_index_delete_slices_ex(&g_ob_hashtable, \
            bs_key, sn, dec_alloc, space_chain)

#define ob_index_delete_block(bkey, sn, dec_alloc, space_chain) \
    ob_index_delete_block_ex(&g_ob_hashtable, \
            bkey, sn, dec_alloc, space_chain)

#define ob_index_get_slices(bs_key, sarray) \
    ob_index_get_slices_ex(&g_ob_hashtable, bs_key, sarray)

#define ob_index_get_slice_count(bs_key) \
    ob_index_get_slice_count_ex(&g_ob_hashtable, bs_key)

#define ob_index_get_ob_entry(bkey) \
    ob_index_get_ob_entry_ex(&g_ob_hashtable, bkey)

#define ob_index_ob_entry_release(ob) \
    ob_index_ob_entry_release_ex(ob, 1)

#define ob_index_alloc_slice(bkey) \
    ob_index_alloc_slice_ex(&g_ob_hashtable, bkey, 1)

#define ob_index_init_htable(ht) \
    ob_index_init_htable_ex(ht, STORAGE_CFG.object_block.  \
            hashtable_capacity)

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
            struct fc_queue_info *space_chain);

    int ob_index_update_slice_ex(OBHashtable *htable, const DASliceEntry *se,
            const DATrunkSpaceInfo *space, int *update_count,
            FSSliceSpaceLogRecord *record, const bool call_by_reclaim);

    int ob_index_delete_slices_ex(OBHashtable *htable,
            const FSBlockSliceKeyInfo *bs_key, uint64_t *sn,
            int *dec_alloc, struct fc_queue_info *space_chain);

    int ob_index_delete_block_ex(OBHashtable *htable, const FSBlockKey *bkey,
            uint64_t *sn, int *dec_alloc, struct fc_queue_info *space_chain);

    static inline void ob_index_ob_entry_release_ex(
            OBEntry *ob, const int dec_count)
    {
        bool need_free;

        if (g_ob_hashtable.need_reclaim) {
            if (__sync_sub_and_fetch(&ob->db_args->
                        ref_count, dec_count) == 0)
            {
                need_free = true;
                if (ob->db_args->slices != NULL) {
                    uniq_skiplist_free(ob->db_args->slices);
                    ob->db_args->slices = NULL;
                }
            } else {
                need_free = false;
            }
        } else {
            need_free = true;
        }

        if (need_free) {
            uniq_skiplist_free(ob->slices);
            ob->slices = NULL;
            fast_mblock_free_object(ob->allocator, ob);
        }
    }

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
            OBSliceReadBufferArray *sarray);

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

    static inline void ob_index_init_slice_rbuffer_array(
            OBSliceReadBufferArray *array)
    {
        array->pairs = NULL;
        array->alloc = array->count = 0;
    }

    static inline void ob_index_free_slice_rbuffer_array(
            OBSliceReadBufferArray *array)
    {
        if (array->pairs != NULL) {
            free(array->pairs);
            array->pairs = NULL;
            array->alloc = array->count = 0;
        }
    }

    int ob_index_add_slice_to_wbuffer_chain(FSSliceSpaceLogRecord *record,
            SFBinlogWriterBuffer **slice_tail, OBSliceEntry *slice,
            const time_t timestamp, const int64_t sn, const char source);

    int ob_index_del_slice_to_wbuffer_chain(FSSliceSpaceLogRecord *record,
            const FSBlockSliceKeyInfo *bs_key, const time_t timestamp,
            const int64_t sn, const int64_t data_version, const char source);

    int ob_index_del_block_to_wbuffer_chain(FSSliceSpaceLogRecord *record,
            const FSBlockKey *bkey, const time_t timestamp, const int64_t sn,
            const int64_t data_version, const char source);

    int ob_index_add_slice_by_binlog(const uint64_t sn, OBSliceEntry *slice);

    static inline int ob_index_delete_slices_by_binlog(const uint64_t sn,
            const FSBlockSliceKeyInfo *bs_key)
    {
        int dec_alloc;
        return ob_index_delete_slices(bs_key,
                (uint64_t *)&sn, &dec_alloc, NULL);
    }

    static inline int ob_index_delete_block_by_binlog(
            const uint64_t sn, const FSBlockKey *bkey)
    {
        int dec_alloc;
        return ob_index_delete_block(bkey, (uint64_t *)&sn,
                &dec_alloc, NULL);
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

    void ob_index_get_ob_and_slice_counts(int64_t *ob_count,
            int64_t *slice_count);

    int64_t ob_index_get_total_slice_count();

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

    int ob_index_unpack_ob_entry(OBSegment *segment, OBEntry *ob,
            UniqSkiplist *sl, const SFSerializerFieldValue *fv);

    OBSegment *ob_index_get_segment(const FSBlockKey *bkey);

    static inline void ob_index_segment_lock(OBSegment *segment)
    {
        PTHREAD_MUTEX_LOCK(&segment->lcp.lock);
    }

    static inline void ob_index_segment_unlock(OBSegment *segment)
    {
        PTHREAD_MUTEX_UNLOCK(&segment->lcp.lock);
    }

    int ob_index_alloc_db_slices(OBEntry *ob);

    int ob_index_load_db_slices(OBSegment *segment, OBEntry *ob);

    int ob_index_add_slice_by_db(OBSegment *segment, OBEntry *ob,
            const int64_t data_version, const DASliceType type,
            const FSSliceSize *ssize, const DATrunkSpaceInfo *space);

    int ob_index_delete_slice_by_db(OBSegment *segment,
            OBEntry *ob, const FSSliceSize *ssize);

    int ob_index_delete_block_by_db(OBSegment *segment, OBEntry *ob);

#ifdef __cplusplus
}
#endif

#endif
