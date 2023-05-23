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
#include "../server_global.h"

typedef struct fs_db_fetch_context {
    DASynchronizedReadContext read_ctx;
    SFSerializerIterator it;
} FSDBFetchContext;

typedef struct {
    pthread_lock_cond_pair_t lcp; //for lock and notify

    struct {
        UniqSkiplistFactory factory;
        struct fast_mblock_man ob;     //for ob_entry
        struct fast_mblock_man slice;  //for slice_entry
    } allocators;

    /* following fields for storage engine */
    FSDBFetchContext db_fetch_ctx;
    struct fc_list_head lru;       //element: OBEntry
    bool use_extra_allocator;
} OBSegment;

#ifdef __cplusplus
extern "C" {
#endif

#define ob_index_add_slice(bkey, slice, trunk, sn, inc_alloc, space_chain) \
    ob_index_add_slice_ex(&G_OB_HASHTABLE, bkey, slice, \
            trunk, sn, inc_alloc, space_chain)

#define ob_index_update_slice(se, slice, update_count,   \
        record, slice_type, call_by_reclaim)  \
    ob_index_update_slice_ex(&G_OB_HASHTABLE, se, slice, update_count, \
            record, slice_type, call_by_reclaim)

#define ob_index_delete_slice(bs_key, sn, dec_alloc, space_chain) \
    ob_index_delete_slice_ex(&G_OB_HASHTABLE, \
            bs_key, sn, dec_alloc, space_chain)

#define ob_index_delete_block(bkey, sn, dec_alloc, space_chain) \
    ob_index_delete_block_ex(&G_OB_HASHTABLE, \
            bkey, sn, dec_alloc, space_chain)

#define ob_index_get_slices(bs_key, sarray) \
    ob_index_get_slices_ex(&G_OB_HASHTABLE, bs_key, sarray)

#define ob_index_get_slice_count(bs_key) \
    ob_index_get_slice_count_ex(&G_OB_HASHTABLE, bs_key)

#define ob_index_get_ob_entry(bkey) \
    ob_index_get_ob_entry_ex(&G_OB_HASHTABLE, bkey, false)

#define ob_index_ob_entry_release(ob) \
    ob_index_ob_entry_release_ex(ob, 1)

#define ob_index_alloc_slice(bkey) \
    ob_index_alloc_slice_ex(&G_OB_HASHTABLE, bkey, 1)

#define ob_index_dump_slices_to_file(start_index,  \
        end_index, filename, slice_count, source)  \
    ob_index_dump_slices_to_file_ex(&G_OB_HASHTABLE, \
            start_index, end_index, filename, slice_count, source, \
            end_index == G_OB_HASHTABLE.capacity, false)

#define ob_index_remove_slices_to_file(start_index, \
        end_index, filename, slice_count, source)   \
    ob_index_remove_slices_to_file_ex(&G_OB_HASHTABLE, start_index, \
            end_index, filename, slice_count, source)

#define ob_index_remove_slices_to_file_for_reclaim(filename, slice_count) \
    ob_index_remove_slices_to_file_for_reclaim_ex(&G_OB_HASHTABLE, \
            filename, slice_count)

#define ob_index_dump_replica_binlog_to_file(data_group_id, \
        padding_data_version, filename, \
        total_slice_count, total_replica_count) \
    ob_index_dump_replica_binlog_to_file_ex(&G_OB_HASHTABLE, \
            data_group_id, padding_data_version, filename, \
            total_slice_count, total_replica_count)

    int ob_index_init();
    void ob_index_destroy();

    int ob_index_init_htable(OBHashtable *htable, const int64_t capacity,
            const bool need_reclaim);
    void ob_index_destroy_htable(OBHashtable *htable);

    int ob_index_add_slice_ex(OBHashtable *htable, const FSBlockKey *bkey,
            OBSliceEntry *slice, DATrunkFileInfo *trunk, uint64_t *sn,
            int *inc_alloc, struct fc_queue_info *space_chain);

    int ob_index_update_slice_ex(OBHashtable *htable, const DASliceEntry *se,
            const DATrunkSpaceInfo *space, int *update_count,
            FSSliceSpaceLogRecord *record, const DASliceType slice_type,
            const bool call_by_reclaim);

    int ob_index_delete_slice_ex(OBHashtable *htable,
            const FSBlockSliceKeyInfo *bs_key, uint64_t *sn,
            int *dec_alloc, struct fc_queue_info *space_chain);

    int ob_index_delete_block_ex(OBHashtable *htable, const FSBlockKey *bkey,
            uint64_t *sn, int *dec_alloc, struct fc_queue_info *space_chain);

    void ob_index_ob_entry_release_ex(OBEntry *ob, const int dec_count);

    OBEntry *ob_index_get_ob_entry_ex(OBHashtable *htable,
            const FSBlockKey *bkey, const bool create_flag);

    OBSliceEntry *ob_index_alloc_slice_ex(OBHashtable *htable,
            const FSBlockKey *bkey, const int init_refer);

    static inline int64_t ob_index_generate_alone_sn()
    {
        const int data_group_id = 0;
        const int64_t data_version = 0;
        int64_t sn;

        sn = __sync_add_and_fetch(&SLICE_BINLOG_SN, 1);
        committed_version_add(data_group_id, data_version, sn);
        return sn;
    }

    static inline int64_t ob_index_batch_generate_alone_sn(const int count)
    {
        const int data_group_id = 0;
        const int64_t data_version = 0;
        int64_t sn;
        int i;

        sn = __sync_add_and_fetch(&SLICE_BINLOG_SN, count) - (count - 1);
        for (i=0; i<count; i++) {
            committed_version_add(data_group_id, data_version, sn + i);
        }
        return sn;
    }

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

    int ob_index_add_slice_by_binlog(const uint64_t sn,
            const int64_t data_version, const FSBlockSliceKeyInfo *bs_key,
            const DASliceType slice_type, const DATrunkSpaceInfo *space);

    static inline int ob_index_delete_slice_by_binlog(const uint64_t sn,
            const FSBlockSliceKeyInfo *bs_key)
    {
        int dec_alloc;
        return ob_index_delete_slice(bs_key,
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
            const char *filename, int64_t *slice_count, const int source,
            const bool need_padding, const bool need_lock);

    int ob_index_remove_slices_to_file_ex(OBHashtable *htable,
            const int64_t start_index, const int64_t end_index,
            const char *filename, int64_t *slice_count, const int source);

    int ob_index_remove_slices_to_file_for_reclaim_ex(OBHashtable *htable,
            const char *filename, int64_t *slice_count);

    int ob_index_dump_replica_binlog_to_file_ex(OBHashtable *htable,
            const int data_group_id, const int64_t padding_data_version,
            const char *filename, int64_t *total_slice_count,
            int64_t *total_replica_count);

#ifdef FS_DUMP_SLICE_FOR_DEBUG
    int ob_index_dump_slice_index_to_file(const char *filename,
            int64_t *total_block_count, int64_t *total_slice_count);
#endif

    OBSegment *ob_index_get_segment(const FSBlockKey *bkey);

    static inline int ob_index_alloc_db_slices(OBSegment *segment, OBEntry *ob)
    {
        const int init_level_count = 2;
        if ((ob->db_args->slices=uniq_skiplist_new(&segment->allocators.
                        factory, init_level_count)) != NULL)
        {
            return 0;
        } else {
            return ENOMEM;
        }
    }

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
