
#ifndef _FS_CLIENT_H
#define _FS_CLIENT_H

#include "fs_proto.h"
#include "fs_types.h"
#include "fs_func.h"
#include "client_types.h"
#include "client_func.h"
#include "client_global.h"
#include "client_proto.h"
#include "simple_connection_manager.h"

#ifdef __cplusplus
extern "C" {
#endif

static inline void fs_set_block_key(FSBlockKey *bkey,
        const int64_t oid, const int64_t offset)
{
    bkey->oid = oid;
    bkey->offset = FS_FILE_BLOCK_ALIGN(offset);
    fs_calc_block_hashcode(bkey);
}

static inline void fs_set_slice_size(FSBlockSliceKeyInfo *bs_key,
        const int64_t offset, const int current_size)
{
    bs_key->slice.offset = offset - bs_key->block.offset;
    if (bs_key->slice.offset + current_size <= FS_FILE_BLOCK_SIZE) {
        bs_key->slice.length = current_size;
    } else {
        bs_key->slice.length = FS_FILE_BLOCK_SIZE - bs_key->slice.offset;
    }
}

static inline void fs_set_block_slice(FSBlockSliceKeyInfo *bs_key,
        const int64_t oid, const int64_t offset, const int current_size)
{
    fs_set_block_key(&bs_key->block, oid, offset);
    fs_set_slice_size(bs_key, offset, current_size);
}

static inline void fs_next_block_key(FSBlockKey *bkey)
{
    bkey->offset += FS_FILE_BLOCK_SIZE;
    fs_calc_block_hashcode(bkey);
}

static inline void fs_next_block_slice_key(FSBlockSliceKeyInfo *bs_key,
        const int current_size)
{
    fs_next_block_key(&bs_key->block);

    bs_key->slice.offset = 0;
    if (current_size <= FS_FILE_BLOCK_SIZE) {
        bs_key->slice.length = current_size;
    } else {
        bs_key->slice.length = FS_FILE_BLOCK_SIZE;
    }
}

int fs_unlink_file(FSClientContext *client_ctx, const int64_t oid,
        const int64_t file_size);

int fs_cluster_stat(FSClientContext *client_ctx, const int data_group_id,
        FSClientClusterStatEntry *stats, const int size, int *count);

int fs_client_slice_write(FSClientContext *client_ctx,
        const FSBlockSliceKeyInfo *bs_key, const char *data,
        int *write_bytes, int *inc_alloc);

#ifdef __cplusplus
}
#endif

#endif
