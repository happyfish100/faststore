
#ifndef _TRUNK_FD_CACHE_H
#define _TRUNK_FD_CACHE_H

#include "fastcommon/fc_list.h"
#include "../../common/fs_types.h"

typedef struct trunk_id_fd_pair {
    int64_t trunk_id;
    int fd;
} TrunkIdFDPair;

typedef struct trunk_fd_cache_entry {
    TrunkIdFDPair pair;
    struct fc_list_head dlink;
    struct trunk_fd_cache_entry *next;  //for hashtable
} TrunkFDCacheEntry;

typedef struct {
    TrunkFDCacheEntry **buckets;
    unsigned int size;
} TrunkFDCacheHashtable;

typedef struct {
    TrunkFDCacheHashtable htable;
    struct {
        int capacity;
        int count;
        struct fc_list_head head;
    } lru;
    struct fast_mblock_man allocator;
} TrunkFDCacheContext;

#ifdef __cplusplus
extern "C" {
#endif

    int trunk_fd_cache_init(TrunkFDCacheContext *cache_ctx, const int capacity);

    //return fd, -1 for not exist
    int trunk_fd_cache_get(TrunkFDCacheContext *cache_ctx,
            const int64_t trunk_id);

    int trunk_fd_cache_add(TrunkFDCacheContext *cache_ctx,
            const int64_t trunk_id, const int fd);

    int trunk_fd_cache_delete(TrunkFDCacheContext *cache_ctx,
            const int64_t trunk_id);

#ifdef __cplusplus
}
#endif

#endif
