//server_storage.h

#ifndef _SERVER_STORAGE_H_
#define _SERVER_STORAGE_H_

#include "storage/storage_config.h"
#include "storage/store_path_index.h"
#include "storage/trunk_id_info.h"
#include "storage/trunk_prealloc.h"
//#include "storage/trunk_reclaim.h"
#include "storage/trunk_allocator.h"
#include "storage/storage_allocator.h"
#include "storage/object_block_index.h"
#include "storage/slice_op.h"

#ifdef __cplusplus
extern "C" {
#endif

int server_storage_init();
void server_storage_destroy();
void server_storage_terminate();

#ifdef __cplusplus
}
#endif

#endif
