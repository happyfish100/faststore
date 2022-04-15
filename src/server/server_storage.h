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

//server_storage.h

#ifndef _SERVER_STORAGE_H_
#define _SERVER_STORAGE_H_

#include "storage/storage_config.h"
#include "storage/store_path_index.h"
#include "storage/trunk_id_info.h"
#include "storage/trunk_maker.h"
#include "storage/trunk_prealloc.h"
#include "storage/trunk_reclaim.h"
#include "storage/trunk_allocator.h"
#include "storage/storage_allocator.h"
#include "storage/object_block_index.h"
#include "storage/slice_op.h"

#ifdef __cplusplus
extern "C" {
#endif

int server_storage_init();
void server_storage_destroy();

#ifdef __cplusplus
}
#endif

#endif
