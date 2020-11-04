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


#ifndef _TRUNK_MAKER_H
#define _TRUNK_MAKER_H

#include "fastcommon/uniq_skiplist.h"
#include "fastcommon/multi_skiplist.h"
#include "../../common/fs_types.h"
#include "storage_config.h"
#include "trunk_allocator.h"

typedef void (*trunk_allocate_done_callback)(FSTrunkAllocator *allocator,
        const int result, void *arg);

#ifdef __cplusplus
extern "C" {
#endif

    int trunk_maker_init();

    int trunk_maker_allocate_ex(FSTrunkAllocator *allocator,
            trunk_allocate_done_callback callback, void *arg);

#define trunk_maker_allocate(allocator) \
    trunk_maker_allocate_ex(allocator, NULL, NULL)

#ifdef __cplusplus
}
#endif

#endif
