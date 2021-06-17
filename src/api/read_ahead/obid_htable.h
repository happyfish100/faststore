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


#ifndef _READ_AHEAD_OBID_HTABLE_H
#define _READ_AHEAD_OBID_HTABLE_H

#include "../fs_api_types.h"

#ifdef __cplusplus
extern "C" {
#endif

    int preread_obid_htable_init(const int64_t htable_capacity,
            const int shared_lock_count);

    int preread_obid_htable_insert(FSAPIOperationContext *op_ctx,
            const FSSliceSize *ssize, FSAPIBuffer *buffer);

    int preread_obid_htable_delete(const int64_t oid,
            const int64_t bid, const int64_t tid);

    int preread_invalidate_conflict_cache(FSAPIOperationContext *op_ctx);

#ifdef __cplusplus
}
#endif

#endif
