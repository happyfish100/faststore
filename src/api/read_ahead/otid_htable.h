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

#ifndef _PREREAD_OTID_HTABLE_H
#define _PREREAD_OTID_HTABLE_H

#include "sf/sf_sharding_htable.h"
#include "../fs_api_types.h"
#include "../fs_api_buffer_pool.h"

#ifdef __cplusplus
extern "C" {
#endif

    int preread_otid_htable_init(const int sharding_count,
            const int64_t htable_capacity,
            const int allocator_count, int64_t element_limit,
            const int64_t min_ttl_ms, const int64_t max_ttl_ms,
            const double low_water_mark_ratio);

    int preread_otid_htable_insert(FSAPIOperationContext *op_ctx,
            FSAPIBuffer *buffer);

    int preread_slice_read(FSAPIOperationContext *op_ctx,
            char *buff, int *read_bytes);

#ifdef __cplusplus
}
#endif

#endif
