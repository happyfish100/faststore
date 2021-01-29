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

//shared_thread_pool.h

#ifndef _SHARED_THREAD_POOL_H_
#define _SHARED_THREAD_POOL_H_

#include "server_global.h"

#ifdef __cplusplus
extern "C" {
#endif

int shared_thread_pool_init();
void shared_thread_pool_destroy();

static inline int shared_thread_pool_run(
        fc_thread_pool_callback func, void *arg)
{
    return fc_thread_pool_run(&THREAD_POOL, func, arg);
}

#ifdef __cplusplus
}
#endif

#endif
