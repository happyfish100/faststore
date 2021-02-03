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

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include "shared_thread_pool.h"

int shared_thread_pool_init()
{
    int result;
    int limit1;
    int limit2;
    int limit;
    const int max_idle_time = 60;
    const int min_idle_count = 0;

    limit1 = FS_DATA_RECOVERY_THREADS_LIMIT * (2 +
            RECOVERY_THREADS_PER_DATA_GROUP) + 4;
    limit2 = DATA_THREAD_COUNT;
    limit = FC_MAX(limit1, limit2);
    if ((result=fc_thread_pool_init(&THREAD_POOL, "shared_tpool", limit,
                    SF_G_THREAD_STACK_SIZE, max_idle_time, min_idle_count,
                    (bool *)&SF_G_CONTINUE_FLAG)) != 0)
    {
        return result;
    }

    return 0;
}

void shared_thread_pool_destroy()
{
    fc_thread_pool_destroy(&THREAD_POOL);
}
