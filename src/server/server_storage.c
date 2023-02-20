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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "binlog/trunk_binlog.h"
#include "server_storage.h"

static int slice_storage_engine_init()
{
    int result;

    if ((result=block_serializer_init()) != 0) {
        return result;
    }

    if ((result=change_notify_init()) != 0) {
        return result;
    }

    if ((result=STORAGE_ENGINE_START_API()) != 0) {
        return result;
    }

    if ((result=event_dealer_init()) != 0) {
        return result;
    }

    return 0;
}

int server_storage_init()
{
    int result;

    if ((result=storage_allocator_init()) != 0) {
        return result;
    }

    if ((result=trunk_binlog_init()) != 0) {
        return result;
    }

    if ((result=ob_index_init()) != 0) {
        return result;
    }

    if ((result=trunk_maker_init()) != 0) {
        return result;
    }

    if (STORAGE_ENABLED) {
        if ((result=slice_storage_engine_init()) != 0) {
            return result;
        }
    }

    return 0;
}

void server_storage_destroy()
{
    trunk_binlog_destroy();
    trunk_id_info_destroy();
}
