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

#ifndef _FS_DB_INTERFACE_H
#define _FS_DB_INTERFACE_H

#include "fastcommon/ini_file_reader.h"
#include "diskallocator/dio/trunk_read_thread.h"
#include "diskallocator/global.h"
#include "common/fs_server_types.h"

#define MEMORY_LIMIT_LEVEL0_RATIO  0.60
#define MEMORY_LIMIT_LEVEL1_RATIO  (1.00 - MEMORY_LIMIT_LEVEL0_RATIO)


typedef int (*fs_storage_engine_init_func)(IniFullContext *ini_ctx,
        const int my_server_id, const int file_block_size,
        const FSStorageEngineConfig *db_cfg,
        const DADataConfig *data_cfg);

typedef int (*fs_storage_engine_start_func)();

typedef void (*fs_storage_engine_terminate_func)();

typedef int (*fs_storage_engine_store_func)(const FSDBUpdateBlockArray *array);

typedef int (*fs_storage_engine_redo_func)(const FSDBUpdateBlockArray *array);

typedef int (*fs_storage_engine_fetch_func)(const struct sf_block_key *bkey,
        DASynchronizedReadContext *rctx);

typedef int (*fs_storage_engine_walk_func)(
        fs_storage_engine_walk_callback callback, void *arg);

typedef struct fs_storage_engine_interface {
    fs_storage_engine_init_func init;
    fs_storage_engine_start_func start;
    fs_storage_engine_terminate_func terminate;
    fs_storage_engine_store_func store;
    fs_storage_engine_redo_func redo;
    fs_storage_engine_fetch_func fetch;
    fs_storage_engine_walk_func walk;
} FSStorageEngineInterface;

#endif
