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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "fastcommon/logger.h"
#include "faststore/client/fs_client.h"

static void usage(char *argv[])
{
    fprintf(stderr, "Usage: %s [-c config_filename=%s] <-i oid> "
            "[-O block_offset=0] [-o slice_offset=0] "
            "[-l slice_length=0 for auto] "
            "[-n namespace or poolname=fs]\n", argv[0],
            FS_CLIENT_DEFAULT_CONFIG_FILENAME);
}

int main(int argc, char *argv[])
{
    const bool publish = false;
    const char *config_filename = FS_CLIENT_DEFAULT_CONFIG_FILENAME;
    string_t poolname;
    int ch;
    int result;
    int blk_offset_remain;
    int dec_alloc;
    char *ns;
    char *caption;
    char *endptr;
    FSBlockSliceKeyInfo bs_key;

    if (argc < 2) {
        usage(argv);
        return EINVAL;
    }

    ns = "fs";
    bs_key.block.oid = 0;
    bs_key.block.offset = 0;
    bs_key.slice.offset = 0;
    bs_key.slice.length = 0;
    while ((ch=getopt(argc, argv, "hc:O:o:i:l:n:")) != -1) {
        switch (ch) {
            case 'h':
                usage(argv);
                return 0;
            case 'c':
                config_filename = optarg;
                break;
            case 'n':
                ns = optarg;
                break;
            case 'i':
                bs_key.block.oid = strtol(optarg, &endptr, 10);
                break;
            case 'O':
                bs_key.block.offset = strtol(optarg, &endptr, 10);
                break;
            case 'o':
                bs_key.slice.offset = strtol(optarg, &endptr, 10);
                break;
            case 'l':
                bs_key.slice.length = strtol(optarg, &endptr, 10);
                break;
            default:
                usage(argv);
                return EINVAL;
        }
    }

    if (bs_key.block.oid == 0) {
        fprintf(stderr, "expect oid\n");
        usage(argv);
        return EINVAL;
    }

    log_init();
    //g_log_context.log_level = LOG_DEBUG;

    blk_offset_remain = bs_key.block.offset % FS_FILE_BLOCK_SIZE;
    bs_key.block.offset -= blk_offset_remain;
    bs_key.slice.offset += blk_offset_remain;
    if (bs_key.slice.offset >= FS_FILE_BLOCK_SIZE) {
        logError("file: "__FILE__", line: %d, "
                "invalid slice offset: %d > block size: %d",
                __LINE__, bs_key.slice.offset, FS_FILE_BLOCK_SIZE);
        return EINVAL;
    }
    if (bs_key.slice.length == 0) {
        bs_key.slice.length = FS_FILE_BLOCK_SIZE - bs_key.slice.offset;
    } else if (bs_key.slice.offset + bs_key.slice.length > FS_FILE_BLOCK_SIZE) {
        bs_key.slice.length = FS_FILE_BLOCK_SIZE - bs_key.slice.offset;
    }

    FC_SET_STRING(poolname, ns);
    if ((result=fs_client_init_with_auth_ex1(&g_fs_client_vars.client_ctx,
                    &g_fcfs_auth_client_vars.client_ctx, config_filename,
                    NULL, NULL, false, &poolname, publish)) != 0)
    {
        return result;
    }

    fs_calc_block_hashcode(&bs_key.block);
    if (bs_key.slice.offset == 0 &&
            bs_key.slice.length == FS_FILE_BLOCK_SIZE)
    {
        caption = "block";
        result = fs_client_block_delete(&g_fs_client_vars.
                    client_ctx, &bs_key.block, &dec_alloc);
    } else {
        caption = "slice";
        result = fs_client_slice_delete(&g_fs_client_vars.
                client_ctx, &bs_key, &dec_alloc);
    }
    if (result != 0) {
        logError("file: "__FILE__", line: %d, "
                "delete %s fail, errno: %d, error info: %s",
                __LINE__, caption, result, STRERROR(result));
        return result;
    }

    return 0;
}
