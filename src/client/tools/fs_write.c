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
            "[-n namespace or poolname=fs] <filename>\n",
            argv[0], FS_CLIENT_DEFAULT_CONFIG_FILENAME);
}

int main(int argc, char *argv[])
{
    const bool publish = false;
    const char *config_filename = FS_CLIENT_DEFAULT_CONFIG_FILENAME;
    string_t poolname;
    int ch;
    int result;
    int blk_offset_remain;
    int write_bytes;
    int inc_alloc;
    int64_t file_size;
    char *ns;
    char *filename;
    char *endptr;
    FSBlockSliceKeyInfo bs_key;
    char *out_buff;

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

    if (optind >= argc) {
        usage(argv);
        return EINVAL;
    }

    if (bs_key.block.oid == 0) {
        fprintf(stderr, "expect oid\n");
        usage(argv);
        return EINVAL;
    }

    log_init();
    //g_log_context.log_level = LOG_DEBUG;

    filename = argv[optind];
    if ((result=getFileContent(filename, &out_buff, &file_size)) != 0) {
        return result;
    }
    if (file_size == 0) {
        logError("file: "__FILE__", line: %d, "
                "empty file: %s", __LINE__, filename);
        return ENOENT;
    }

    blk_offset_remain = bs_key.block.offset % FS_FILE_BLOCK_SIZE;
    bs_key.block.offset -= blk_offset_remain;
    bs_key.slice.offset += blk_offset_remain;

    if (bs_key.slice.length == 0 || bs_key.slice.length > file_size) {
        bs_key.slice.length = file_size;
    }
    if (bs_key.slice.offset >= FS_FILE_BLOCK_SIZE) {
        logError("file: "__FILE__", line: %d, "
                "invalid slice offset: %d > block size: %d",
                __LINE__, bs_key.slice.offset, FS_FILE_BLOCK_SIZE);
        return EINVAL;
    }
    if (bs_key.slice.offset + bs_key.slice.length > FS_FILE_BLOCK_SIZE) {
        bs_key.slice.length = FS_FILE_BLOCK_SIZE - bs_key.slice.offset;
    }

    logInfo("block offset: %"PRId64", slice {offset: %d, length: %d}",
            bs_key.block.offset, bs_key.slice.offset, bs_key.slice.length);

    FC_SET_STRING(poolname, ns);
    if ((result=fs_client_init_with_auth_ex1(&g_fs_client_vars.client_ctx,
                    &g_fcfs_auth_client_vars.client_ctx, config_filename,
                    NULL, NULL, false, &poolname, publish)) != 0)
    {
        return result;
    }

    fs_calc_block_hashcode(&bs_key.block);
    if ((result=fs_client_slice_write(&g_fs_client_vars.
                    client_ctx, &bs_key, out_buff,
                    &write_bytes, &inc_alloc)) != 0)
    {
        logError("file: "__FILE__", line: %d, "
                "slice write fail, errno: %d, error info: %s",
                __LINE__, result, STRERROR(result));
        return result;
    }

    return 0;
}
