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
    fprintf(stderr, "Usage: %s [-c config_filename=%s] [-i oid=1] "
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
    int64_t file_size;
    int blk_offset_remain;
    char *ns;
    char *slice_filename;
    char *endptr;
    FSBlockSliceKeyInfo bs_key;
    char *out_buff;
    char *in_buff;
    int write_bytes;
    int read_bytes;
    int inc_alloc;

    if (argc < 2) {
        usage(argv);
        return 1;
    }

    ns = "fs";
    bs_key.block.oid = 1;
    bs_key.block.offset = 0;
    bs_key.slice.offset = 0;
    bs_key.slice.length = 0;
    while ((ch=getopt(argc, argv, "hc:i:O:o:l:n:")) != -1) {
        switch (ch) {
            case 'h':
                usage(argv);
                break;
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
                return 1;
        }
    }

    if (optind >= argc) {
        usage(argv);
        return 1;
    }

    log_init();
    //g_log_context.log_level = LOG_DEBUG;

    slice_filename = argv[optind];
    if ((result=getFileContent(slice_filename, &out_buff, &file_size)) != 0) {
        return result;
    }
    if (file_size == 0) {
        logError("file: "__FILE__", line: %d, "
                "empty file: %s", __LINE__, slice_filename);
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
        return result;
    }

    in_buff = (char *)fc_malloc(bs_key.slice.length);
    if (in_buff == NULL) {
        return ENOMEM;
    }

    memset(in_buff, 0, bs_key.slice.length);
    if ((result=fs_client_slice_read(&g_fs_client_vars.
                    client_ctx, &bs_key, in_buff, &read_bytes)) != 0)
    {
        return result;
    }
    if (read_bytes != bs_key.slice.length) {
        logError("file: "__FILE__", line: %d, "
                "read bytes: %d != slice length: %d",
                __LINE__, read_bytes, bs_key.slice.length);
        return EINVAL;
    }

    result = memcmp(in_buff, out_buff, bs_key.slice.length);
    if (result != 0) {
        printf("read and write buffer compare result: %d != 0\n", result);
        return EINVAL;
    }

    return 0;
}
