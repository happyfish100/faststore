#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "fastcommon/logger.h"
#include "faststore/fs_func.h"
#include "faststore/fs_client.h"

static void usage(char *argv[])
{
    fprintf(stderr, "Usage: %s [-c config_filename] [-i oid=1] "
            "[-O block_offset=0] [-o slice_offset=0] "
            "[-l slice_length=0 for auto] <filename>\n", argv[0]);
}

int main(int argc, char *argv[])
{
    const char *config_filename = "/etc/fstore/client.conf";
	int ch;
	int result;
    int64_t file_size;
    int blk_offset_remain;
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

    bs_key.block.oid = 1;
    bs_key.block.offset = 0;
    bs_key.slice.offset = 0;
    bs_key.slice.length = 0;
    while ((ch=getopt(argc, argv, "hc:o:b:i:l:")) != -1) {
        switch (ch) {
            case 'h':
                usage(argv);
                break;
            case 'c':
                config_filename = optarg;
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

    if ((result=fs_client_init(config_filename)) != 0) {
        return result;
    }

    fs_calc_block_hashcode(&bs_key.block);
    if ((result=fs_client_proto_slice_write(&g_fs_client_vars.
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
    if ((result=fs_client_proto_slice_read(&g_fs_client_vars.
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
