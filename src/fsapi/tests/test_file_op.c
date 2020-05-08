#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "fastcommon/logger.h"
#include "fsapi/fs_api.h"

static void usage(char *argv[])
{
    fprintf(stderr, "Usage: %s [-c fdir_config_filename] "
            "[-C fs_config_filename] -n [namespace=fs] "
            "[-o offset=0] [-l length=0 for auto] [-A append mode] "
            "[-T truncate mode] -i <input_filename> <filename>\n", argv[0]);
}

int main(int argc, char *argv[])
{
    const char *fdir_config_filename = "/etc/fdir/client.conf";
    const char *fs_config_filename = "/etc/fstore/client.conf";
	int ch;
	int result;
    int open_flags;
    int64_t offset = 0;
    int64_t file_size;
    int length = 0;
    FSAPIFileInfo fi;
    char *ns = "fs";
    char *input_filename = NULL;
    char *filename;
    char *endptr;
    char *out_buff;
    char *in_buff;
    int write_bytes;
    int read_bytes;

    if (argc < 2) {
        usage(argv);
        return 1;
    }

    open_flags = 0;
    while ((ch=getopt(argc, argv, "hc:C:o:n:i:l:AT")) != -1) {
        switch (ch) {
            case 'h':
                usage(argv);
                break;
            case 'c':
                fdir_config_filename = optarg;
                break;
            case 'C':
                fs_config_filename = optarg;
                break;
            case 'i':
                input_filename = optarg;
                break;
            case 'n':
                ns = optarg;
                break;
            case 'o':
                offset = strtol(optarg, &endptr, 10);
                break;
            case 'l':
                length = strtol(optarg, &endptr, 10);
                break;
            case 'A':
                open_flags |= O_APPEND;
                break;
            case 'T':
                open_flags |= O_TRUNC;
                break;
            default:
                usage(argv);
                return 1;
        }
    }

    if (input_filename == NULL) {
        fprintf(stderr, "expect input filename\n");
        usage(argv);
        return 1;
    }

    if (optind >= argc) {
        fprintf(stderr, "expect filename\n");
        usage(argv);
        return 1;
    }

    log_init();
    //g_log_context.log_level = LOG_DEBUG;

    filename = argv[optind];
    if ((result=getFileContent(input_filename, &out_buff, &file_size)) != 0) {
        return result;
    }
    if (file_size == 0) {
        logError("file: "__FILE__", line: %d, "
                "empty file: %s", __LINE__, input_filename);
        return ENOENT;
    }
    if (length == 0) {
        length = file_size;
    }

    if ((result=fs_api_init(ns, fdir_config_filename,
                    fs_config_filename)) != 0)
    {
        return result;
    }

    if ((result=fsapi_open(&fi, filename, O_CREAT | O_WRONLY | open_flags,
                    0755)) != 0)
    {
        return result;
    }

    if (offset == 0) {
        result = fsapi_write(&fi, out_buff, length, &write_bytes);
    } else {
        result = fsapi_pwrite(&fi, out_buff, length, offset, &write_bytes);
    }
    if (result != 0) {
        logError("file: "__FILE__", line: %d, "
                "write to file %s fail, offset: %"PRId64", length: %d, "
                "write_bytes: %d, errno: %d, error info: %s",
                __LINE__, filename, offset, length, write_bytes,
                result, STRERROR(result));
        return result;
    }
    fsapi_close(&fi);

    if ((result=fsapi_open(&fi, filename, O_RDONLY, 0755)) != 0) {
        return result;
    }
    in_buff = (char *)malloc(length);
    if (in_buff == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, length);
        return ENOMEM;
    }

    memset(in_buff, 0, length);
    if (offset == 0) {
        result = fsapi_read(&fi, in_buff, length, &read_bytes);
    } else {
        result = fsapi_pread(&fi, in_buff, length, offset, &read_bytes);
    }

    if (result != 0) {
        logError("file: "__FILE__", line: %d, "
                "read from file %s fail, offset: %"PRId64", length: %d, "
                "read_bytes: %d, errno: %d, error info: %s",
                __LINE__, filename, offset, length, read_bytes,
                result, STRERROR(result));
        return result;
    }

    if (read_bytes != length) {
        logError("file: "__FILE__", line: %d, "
                "read bytes: %d != slice length: %d",
                __LINE__, read_bytes, length);
        return EINVAL;
    }

    result = memcmp(in_buff, out_buff, length);
    if (result != 0) {
        printf("read and write buffer compare result: %d != 0\n", result);
        return EINVAL;
    }

    fsapi_close(&fi);
    return 0;
}
