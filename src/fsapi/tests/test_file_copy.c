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
    fprintf(stderr, "Usage: %s [-c config_filename] "
            "-n [namespace=fs] <input_local_filename> "
            "<fs_filename>\n\n", argv[0]);
}

const char *config_filename = "/etc/fsapi/fuse.conf";
static char *ns = "fs";
static char *input_filename;
static char *fs_filename;

static int copy_file()
{
	int result;
    FSAPIFileInfo fi;
    char *file_content;
    int write_bytes;
    int64_t file_size;

    if ((result=getFileContent(input_filename,
                    &file_content, &file_size)) != 0)
    {
        return result;
    }

    if ((result=fs_api_pooled_init(ns, config_filename)) != 0) {
        return result;
    }

    if ((result=fsapi_open(&fi, fs_filename,
                    O_CREAT | O_WRONLY, 0755)) != 0)
    {
        return result;
    }

    result = fsapi_write(&fi, file_content, file_size, &write_bytes);
    if (result != 0) {
        logError("file: "__FILE__", line: %d, "
                "write to file %s fail, length: %"PRId64", "
                "write_bytes: %d, errno: %d, error info: %s",
                __LINE__, fs_filename, file_size, write_bytes,
                result, STRERROR(result));
        return result;
    }
    fsapi_close(&fi);

    printf("write bytes: %d\n", write_bytes); 
    return 0;
}

int main(int argc, char *argv[])
{
	int ch;

    if (argc < 3) {
        usage(argv);
        return 1;
    }

    while ((ch=getopt(argc, argv, "hc:n:")) != -1) {
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
            default:
                usage(argv);
                return 1;
        }
    }

    if (optind + 1 >= argc) {
        usage(argv);
        return 1;
    }

    log_init();
    //g_log_context.log_level = LOG_DEBUG;

    input_filename = argv[optind];
    fs_filename = argv[optind + 1];

    return copy_file();
}
