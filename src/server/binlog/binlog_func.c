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
#include <fcntl.h>
#include <pthread.h>
#include "fastcommon/logger.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "binlog_func.h"

int binlog_buffer_init_ex(ServerBinlogBuffer *buffer, const int size)
{
    buffer->buff = (char *)malloc(size);
    if (buffer->buff == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, size);
        return ENOMEM;
    }

    buffer->current = buffer->end = buffer->buff;
    buffer->size = size;
    return 0;
}
