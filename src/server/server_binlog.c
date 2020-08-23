#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <fcntl.h>
#include <pthread.h>
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "server_global.h"
#include "server_binlog.h"
#include "binlog/binlog_check.h"

static int do_binlog_check()
{
    int flags;
    int result;

    if (BINLOG_CHECK_LAST_SECONDS <= 0) {
        return 0;
    }

    if ((result=binlog_consistency_check(&flags)) != 0) {
        return result;
    }

    //TODO
    return 0;
}

int server_binlog_init()
{
    int result;

    if ((result=slice_binlog_init()) != 0) {
        return result;
    }

    if ((result=replica_binlog_init()) != 0) {
        return result;
    }

	return do_binlog_check();
}

void server_binlog_destroy()
{
    slice_binlog_destroy();
    replica_binlog_destroy();
}
 
void server_binlog_terminate()
{
}
