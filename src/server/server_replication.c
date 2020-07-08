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
#include "server_global.h"
#include "server_replication.h"

int server_replication_init()
{
    int result;

    if ((result=replication_common_init()) != 0) {
        return result;
    }

    if ((result=replication_caller_init()) != 0) {
        return result;
    }

    if ((result=replication_callee_init()) != 0) {
        return result;
    }

	return 0;
}

void server_replication_destroy()
{
    replication_common_destroy();
    replication_caller_destroy();
}
 
void server_replication_terminate()
{
}
