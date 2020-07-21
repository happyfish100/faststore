#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include "fastcommon/logger.h"
#include "server_global.h"
#include "server_recovery.h"

int server_recovery_init()
{
    int result;

    if ((result=recovery_thread_init()) != 0) {
        return result;
    }

	return 0;
}

void server_recovery_destroy()
{
    recovery_thread_destroy();
}
 
void server_recovery_terminate()
{
}
