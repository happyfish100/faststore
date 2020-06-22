#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "binlog/trunk_binlog.h"
#include "server_storage.h"

int server_storage_init()
{
    int result;

    if ((result=storage_allocator_init()) != 0) {
        return result;
    }

    if ((result=trunk_prealloc_init()) != 0) {
        return result;
    }

    if ((result=trunk_binlog_init()) != 0) {
        return result;
    }

    if ((result=ob_index_init()) != 0) {
        return result;
    }

    if ((result=storage_allocator_prealloc_trunk_freelists()) != 0) {
        return result;
    }

	return 0;
}

void server_storage_destroy()
{
    trunk_binlog_destroy();
}
 
void server_storage_terminate()
{
}
