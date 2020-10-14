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
    replication_callee_destroy();
}
 
void server_replication_terminate()
{
}
