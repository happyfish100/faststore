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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include "fastcommon/logger.h"
#include "server_global.h"
#include "server_recovery.h"

int server_recovery_init(const char *config_filename)
{
    int result;

    if ((result=recovery_thread_init()) != 0) {
        return result;
    }

    if ((result=data_recovery_init(config_filename)) != 0) {
        return result;
    }

	return 0;
}

void server_recovery_destroy()
{
    recovery_thread_destroy();
    data_recovery_destroy();
}
 
void server_recovery_terminate()
{
}
