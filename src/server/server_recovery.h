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

//server_recovery.h

#ifndef _SERVER_RECOVERY_H_
#define _SERVER_RECOVERY_H_

#include "recovery/recovery_thread.h"
#include "recovery/data_recovery.h"

#ifdef __cplusplus
extern "C" {
#endif

int server_recovery_init();
void server_recovery_destroy();
void server_recovery_terminate();

#ifdef __cplusplus
}
#endif

#endif
