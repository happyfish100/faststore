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


#ifndef _FS_EVENT_DEALER_H
#define _FS_EVENT_DEALER_H

#include "../server_types.h"
#include "change_notify.h"

#ifdef __cplusplus
extern "C" {
#endif

    int event_dealer_init();

    int64_t event_dealer_get_last_data_version();

    int event_dealer_do(FSChangeNotifyEvent *head, int *count);

    void event_dealer_free_buffers(FSDBUpdateBlockArray *array);

#ifdef __cplusplus
}
#endif

#endif
