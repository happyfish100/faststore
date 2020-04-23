#ifndef _FS_SIMPLE_CONNECTION_MANAGER_H
#define _FS_SIMPLE_CONNECTION_MANAGER_H

#include "client_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int fs_simple_connection_manager_init_ex(FSConnectionManager *conn_manager,
        const int max_count_per_entry, const int max_idle_time);

static inline int fs_simple_connection_manager_init(
        FSConnectionManager *conn_manager)
{
    const int max_count_per_entry = 0;
    const int max_idle_time = 4 * 3600;
    return fs_simple_connection_manager_init_ex(conn_manager,
            max_count_per_entry, max_idle_time);
}

void fs_simple_connection_manager_destroy(FSConnectionManager *conn_manager);

#ifdef __cplusplus
}
#endif

#endif
