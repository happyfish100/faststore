
#ifndef _TRUNK_BINLOG_H
#define _TRUNK_BINLOG_H

#include "../../common/fs_types.h"
#include "storage_config.h"

#ifdef __cplusplus
extern "C" {
#endif

    int trunk_binlog_init();
    void trunk_binlog_destroy();

    int trunk_binlog_get_current_write_index();

    int trunk_binlog_write(const char op_type, const int path_index,
            const FSTrunkIdInfo *id_info, const int64_t file_size);

#ifdef __cplusplus
}
#endif

#endif
