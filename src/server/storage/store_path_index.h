
#ifndef _STORE_PATH_INDEX_H
#define _STORE_PATH_INDEX_H

#include "../../common/fs_types.h"
#include "storage_config.h"

#ifdef __cplusplus
extern "C" {
#endif

    int store_path_index_init();

    void store_path_index_destroy();

    int store_path_index_count();

    int store_path_index_max();

    int store_path_index_get(const char *path);

    int store_path_index_add(const char *path, const char *mark, int *index);

    int store_path_index_save();

#ifdef __cplusplus
}
#endif

#endif
