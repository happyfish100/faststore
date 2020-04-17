
#ifndef _TRUNK_ID_INFO_H
#define _TRUNK_ID_INFO_H

#include "../../common/fs_types.h"
#include "storage_config.h"

#ifdef __cplusplus
extern "C" {
#endif

    int trunk_id_info_init();

    int trunk_id_info_add(const int path_index, const FSTrunkIdInfo *id_info);

    int trunk_id_info_delete(const int path_index, const FSTrunkIdInfo *id_info);

    int trunk_id_info_generate(const int path_index, FSTrunkIdInfo *id_info);

#ifdef __cplusplus
}
#endif

#endif
