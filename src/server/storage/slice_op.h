
#ifndef _SLICE_OP_H
#define _SLICE_OP_H

#include "../../common/fs_types.h"
#include "storage_config.h"

#ifdef __cplusplus
extern "C" {
#endif

    int slice_write(const FSBlockKey *bkey, const int slice_offset,
            string_t *data);

#ifdef __cplusplus
}
#endif

#endif
