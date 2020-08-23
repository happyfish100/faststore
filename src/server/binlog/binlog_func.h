//binlog_func.h

#ifndef _BINLOG_FUNC_H_
#define _BINLOG_FUNC_H_

#include "binlog_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int binlog_buffer_init_ex(ServerBinlogBuffer *buffer, const int size);

int binlog_unpack_ts_and_dv(const string_t *line, time_t *timestamp,
        uint64_t *data_version, char *error_info);

static inline int binlog_buffer_init(ServerBinlogBuffer *buffer)
{
    const int size = BINLOG_BUFFER_SIZE;
    return binlog_buffer_init_ex(buffer, size);
}

static inline void binlog_buffer_destroy(ServerBinlogBuffer *buffer)
{
    if (buffer->buff != NULL) {
        free(buffer->buff);
        buffer->current = buffer->end = buffer->buff = NULL;
        buffer->size = 0;
    }
}

#ifdef __cplusplus
}
#endif

#endif
