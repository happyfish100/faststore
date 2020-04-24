//binlog_writer.h

#ifndef _BINLOG_WRITER_H_
#define _BINLOG_WRITER_H_

#include "binlog_types.h"

#ifdef __cplusplus
extern "C" {
#endif

extern struct common_blocked_queue *g_binlog_writer_queue;

int binlog_writer_init();
void binlog_writer_finish();

int binlog_get_current_write_index();
void binlog_get_current_write_position(FSBinlogFilePosition *position);

#define push_to_binlog_write_queue(rbuffer)  \
    common_blocked_queue_push(g_binlog_writer_queue, rbuffer)

#ifdef __cplusplus
}
#endif

#endif
