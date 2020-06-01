//binlog_writer.h

#ifndef _BINLOG_WRITER_H_
#define _BINLOG_WRITER_H_

#include "fastcommon/fc_queue.h"
#include "binlog_types.h"

typedef struct binlog_writer_buffer {
    BufferInfo bf;
    struct binlog_writer_buffer *next;
} BinlogWriterBuffer;

typedef struct {
    char *filepath;
    char *filename;
    int binlog_index;
    int binlog_compress_index;
    int file_size;
    int fd;
    ServerBinlogBuffer binlog_buffer;
    struct fast_mblock_man mblock;
    struct fc_queue queue;
    volatile bool thread_running;
} BinlogWriterContext;

#ifdef __cplusplus
extern "C" {
#endif

int binlog_writer_init(BinlogWriterContext *writer,
        const char *filepath, const int max_record_size);

void binlog_writer_finish(BinlogWriterContext *writer);

int binlog_get_current_write_index(BinlogWriterContext *writer);
void binlog_get_current_write_position(BinlogWriterContext *writer,
        FSBinlogFilePosition *position);

static inline BinlogWriterBuffer *binlog_writer_alloc_buffer(
        BinlogWriterContext *writer)
{
    return (BinlogWriterBuffer *)fast_mblock_alloc_object(&writer->mblock);
}

#define push_to_binlog_write_queue(writer, buffer) \
    fc_queue_push(&(writer)->queue, buffer)

#ifdef __cplusplus
}
#endif

#endif
