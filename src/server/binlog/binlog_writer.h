//binlog_writer.h

#ifndef _BINLOG_WRITER_H_
#define _BINLOG_WRITER_H_

#include "fastcommon/fc_queue.h"
#include "binlog_types.h"

#define FS_BINLOG_WRITER_TYPE_ORDER_BY_NONE    0
#define FS_BINLOG_WRITER_TYPE_ORDER_BY_VERSION 1

typedef struct binlog_writer_buffer {
    int64_t version;
    BufferInfo bf;
    struct binlog_writer_buffer *next;
} BinlogWriterBuffer;

typedef struct binlog_writer_buffer_ring {
    BinlogWriterBuffer **entries;
    BinlogWriterBuffer **start; //for consumer
    BinlogWriterBuffer **end;   //for producer
    int count;
    int max_count;
    int size;
} BinlogWriterBufferRing;

typedef struct {
    const char *subdir_name;
    int order_by;
    int binlog_index;
    int binlog_compress_index;
    int fd;
    int64_t file_size;
    char *filename;
    struct {
        BinlogWriterBufferRing ring;
        int64_t next;
    } version_ctx;
    ServerBinlogBuffer binlog_buffer;
    struct fast_mblock_man mblock;
    struct fc_queue queue;
    volatile bool thread_running;
} BinlogWriterContext;

#ifdef __cplusplus
extern "C" {
#endif

int binlog_writer_init_ex(BinlogWriterContext *writer, const int order_by,
        const char *subdir_name, const int max_record_size);

static inline int binlog_writer_init(BinlogWriterContext *writer,
        const char *subdir_name, const int max_record_size)
{
    return binlog_writer_init_ex(writer, FS_BINLOG_WRITER_TYPE_ORDER_BY_NONE,
            subdir_name, max_record_size);
}

int binlog_writer_init_by_version(BinlogWriterContext *writer,
        const char *subdir_name, const int max_record_size,
        const int64_t next_version, const int ring_size);

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
