#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <pthread.h>
#include "fastcommon/logger.h"
#include "fastcommon/sockopt.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/pthread_func.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "binlog_func.h"
#include "binlog_writer.h"

#define BINLOG_FILE_MAX_SIZE   (1024 * 1024 * 1024)
#define BINLOG_INDEX_FILENAME  BINLOG_FILE_PREFIX"_index.dat"

#define BINLOG_INDEX_ITEM_CURRENT_WRITE     "current_write"
#define BINLOG_INDEX_ITEM_CURRENT_COMPRESS  "current_compress"

#define GET_BINLOG_FILENAME(writer) \
    sprintf(writer->filename, "%s/%s"BINLOG_FILE_EXT_FMT,  \
            writer->filepath, BINLOG_FILE_PREFIX, writer->binlog_index)

static int write_to_binlog_index_file(BinlogWriterContext *writer)
{
    char full_filename[PATH_MAX];
    char buff[256];
    int result;
    int len;

    snprintf(full_filename, sizeof(full_filename),
            "%s/%s", writer->filepath, BINLOG_INDEX_FILENAME);

    len = sprintf(buff, "%s=%d\n"
            "%s=%d\n",
            BINLOG_INDEX_ITEM_CURRENT_WRITE,
            writer->binlog_index,
            BINLOG_INDEX_ITEM_CURRENT_COMPRESS,
            writer->binlog_compress_index);
    if ((result=safeWriteToFile(full_filename, buff, len)) != 0) {
        logError("file: "__FILE__", line: %d, "
            "write to file \"%s\" fail, "
            "errno: %d, error info: %s",
            __LINE__, full_filename,
            result, STRERROR(result));
    }

    return result;
}

static int get_binlog_index_from_file(BinlogWriterContext *writer)
{
    char full_filename[PATH_MAX];
    IniContext ini_context;
    int result;

    snprintf(full_filename, sizeof(full_filename),
            "%s/%s", writer->filepath, BINLOG_INDEX_FILENAME);
    if (access(full_filename, F_OK) != 0) {
        if (errno == ENOENT) {
            writer->binlog_index = 0;
            return write_to_binlog_index_file(writer);
        }
    }

    if ((result=iniLoadFromFile(full_filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load from file \"%s\" fail, error code: %d",
                __LINE__, full_filename, result);
        return result;
    }

    writer->binlog_index = iniGetIntValue(NULL,
            BINLOG_INDEX_ITEM_CURRENT_WRITE, &ini_context, 0);
    writer->binlog_compress_index = iniGetIntValue(NULL,
            BINLOG_INDEX_ITEM_CURRENT_COMPRESS, &ini_context, 0);

    iniFreeContext(&ini_context);
    return 0;
}

static int open_writable_binlog(BinlogWriterContext *writer)
{
    if (writer->fd >= 0) {
        close(writer->fd);
    }

    GET_BINLOG_FILENAME(writer);
    writer->fd = open(writer->filename,
            O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (writer->fd < 0) {
        logError("file: "__FILE__", line: %d, "
                "open file \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, writer->filename,
                errno, STRERROR(errno));
        return errno != 0 ? errno : EACCES;
    }

    writer->file_size = lseek(writer->fd, 0, SEEK_END);
    if (writer->file_size < 0) {
        logError("file: "__FILE__", line: %d, "
                "lseek file \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, writer->filename,
                errno, STRERROR(errno));
        return errno != 0 ? errno : EIO;
    }

    return 0;
}

static int open_next_binlog(BinlogWriterContext *writer)
{
    GET_BINLOG_FILENAME(writer);
    if (access(writer->filename, F_OK) == 0) {
        char bak_filename[PATH_MAX];
        char date_str[32];

        sprintf(bak_filename, "%s.%s", writer->filename,
                formatDatetime(g_current_time, "%Y%m%d%H%M%S",
                    date_str, sizeof(date_str)));
        if (rename(writer->filename, bak_filename) == 0) { 
            logWarning("file: "__FILE__", line: %d, "
                    "binlog file %s exist, rename to %s",
                    __LINE__, writer->filename, bak_filename);
        } else {
            logError("file: "__FILE__", line: %d, "
                    "rename binlog %s to backup %s fail, "
                    "errno: %d, error info: %s",
                    __LINE__, writer->filename, bak_filename,
                    errno, STRERROR(errno));
            return errno != 0 ? errno : EPERM;
        }
    }

    return open_writable_binlog(writer);
}

static int do_write_to_file(BinlogWriterContext *writer,
        char *buff, const int len)
{
    int result;

    if (fc_safe_write(writer->fd, buff, len) != len) {
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "write to binlog file \"%s\" fail, fd: %d, "
                "errno: %d, error info: %s",
                __LINE__, writer->filename,
                writer->fd, result, STRERROR(result));
        return result;
    }

    if (fsync(writer->fd) != 0) {
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "fsync to binlog file \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, writer->filename,
                result, STRERROR(result));
        return result;
    }

    writer->file_size += len;
    return 0;
}

static int check_write_to_file(BinlogWriterContext *writer,
        char *buff, const int len)
{
    int result;

    if (writer->file_size + len <= BINLOG_FILE_MAX_SIZE) {
        return do_write_to_file(writer, buff, len);
    }

    writer->binlog_index++;  //binlog rotate
    if ((result=write_to_binlog_index_file(writer)) == 0) {
        result = open_next_binlog(writer);
    }

    if (result != 0) {
        logError("file: "__FILE__", line: %d, "
                "open binlog file \"%s\" fail",
                __LINE__, writer->filename);
        return result;
    }

    return do_write_to_file(writer, buff, len);
}

static int binlog_write_to_file(BinlogWriterContext *writer)
{
    int result;
    int len;

    len = BINLOG_BUFFER_LENGTH(writer->binlog_buffer);
    if (len == 0) {
        return 0;
    }

    result = check_write_to_file(writer, writer->binlog_buffer.buff, len);
    writer->binlog_buffer.end = writer->binlog_buffer.buff;
    return result;
}

int binlog_get_current_write_index(BinlogWriterContext *writer)
{
    if (writer->binlog_index < 0) {
        get_binlog_index_from_file(writer);
    }

    return writer->binlog_index;
}

void binlog_get_current_write_position(BinlogWriterContext *writer,
        FSBinlogFilePosition *position)
{
    position->index = writer->binlog_index;
    position->offset = writer->file_size;
}

static inline int deal_binlog_one_record(BinlogWriterContext *writer,
        BinlogWriterBuffer *wbuffer)
{
    int result;

    if (wbuffer->bf.length >= writer->binlog_buffer.size / 4) {
        if (BINLOG_BUFFER_LENGTH(writer->binlog_buffer) > 0) {
            if ((result=binlog_write_to_file(writer)) != 0) {
                return result;
            }
        }

        return check_write_to_file(writer,wbuffer->bf.buff,
                wbuffer->bf.length);
    }

    if (writer->file_size + BINLOG_BUFFER_LENGTH(writer->
                binlog_buffer) + wbuffer->bf.length > BINLOG_FILE_MAX_SIZE)
    {
        if ((result=binlog_write_to_file(writer)) != 0) {
            return result;
        }
    } else if (writer->binlog_buffer.size - BINLOG_BUFFER_LENGTH(
                writer->binlog_buffer) < wbuffer->bf.length)
    {
        if ((result=binlog_write_to_file(writer)) != 0) {
            return result;
        }
    }

    memcpy(writer->binlog_buffer.end,
            wbuffer->bf.buff, wbuffer->bf.length);
    writer->binlog_buffer.end += wbuffer->bf.length;
    return 0;
}

static int deal_binlog_records(BinlogWriterContext *writer,
        BinlogWriterBuffer *wb_head)
{
    int result;
    BinlogWriterBuffer *wbuffer;
    BinlogWriterBuffer *deleted;

    wbuffer = wb_head;
    do {
        if ((result=deal_binlog_one_record(writer, wbuffer)) != 0) {
            return result;
        }

        deleted = wbuffer;
        wbuffer = wbuffer->next;
        fast_mblock_free_object(&writer->mblock, deleted);
    } while (wbuffer != NULL);

    return binlog_write_to_file(writer);
}

void binlog_writer_finish(BinlogWriterContext *writer)
{
    BinlogWriterBuffer *wb_head;
    int count;

    if (writer->filename != NULL) {
        count = 0;
        while (writer->thread_running && ++count < 100) {
            usleep(100 * 1000);
        }
        
        if (writer->thread_running) {
            logWarning("file: "__FILE__", line: %d, "
                    "binlog write thread still running, "
                    "exit anyway!", __LINE__);
        }

        wb_head = (BinlogWriterBuffer *)fc_queue_try_pop_all(&writer->queue);
        if (wb_head != NULL) {
            deal_binlog_records(writer, wb_head);
        }
    }

    if (writer->fd >= 0) {
        close(writer->fd);
        writer->fd = -1;
    }
}

static void *binlog_writer_func(void *arg)
{
    BinlogWriterContext *writer;
    BinlogWriterBuffer *wb_head;

    writer = (BinlogWriterContext *)arg;
    writer->thread_running = true;
    while (SF_G_CONTINUE_FLAG) {
        wb_head = (BinlogWriterBuffer *)fc_queue_pop_all(&writer->queue);
        if (wb_head == NULL) {
            continue;
        }

        if (deal_binlog_records(writer, wb_head) != 0) {
            logCrit("file: "__FILE__", line: %d, "
                    "deal_binlog_records fail, program exit!", __LINE__);
            SF_G_CONTINUE_FLAG = false;
        }
    }

    writer->thread_running = false;
    return NULL;
}

static int binlog_wbuffer_alloc_init(void *element, void *args)
{
    BinlogWriterBuffer *wbuffer;

    wbuffer = (BinlogWriterBuffer *)element;
    wbuffer->bf.alloc_size = (long)args;
    wbuffer->bf.buff = (char *)(wbuffer + 1);
    return 0;
}

int binlog_writer_init(BinlogWriterContext *writer,
        const char *filepath, const int max_record_size)
{
    int result;
    int path_len;
    const int alloc_elements_once = 1024;
    pthread_t tid;

    if ((result=binlog_buffer_init(&writer->binlog_buffer)) != 0) {
        return result;
    }

    if ((result=fast_mblock_init_ex2(&writer->mblock, "binlog_wbuffer",
                    sizeof(BinlogWriterBuffer) + max_record_size,
                    alloc_elements_once, binlog_wbuffer_alloc_init,
                    (void *)((long)max_record_size), true,
                    NULL, NULL, NULL)) != 0)
    {
        return result;
    }

    if ((result=fc_queue_init(&writer->queue, (unsigned long)
                    (&((BinlogWriterBuffer *)NULL)->next))) != 0)
    {
        return result;
    }

    path_len = strlen(filepath);
    writer->filepath = (char *)malloc(path_len + 1);
    if (writer->filepath == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__,
                path_len + 1);
        return ENOMEM;
    }
    memcpy(writer->filepath, filepath, path_len + 1);

    writer->filename = (char *)malloc(path_len + 32);
    if (writer->filename == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__,
                path_len + 32);
        return ENOMEM;
    }

    if ((result=get_binlog_index_from_file(writer)) != 0) {
        return result;
    }

    if ((result=open_writable_binlog(writer)) != 0) {
        return result;
    }

    return fc_create_thread(&tid, binlog_writer_func, writer,
            SF_G_THREAD_STACK_SIZE);
}
