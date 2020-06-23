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
    sprintf(writer->file.name, "%s/%s/%s"BINLOG_FILE_EXT_FMT,  \
            DATA_PATH_STR, writer->cfg.subdir_name, BINLOG_FILE_PREFIX, \
            writer->binlog.index)

static int write_to_binlog_index_file(BinlogWriterInfo *writer)
{
    char full_filename[PATH_MAX];
    char buff[256];
    int result;
    int len;

    snprintf(full_filename, sizeof(full_filename), "%s/%s/%s",
            DATA_PATH_STR, writer->cfg.subdir_name, BINLOG_INDEX_FILENAME);

    len = sprintf(buff, "%s=%d\n"
            "%s=%d\n",
            BINLOG_INDEX_ITEM_CURRENT_WRITE,
            writer->binlog.index,
            BINLOG_INDEX_ITEM_CURRENT_COMPRESS,
            writer->binlog.compress_index);
    if ((result=safeWriteToFile(full_filename, buff, len)) != 0) {
        logError("file: "__FILE__", line: %d, "
            "write to file \"%s\" fail, "
            "errno: %d, error info: %s",
            __LINE__, full_filename,
            result, STRERROR(result));
    }

    return result;
}

static int get_binlog_index_from_file(BinlogWriterInfo *writer)
{
    char full_filename[PATH_MAX];
    IniContext ini_context;
    int result;

    snprintf(full_filename, sizeof(full_filename), "%s/%s/%s",
            DATA_PATH_STR, writer->cfg.subdir_name, BINLOG_INDEX_FILENAME);
    if (access(full_filename, F_OK) != 0) {
        if (errno == ENOENT) {
            writer->binlog.index = 0;
            return write_to_binlog_index_file(writer);
        }
    }

    if ((result=iniLoadFromFile(full_filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load from file \"%s\" fail, error code: %d",
                __LINE__, full_filename, result);
        return result;
    }

    writer->binlog.index = iniGetIntValue(NULL,
            BINLOG_INDEX_ITEM_CURRENT_WRITE, &ini_context, 0);
    writer->binlog.compress_index = iniGetIntValue(NULL,
            BINLOG_INDEX_ITEM_CURRENT_COMPRESS, &ini_context, 0);

    iniFreeContext(&ini_context);
    return 0;
}

static int open_writable_binlog(BinlogWriterInfo *writer)
{
    if (writer->file.fd >= 0) {
        close(writer->file.fd);
    }

    GET_BINLOG_FILENAME(writer);
    writer->file.fd = open(writer->file.name,
            O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (writer->file.fd < 0) {
        logError("file: "__FILE__", line: %d, "
                "open file \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, writer->file.name,
                errno, STRERROR(errno));
        return errno != 0 ? errno : EACCES;
    }

    writer->file.size = lseek(writer->file.fd, 0, SEEK_END);
    if (writer->file.size < 0) {
        logError("file: "__FILE__", line: %d, "
                "lseek file \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, writer->file.name,
                errno, STRERROR(errno));
        return errno != 0 ? errno : EIO;
    }

    return 0;
}

static int open_next_binlog(BinlogWriterInfo *writer)
{
    GET_BINLOG_FILENAME(writer);
    if (access(writer->file.name, F_OK) == 0) {
        char bak_filename[PATH_MAX];
        char date_str[32];

        sprintf(bak_filename, "%s.%s", writer->file.name,
                formatDatetime(g_current_time, "%Y%m%d%H%M%S",
                    date_str, sizeof(date_str)));
        if (rename(writer->file.name, bak_filename) == 0) {
            logWarning("file: "__FILE__", line: %d, "
                    "binlog file %s exist, rename to %s",
                    __LINE__, writer->file.name, bak_filename);
        } else {
            logError("file: "__FILE__", line: %d, "
                    "rename binlog %s to backup %s fail, "
                    "errno: %d, error info: %s",
                    __LINE__, writer->file.name, bak_filename,
                    errno, STRERROR(errno));
            return errno != 0 ? errno : EPERM;
        }
    }

    return open_writable_binlog(writer);
}

static int do_write_to_file(BinlogWriterInfo *writer,
        char *buff, const int len)
{
    int result;

    if (fc_safe_write(writer->file.fd, buff, len) != len) {
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "write to binlog file \"%s\" fail, fd: %d, "
                "errno: %d, error info: %s",
                __LINE__, writer->file.name,
                writer->file.fd, result, STRERROR(result));
        return result;
    }

    if (fsync(writer->file.fd) != 0) {
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "fsync to binlog file \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, writer->file.name,
                result, STRERROR(result));
        return result;
    }

    writer->file.size += len;
    return 0;
}

static int check_write_to_file(BinlogWriterInfo *writer,
        char *buff, const int len)
{
    int result;

    if (writer->file.size + len <= BINLOG_FILE_MAX_SIZE) {
        return do_write_to_file(writer, buff, len);
    }

    writer->binlog.index++;  //binlog rotate
    if ((result=write_to_binlog_index_file(writer)) == 0) {
        result = open_next_binlog(writer);
    }

    if (result != 0) {
        logError("file: "__FILE__", line: %d, "
                "open binlog file \"%s\" fail",
                __LINE__, writer->file.name);
        return result;
    }

    return do_write_to_file(writer, buff, len);
}

static int binlog_write_to_file(BinlogWriterInfo *writer)
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

int binlog_get_current_write_index(BinlogWriterInfo *writer)
{
    if (writer->binlog.index < 0) {
        get_binlog_index_from_file(writer);
    }

    return writer->binlog.index;
}

void binlog_get_current_write_position(BinlogWriterInfo *writer,
        FSBinlogFilePosition *position)
{
    position->index = writer->binlog.index;
    position->offset = writer->file.size;
}

static inline int deal_binlog_one_record(BinlogWriterBuffer *wb)
{
    int result;

    if (wb->bf.length >= wb->writer->binlog_buffer.size / 4) {
        if (BINLOG_BUFFER_LENGTH(wb->writer->binlog_buffer) > 0) {
            if ((result=binlog_write_to_file(wb->writer)) != 0) {
                return result;
            }
        }

        return check_write_to_file(wb->writer,
                wb->bf.buff, wb->bf.length);
    }

    if (wb->writer->file.size + BINLOG_BUFFER_LENGTH(wb->writer->
                binlog_buffer) + wb->bf.length > BINLOG_FILE_MAX_SIZE)
    {
        if ((result=binlog_write_to_file(wb->writer)) != 0) {
            return result;
        }
    } else if (wb->writer->binlog_buffer.size - BINLOG_BUFFER_LENGTH(
                wb->writer->binlog_buffer) < wb->bf.length)
    {
        if ((result=binlog_write_to_file(wb->writer)) != 0) {
            return result;
        }
    }

    memcpy(wb->writer->binlog_buffer.end,
            wb->bf.buff, wb->bf.length);
    wb->writer->binlog_buffer.end += wb->bf.length;
    return 0;
}

static void repush_to_queue(BinlogWriterThread *thread, BinlogWriterBuffer *wb)
{
    BinlogWriterBuffer *previous;
    BinlogWriterBuffer *current;

    PTHREAD_MUTEX_LOCK(&thread->queue.lock);
    if (thread->queue.head == NULL) {
        wb->next = NULL;
        thread->queue.head = thread->queue.tail = wb;
    } else if (wb->version <= ((BinlogWriterBuffer *)
                thread->queue.head)->version)
    {
        wb->next = thread->queue.head;
        thread->queue.head = wb;
    } else if (wb->version > ((BinlogWriterBuffer *)
                thread->queue.tail)->version)
    {
        wb->next = NULL;
        ((BinlogWriterBuffer *)thread->queue.tail)->next = wb;
        thread->queue.tail = wb;
    } else {
        previous = thread->queue.head;
        current = ((BinlogWriterBuffer *)thread->queue.head)->next;
        while (current != NULL && wb->version > current->version) {
            previous = current;
            current = current->next;
        }

        wb->next = previous->next;
        previous->next = wb;
    }
    PTHREAD_MUTEX_UNLOCK(&thread->queue.lock);
}

#define DEAL_CURRENT_VERSION_WBUFFER(writer, wb) \
    do { \
        deal_binlog_one_record(wb);  \
        fast_mblock_free_object(&writer->thread->mblock, wb);  \
        ++writer->version_ctx.next;  \
    } while (0)

static void deal_record_by_version(BinlogWriterBuffer *wb)
{
    BinlogWriterInfo *writer;
    BinlogWriterBuffer **current;
    int64_t distance;
    int index;
    bool expand;

    writer = wb->writer;
    distance = wb->version - writer->version_ctx.next;
    if (distance >= (writer->version_ctx.ring.size - 1)) {
        logWarning("file: "__FILE__", line: %d, "
                "current version: %"PRId64" is too large, "
                "exceeds %"PRId64" + %d", __LINE__,
                wb->version, writer->version_ctx.next,
                writer->version_ctx.ring.size - 1);
        repush_to_queue(writer->thread, wb);
        return;
    }

    current = writer->version_ctx.ring.entries + wb->version %
        writer->version_ctx.ring.size;
    if (current == writer->version_ctx.ring.start) {
        DEAL_CURRENT_VERSION_WBUFFER(writer, wb);

        index = writer->version_ctx.ring.start - writer->version_ctx.ring.entries;
        if (writer->version_ctx.ring.start == writer->version_ctx.ring.end) {
            writer->version_ctx.ring.start = writer->version_ctx.ring.end =
                writer->version_ctx.ring.entries +
                (++index) % writer->version_ctx.ring.size;
            return;
        }

        writer->version_ctx.ring.start = writer->version_ctx.ring.entries +
            (++index) % writer->version_ctx.ring.size;
        while (writer->version_ctx.ring.start != writer->version_ctx.ring.end &&
                *(writer->version_ctx.ring.start) != NULL)
        {
            DEAL_CURRENT_VERSION_WBUFFER(writer, *(writer->version_ctx.ring.start));
            *(writer->version_ctx.ring.start) = NULL;

            writer->version_ctx.ring.start = writer->version_ctx.ring.entries +
                (++index) % writer->version_ctx.ring.size;
            writer->version_ctx.ring.count--;
        }
        return;
    }

    *current = wb;
    writer->version_ctx.ring.count++;

    if (writer->version_ctx.ring.count > writer->version_ctx.ring.max_count) {
        writer->version_ctx.ring.max_count = writer->version_ctx.ring.count;
        logInfo("%s max ring.count ==== %d", writer->cfg.subdir_name,
                writer->version_ctx.ring.count);
    }

    if (writer->version_ctx.ring.start == writer->version_ctx.ring.end) { //empty
        expand = true;
    } else if (writer->version_ctx.ring.end > writer->version_ctx.ring.start) {
        expand = !(current > writer->version_ctx.ring.start &&
                current < writer->version_ctx.ring.end);
    } else {
        expand = (current >= writer->version_ctx.ring.end &&
                current < writer->version_ctx.ring.start);
    }

    if (expand) {
        writer->version_ctx.ring.end = writer->version_ctx.ring.entries +
            (wb->version + 1) % writer->version_ctx.ring.size;
    }
}

static inline void add_to_flush_writer_array(BinlogWriterThread *thread,
        BinlogWriterInfo *writer)
{
    struct binlog_writer_info **entry;
    struct binlog_writer_info **end;

    if (thread->flush_writers.count == 0) {
        thread->flush_writers.entries[thread->flush_writers.count++] = writer;
        return;
    }

    if (thread->flush_writers.entries[0] == writer) {
        return;
    }

    end = thread->flush_writers.entries + thread->flush_writers.count;
    for (entry=thread->flush_writers.entries+1; entry<end; entry++) {
        if (*entry == writer) {
            return;
        }
    }

    thread->flush_writers.entries[thread->flush_writers.count++] = writer;
}

static inline int flush_writer_files(BinlogWriterThread *thread)
{
    struct binlog_writer_info **entry;
    struct binlog_writer_info **end;
    int result;

    if (thread->flush_writers.count == 1) {
        return binlog_write_to_file(thread->flush_writers.entries[0]);
    }

    end = thread->flush_writers.entries + thread->flush_writers.count;
    for (entry=thread->flush_writers.entries; entry<end; entry++) {
        if ((result=binlog_write_to_file(*entry)) != 0) {
            return result;
        }
    }

    return 0;
}

static int deal_binlog_records(BinlogWriterThread *thread,
        BinlogWriterBuffer *wb_head)
{
    int result;
    BinlogWriterBuffer *wbuffer;
    BinlogWriterBuffer *current;

    thread->flush_writers.count = 0;
    wbuffer = wb_head;

    if (thread->order_by == FS_BINLOG_WRITER_TYPE_ORDER_BY_VERSION) {
        do {
            current = wbuffer;
            wbuffer = wbuffer->next;

            add_to_flush_writer_array(thread, current->writer);
            deal_record_by_version(current);
        } while (wbuffer != NULL);
    } else {
        do {
            if ((result=deal_binlog_one_record(wbuffer)) != 0) {
                return result;
            }

            current = wbuffer;
            wbuffer = wbuffer->next;

            add_to_flush_writer_array(thread, current->writer);
            fast_mblock_free_object(&current->writer->
                    thread->mblock, current);
        } while (wbuffer != NULL);
    }

    return flush_writer_files(thread);
}

void binlog_writer_finish(BinlogWriterInfo *writer)
{
    BinlogWriterBuffer *wb_head;
    int count;

    if (writer->file.name != NULL) {
        fc_queue_terminate(&writer->thread->queue);

        count = 0;
        while (writer->thread->running && ++count < 100) {
            usleep(100 * 1000);
        }
        
        if (writer->thread->running) {
            logWarning("file: "__FILE__", line: %d, "
                    "%s binlog write thread still running, "
                    "exit anyway!", __LINE__, writer->cfg.subdir_name);
        }

        wb_head = (BinlogWriterBuffer *)fc_queue_try_pop_all(
                &writer->thread->queue);
        if (wb_head != NULL) {
            deal_binlog_records(writer->thread, wb_head);
        }

        free(writer->file.name);
        writer->file.name = NULL;
    }

    if (writer->file.fd >= 0) {
        close(writer->file.fd);
        writer->file.fd = -1;
    }
}

static void *binlog_writer_func(void *arg)
{
    BinlogWriterThread *thread;
    BinlogWriterBuffer *wb_head;

    thread = (BinlogWriterThread *)arg;
    thread->running = true;
    while (SF_G_CONTINUE_FLAG) {
        wb_head = (BinlogWriterBuffer *)fc_queue_pop_all(&thread->queue);
        if (wb_head == NULL) {
            continue;
        }

        if (deal_binlog_records(thread, wb_head) != 0) {
            logCrit("file: "__FILE__", line: %d, "
                    "deal_binlog_records fail, program exit!", __LINE__);
            SF_G_CONTINUE_FLAG = false;
        }
    }

    thread->running = false;
    return NULL;
}

static int binlog_wbuffer_alloc_init(void *element, void *args)
{
    BinlogWriterBuffer *wbuffer;
    BinlogWriterInfo *writer;

    wbuffer = (BinlogWriterBuffer *)element;
    writer = (BinlogWriterInfo *)args;
    wbuffer->bf.alloc_size = writer->cfg.max_record_size;
    wbuffer->bf.buff = (char *)(wbuffer + 1);
    wbuffer->writer = writer;
    return 0;
}

int binlog_writer_init_normal(BinlogWriterInfo *writer,
        const char *subdir_name)
{
    int result;
    int path_len;
    bool create;
    char filepath[PATH_MAX];

    if ((result=binlog_buffer_init(&writer->binlog_buffer)) != 0) {
        return result;
    }

    path_len = snprintf(filepath, sizeof(filepath), "%s/%s",
            DATA_PATH_STR, subdir_name);
    if ((result=fc_check_mkdir_ex(filepath, 0775, &create)) != 0) {
        return result;
    }
    if (create) {
        SF_CHOWN_RETURN_ON_ERROR(filepath, geteuid(), getegid());
    }

    writer->file.fd = -1;
    writer->cfg.subdir_name = subdir_name;
    writer->file.name = (char *)malloc(path_len + 32);
    if (writer->file.name == NULL) {
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

    return 0;
}

int binlog_writer_init_by_version(BinlogWriterInfo *writer,
        const char *subdir_name, const int64_t next_version,
        const int ring_size)
{
    int bytes;

    writer->version_ctx.ring.size = ring_size;
    bytes = sizeof(BinlogWriterBuffer *) * writer->version_ctx.ring.size;
    writer->version_ctx.ring.entries = (BinlogWriterBuffer **)malloc(bytes);
    if (writer->version_ctx.ring.entries == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(writer->version_ctx.ring.entries, 0, bytes);

    writer->version_ctx.next = next_version;
    writer->version_ctx.ring.count = 0;
    writer->version_ctx.ring.max_count = 0;
    writer->version_ctx.ring.start = writer->version_ctx.ring.end =
        writer->version_ctx.ring.entries;
    return binlog_writer_init_normal(writer, subdir_name);
}

int binlog_writer_init_thread_ex(BinlogWriterThread *thread,
        BinlogWriterInfo *writer, const int order_by,
        const int max_record_size, const int writer_count)
{
    const int alloc_elements_once = 1024;
    pthread_t tid;
    int result;
    int bytes;

    thread->order_by = order_by;
    writer->cfg.max_record_size = max_record_size;
    writer->thread = thread;
    if ((result=fast_mblock_init_ex2(&thread->mblock, "binlog_wbuffer",
                    sizeof(BinlogWriterBuffer) + max_record_size,
                    alloc_elements_once, binlog_wbuffer_alloc_init,
                    writer, true, NULL, NULL, NULL)) != 0)
    {
        return result;
    }

    if ((result=fc_queue_init(&thread->queue, (unsigned long)
                    (&((BinlogWriterBuffer *)NULL)->next))) != 0)
    {
        return result;
    }

    bytes = sizeof(struct binlog_writer_info *) * writer_count;
    thread->flush_writers.entries = (struct binlog_writer_info **)malloc(bytes);
    if (thread->flush_writers.entries == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    thread->flush_writers.alloc = writer_count;
    thread->flush_writers.count = 0;

    return fc_create_thread(&tid, binlog_writer_func, thread,
            SF_G_THREAD_STACK_SIZE);
}
