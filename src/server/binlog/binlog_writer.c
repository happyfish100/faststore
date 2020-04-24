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

typedef struct {
    char filename[PATH_MAX];
    int binlog_index;
    int binlog_compress_index;
    int file_size;
    int fd;
    ServerBinlogBuffer binlog_buffer;
    struct common_blocked_queue queue;
} BinlogWriterContext;

static BinlogWriterContext writer_context = {{'\0'}, -1, 0, 0, -1};
static volatile bool write_thread_running = false;
struct common_blocked_queue *g_binlog_writer_queue = NULL;

static int write_to_binlog_index_file()
{
    char full_filename[PATH_MAX];
    char buff[256];
    int result;
    int len;

    snprintf(full_filename, sizeof(full_filename),
            "%s/%s", DATA_PATH_STR, BINLOG_INDEX_FILENAME);

    len = sprintf(buff, "%s=%d\n"
            "%s=%d\n",
            BINLOG_INDEX_ITEM_CURRENT_WRITE,
            writer_context.binlog_index,
            BINLOG_INDEX_ITEM_CURRENT_COMPRESS,
            writer_context.binlog_compress_index);
    if ((result=safeWriteToFile(full_filename, buff, len)) != 0) {
        logError("file: "__FILE__", line: %d, "
            "write to file \"%s\" fail, "
            "errno: %d, error info: %s",
            __LINE__, full_filename,
            result, STRERROR(result));
    }

    return result;
}

static int get_binlog_index_from_file()
{
    char full_filename[PATH_MAX];
    IniContext ini_context;
    int result;

    snprintf(full_filename, sizeof(full_filename),
            "%s/%s", DATA_PATH_STR, BINLOG_INDEX_FILENAME);
    if (access(full_filename, F_OK) != 0) {
        if (errno == ENOENT) {
            writer_context.binlog_index = 0;
            return write_to_binlog_index_file();
        }
    }

    if ((result=iniLoadFromFile(full_filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load from file \"%s\" fail, error code: %d",
                __LINE__, full_filename, result);
        return result;
    }

    writer_context.binlog_index = iniGetIntValue(NULL,
            BINLOG_INDEX_ITEM_CURRENT_WRITE, &ini_context, 0);
    writer_context.binlog_compress_index = iniGetIntValue(NULL,
            BINLOG_INDEX_ITEM_CURRENT_COMPRESS, &ini_context, 0);

    iniFreeContext(&ini_context);
    return 0;
}

static int open_writable_binlog()
{
    if (writer_context.fd >= 0) {
        close(writer_context.fd);
    }

    GET_BINLOG_FILENAME(writer_context.filename, sizeof(writer_context.
                filename), writer_context.binlog_index);
    writer_context.fd = open(writer_context.filename,
            O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (writer_context.fd < 0) {
        logError("file: "__FILE__", line: %d, "
                "open file \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, writer_context.filename,
                errno, STRERROR(errno));
        return errno != 0 ? errno : EACCES;
    }

    writer_context.file_size = lseek(writer_context.fd, 0, SEEK_END);
    if (writer_context.file_size < 0) {
        logError("file: "__FILE__", line: %d, "
                "lseek file \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, writer_context.filename,
                errno, STRERROR(errno));
        return errno != 0 ? errno : EIO;
    }

    return 0;
}

static int open_next_binlog()
{
    GET_BINLOG_FILENAME(writer_context.filename, sizeof(writer_context.
                filename), writer_context.binlog_index);
    if (access(writer_context.filename, F_OK) == 0) {
        char bak_filename[PATH_MAX];
        char date_str[32];

        sprintf(bak_filename, "%s.%s", writer_context.filename,
                formatDatetime(g_current_time, "%Y%m%d%H%M%S",
                    date_str, sizeof(date_str)));
        if (rename(writer_context.filename, bak_filename) == 0) { 
            logWarning("file: "__FILE__", line: %d, "
                    "binlog file %s exist, rename to %s",
                    __LINE__, writer_context.filename, bak_filename);
        } else {
            logError("file: "__FILE__", line: %d, "
                    "rename binlog %s to backup %s fail, "
                    "errno: %d, error info: %s",
                    __LINE__, writer_context.filename, bak_filename,
                    errno, STRERROR(errno));
            return errno != 0 ? errno : EPERM;
        }
    }

    return open_writable_binlog();
}

static int do_write_to_file(char *buff, const int len)
{
    int result;

    if (fc_safe_write(writer_context.fd, buff, len) != len) {
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "write to binlog file \"%s\" fail, fd: %d, "
                "errno: %d, error info: %s",
                __LINE__, writer_context.filename,
                writer_context.fd, result, STRERROR(result));
        return result;
    } else if (fsync(writer_context.fd) != 0) {
        result = errno != 0 ? errno : EIO;
        logError("file: "__FILE__", line: %d, "
                "fsync to binlog file \"%s\" fail, "
                "errno: %d, error info: %s",
                __LINE__, writer_context.filename,
                result, STRERROR(result));
        return result;
    }

    writer_context.file_size += len;
    return 0;
}

static int check_write_to_file(char *buff, const int len)
{
    int result;

    if (writer_context.file_size + len <= BINLOG_FILE_MAX_SIZE) {
        return do_write_to_file(buff, len);
    }

    writer_context.binlog_index++;  //binlog rotate
    if ((result=write_to_binlog_index_file()) == 0) {
        result = open_next_binlog();
    }

    if (result != 0) {
        logError("file: "__FILE__", line: %d, "
                "open binlog file \"%s\" fail",
                __LINE__, writer_context.filename);
        return result;
    }

    return do_write_to_file(buff, len);
}

static int binlog_write_to_file()
{
    int result;
    int len;

    len = BINLOG_BUFFER_LENGTH(writer_context.binlog_buffer);
    if (len == 0) {
        return 0;
    }

    result = check_write_to_file(writer_context.binlog_buffer.buff, len);
    writer_context.binlog_buffer.end = writer_context.binlog_buffer.buff;
    return result;
}

int binlog_get_current_write_index()
{
    if (writer_context.binlog_index < 0) {
        get_binlog_index_from_file();
    }

    return writer_context.binlog_index;
}

void binlog_get_current_write_position(FSBinlogFilePosition *position)
{
    position->index = writer_context.binlog_index;
    position->offset = writer_context.file_size;
}

static inline int deal_binlog_one_record(ServerBinlogRecordBuffer *rb)
{
    int result;

    if (rb->buffer.length >= writer_context.binlog_buffer.size / 4) {
        if (BINLOG_BUFFER_LENGTH(writer_context.binlog_buffer) > 0) {
            if ((result=binlog_write_to_file()) != 0) {
                return result;
            }
        }

        return check_write_to_file(rb->buffer.data, rb->buffer.length);
    }

    if (writer_context.file_size + BINLOG_BUFFER_LENGTH(writer_context.
                binlog_buffer) + rb->buffer.length > BINLOG_FILE_MAX_SIZE)
    {
        if ((result=binlog_write_to_file()) != 0) {
            return result;
        }
    } else if (writer_context.binlog_buffer.size - BINLOG_BUFFER_LENGTH(
                writer_context.binlog_buffer) < rb->buffer.length)
    {
        if ((result=binlog_write_to_file()) != 0) {
            return result;
        }
    }

    memcpy(writer_context.binlog_buffer.end,
            rb->buffer.data, rb->buffer.length);
    writer_context.binlog_buffer.end += rb->buffer.length;
    return 0;
}

static int deal_binlog_records(struct common_blocked_node *node)
{
    ServerBinlogRecordBuffer *rb;
    int result;
    static int max_wait_count = 0;
    int wait_count;

    do {
        rb = (ServerBinlogRecordBuffer *)node->data;

        wait_count = 0;
        while ((rb->data_version > DATA_CURRENT_VERSION) &&
                (++wait_count < 100))
        {
            usleep(1000);
        }

        if (wait_count > 0) {
            if (max_wait_count < wait_count) {
                max_wait_count = wait_count;
                logWarning("file: "__FILE__", line: %d, "
                        "curent write data version: %"PRId64" "
                        "reach max wait count: %d", __LINE__,
                        rb->data_version, wait_count);
            }
            if (wait_count == 100) {
                logError("file: "__FILE__", line: %d, "
                        "wait curent write data version: %"PRId64" "
                        "timeout", __LINE__, rb->data_version);
            }
        }

        if ((result=deal_binlog_one_record(rb)) != 0) {
            return result;
        }

        rb->release_func(rb);
        node = node->next;
    } while (node != NULL);

    return binlog_write_to_file();
}

void binlog_writer_finish()
{
    struct common_blocked_node *node;
    int count;

    if (g_binlog_writer_queue != NULL) {
        count = 0;
        while (write_thread_running && ++count < 100) {
            usleep(100 * 1000);
        }
        
        if (write_thread_running) {
            logWarning("file: "__FILE__", line: %d, "
                    "binlog write thread still running, "
                    "exit anyway!", __LINE__);
        }

        node = common_blocked_queue_try_pop_all_nodes(g_binlog_writer_queue);
        if (node != NULL) {
            deal_binlog_records(node);
            common_blocked_queue_free_all_nodes(g_binlog_writer_queue, node);
        }
        g_binlog_writer_queue = NULL;
    }

    if (writer_context.fd >= 0) {
        close(writer_context.fd);
        writer_context.fd = -1;
    }
}

static void *binlog_writer_func(void *arg)
{
    struct common_blocked_node *node;

    write_thread_running = true;
    g_binlog_writer_queue = &writer_context.queue;
    while (SF_G_CONTINUE_FLAG) {
        node = common_blocked_queue_pop_all_nodes(g_binlog_writer_queue);
        if (node == NULL) {
            continue;
        }

        if (deal_binlog_records(node) != 0) {
            logCrit("file: "__FILE__", line: %d, "
                    "deal_binlog_records fail, program exit!", __LINE__);
            SF_G_CONTINUE_FLAG = false;
        }
        common_blocked_queue_free_all_nodes(g_binlog_writer_queue, node);
    }

    write_thread_running = false;
    return NULL;
}

int binlog_writer_init()
{
    int result;
    pthread_t tid;

    if ((result=binlog_buffer_init(&writer_context.binlog_buffer)) != 0) {
        return result;
    }

    if ((result=common_blocked_queue_init_ex(&writer_context.queue, 10240)) != 0) {
        return result;
    }
    if ((result=get_binlog_index_from_file()) != 0) {
        return result;
    }

    if ((result=open_writable_binlog()) != 0) {
        return result;
    }

    return fc_create_thread(&tid, binlog_writer_func, NULL,
            SF_G_THREAD_STACK_SIZE);
}
