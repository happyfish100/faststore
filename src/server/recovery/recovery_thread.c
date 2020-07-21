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
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/common_blocked_queue.h"
#include "fastcommon/thread_pool.h"
#include "sf/sf_global.h"
#include "sf/sf_service.h"
#include "../server_global.h"
#include "../server_group_info.h"
#include "recovery_thread.h"

typedef struct {
    pthread_t tid;
    struct common_blocked_queue queue;
    FCThreadPool tpool;
} RecoveryThreadContext;

static RecoveryThreadContext recovery_thread_ctx;

static void *recovery_thread_entrance(void *arg)
{
    FSClusterDataServerInfo *ds;

    while (SF_G_CONTINUE_FLAG) {
        ds = (FSClusterDataServerInfo *)common_blocked_queue_pop(
                &recovery_thread_ctx.queue);
        if (ds != NULL) {
        }
    }

    return NULL;
}

int recovery_thread_init()
{
    const int alloc_elements_once = 256;
    const int limit = 2;
    const int max_idle_time = 60;
    const int min_idle_count = 0;
    int result;

    if ((result=common_blocked_queue_init_ex(
                    &recovery_thread_ctx.queue,
                    alloc_elements_once)) != 0)
    {
        return result;
    }

    if ((result=fc_thread_pool_init(&recovery_thread_ctx.tpool,
                    limit, SF_G_THREAD_STACK_SIZE, max_idle_time,
                    min_idle_count, (bool *)&SF_G_CONTINUE_FLAG)) != 0)
    {
        return result;
    }

    return fc_create_thread(&recovery_thread_ctx.tid,
            recovery_thread_entrance, NULL, SF_G_THREAD_STACK_SIZE);
}

void recovery_thread_destroy()
{
    common_blocked_queue_destroy(&recovery_thread_ctx.queue);
    fc_thread_pool_destroy(&recovery_thread_ctx.tpool);
}

int recovery_thread_push_to_queue(FSClusterDataServerInfo *ds)
{
    return common_blocked_queue_push(&recovery_thread_ctx.queue, ds);
}
