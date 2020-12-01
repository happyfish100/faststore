#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <inttypes.h>
#include <sys/time.h>
#include "fastcommon/logger.h"
#include "fastcommon/shared_func.h"
#include "sf/sf_global.h"
#include "fs_api_allocator.h"
#include "timeout_handler.h"
#include "combine_handler.h"
#include "otid_htable.h"
#include "obid_htable.h"
#include "fs_api.h"

volatile int thread_count = 0;
void *thread_run(void *arg)
{
    long thread_index;
    FSAPIOperationContext op_ctx;
    bool combined;
    int64_t tid;
    int64_t offset;
    char *buff;
    int length;
    int i;
    int result;
    int conflict_count;
    int total_conflict_count;

    thread_index = (long)arg;

    __sync_add_and_fetch(&thread_count, 1);
    offset = 0;
    length = 8 * 1024;
    buff = (char *)malloc(length);
    memset(buff, 0, length);

    total_conflict_count = 0;
    offset = 0;
    op_ctx.bs_key.block.oid = 10000000 * thread_index;
    op_ctx.bs_key.block.oid = 123456;
    //op_ctx.tid = (long)pthread_self();
    //tid = getpid() + thread_index;
    tid = getpid();
    FS_API_SET_CTX_AND_TID(op_ctx, tid);
    op_ctx.allocator_ctx = fs_api_allocator_get(op_ctx.tid);

    printf("tid: %"PRId64", thread_index: %ld\n", op_ctx.tid, thread_index);
    for (i=0; i< 1000 * 1000; i++) {
        /*
        if (i % 10000 == 0) {
            op_ctx.bs_key.block.oid++;
            offset += length / 2;
        } else {
            offset += length;
        }
        */

        offset += length;

        op_ctx.bid = offset / FS_FILE_BLOCK_SIZE;
        op_ctx.bs_key.block.offset = FS_FILE_BLOCK_ALIGN(offset);
        op_ctx.bs_key.slice.offset = offset - op_ctx.bs_key.block.offset;
        if (op_ctx.bs_key.slice.offset + length <= FS_FILE_BLOCK_SIZE) {
            op_ctx.bs_key.slice.length = length;
        } else {
            op_ctx.bs_key.slice.length = FS_FILE_BLOCK_SIZE -
                op_ctx.bs_key.slice.offset;
        }

        if (obid_htable_check_conflict_and_wait(&op_ctx, &conflict_count) == 0) {
            if (conflict_count > 0) {
                total_conflict_count += conflict_count;
            }
        }

        if ((result=otid_htable_insert(&op_ctx, buff, &combined)) != 0) {
            break;
        }

        /*
        printf("g_timer_ms_ctx.current_time_ms: %"PRId64", slice offset: %d, "
                "length: %d, result: %d, combined: %d\n",
                g_timer_ms_ctx.current_time_ms, op_ctx.bs_key.slice.offset,
                op_ctx.bs_key.slice.length, result, combined);
                */

        if (i % 10000 == 0) {
            fc_sleep_ms(10);
        }
    }

    printf("thread: %ld, total_conflict_count: %d\n",
            thread_index, total_conflict_count);

    __sync_sub_and_fetch(&thread_count, 1);
    return NULL;
}

int main(int argc, char *argv[])
{
#define THREAD_COUNT 4
    int result;
    int i;
    pthread_t tids[THREAD_COUNT];
    IniFullContext ini_ctx;

    log_init();
    g_timer_ms_ctx.current_time_ms = get_current_time_ms();

    if ((result=fs_api_init(&ini_ctx)) != 0) {
        return result;
    }
    if ((result=fs_api_combine_thread_start()) != 0) {
        return result;
    }
    for (i=0; i<THREAD_COUNT; i++) {
        fc_create_thread(tids + i, thread_run, (void *)((long)i),
                SF_G_THREAD_STACK_SIZE);
    }

    do {
        fc_sleep_ms(100);
    } while (__sync_add_and_fetch(&thread_count, 0) > 0);
    fs_api_terminate();
    return 0;
}
