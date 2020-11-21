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
#include "time_handler.h"
#include "otid_htable.h"

volatile int thread_count = 0;
void *thread_run(void *arg)
{
    FSAPITwoIdsHashKey key;
    int64_t offset;
    int length;
    int successive_count;
    int i;
    int result;

    __sync_add_and_fetch(&thread_count, 1);
    offset = 0;
    length = 8192;

    key.oid = 123456;
    key.tid = getpid();
    for (i=0; i<50 * 1000 * 1000; i++) {
        if (i % 1000 == 0) {
            key.oid++;
            offset += length / 2;
        } else {
            offset += length;
        }
        result = otid_htable_insert(&key, offset, length, &successive_count);
        /*
        printf("g_current_time_ms: %"PRId64", result: %d, "
                "successive_count: %d\n", g_current_time_ms,
                result, successive_count);
        */

        if (i % 10000 == 0) {
            fc_sleep_ms(10);
        }
    }

    __sync_sub_and_fetch(&thread_count, 1);
    return NULL;
}

int main(int argc, char *argv[])
{
#define THREAD_COUNT 4
    int result;
    int i;
    pthread_t tids[THREAD_COUNT];
    const int precision_ms = 10;
    const int slot_count = 10240;
    const int allocator_count = 16;
    int64_t element_limit = 0;
    const int sharding_count = 163;
    const int64_t htable_capacity = 1403641;
    const int64_t min_ttl_ms = 100;
    const int64_t max_ttl_ms = 86400 * 1000;

    log_init();
    g_current_time_ms = get_current_time_ms();
    if ((result=time_handler_init(precision_ms, slot_count)) != 0) {
        return result;
    }

    if ((result=otid_htable_init(sharding_count, htable_capacity,
                    allocator_count, element_limit,
                    min_ttl_ms, max_ttl_ms)) != 0)
    {
        return result;
    }

    for (i=0; i<THREAD_COUNT; i++) {
        fc_create_thread(tids + i, thread_run, NULL,
                SF_G_THREAD_STACK_SIZE);
    }

    do {
        fc_sleep_ms(20);
    } while (__sync_add_and_fetch(&thread_count, 0) > 0);
    return 0;
}
