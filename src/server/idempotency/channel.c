#include <limits.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fast_mblock.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "channel_htable.h"
#include "channel.h"

typedef struct {
    IdempotencyChannel **buckets;
    uint32_t capacity;
    uint32_t count;
    pthread_mutex_t lock;
} ChannelHashtable;

typedef struct {
    struct {
        uint32_t max;
        uint32_t current;
    } channel_ids;
    struct fast_mblock_man channel_allocator;
    ChannelHashtable delay_free_htable; //for delay free
    ChannelHTableContext htable_ctx;

    struct {
        uint32_t reserve_interval;  //channel reserve interval in seconds
        time_t last_check_time;
        FastTimer timer;
    } timeout_ctx;

} ChannelContext;

static ChannelContext channel_context;

static int init_htable(ChannelHashtable *htable, const int hint_capacity)
{
    int result;
    int bytes;

    if ((result=init_pthread_lock(&htable->lock)) != 0) {
        return result;
    }

    if (hint_capacity < 128) {
        htable->capacity = 163;
    } else {
        htable->capacity = fc_ceil_prime(hint_capacity);
    }
    bytes = sizeof(IdempotencyChannel *) * htable->capacity;
    htable->buckets = (IdempotencyChannel **)fc_malloc(bytes);
    if (htable->buckets == NULL) {
        return ENOMEM;
    }
    memset(htable->buckets, 0, bytes);
    htable->count = 0;

    return 0;
}

static int idempotency_channel_alloc_init(void *element, void *args)
{
    IdempotencyChannel *channel;

    channel = (IdempotencyChannel *)element;
    channel->id = ++channel_context.channel_ids.current;
    channel->request_htable.buckets = (IdempotencyRequest **)(channel + 1);
    return init_pthread_lock(&channel->request_htable.lock);
}

int idempotency_channel_init(const uint32_t max_channel_id,
        const int request_hint_capacity,
        const uint32_t reserve_interval,
        const uint32_t shared_lock_count)
{
    int result;
    int request_htable_capacity;
    int element_size;

    request_htable_capacity = fc_ceil_prime(request_hint_capacity);
    idempotency_request_init(request_htable_capacity);

    element_size = sizeof(IdempotencyChannel) + sizeof(IdempotencyRequest *) *
        request_htable_capacity;
    if ((result=fast_mblock_init_ex1(&channel_context.channel_allocator,
                    "channel_info", element_size, 1024, max_channel_id,
                    idempotency_channel_alloc_init, NULL, true)) != 0)
    {
        return result;
    }

    if ((result=fast_timer_init(&channel_context.timeout_ctx.timer,
                    2 * reserve_interval + 1,
                    get_current_time())) != 0)
    {
        return result;
    }

    channel_context.channel_ids.max = max_channel_id;
    channel_context.channel_ids.current = 0;
    channel_context.timeout_ctx.last_check_time = get_current_time();
    channel_context.timeout_ctx.reserve_interval = reserve_interval;
    if ((result=init_htable(&channel_context.delay_free_htable,
                    max_channel_id / 100)) != 0)
    {
        return result;
    }

    return idempotency_channel_htable_init(&channel_context.htable_ctx,
            shared_lock_count, max_channel_id / 10);
}

static void add_to_delay_free_htable(IdempotencyChannel *channel)
{
    IdempotencyChannel **bucket;

    bucket = channel_context.delay_free_htable.buckets + channel->id %
        channel_context.delay_free_htable.capacity;
    PTHREAD_MUTEX_LOCK(&channel_context.delay_free_htable.lock);
    channel->next = *bucket;
    *bucket = channel;
    channel_context.delay_free_htable.count++;

    fast_timer_add(&channel_context.timeout_ctx.timer, &channel->timer);
    PTHREAD_MUTEX_UNLOCK(&channel_context.delay_free_htable.lock);
}

IdempotencyChannel *idempotency_channel_find_and_hold(
        const uint32_t channel_id)
{
    IdempotencyChannel *channel;
    if ((channel=idempotency_channel_htable_find(&channel_context.
                    htable_ctx, channel_id)) == NULL)
    {
        return NULL;
    }

    __sync_add_and_fetch(&channel->ref_count, 1);
    return channel;
}

static IdempotencyChannel *htable_remove(const uint32_t channel_id,
        const bool need_lock, const bool remove_timer)
{
    IdempotencyChannel **bucket;
    IdempotencyChannel *previous;
    IdempotencyChannel *channel;

    bucket = channel_context.delay_free_htable.buckets + channel_id %
        channel_context.delay_free_htable.capacity;
    previous = NULL;
    if (need_lock) {
        PTHREAD_MUTEX_LOCK(&channel_context.delay_free_htable.lock);
    }
    channel = *bucket;
    while (channel != NULL) {
        if (channel->id == channel_id) {
            if (previous == NULL) {
                *bucket = channel->next;
            } else {
                previous->next = channel->next;
            }
            channel_context.delay_free_htable.count--;
            if (remove_timer) {
                fast_timer_remove(&channel_context.timeout_ctx.timer,
                        &channel->timer);
            }
            break;
        }

        previous = channel;
        channel = channel->next;
    }

    if (need_lock) {
        PTHREAD_MUTEX_UNLOCK(&channel_context.delay_free_htable.lock);
    }
    return channel;
}

static void do_free_channel(IdempotencyChannel *channel)
{
    IdempotencyRequest *head;
    IdempotencyRequest *deleted;

    head = idempotency_request_htable_clear(&channel->request_htable);
    while (head != NULL) {
        deleted = head;
        head = head->next;

        fast_mblock_free_object(deleted->allocator, deleted);
    }
    fast_mblock_free_object(&channel_context.channel_allocator, channel);
}

static void recycle_timeout_entries()
{
    uint32_t channel_id;
    FastTimerEntry head;
    FastTimerEntry *entry;
    IdempotencyChannel *channel;

    PTHREAD_MUTEX_LOCK(&channel_context.delay_free_htable.lock);
    if (g_current_time - channel_context.timeout_ctx.last_check_time <= 10) {
        PTHREAD_MUTEX_UNLOCK(&channel_context.delay_free_htable.lock);
        return;
    }

    channel_context.timeout_ctx.last_check_time = g_current_time;
    fast_timer_timeouts_get(&channel_context.timeout_ctx.timer,
            g_current_time, &head);
    entry = head.next;
    while (entry != NULL) {
        channel_id = ((IdempotencyChannel *)entry)->id;
        entry = entry->next;

        if ((channel=htable_remove(channel_id, false, false)) != NULL) {
            do_free_channel(channel);
        }
    }

    PTHREAD_MUTEX_UNLOCK(&channel_context.delay_free_htable.lock);
}

IdempotencyChannel *idempotency_channel_alloc(const uint32_t channel_id)
{
    IdempotencyChannel *channel;

    do {
        if (channel_id != 0) {
            if ((channel=htable_remove(channel_id, true, true)) != NULL) {
                break;
            }
        }

        if (channel_context.delay_free_htable.count > 0 && (g_current_time -
                    channel_context.timeout_ctx.last_check_time) > 10)
        {
            recycle_timeout_entries();
        }

        if ((channel=(IdempotencyChannel *)fast_mblock_alloc_object(
                        &channel_context.channel_allocator)) == NULL)
        {
            return NULL;
        }
    } while (0);

    __sync_add_and_fetch(&channel->ref_count, 1);
    return channel;
}

void idempotency_channel_release(IdempotencyChannel *channel,
        const bool is_holder)
{
    if (is_holder) {
        channel->timer.expires = g_current_time +
            channel_context.timeout_ctx.reserve_interval;
    }

    if (__sync_sub_and_fetch(&channel->ref_count, 1) == 0) {
        if (channel->timer.expires <= g_current_time) {  //expired
            do_free_channel(channel);
        } else {
            add_to_delay_free_htable(channel);
        }
    }
}

void idempotency_channel_free(IdempotencyChannel *channel)
{
    if (__sync_sub_and_fetch(&channel->ref_count, 1) == 0) {
        do_free_channel(channel);
    } else {
        channel->timer.expires = g_current_time +
            channel_context.timeout_ctx.reserve_interval;
    }
}
