#include <limits.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fastcommon/fast_mblock.h"
#include "fastcommon/sched_thread.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "channel_info.h"

typedef struct {
    FSChannelInfo **buckets;
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
    ChannelHashtable htable; //for delay free

    struct {
        uint32_t reserve_interval;  //channel reserve interval in seconds
        time_t last_check_time;
        FastTimer timer;
    } timeout_ctx;
} ChannelInfoContext;

static ChannelInfoContext channel_context;

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
    bytes = sizeof(FSChannelInfo *) * htable->capacity;
    htable->buckets = fc_malloc(bytes);
    if (htable->buckets == NULL) {
        return ENOMEM;
    }
    memset(htable->buckets, 0, bytes);
    htable->count = 0;

    return 0;
}

static int channel_info_alloc_init(void *element, void *args)
{
    ((FSChannelInfo *)element)->id = ++channel_context.channel_ids.current;
    return 0;
}

int channel_info_init(const uint32_t max_channel_id,
        const uint32_t reserve_interval)
{
    int result;

    if ((result=fast_mblock_init_ex1(&channel_context.channel_allocator,
                    "channel_info", sizeof(FSChannelInfo), 1024,
                    max_channel_id, channel_info_alloc_init,
                    NULL, true)) != 0)
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
    if ((result=init_htable(&channel_context.htable,
                    max_channel_id / 100)) != 0)
    {
        return result;
    }
    return 0;
}

static void htable_add(FSChannelInfo *channel)
{
    FSChannelInfo **bucket;

    bucket = channel_context.htable.buckets + channel->id %
        channel_context.htable.capacity;
    PTHREAD_MUTEX_LOCK(&channel_context.htable.lock);
    channel->next = *bucket;
    *bucket = channel;
    channel_context.htable.count++;

    channel->timer.expires = g_current_time +
        channel_context.timeout_ctx.reserve_interval;
    fast_timer_add(&channel_context.timeout_ctx.timer, &channel->timer);
    PTHREAD_MUTEX_UNLOCK(&channel_context.htable.lock);
}

static FSChannelInfo *htable_remove(const uint32_t channel_id,
        const bool need_lock, const bool remove_timer)
{
    FSChannelInfo **bucket;
    FSChannelInfo *previous;
    FSChannelInfo *channel;

    bucket = channel_context.htable.buckets + channel_id %
        channel_context.htable.capacity;
    previous = NULL;
    if (need_lock) {
        PTHREAD_MUTEX_LOCK(&channel_context.htable.lock);
    }
    channel = *bucket;
    while (channel != NULL) {
        if (channel->id == channel_id) {
            if (previous == NULL) {
                *bucket = channel->next;
            } else {
                previous->next = channel->next;
            }
            channel_context.htable.count--;
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
        PTHREAD_MUTEX_UNLOCK(&channel_context.htable.lock);
    }
    return channel;
}

static void recycle_timeout_entries()
{
    uint32_t channel_id;
    FastTimerEntry head;
    FastTimerEntry *entry;
    FSChannelInfo *channel;

    PTHREAD_MUTEX_LOCK(&channel_context.htable.lock);
    if (g_current_time - channel_context.timeout_ctx.last_check_time <= 10) {
        PTHREAD_MUTEX_UNLOCK(&channel_context.htable.lock);
        return;
    }

    channel_context.timeout_ctx.last_check_time = g_current_time;
    fast_timer_timeouts_get(&channel_context.timeout_ctx.timer,
            g_current_time, &head);
    entry = head.next;
    while (entry != NULL) {
        channel_id = ((FSChannelInfo *)entry)->id;
        entry = entry->next;

        if ((channel=htable_remove(channel_id, false, false)) != NULL) {
            fast_mblock_free_object(&channel_context.
                    channel_allocator, channel);
        }
    }

    PTHREAD_MUTEX_UNLOCK(&channel_context.htable.lock);
}

FSChannelInfo *channel_info_alloc(const uint32_t channel_id)
{
    FSChannelInfo *channel;

    if (channel_id != 0) {
        if ((channel=htable_remove(channel_id, true, true)) != NULL) {
            return channel;
        }
    }

    if (channel_context.htable.count > 0 && (g_current_time -
            channel_context.timeout_ctx.last_check_time) > 10)
    {
        recycle_timeout_entries();
    }

    return (FSChannelInfo *)fast_mblock_alloc_object(
            &channel_context.channel_allocator);
}

void channel_info_release(FSChannelInfo *channel)
{
    htable_add(channel);
}

void channel_info_free(FSChannelInfo *channel)
{
    fast_mblock_free_object(&channel_context.channel_allocator, channel);
}
